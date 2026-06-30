# Databricks notebook source
# EDH Incremental Cost Estimator
# Triggered by the Ingestio app — reads inputs via widgets,
# runs full cost calculation, and writes results to Delta table.

# COMMAND ----------

# Metadata widgets
dbutils.widgets.text(    "request_id",             "")
dbutils.widgets.text(    "business_unit",           "")
dbutils.widgets.text(    "request_date",            "")
dbutils.widgets.text(    "requestor",               "")
dbutils.widgets.text(    "business_justification",  "")
dbutils.widgets.dropdown("primary_key_available",   "Yes",             ["Yes", "No"])
dbutils.widgets.dropdown("delete_handling",         "Soft",            ["Hard", "Soft", "Ignore"])
dbutils.widgets.dropdown("schema_stability",        "Stable",          ["Stable", "Occasionally Changes", "Highly Dynamic"])
dbutils.widgets.dropdown("cdc_method",              "Not Applicable",  ["Timestamp", "Log Based", "Not Applicable"])
dbutils.widgets.dropdown("contains_phi",            "No",              ["Yes", "No"])

# Calculation widgets
dbutils.widgets.dropdown("source_type",         "SQL Server",    ["SQL Server", "Postgres", "Sybase", "S3", "SFTP"])
dbutils.widgets.dropdown("data_format",         "JDBC Tabular",  ["JDBC Tabular", "CSV", "XLS", "XLSB", "Parquet"])
dbutils.widgets.text(    "additional_gb",       "10")
dbutils.widgets.dropdown("load_type",           "Bulk",          ["Bulk", "CDC"])
dbutils.widgets.dropdown("ingestion_frequency", "Daily",         ["Daily", "Weekly", "Monthly"])
dbutils.widgets.dropdown("save_results",        "true",          ["true", "false"])

# COMMAND ----------

# ============================================================
# SECTION 1: LOOKUP TABLES
# ============================================================

# ----------------------------
# Observed cluster-level throughput (GB/hr)
# Source: EDH-PIPELINE-DAILY actuals, 2026-06-26
# estimated_uncompressed_gb_per_hour from calibration query
# These are the TOTAL cluster rates (not per-worker) observed
# during each task's window. runtime = additional_gb / throughput.
#
# Postgres not directly measured — set equal to SQL Server Bulk.
# SFTP is the avg of two concurrent tasks (Integration Template
# 1.743 GB/hr and Workday 0.626 GB/hr) that shared the cluster.
# ----------------------------
THROUGHPUT_GB_HR = {
    "SQL Server": 10.79,
    "Postgres":   10.79,
    "S3":         27.84,
    "SFTP":        1.19,
    "Sybase":      3.09,
}

# ----------------------------
# Observed typical worker count during each source's task window
# Source: system.compute.node_timeline, 2026-06-26
# Used for DBU rate and VM node-count calculation ONLY — not for
# runtime (runtime comes from THROUGHPUT_GB_HR directly).
# Sybase is single-node fixed — no workers, driver only.
# ----------------------------
TYPICAL_WORKERS = {
    "SQL Server": 1.8,
    "Postgres":   1.8,
    "S3":         2.9,
    "SFTP":       1.5,
}
VM_RATE_PER_NODE = 1.808   # Standard_D32ds_v5 on-demand $/hr
                            # Applies to both L-1-5 and L-SYBASE clusters
                            # Source: Vantage tracker — confirm against your actual Azure bill

CLUSTER_CONFIG = {
    "SQL Server": {"type": "multi",  "dbu_driver": 16, "dbu_per_worker": 6.4},
    "Postgres":   {"type": "multi",  "dbu_driver": 16, "dbu_per_worker": 6.4},
    "S3":         {"type": "multi",  "dbu_driver": 16, "dbu_per_worker": 6.4},
    "SFTP":       {"type": "multi",  "dbu_driver": 16, "dbu_per_worker": 6.4},
    "Sybase":     {"type": "single", "dbu_fixed":   8},
}

LAYER_CONFIG = {
    "SQL Server": {"layers": ["Bronze", "Silver", "Gold"], "layer_multiplier": 2.4},
    "Postgres":   {"layers": ["Bronze", "Silver", "Gold"], "layer_multiplier": 2.4},
    "Sybase":     {"layers": ["Bronze", "Silver", "Gold"], "layer_multiplier": 2.4},
    "S3":         {"layers": ["Bronze", "Silver", "Gold"], "layer_multiplier": 2.4},
    "SFTP":       {"layers": ["Bronze"],                   "layer_multiplier": 1.0}
}

COMPRESSION_RATIO_BY_FORMAT = {
    "CSV":          0.30,
    "XLS":          0.40,
    "XLSB":         0.30,
    "Parquet":      1.00,
    "JDBC Tabular": 0.30
}

# Note: Any data format can be paired with any source type.
# Compression ratio is driven purely by DATA_FORMAT, not source.

TRANSFORMATION_PROPORTIONS = {
    "Silver":  40 / 238,
    "Gold":    34 / 238,
    "MDM":     40 / 238,
    "RT_Mart": 16 / 238
}

CDC_RUNTIME_FACTOR = 0.10   # CDC processes ~10% of data vs Bulk per run

FREQUENCY_MAP = {
    "Daily":   30,
    "Weekly":  4,
    "Monthly": 1
}

COST_PER_DBU         = 0.30
STORAGE_PRICE_PER_GB = 0.023
ENDPOINT_COST_PER_GB = 0.01
VARIANCE_FACTOR      = 0.20

USE_SPOT             = False
SPOT_DISCOUNT_FACTOR = 0.22

TRANSACTION_MULTIPLIER = {
    1: 0.000064,
    2: 0.0001382,
    3: 0.0001980
}

NETWORK_MULTIPLIER = {
    1: 1.0,
    2: 3.0,
    3: 4.8
}

# COMMAND ----------

# ============================================================
# SECTION 2: COMPUTE COST CALCULATION
# (Worker partition-formula machinery fully retired —
#  MB_PER_PARTITION, WAVES_PER_CORE, CORES_PER_NODE,
#  get_worker_count(), get_effective_dbu_hr(),
#  get_transformation_dbu_hr() all removed.
#  TYPICAL_WORKERS and THROUGHPUT_GB_HR from real actuals
#  replace everything that was estimated.)
# ============================================================

import math

def calculate_compute_cost(source_type, additional_gb, load_type, runs_per_month):
    """
    Compute cost uses two observed lookup tables (both from Jun-26 actuals):

      THROUGHPUT_GB_HR  — drives runtime only
      TYPICAL_WORKERS   — drives DBU rate and VM node count only

    Formula:
      runtime_hrs       = additional_gb / THROUGHPUT_GB_HR[source]
      if CDC: runtime  *= 0.10

      For multi-node:
        ingestion_dbu_hr   = 16 + (typical_workers × 6.4)
        ingestion_nodes    = 1 + typical_workers
      For Sybase (single-node):
        ingestion_dbu_hr   = 8 (fixed)
        ingestion_nodes    = 1

      Transformation (Silver+Gold, always on L-1-5, same workers):
        same dbu_hr and nodes as multi-node above
        scaled by stage proportion of total runtime

      ingestion_dbu_cost   = runtime × dbu_hr × $0.30 × runs
      ingestion_vm_cost    = runtime × nodes  × $1.808 × runs
      transformation costs = same formula applied to each stage
    """
    layers = LAYER_CONFIG[source_type]["layers"]
    cfg    = CLUSTER_CONFIG[source_type]
    vm_rate = VM_RATE_PER_NODE * (SPOT_DISCOUNT_FACTOR if USE_SPOT else 1.0)

    # ---- Step 1: Runtime from observed cluster throughput ----
    ingestion_runtime_hrs = additional_gb / THROUGHPUT_GB_HR[source_type]
    if load_type == "CDC":
        ingestion_runtime_hrs *= CDC_RUNTIME_FACTOR

    # ---- Step 2: DBU rate and node count from observed typical workers ----
    if cfg["type"] == "single":
        ingestion_dbu_hr = cfg["dbu_fixed"]       # Sybase: 8 DBU/hr fixed
        ingestion_nodes  = 1
    else:
        workers          = TYPICAL_WORKERS[source_type]
        ingestion_dbu_hr = cfg["dbu_driver"] + (workers * cfg["dbu_per_worker"])
        ingestion_nodes  = 1 + workers             # 1 driver + typical workers

    # Transformation always on L-1-5 — use SQL Server's typical workers
    # (same cluster, same observed worker behavior)
    l1_5_workers        = TYPICAL_WORKERS["SQL Server"]
    transformation_dbu_hr = 16 + (l1_5_workers * 6.4)
    transformation_nodes  = 1 + l1_5_workers

    # ---- Step 3: Ingestion cost (DBU + VM) ----
    ingestion_dbu_cost = ingestion_runtime_hrs * ingestion_dbu_hr * COST_PER_DBU * runs_per_month
    ingestion_vm_cost  = ingestion_runtime_hrs * ingestion_nodes  * vm_rate       * runs_per_month
    ingestion_cost     = ingestion_dbu_cost + ingestion_vm_cost

    # ---- Step 4: Transformation cost (DBU + VM, Silver+Gold only) ----
    transformation_dbu_cost = 0
    transformation_vm_cost  = 0
    if "Gold" in layers:
        for stage, proportion in TRANSFORMATION_PROPORTIONS.items():
            if stage in ["Silver", "Gold"]:
                stage_runtime         = ingestion_runtime_hrs * proportion
                transformation_dbu_cost += stage_runtime * transformation_dbu_hr * COST_PER_DBU * runs_per_month
                transformation_vm_cost  += stage_runtime * transformation_nodes  * vm_rate       * runs_per_month
    transformation_cost = transformation_dbu_cost + transformation_vm_cost

    total_dbu_cost = ingestion_dbu_cost + transformation_dbu_cost
    total_vm_cost  = ingestion_vm_cost  + transformation_vm_cost
    total_compute  = ingestion_cost     + transformation_cost

    return {
        "ingestion_cost":        round(ingestion_cost, 4),
        "transformation_cost":   round(transformation_cost, 4),
        "total_compute":         round(total_compute, 4),
        "total_dbu_cost":        round(total_dbu_cost, 4),
        "total_vm_cost":         round(total_vm_cost, 4),
        "runtime_hrs":           round(ingestion_runtime_hrs, 4),
        "throughput_gb_hr":      THROUGHPUT_GB_HR[source_type],
        "ingestion_dbu_hr":      round(ingestion_dbu_hr, 2),
        "ingestion_nodes":       ingestion_nodes,
        "typical_workers":       TYPICAL_WORKERS.get(source_type, "N/A (Single Node)"),
        "transformation_dbu_hr": round(transformation_dbu_hr, 2) if "Gold" in layers else 0.0,
        "vm_rate_per_node_hr":   round(vm_rate, 4),
    }

# COMMAND ----------

# ============================================================
# SECTION 4: STORAGE COST CALCULATION
# ============================================================

def calculate_storage_cost(source_type, data_format, additional_gb, runs_per_month):
    compression      = COMPRESSION_RATIO_BY_FORMAT[data_format]
    layer_multiplier = LAYER_CONFIG[source_type]["layer_multiplier"]
    num_layers       = len(LAYER_CONFIG[source_type]["layers"])
    transaction_mult = TRANSACTION_MULTIPLIER[num_layers]
    compressed_gb    = additional_gb * compression

    data_storage_cost = compressed_gb * layer_multiplier * STORAGE_PRICE_PER_GB
    transaction_cost  = compressed_gb * transaction_mult * runs_per_month
    total_storage     = data_storage_cost + transaction_cost

    return {
        "compression_ratio": compression,
        "compressed_gb":     round(compressed_gb, 4),
        "data_storage_cost": round(data_storage_cost, 4),
        "transaction_cost":  round(transaction_cost, 6),
        "total_storage":     round(total_storage, 4)
    }

# COMMAND ----------

# ============================================================
# SECTION 5: NETWORKING COST CALCULATION
# ============================================================

def calculate_networking_cost(source_type, data_format, additional_gb, runs_per_month):
    compression   = COMPRESSION_RATIO_BY_FORMAT[data_format]
    num_layers    = len(LAYER_CONFIG[source_type]["layers"])
    network_mult  = NETWORK_MULTIPLIER[num_layers]
    compressed_gb = additional_gb * compression

    networking_cost = compressed_gb * network_mult * ENDPOINT_COST_PER_GB * runs_per_month

    return {
        "networking_cost":    round(networking_cost, 4),
        "network_multiplier": network_mult
    }

# COMMAND ----------

# ============================================================
# SECTION 6: EFFORT ESTIMATION
# ============================================================
# Lighter version of the new-source effort engine
# (EDH_New_Source_Estimator_Job.py), adapted for "additional data to an
# existing source" rather than onboarding from scratch. Same overall
# methodology (weighted complexity score -> Simple/Medium/Complex bucket
# -> PHASE_EFFORT day ranges -> testing as % of build), but:
#
#   - NO NEW INPUT PARAMETERS. Reuses 5 fields already on this form
#     (cdc_method, schema_stability, data_format, primary_key_available,
#     delete_handling) instead of asking new questions, since this
#     onboarding flow assumes the source connection/cluster already
#     exists - only the work specific to the NEW DATA being added is
#     being estimated, not a whole new pipeline build.
#   - LIGHTER PHASE LIST: Design and Build-Orchestration are dropped
#     entirely (architecture and orchestration already exist for an
#     established source), unlike the new-source engine's 7 phases.
#   - NO num_resources / calendar_duration, for consistency with the
#     new-source notebook (removed there per explicit request).
#
# Same honest caveat as every other constant in this project: these
# weights, scores, and day-ranges are invented, not calibrated against
# real historical effort logs. They are internally consistent with each
# other, not independently verified.

EXISTING_COMPLEXITY_WEIGHTS = {
    'cdc_method':             0.30,   # biggest driver: CDC/log-based build+validation is the main effort source here
    'schema_stability':       0.25,   # reused field, previously metadata-only - now actually drives a number
    'data_format':            0.15,
    'primary_key_available':  0.15,
    'delete_handling':        0.15,
}
# Weights sum to 1.00

EXISTING_SCORING_RUBRICS = {
    'cdc_method': {
        'Not Applicable': 15,   # Bulk - no CDC logic to build/validate
        'Timestamp':       45,
        'Log Based':       75,
    },
    'schema_stability': {
        'Stable':                15,
        'Occasionally Changes':  45,
        'Highly Dynamic':        80,
    },
    'data_format': {
        'JDBC Tabular': 15,
        'Parquet':      15,   # already structured/columnar, minimal parsing effort
        'CSV':          35,
        'XLS':          50,
        'XLSB':         50,
    },
    'primary_key_available': {
        'Yes': 15,   # straightforward merge/incremental logic
        'No':  60,   # harder merge/incremental/de-dup logic without a PK
    },
    'delete_handling': {
        'Ignore': 15,
        'Soft':   40,
        'Hard':   65,   # physical delete logic + testing is the most effort-intensive option
    },
}

def calculate_existing_complexity_score(
    cdc_method: str, schema_stability: str, data_format: str,
    primary_key_available: str, delete_handling: str,
) -> dict:
    scores = {
        'cdc_method':             EXISTING_SCORING_RUBRICS['cdc_method'].get(cdc_method, 50),
        'schema_stability':       EXISTING_SCORING_RUBRICS['schema_stability'].get(schema_stability, 50),
        'data_format':            EXISTING_SCORING_RUBRICS['data_format'].get(data_format, 50),
        'primary_key_available':  EXISTING_SCORING_RUBRICS['primary_key_available'].get(primary_key_available, 50),
        'delete_handling':        EXISTING_SCORING_RUBRICS['delete_handling'].get(delete_handling, 50),
    }
    weighted_scores = {k: v * EXISTING_COMPLEXITY_WEIGHTS[k] for k, v in scores.items()}
    total_score = sum(weighted_scores.values())
    if total_score <= 30:
        level = 'Simple'
    elif total_score <= 70:
        level = 'Medium'
    else:
        level = 'Complex'
    return {
        'raw_scores': scores,
        'weighted_scores': {k: round(v, 1) for k, v in weighted_scores.items()},
        'total_score': round(total_score, 1),
        'complexity_level': level,
    }


# Lighter phase list - no Design, no Build-Orchestration (infrastructure
# already exists for an established source). Day ranges deliberately
# smaller than the new-source PHASE_EFFORT table, since this is scoped
# to extending/adding to an existing pipeline, not building one.
EXISTING_PHASE_EFFORT = {
    'discovery':            {'Simple': {'min': 1, 'max': 2}, 'Medium': {'min': 2, 'max': 3}, 'Complex': {'min': 3, 'max': 5}},
    'build_ingestion':      {'Simple': {'min': 1, 'max': 2}, 'Medium': {'min': 2, 'max': 4}, 'Complex': {'min': 4, 'max': 6}},
    'build_transformation': {'Simple': {'min': 1, 'max': 3}, 'Medium': {'min': 3, 'max': 5}, 'Complex': {'min': 5, 'max': 8}},
    'deployment':           {'Simple': {'min': 1, 'max': 1}, 'Medium': {'min': 1, 'max': 2}, 'Complex': {'min': 2, 'max': 3}},
    'documentation':        {'Simple': {'min': 1, 'max': 1}, 'Medium': {'min': 1, 'max': 2}, 'Complex': {'min': 2, 'max': 2}},
}

EXISTING_TESTING_PERCENTAGE = 0.25   # same convention as new-source engine

def estimate_existing_effort(
    cdc_method: str, schema_stability: str, data_format: str,
    primary_key_available: str, delete_handling: str,
    complexity_override: str = None,
) -> dict:
    complexity = calculate_existing_complexity_score(
        cdc_method=cdc_method, schema_stability=schema_stability, data_format=data_format,
        primary_key_available=primary_key_available, delete_handling=delete_handling,
    )
    level = complexity_override if complexity_override in ['Simple', 'Medium', 'Complex'] else complexity['complexity_level']

    phases = {}
    for phase, ranges in EXISTING_PHASE_EFFORT.items():
        r = ranges[level]
        midpoint = (r['min'] + r['max']) / 2
        phases[phase] = {'min': r['min'], 'max': r['max'], 'estimate': midpoint}

    build_min = phases['build_ingestion']['min'] + phases['build_transformation']['min']
    build_max = phases['build_ingestion']['max'] + phases['build_transformation']['max']
    build_est = phases['build_ingestion']['estimate'] + phases['build_transformation']['estimate']

    testing_min = math.ceil(build_min * EXISTING_TESTING_PERCENTAGE)
    testing_max = math.ceil(build_max * EXISTING_TESTING_PERCENTAGE)
    testing_est = round(build_est * EXISTING_TESTING_PERCENTAGE, 1)
    phases['testing'] = {'min': testing_min, 'max': testing_max, 'estimate': testing_est}

    total_min = sum(p['min'] for p in phases.values())
    total_max = sum(p['max'] for p in phases.values())
    total_est = sum(p['estimate'] for p in phases.values())

    return {
        'complexity': complexity, 'complexity_level': level,
        'phases': phases,
        'build_subtotal': {'min': build_min, 'max': build_max, 'estimate': build_est},
        'total_effort_days': {'min': total_min, 'max': total_max, 'estimate': total_est},
    }


# ============================================================

def apply_variance(value, variance=VARIANCE_FACTOR):
    return round(value * (1 - variance), 2), round(value * (1 + variance), 2)

# COMMAND ----------

# ============================================================
# SECTION 8: INPUT VALIDATION
# ============================================================

def validate_inputs(source_type, data_format, additional_gb, load_type,
                    ingestion_frequency, primary_key_available,
                    delete_handling, schema_stability, cdc_method,
                    contains_phi):

    if source_type not in THROUGHPUT_GB_HR:
        raise ValueError(f"Invalid source type. Choose from: {list(THROUGHPUT_GB_HR.keys())}")
    if data_format not in COMPRESSION_RATIO_BY_FORMAT:
        raise ValueError(
            f"Invalid data format '{data_format}'. "
            f"Valid formats: {list(COMPRESSION_RATIO_BY_FORMAT.keys())}"
        )
    if additional_gb <= 0:
        raise ValueError("Additional GB must be greater than 0")
    if load_type not in ["Bulk", "CDC"]:
        raise ValueError("Invalid load type. Choose from: ['Bulk', 'CDC']")
    if ingestion_frequency not in FREQUENCY_MAP:
        raise ValueError(f"Invalid frequency. Choose from: {list(FREQUENCY_MAP.keys())}")
    if primary_key_available not in ["Yes", "No"]:
        raise ValueError("Primary key must be Yes or No")
    if delete_handling not in ["Hard", "Soft", "Ignore"]:
        raise ValueError("Invalid delete handling. Choose from: Hard / Soft / Ignore")
    if schema_stability not in ["Stable", "Occasionally Changes", "Highly Dynamic"]:
        raise ValueError("Invalid schema stability.")
    if cdc_method not in ["Timestamp", "Log Based", "Not Applicable"]:
        raise ValueError("Invalid CDC method.")
    if load_type == "CDC" and cdc_method == "Not Applicable":
        raise ValueError("CDC Method cannot be 'Not Applicable' when Load Type is CDC")
    if load_type == "Bulk" and cdc_method != "Not Applicable":
        raise ValueError("CDC Method should be 'Not Applicable' when Load Type is Bulk")
    if contains_phi not in ["Yes", "No"]:
        raise ValueError("Contains PHI must be Yes or No")

# COMMAND ----------

# ============================================================
# SECTION 9: MAIN ESTIMATOR FUNCTION
# ============================================================

def estimate_cost(
    business_unit, request_date, requestor, business_justification,
    primary_key_available, delete_handling, schema_stability, cdc_method,
    contains_phi,
    source_type, data_format, additional_gb, load_type, ingestion_frequency
):
    validate_inputs(source_type, data_format, additional_gb, load_type,
                    ingestion_frequency, primary_key_available,
                    delete_handling, schema_stability, cdc_method,
                    contains_phi)

    runs_per_month = FREQUENCY_MAP[ingestion_frequency]
    layers         = LAYER_CONFIG[source_type]["layers"]

    compute    = calculate_compute_cost(source_type, additional_gb, load_type, runs_per_month)
    storage    = calculate_storage_cost(source_type, data_format, additional_gb, runs_per_month)
    networking = calculate_networking_cost(source_type, data_format, additional_gb, runs_per_month)

    total_monthly_cost = round(
        compute["total_compute"] + storage["total_storage"] + networking["networking_cost"], 2
    )
    total_annual_cost = round(total_monthly_cost * 12, 2)

    # NOTE: +-20% variance removed from this notebook's result table per
    # explicit request. apply_variance() and VARIANCE_FACTOR are still
    # defined above (SECTION 7) and used by the new combined result
    # table (edh_combined_estimations) instead - see SECTION 13.

    return {
        "business_unit":          business_unit,
        "request_date":           request_date,
        "requestor":              requestor,
        "business_justification": business_justification,
        "primary_key_available":  primary_key_available,
        "delete_handling":        delete_handling,
        "schema_stability":       schema_stability,
        "cdc_method":             cdc_method,
        "contains_phi":           contains_phi,
        "source_type":            source_type,
        "data_format":            data_format,
        "additional_gb":          additional_gb,
        "load_type":              load_type,
        "ingestion_frequency":    ingestion_frequency,
        "runs_per_month":         runs_per_month,
        "layers":                 ", ".join(layers),
        "typical_workers":        compute["typical_workers"],
        "throughput_gb_hr":       compute["throughput_gb_hr"],
        "ingestion_dbu_hr":       compute["ingestion_dbu_hr"],
        "transformation_dbu_hr":  compute["transformation_dbu_hr"],
        "vm_rate_per_node_hr":    compute["vm_rate_per_node_hr"],
        "ingestion_nodes":        compute["ingestion_nodes"],
        "runtime_hrs":            compute["runtime_hrs"],
        "ingestion_cost":         compute["ingestion_cost"],
        "transformation_cost":    compute["transformation_cost"],
        "total_dbu_cost":         compute["total_dbu_cost"],
        "total_vm_cost":          compute["total_vm_cost"],
        "compute_cost":           compute["total_compute"],
        "compression_ratio":      storage["compression_ratio"],
        "compressed_gb":          storage["compressed_gb"],
        "data_storage_cost":      storage["data_storage_cost"],
        "transaction_cost":       storage["transaction_cost"],
        "storage_cost":           storage["total_storage"],
        "network_multiplier":     networking["network_multiplier"],
        "networking_cost":        networking["networking_cost"],
        "total_monthly_cost":     total_monthly_cost,
        "total_annual_cost":      total_annual_cost,
    }

# COMMAND ----------

# ============================================================
# SECTION 10: READ WIDGET INPUTS
# ============================================================

request_id             = dbutils.widgets.get("request_id")
business_unit          = dbutils.widgets.get("business_unit")
request_date           = dbutils.widgets.get("request_date")
requestor              = dbutils.widgets.get("requestor")
business_justification = dbutils.widgets.get("business_justification")
primary_key_available  = dbutils.widgets.get("primary_key_available")
delete_handling        = dbutils.widgets.get("delete_handling")
schema_stability       = dbutils.widgets.get("schema_stability")
cdc_method             = dbutils.widgets.get("cdc_method")
contains_phi           = dbutils.widgets.get("contains_phi")
source_type            = dbutils.widgets.get("source_type")
data_format            = dbutils.widgets.get("data_format")
additional_gb          = float(dbutils.widgets.get("additional_gb"))
load_type              = dbutils.widgets.get("load_type")
ingestion_frequency    = dbutils.widgets.get("ingestion_frequency")
save_results           = dbutils.widgets.get("save_results").lower() == "true"

# COMMAND ----------

# ============================================================
# SECTION 11: RUN ESTIMATOR
# ============================================================

result = estimate_cost(
    business_unit          = business_unit,
    request_date           = request_date,
    requestor              = requestor,
    business_justification = business_justification,
    primary_key_available  = primary_key_available,
    delete_handling        = delete_handling,
    schema_stability       = schema_stability,
    cdc_method             = cdc_method,
    contains_phi           = contains_phi,
    source_type            = source_type,
    data_format            = data_format,
    additional_gb          = additional_gb,
    load_type              = load_type,
    ingestion_frequency    = ingestion_frequency,
)

effort = estimate_existing_effort(
    cdc_method             = cdc_method,
    schema_stability        = schema_stability,
    data_format             = data_format,
    primary_key_available   = primary_key_available,
    delete_handling          = delete_handling,
)

# COMMAND ----------

# ============================================================
# SECTION 12: DISPLAY RESULTS
# ============================================================

print("=" * 65)
print("       EDH INCREMENTAL COST ESTIMATOR")
print("=" * 65)
print(f"\n  REQUEST DETAILS")
print(f"  Business Unit    : {result['business_unit']}")
print(f"  Request Date     : {result['request_date']}")
print(f"  Requestor        : {result['requestor']}")
print(f"  Justification    : {result['business_justification']}")
print(f"\n  TECHNICAL DETAILS")
print(f"  Primary Key      : {result['primary_key_available']}")
print(f"  Delete Handling  : {result['delete_handling']}")
print(f"  Schema Stability : {result['schema_stability']}")
print(f"  CDC Method       : {result['cdc_method']}")
print(f"  Contains PHI     : {result['contains_phi']}")
print(f"\n  CALCULATION INPUTS")
print(f"  Source Type      : {result['source_type']}")
print(f"  Data Format      : {result['data_format']}")
print(f"  Compression Ratio: {result['compression_ratio']}")
print(f"  Additional Volume: {result['additional_gb']} GB")
print(f"  Load Type        : {result['load_type']}")
print(f"  Frequency        : {result['ingestion_frequency']}")
print(f"  Runs Per Month   : {result['runs_per_month']}")
print(f"  Layers           : {result['layers']}")
print(f"\n{'-' * 65}")
print(f"  COMPUTE")
print(f"  Throughput       : {result['throughput_gb_hr']} GB/hr  (Jun-26 observed cluster rate)")
print(f"  Typical Workers  : {result['typical_workers']}  (Jun-26 avg, drives DBU/VM rate only)")
print(f"  Ingestion DBU/hr : {result['ingestion_dbu_hr']}")
print(f"  Transform DBU/hr : {result['transformation_dbu_hr'] if result['transformation_dbu_hr'] > 0 else 'N/A (Bronze only)'}")
print(f"  VM rate/node/hr  : ${result['vm_rate_per_node_hr']}  ({'Spot' if USE_SPOT else 'On-demand'})")
print(f"  Ingestion nodes  : {result['ingestion_nodes']}")
print(f"  DBU cost         : ${result['total_dbu_cost']}")
print(f"  VM cost          : ${result['total_vm_cost']}")
print(f"  Ingestion Runtime: {result['runtime_hrs']} hrs")
print(f"  Ingestion Cost   : ${result['ingestion_cost']}")
print(f"  Transform Cost   : ${result['transformation_cost']}")
print(f"  Total Compute    : ${result['compute_cost']}")
print(f"\n{'-' * 65}")
print(f"  STORAGE")
print(f"  Compressed GB    : {result['compressed_gb']} GB")
print(f"  Data Storage     : ${result['data_storage_cost']}")
print(f"  Transactions     : ${result['transaction_cost']}")
print(f"  Total Storage    : ${result['storage_cost']}")
print(f"\n{'-' * 65}")
print(f"  NETWORKING")
print(f"  Network Mult.    : {result['network_multiplier']}x")
print(f"  Total Networking : ${result['networking_cost']}")
print(f"\n{'-' * 65}")
print(f"  EFFORT ESTIMATE")
print(f"  Complexity       : {effort['complexity_level']} (score {effort['complexity']['total_score']}/100)")
print(f"  Total Effort     : {effort['total_effort_days']['estimate']} person-days "
      f"({effort['total_effort_days']['min']}-{effort['total_effort_days']['max']} range)")
print(f"\n{'=' * 65}")
print(f"  MONTHLY TOTAL    : ${result['total_monthly_cost']}")
print(f"  ANNUAL TOTAL     : ${result['total_annual_cost']}")
print(f"{'=' * 65}")

# COMMAND ----------

# ============================================================
# SECTION 13: SAVE RAW REQUEST TO DELTA
# ============================================================

existingsource_request_schema = StructType([
    StructField("request_id",              StringType(),    True),
    StructField("submission_timestamp",    TimestampType(), True),
    StructField("business_unit",           StringType(),    True),
    StructField("request_date",            StringType(),    True),
    StructField("requestor",               StringType(),    True),
    StructField("business_justification",  StringType(),    True),
    StructField("primary_key_available",   StringType(),    True),
    StructField("delete_handling",         StringType(),    True),
    StructField("schema_stability",        StringType(),    True),
    StructField("cdc_method",              StringType(),    True),
    StructField("contains_phi",            StringType(),    True),
    StructField("source_type",             StringType(),    True),
    StructField("data_format",             StringType(),    True),
    StructField("additional_gb",           DoubleType(),    True),
    StructField("load_type",               StringType(),    True),
    StructField("ingestion_frequency",     StringType(),    True),
])

existingsource_request_row = [(
    request_id, datetime.now(timezone.utc),
    business_unit, request_date, requestor, business_justification,
    primary_key_available, delete_handling, schema_stability, cdc_method, contains_phi,
    source_type, data_format, float(additional_gb), load_type, ingestion_frequency,
)]

if save_results:
    df_request = spark.createDataFrame(existingsource_request_row, existingsource_request_schema)
    df_request.write.format("delta").mode("append").option("mergeSchema", "true") \
        .saveAsTable("edh.ingestion.edh_existingsource_requests")
    print(f"Saved request to edh.ingestion.edh_existingsource_requests — request_id={request_id}")

# COMMAND ----------

# ============================================================
# SECTION 14: SAVE TO DELTA TABLE
# ============================================================

from pyspark.sql.types import (
    StructType, StructField,
    StringType, DoubleType, IntegerType, TimestampType
)
from datetime import datetime, timezone

schema = StructType([
    StructField("request_id",             StringType(),    True),
    StructField("estimation_timestamp",   TimestampType(), True),
    # Metadata
    StructField("business_unit",          StringType(),    True),
    StructField("request_date",           StringType(),    True),
    StructField("requestor",              StringType(),    True),
    StructField("business_justification", StringType(),    True),
    StructField("primary_key_available",  StringType(),    True),
    StructField("delete_handling",        StringType(),    True),
    StructField("schema_stability",       StringType(),    True),
    StructField("cdc_method",             StringType(),    True),
    StructField("contains_phi",           StringType(),    True),
    # Calculation inputs
    StructField("source_type",            StringType(),    True),
    StructField("data_format",            StringType(),    True),
    StructField("additional_gb",          DoubleType(),    True),
    StructField("load_type",              StringType(),    True),
    StructField("ingestion_frequency",    StringType(),    True),
    StructField("runs_per_month",         IntegerType(),   True),
    StructField("layers",                 StringType(),    True),
    # Compute
    StructField("typical_workers",        DoubleType(),    True),
    StructField("throughput_gb_hr",       DoubleType(),    True),
    StructField("ingestion_dbu_hr",       DoubleType(),    True),
    StructField("transformation_dbu_hr",  DoubleType(),    True),
    StructField("vm_rate_per_node_hr",    DoubleType(),    True),
    StructField("ingestion_nodes",        DoubleType(),    True),
    StructField("total_dbu_cost",         DoubleType(),    True),
    StructField("total_vm_cost",          DoubleType(),    True),
    StructField("runtime_hrs",            DoubleType(),    True),
    StructField("ingestion_cost",         DoubleType(),    True),
    StructField("transformation_cost",    DoubleType(),    True),
    StructField("compute_cost",           DoubleType(),    True),
    # Storage
    StructField("compression_ratio",      DoubleType(),    True),
    StructField("compressed_gb",          DoubleType(),    True),
    StructField("data_storage_cost",      DoubleType(),    True),
    StructField("transaction_cost",       DoubleType(),    True),
    StructField("storage_cost",           DoubleType(),    True),
    # Networking
    StructField("network_multiplier",     DoubleType(),    True),
    StructField("networking_cost",        DoubleType(),    True),
    # Totals
    StructField("total_monthly_cost",     DoubleType(),    True),
    StructField("total_annual_cost",      DoubleType(),    True),
    # Effort estimate
    StructField("effort_complexity_score",          DoubleType(), True),
    StructField("effort_complexity_level",          StringType(), True),
    StructField("effort_discovery_days",            DoubleType(), True),
    StructField("effort_build_ingestion_days",      DoubleType(), True),
    StructField("effort_build_transformation_days", DoubleType(), True),
    StructField("effort_testing_days",              DoubleType(), True),
    StructField("effort_deployment_days",           DoubleType(), True),
    StructField("effort_documentation_days",        DoubleType(), True),
    StructField("effort_total_days_min",            DoubleType(), True),
    StructField("effort_total_days_estimate",       DoubleType(), True),
    StructField("effort_total_days_max",            DoubleType(), True),
])

row = [(
    request_id,
    datetime.now(timezone.utc),
    result["business_unit"],
    result["request_date"],
    result["requestor"],
    result["business_justification"],
    result["primary_key_available"],
    result["delete_handling"],
    result["schema_stability"],
    result["cdc_method"],
    result["contains_phi"],
    result["source_type"],
    result["data_format"],
    float(result["additional_gb"]),
    result["load_type"],
    result["ingestion_frequency"],
    int(result["runs_per_month"]),
    result["layers"],
    float(result["typical_workers"]) if result["typical_workers"] != "N/A (Single Node)" else 0.0,
    float(result["throughput_gb_hr"]),
    float(result["ingestion_dbu_hr"]),
    float(result["transformation_dbu_hr"]),
    float(result["vm_rate_per_node_hr"]),
    float(result["ingestion_nodes"]),
    float(result["total_dbu_cost"]),
    float(result["total_vm_cost"]),
    float(result["runtime_hrs"]),
    float(result["ingestion_cost"]),
    float(result["transformation_cost"]),
    float(result["compute_cost"]),
    float(result["compression_ratio"]),
    float(result["compressed_gb"]),
    float(result["data_storage_cost"]),
    float(result["transaction_cost"]),
    float(result["storage_cost"]),
    float(result["network_multiplier"]),
    float(result["networking_cost"]),
    float(result["total_monthly_cost"]),
    float(result["total_annual_cost"]),
    float(effort["complexity"]["total_score"]),
    effort["complexity_level"],
    float(effort["phases"]["discovery"]["estimate"]),
    float(effort["phases"]["build_ingestion"]["estimate"]),
    float(effort["phases"]["build_transformation"]["estimate"]),
    float(effort["phases"]["testing"]["estimate"]),
    float(effort["phases"]["deployment"]["estimate"]),
    float(effort["phases"]["documentation"]["estimate"]),
    float(effort["total_effort_days"]["min"]),
    float(effort["total_effort_days"]["estimate"]),
    float(effort["total_effort_days"]["max"]),
)]

if save_results:
    df = spark.createDataFrame(row, schema)
    df.write \
      .format("delta") \
      .mode("append") \
      .option("mergeSchema", "true") \
      .saveAsTable("edh.ingestion.edh_cost_estimations")
    print(f"Saved to edh.ingestion.edh_cost_estimations — request_id={request_id}")
else:
    print("save_results=false — skipping Delta table write.")

# COMMAND ----------

# ============================================================
# SECTION 15: SAVE TO COMBINED RESULT TABLE (both ingestion types)
# ============================================================
# Shared dashboard table written by BOTH this notebook and
# EDH_New_Source_Estimator_Job.py. Schema must stay identical across
# both notebooks - any change here needs the matching change made in
# the other notebook's equivalent section too.
#
# +-20% variance is applied HERE (not in the per-flow table above,
# which no longer carries it per explicit request) using the same
# apply_variance()/VARIANCE_FACTOR already defined in SECTION 7.

combined_compute_low,    combined_compute_high    = apply_variance(result["compute_cost"])
combined_storage_low,    combined_storage_high    = apply_variance(result["storage_cost"])
combined_networking_low, combined_networking_high = apply_variance(result["networking_cost"])
combined_total_low,      combined_total_high      = apply_variance(result["total_monthly_cost"])
combined_annual_low,     combined_annual_high     = apply_variance(result["total_annual_cost"])

combined_schema = StructType([
    StructField("request_id",              StringType(),    True),
    StructField("estimation_timestamp",    TimestampType(), True),
    StructField("ingestion_type",          StringType(),    True),
    StructField("business_unit",           StringType(),    True),
    StructField("requestor",               StringType(),    True),
    StructField("request_date",            StringType(),    True),
    StructField("contains_phi",            StringType(),    True),
    StructField("compute_cost_monthly",    DoubleType(),    True),
    StructField("compute_cost_low",        DoubleType(),    True),
    StructField("compute_cost_high",       DoubleType(),    True),
    StructField("storage_cost_monthly",    DoubleType(),    True),
    StructField("storage_cost_low",        DoubleType(),    True),
    StructField("storage_cost_high",       DoubleType(),    True),
    StructField("networking_cost_monthly", DoubleType(),    True),
    StructField("networking_cost_low",     DoubleType(),    True),
    StructField("networking_cost_high",    DoubleType(),    True),
    StructField("total_cost_monthly",      DoubleType(),    True),
    StructField("total_cost_monthly_low",  DoubleType(),    True),
    StructField("total_cost_monthly_high", DoubleType(),    True),
    StructField("total_cost_annual",       DoubleType(),    True),
    StructField("total_cost_annual_low",   DoubleType(),    True),
    StructField("total_cost_annual_high",  DoubleType(),    True),
])

combined_row = [(
    request_id,
    datetime.now(timezone.utc),
    "Existing Source",
    result["business_unit"],
    result["requestor"],
    result["request_date"],
    result["contains_phi"],
    float(result["compute_cost"]),       float(combined_compute_low),    float(combined_compute_high),
    float(result["storage_cost"]),       float(combined_storage_low),    float(combined_storage_high),
    float(result["networking_cost"]),    float(combined_networking_low), float(combined_networking_high),
    float(result["total_monthly_cost"]), float(combined_total_low),      float(combined_total_high),
    float(result["total_annual_cost"]),  float(combined_annual_low),     float(combined_annual_high),
)]

if save_results:
    df_combined = spark.createDataFrame(combined_row, combined_schema)
    df_combined.write \
      .format("delta") \
      .mode("append") \
      .option("mergeSchema", "true") \
      .saveAsTable("edh.ingestion.edh_combined_estimations")
    print(f"Saved to edh.ingestion.edh_combined_estimations — request_id={request_id}")

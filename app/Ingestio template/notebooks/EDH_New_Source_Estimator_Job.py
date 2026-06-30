# Databricks notebook source
# EDH New-Source Onboarding Cost & Effort Estimator
# Triggered by the Ingestio app - reads inputs via widgets, runs cost
# (network + storage + compute) and effort estimation, and writes BOTH
# the raw request and the flattened dashboard-ready results to Delta.

# COMMAND ----------

# ============================================================
# SECTION 1: WIDGETS
# ============================================================

# ---- Metadata widgets (no impact on calculation - audit/governance only) ----
dbutils.widgets.text(    "request_id",             "")
dbutils.widgets.text(    "business_unit",           "")
dbutils.widgets.text(    "request_date",            "")
dbutils.widgets.text(    "requestor",               "")
dbutils.widgets.text(    "business_justification",  "")
dbutils.widgets.dropdown("contains_phi",            "No",              ["Yes", "No"])
dbutils.widgets.dropdown("delete_handling",         "Soft",            ["Hard", "Soft", "Ignore"])
dbutils.widgets.dropdown("schema_stability",        "Stable",          ["Stable", "Occasionally Changes", "Highly Dynamic"])
dbutils.widgets.dropdown("cdc_method",              "Not Applicable",  ["Timestamp", "Log Based", "Not Applicable"])

# ---- New-source cost calculation widgets (business-answerable inputs only) ----
dbutils.widgets.text(    "pipeline_name",       "New EDH Pipeline")
dbutils.widgets.text(    "source_gb",           "300")
dbutils.widgets.dropdown("network_source_type", "expressroute_metered",
                         ["azure_same_region", "expressroute_metered", "expressroute_unlimited",
                          "vpn", "aws_s3", "aws_rds", "gcp", "sftp", "api", "cross_region"])
dbutils.widgets.dropdown("copy_interval",       "bulk",  ["bulk", "incremental"])
dbutils.widgets.dropdown("include_egress",      "false", ["true", "false"])
dbutils.widgets.text(    "egress_gb",           "0")
dbutils.widgets.text(    "sla_time_hr",         "2")
dbutils.widgets.dropdown("vm_type",             "Standard_DS5_v2", ["Standard_DS3_v2", "Standard_DS5_v2"])
dbutils.widgets.dropdown("data_distribution",
                         "Not sure",
                         ["Evenly distributed", "Some concentration in a few records",
                          "Highly concentrated in a few records", "Not sure"])
dbutils.widgets.dropdown("delivery_pattern",
                         "Not sure",
                         ["One large batch file/extract", "Many small files or frequent small batches", "Not sure"])
dbutils.widgets.dropdown("partition_key_availability",
                         "Not sure",
                         ["Yes, a clear date/region/key field", "Somewhat", "No clear splitting field", "Not sure"])

# ---- Effort estimation widgets (business/analyst-answerable inputs only) ----
dbutils.widgets.dropdown("complexity_source_type", "external_api",
                         ["internal_sql", "internal_api", "azure_service", "external_sftp",
                          "external_api", "aws_s3", "aws_rds", "gcp", "saas_connector",
                          "legacy_mainframe", "multi_source"])
dbutils.widgets.dropdown("transformation_logic", "medium",
                         ["light", "medium", "heavy"])
dbutils.widgets.dropdown("frequency", "daily",
                         ["adhoc", "weekly", "daily", "hourly", "near_real_time", "real_time"])

dbutils.widgets.dropdown("save_results", "true", ["true", "false"])

# COMMAND ----------

# ============================================================
# SECTION 2: ENGINEERING CONSTANTS
# ============================================================

SKEW_BY_PATTERN = {
    "Evenly distributed":                    0.9,
    "Some concentration in a few records":   0.6,
    "Highly concentrated in a few records":  0.3,
    "Not sure":                              0.6,
}

SMALL_FILES_BY_DELIVERY = {
    "One large batch file/extract":               0.8,
    "Many small files or frequent small batches": 0.4,
    "Not sure":                                   0.5,
}

PARTITIONING_BY_KEY_AVAILABILITY = {
    "Yes, a clear date/region/key field": 1.4,
    "Somewhat":                           1.1,
    "No clear splitting field":           0.8,
    "Not sure":                           1.2,
}

# Collapsed from 6 categories to 3 for business-answerability.
# Values averaged from original 6-category values:
#   light = avg(passthrough=1.1, light_rename_cast=1.3)
#   medium = moderate_joins=1.8 alone (preserves original flat default)
#   heavy = avg(complex_business_rules=2.3, heavy_ml_enrichment=2.8, real_time_streaming=3.3)
COMPLEXITY_FACTOR_BY_TRANSFORMATION = {
    "light":  1.2,
    "medium": 1.8,
    "heavy":  2.8,
}

DBU_COST_HR  = 0.3
DRIVER_NODES = 1

VM_SPECS = {
    "Standard_DS3_v2": {
        "vcpu": 4, "memory_gib": 14,
        "per_node_throughput_gb_hr": 4.5,
        "vm_cost_hr": 0.293,
    },
    "Standard_DS5_v2": {
        "vcpu": 16, "memory_gib": 56,
        "per_node_throughput_gb_hr": 18.0,
        "vm_cost_hr": 1.17,
    },
}

# COMMAND ----------

# ============================================================
# SECTION 3: NETWORK COST
# ============================================================

import math
from datetime import datetime, timezone

def calculate_network_cost(
    source_gb: float,
    source_type: str = "vpn",
    include_egress: bool = False,
    egress_gb: float = 0.0,
) -> dict:

    RATES = {
        "azure_same_region":      0.00,
        "expressroute_metered":   0.025,
        "expressroute_unlimited": 0.00,
        "vpn":                    0.00,
        "aws_s3":                 0.09,
        "aws_rds":                0.09,
        "gcp":                    0.12,
        "sftp":                   0.00,
        "api":                    0.00,
        "private_endpoint":       0.01,
        "internet_egress":        0.087,
        "cross_region":           0.02,
    }

    BRONZE_RATIO = 1.0
    SILVER_RATIO = 0.022
    GOLD_RATIO   = 0.037

    if source_type not in RATES:
        raise ValueError(f"Unknown source_type '{source_type}'. Valid options: {list(RATES.keys())}")

    ingress_rate = RATES[source_type]

    stage1_cost = source_gb * ingress_rate
    stage2_cost = source_gb * BRONZE_RATIO * RATES["private_endpoint"]
    stage3_cost = source_gb * BRONZE_RATIO * RATES["private_endpoint"]
    stage4_cost = source_gb * SILVER_RATIO * RATES["private_endpoint"]
    stage5_cost = source_gb * SILVER_RATIO * RATES["private_endpoint"]
    stage6_cost = source_gb * GOLD_RATIO   * RATES["private_endpoint"]

    egress_cost = egress_gb * RATES["internet_egress"] if include_egress else 0.0

    total_daily   = stage1_cost + stage2_cost + stage3_cost + stage4_cost + stage5_cost + stage6_cost + egress_cost
    total_monthly = total_daily * 30

    return {
        "source_gb":   source_gb,
        "source_type": source_type,
        "breakdown": {
            "stage1_ingress":        round(stage1_cost, 4),
            "stage2_bronze_write":   round(stage2_cost, 4),
            "stage3_silver_read":    round(stage3_cost, 4),
            "stage4_silver_write":   round(stage4_cost, 4),
            "stage5_gold_read":      round(stage5_cost, 4),
            "stage6_gold_write":     round(stage6_cost, 4),
            "egress_to_third_party": round(egress_cost, 4),
        },
        "total_daily_cost":    round(total_daily, 4),
        "total_monthly_cost":  round(total_monthly, 2),
        "cost_per_gb_per_day": round(total_daily / source_gb, 4) if source_gb > 0 else 0,
    }

# COMMAND ----------

# ============================================================
# SECTION 4: STORAGE COST
# ============================================================

def calculate_storage_cost(
    source_gb: float,
    copy_interval: str = "bulk",
) -> dict:

    ADLS_HOT_RATE        = 0.0208
    MANAGED_STORAGE_RATE = 0.023

    BRONZE_RATIO = 1.0
    SILVER_RATIO = 0.022
    GOLD_RATIO   = 0.037

    WRITE_RATE_PER_10K = 0.07
    READ_RATE_PER_10K  = 0.0052
    LIST_RATE_PER_10K  = 0.09

    bronze_gb = source_gb * BRONZE_RATIO
    silver_gb = source_gb * SILVER_RATIO
    gold_gb   = source_gb * GOLD_RATIO
    total_gb  = bronze_gb + silver_gb + gold_gb

    bronze_storage_cost = bronze_gb * ADLS_HOT_RATE
    silver_storage_cost = silver_gb * MANAGED_STORAGE_RATE
    gold_storage_cost   = gold_gb   * MANAGED_STORAGE_RATE
    total_storage_cost  = bronze_storage_cost + silver_storage_cost + gold_storage_cost

    avg_files       = max(1, int((source_gb * 1024) / 128))
    daily_write_ops = avg_files + 3
    daily_read_ops  = avg_files * 2 + 5 if copy_interval == "incremental" else 5
    daily_list_ops  = 4

    monthly_write_ops = daily_write_ops * 30
    monthly_read_ops  = daily_read_ops  * 30
    monthly_list_ops  = daily_list_ops  * 30

    write_ops_cost = (monthly_write_ops / 10000) * WRITE_RATE_PER_10K
    read_ops_cost  = (monthly_read_ops  / 10000) * READ_RATE_PER_10K
    list_ops_cost  = (monthly_list_ops  / 10000) * LIST_RATE_PER_10K
    total_ops_cost = write_ops_cost + read_ops_cost + list_ops_cost

    grand_total_monthly = total_storage_cost + total_ops_cost

    return {
        "source_gb":     source_gb,
        "copy_interval": copy_interval,
        "storage": {
            "bronze_gb":             round(bronze_gb, 2),
            "silver_gb":             round(silver_gb, 3),
            "gold_gb":               round(gold_gb, 3),
            "total_gb":              round(total_gb, 2),
            "bronze_cost_monthly":   round(bronze_storage_cost, 4),
            "silver_cost_monthly":   round(silver_storage_cost, 4),
            "gold_cost_monthly":     round(gold_storage_cost, 4),
            "total_storage_monthly": round(total_storage_cost, 4),
        },
        "operations": {
            "total_ops_monthly": round(total_ops_cost, 4),
        },
        "grand_total_monthly": round(grand_total_monthly, 4),
        "cost_per_gb_monthly": round(grand_total_monthly / source_gb, 4) if source_gb > 0 else 0,
    }

# COMMAND ----------

# ============================================================
# SECTION 5: WORKER NODE SIZING + COMPUTE COST
# ============================================================

def calculate_worker_sizing(
    data_volume_gb: float,
    sla_time_hr: float,
    partitioning: float = 1.2,
    skew: float = 0.6,
    small_files: float = 0.5,
    complexity_factor: float = 1.8,
    per_node_throughput_gb_hr: float = 18.0,
    driver_nodes: int = DRIVER_NODES,
) -> dict:
    combined_adjustment      = partitioning * skew * small_files
    effective_per_node       = per_node_throughput_gb_hr * combined_adjustment
    worker_nodes_raw         = (data_volume_gb * complexity_factor) / (sla_time_hr * effective_per_node)
    worker_nodes_estimated   = math.ceil(worker_nodes_raw)
    total_nodes              = worker_nodes_estimated + driver_nodes
    total_effective_throughput = worker_nodes_estimated * effective_per_node
    estimated_runtime        = (data_volume_gb / total_effective_throughput) * complexity_factor
    meets_sla                = estimated_runtime <= sla_time_hr

    return {
        "combined_adjustment":      round(combined_adjustment, 4),
        "effective_per_node_gb_hr": round(effective_per_node, 2),
        "worker_nodes_raw":         round(worker_nodes_raw, 2),
        "worker_nodes_estimated":   worker_nodes_estimated,
        "total_nodes":              total_nodes,
        "estimated_runtime_hr":     round(estimated_runtime, 4),
        "meets_sla":                meets_sla,
    }


def calculate_compute_cost_from_sizing(
    sizing: dict,
    vm_cost_hr: float = 1.17,
    dbu_cost_hr: float = DBU_COST_HR,
) -> dict:
    total_nodes          = sizing["total_nodes"]
    total_per_node_hr    = vm_cost_hr + dbu_cost_hr
    cluster_cost_per_hr  = total_nodes * total_per_node_hr
    compute_cost_daily   = sizing["estimated_runtime_hr"] * cluster_cost_per_hr
    compute_cost_monthly = compute_cost_daily * 30

    return {
        "total_per_node_hr":    round(total_per_node_hr, 4),
        "cluster_cost_per_hr":  round(cluster_cost_per_hr, 4),
        "compute_cost_daily":   round(compute_cost_daily, 4),
        "compute_cost_monthly": round(compute_cost_monthly, 2),
    }

# COMMAND ----------

# ============================================================
# SECTION 6: EFFORT ESTIMATION ENGINE
# ============================================================

# Renormalized after removing data_quality_rules and dependencies
# (each weight / 0.70, since the remaining 4 originally summed to
# 0.70 - this preserves their original RELATIVE importance to each
# other rather than picking new round numbers arbitrarily).
COMPLEXITY_WEIGHTS = {
    'source_type': 0.286, 'volume': 0.214, 'transformation_logic': 0.357,
    'frequency': 0.143,
}
# Weights sum to 1.000

SCORING_RUBRICS = {
    'source_type': {
        'internal_sql': 10, 'internal_api': 25, 'azure_service': 20,
        'external_sftp': 40, 'external_api': 55, 'aws_s3': 45,
        'aws_rds': 50, 'gcp': 55, 'saas_connector': 60,
        'legacy_mainframe': 85, 'multi_source': 90,
    },
    'volume': {
        'tiny': 10, 'small': 25, 'medium': 45,
        'large': 70, 'very_large': 85, 'massive': 95,
    },
    'transformation_logic': {
        # Collapsed from 6 to 3: light=avg(10,25); medium=50; heavy=avg(70,85,95)
        'light':  18,
        'medium': 50,
        'heavy':  83,
    },
    'frequency': {
        'adhoc': 10, 'weekly': 20, 'daily': 35,
        'hourly': 60, 'near_real_time': 80, 'real_time': 95,
    },
}

# volume_tier is now DERIVED from source_gb instead of being a separate
# question. These GB thresholds are invented round-number buckets.
VOLUME_TIER_THRESHOLDS_GB = [
    (10,   'tiny'),
    (50,   'small'),
    (200,  'medium'),
    (500,  'large'),
    (2000, 'very_large'),
]
VOLUME_TIER_DEFAULT_ABOVE = 'massive'

def get_volume_tier(source_gb: float) -> str:
    for threshold_gb, tier in VOLUME_TIER_THRESHOLDS_GB:
        if source_gb < threshold_gb:
            return tier
    return VOLUME_TIER_DEFAULT_ABOVE

def calculate_complexity_score(
    source_type: str = 'external_api', volume: str = 'medium',
    transformation_logic: str = 'medium', frequency: str = 'daily',
) -> dict:
    scores = {
        'source_type':          SCORING_RUBRICS['source_type'].get(source_type, 50),
        'volume':               SCORING_RUBRICS['volume'].get(volume, 50),
        'transformation_logic': SCORING_RUBRICS['transformation_logic'].get(transformation_logic, 50),
        'frequency':            SCORING_RUBRICS['frequency'].get(frequency, 50),
    }
    weighted_scores = {k: v * COMPLEXITY_WEIGHTS[k] for k, v in scores.items()}
    total_score     = sum(weighted_scores.values())
    if total_score <= 30:
        level = 'Simple'
    elif total_score <= 70:
        level = 'Medium'
    else:
        level = 'Complex'
    return {
        'raw_scores':       scores,
        'weighted_scores':  {k: round(v, 1) for k, v in weighted_scores.items()},
        'total_score':      round(total_score, 1),
        'complexity_level': level,
    }


PHASE_EFFORT = {
    'discovery':            {'Simple': {'min': 2, 'max': 3},  'Medium': {'min': 4, 'max': 6},  'Complex': {'min': 5, 'max': 10}},
    'design':               {'Simple': {'min': 2, 'max': 4},  'Medium': {'min': 5, 'max': 8},  'Complex': {'min': 5, 'max': 10}},
    'build_ingestion':      {'Simple': {'min': 2, 'max': 3},  'Medium': {'min': 4, 'max': 6},  'Complex': {'min': 5, 'max': 10}},
    'build_transformation': {'Simple': {'min': 2, 'max': 4},  'Medium': {'min': 5, 'max': 8},  'Complex': {'min': 5, 'max': 10}},
    'build_orchestration':  {'Simple': {'min': 2, 'max': 3},  'Medium': {'min': 3, 'max': 5},  'Complex': {'min': 5, 'max': 8}},
    'deployment':           {'Simple': {'min': 2, 'max': 4},  'Medium': {'min': 5, 'max': 5},  'Complex': {'min': 5, 'max': 7}},
    'documentation':        {'Simple': {'min': 2, 'max': 3},  'Medium': {'min': 3, 'max': 4},  'Complex': {'min': 4, 'max': 5}},
}

TESTING_PERCENTAGE = 0.25

def estimate_effort(
    pipeline_name: str = 'New Pipeline', source_type: str = 'external_api', volume: str = 'medium',
    transformation_logic: str = 'medium', frequency: str = 'daily',
    complexity_override: str = None,
) -> dict:
    complexity = calculate_complexity_score(
        source_type=source_type, volume=volume, transformation_logic=transformation_logic,
        frequency=frequency,
    )
    level = complexity_override if complexity_override in ['Simple', 'Medium', 'Complex'] else complexity['complexity_level']

    phases = {}
    for phase, ranges in PHASE_EFFORT.items():
        r        = ranges[level]
        midpoint = (r['min'] + r['max']) / 2
        phases[phase] = {'min': r['min'], 'max': r['max'], 'estimate': midpoint}

    build_min = phases['build_ingestion']['min'] + phases['build_transformation']['min'] + phases['build_orchestration']['min']
    build_max = phases['build_ingestion']['max'] + phases['build_transformation']['max'] + phases['build_orchestration']['max']
    build_est = phases['build_ingestion']['estimate'] + phases['build_transformation']['estimate'] + phases['build_orchestration']['estimate']

    testing_min = math.ceil(build_min * TESTING_PERCENTAGE)
    testing_max = math.ceil(build_max * TESTING_PERCENTAGE)
    testing_est = round(build_est * TESTING_PERCENTAGE, 1)
    phases['testing'] = {'min': testing_min, 'max': testing_max, 'estimate': testing_est}

    total_min = sum(p['min'] for p in phases.values())
    total_max = sum(p['max'] for p in phases.values())
    total_est = sum(p['estimate'] for p in phases.values())

    return {
        'pipeline_name':     pipeline_name,
        'complexity':        complexity,
        'complexity_level':  level,
        'phases':            phases,
        'build_subtotal':    {'min': build_min, 'max': build_max, 'estimate': build_est},
        'total_effort_days': {'min': total_min, 'max': total_max, 'estimate': total_est},
    }

# COMMAND ----------

# ============================================================
# SECTION 7: INPUT VALIDATION
# ============================================================

def validate_inputs(source_gb, copy_interval, network_source_type,
                    sla_time_hr, contains_phi, delete_handling,
                    schema_stability, cdc_method, complexity_source_type,
                    transformation_logic, frequency,
                    vm_type,
                    data_distribution, delivery_pattern,
                    partition_key_availability):

    if vm_type not in VM_SPECS:
        raise ValueError(f"Invalid vm_type. Choose from: {list(VM_SPECS.keys())}")
    if data_distribution not in SKEW_BY_PATTERN:
        raise ValueError(f"Invalid data_distribution. Choose from: {list(SKEW_BY_PATTERN.keys())}")
    if delivery_pattern not in SMALL_FILES_BY_DELIVERY:
        raise ValueError(f"Invalid delivery_pattern. Choose from: {list(SMALL_FILES_BY_DELIVERY.keys())}")
    if partition_key_availability not in PARTITIONING_BY_KEY_AVAILABILITY:
        raise ValueError(f"Invalid partition_key_availability. Choose from: {list(PARTITIONING_BY_KEY_AVAILABILITY.keys())}")
    if source_gb <= 0:
        raise ValueError("source_gb must be greater than 0")
    if copy_interval not in ["bulk", "incremental"]:
        raise ValueError("copy_interval must be 'bulk' or 'incremental'")
    if sla_time_hr <= 0:
        raise ValueError("sla_time_hr must be greater than 0")
    if contains_phi not in ["Yes", "No"]:
        raise ValueError("contains_phi must be Yes or No")
    if delete_handling not in ["Hard", "Soft", "Ignore"]:
        raise ValueError("Invalid delete handling. Choose from: Hard / Soft / Ignore")
    if schema_stability not in ["Stable", "Occasionally Changes", "Highly Dynamic"]:
        raise ValueError("Invalid schema stability.")
    if cdc_method not in ["Timestamp", "Log Based", "Not Applicable"]:
        raise ValueError("Invalid CDC method.")
    if copy_interval == "incremental" and cdc_method == "Not Applicable":
        raise ValueError("CDC Method cannot be 'Not Applicable' when Copy Interval is 'incremental'")
    if copy_interval == "bulk" and cdc_method != "Not Applicable":
        raise ValueError("CDC Method should be 'Not Applicable' when Copy Interval is 'bulk'")
    if complexity_source_type not in SCORING_RUBRICS['source_type']:
        raise ValueError(f"Invalid complexity_source_type. Choose from: {list(SCORING_RUBRICS['source_type'].keys())}")
    if transformation_logic not in SCORING_RUBRICS['transformation_logic']:
        raise ValueError(f"Invalid transformation_logic. Choose from: {list(SCORING_RUBRICS['transformation_logic'].keys())}")
    if transformation_logic not in COMPLEXITY_FACTOR_BY_TRANSFORMATION:
        raise ValueError(
            f"transformation_logic '{transformation_logic}' has no matching "
            f"COMPLEXITY_FACTOR_BY_TRANSFORMATION entry - lookup tables are out of sync."
        )
    if frequency not in SCORING_RUBRICS['frequency']:
        raise ValueError(f"Invalid frequency. Choose from: {list(SCORING_RUBRICS['frequency'].keys())}")

# COMMAND ----------

# ============================================================
# SECTION 8: READ WIDGET INPUTS
# ============================================================

request_id             = dbutils.widgets.get("request_id")
business_unit          = dbutils.widgets.get("business_unit")
request_date           = dbutils.widgets.get("request_date")
requestor              = dbutils.widgets.get("requestor")
business_justification = dbutils.widgets.get("business_justification")
contains_phi           = dbutils.widgets.get("contains_phi")
delete_handling        = dbutils.widgets.get("delete_handling")
schema_stability       = dbutils.widgets.get("schema_stability")
cdc_method             = dbutils.widgets.get("cdc_method")

pipeline_name              = dbutils.widgets.get("pipeline_name")
source_gb                  = float(dbutils.widgets.get("source_gb"))
network_source_type        = dbutils.widgets.get("network_source_type")
copy_interval              = dbutils.widgets.get("copy_interval")
include_egress             = dbutils.widgets.get("include_egress").lower() == "true"
egress_gb                  = float(dbutils.widgets.get("egress_gb"))
sla_time_hr                = float(dbutils.widgets.get("sla_time_hr"))
vm_type                    = dbutils.widgets.get("vm_type")
data_distribution          = dbutils.widgets.get("data_distribution")
delivery_pattern           = dbutils.widgets.get("delivery_pattern")
partition_key_availability = dbutils.widgets.get("partition_key_availability")

complexity_source_type = dbutils.widgets.get("complexity_source_type")
transformation_logic   = dbutils.widgets.get("transformation_logic")
frequency              = dbutils.widgets.get("frequency")

save_results = dbutils.widgets.get("save_results").lower() == "true"

validate_inputs(source_gb, copy_interval, network_source_type,
                sla_time_hr, contains_phi, delete_handling,
                schema_stability, cdc_method, complexity_source_type,
                transformation_logic, frequency,
                vm_type,
                data_distribution, delivery_pattern,
                partition_key_availability)

# COMMAND ----------

# ============================================================
# SECTION 9: RUN CALCULATIONS
# ============================================================

network = calculate_network_cost(
    source_gb=source_gb, source_type=network_source_type,
    include_egress=include_egress, egress_gb=egress_gb,
)

storage = calculate_storage_cost(
    source_gb=source_gb, copy_interval=copy_interval,
)

vm_specs = VM_SPECS[vm_type]

resolved_skew              = SKEW_BY_PATTERN[data_distribution]
resolved_small_files       = SMALL_FILES_BY_DELIVERY[delivery_pattern]
resolved_partitioning      = PARTITIONING_BY_KEY_AVAILABILITY[partition_key_availability]
resolved_complexity_factor = COMPLEXITY_FACTOR_BY_TRANSFORMATION[transformation_logic]

sizing = calculate_worker_sizing(
    data_volume_gb=source_gb, sla_time_hr=sla_time_hr,
    per_node_throughput_gb_hr=vm_specs["per_node_throughput_gb_hr"],
    partitioning=resolved_partitioning,
    skew=resolved_skew,
    small_files=resolved_small_files,
    complexity_factor=resolved_complexity_factor,
)

compute = calculate_compute_cost_from_sizing(sizing, vm_cost_hr=vm_specs["vm_cost_hr"])

total_monthly_cost  = round(network["total_monthly_cost"] + storage["grand_total_monthly"] + compute["compute_cost_monthly"], 2)
total_annual_cost   = round(total_monthly_cost * 12, 2)
cost_per_gb_monthly = round(total_monthly_cost / source_gb, 4) if source_gb > 0 else 0

derived_volume_tier = get_volume_tier(source_gb)

effort = estimate_effort(
    pipeline_name=pipeline_name, source_type=complexity_source_type, volume=derived_volume_tier,
    transformation_logic=transformation_logic, frequency=frequency,
)

# COMMAND ----------

# ============================================================
# SECTION 10: DISPLAY RESULTS
# ============================================================

print("=" * 70)
print("  EDH NEW-SOURCE ONBOARDING ESTIMATE")
print("=" * 70)
print(f"\n  REQUEST DETAILS")
print(f"  Pipeline Name     : {pipeline_name}")
print(f"  Business Unit     : {business_unit}")
print(f"  Requestor         : {requestor}")
print(f"  Contains PHI      : {contains_phi}")
print(f"  Delete Handling   : {delete_handling}")
print(f"  Schema Stability  : {schema_stability}")
print(f"  CDC Method        : {cdc_method}")
print(f"\n{'-' * 70}")
print(f"  COST ESTIMATE (Monthly)")
print(f"{'-' * 70}")
print(f"  Network Cost      : ${network['total_monthly_cost']}")
print(f"  Storage Cost      : ${storage['grand_total_monthly']}")
print(f"  Compute Cost      : ${compute['compute_cost_monthly']}")
print(f"  VM Type           : {vm_type}  (throughput={vm_specs['per_node_throughput_gb_hr']} GB/hr, vm_cost=${vm_specs['vm_cost_hr']}/hr)")
print(f"  Data Distribution : {data_distribution}  (skew={resolved_skew})")
print(f"  Delivery Pattern  : {delivery_pattern}  (small_files={resolved_small_files})")
print(f"  Partition Key     : {partition_key_availability}  (partitioning={resolved_partitioning})")
print(f"  Transformation    : {transformation_logic}  (complexity_factor={resolved_complexity_factor})")
print(f"  Worker Nodes      : {sizing['worker_nodes_estimated']} (+{DRIVER_NODES} driver)")
print(f"  Estimated Runtime : {sizing['estimated_runtime_hr']} hr  (Meets SLA: {'YES' if sizing['meets_sla'] else 'NO'})")
print(f"  TOTAL MONTHLY     : ${total_monthly_cost}")
print(f"  TOTAL ANNUAL      : ${total_annual_cost}")
print(f"\n{'-' * 70}")
print(f"  EFFORT ESTIMATE")
print(f"{'-' * 70}")
print(f"  Complexity        : {effort['complexity_level']} (score {effort['complexity']['total_score']}/100)")
print(f"  Volume Tier       : {derived_volume_tier}  (derived from source_gb={source_gb})")
print(f"  Total Effort      : {effort['total_effort_days']['estimate']} person-days "
      f"({effort['total_effort_days']['min']}-{effort['total_effort_days']['max']} range)")
print("=" * 70)

# COMMAND ----------

# ============================================================
# SECTION 11: SAVE RAW REQUEST TO DELTA (table 1 of 2)
# ============================================================

from pyspark.sql.types import (
    StructType, StructField, StringType, DoubleType, IntegerType, BooleanType, TimestampType
)

request_schema = StructType([
    StructField("request_id",                  StringType(),    True),
    StructField("submission_timestamp",        TimestampType(), True),
    StructField("business_unit",               StringType(),    True),
    StructField("request_date",                StringType(),    True),
    StructField("requestor",                   StringType(),    True),
    StructField("business_justification",      StringType(),    True),
    StructField("contains_phi",                StringType(),    True),
    StructField("delete_handling",             StringType(),    True),
    StructField("schema_stability",            StringType(),    True),
    StructField("cdc_method",                  StringType(),    True),
    StructField("pipeline_name",               StringType(),    True),
    StructField("source_gb",                   DoubleType(),    True),
    StructField("network_source_type",         StringType(),    True),
    StructField("copy_interval",               StringType(),    True),
    StructField("include_egress",              BooleanType(),   True),
    StructField("egress_gb",                   DoubleType(),    True),
    StructField("sla_time_hr",                 DoubleType(),    True),
    StructField("vm_type",                     StringType(),    True),
    StructField("data_distribution",           StringType(),    True),
    StructField("delivery_pattern",            StringType(),    True),
    StructField("partition_key_availability",  StringType(),    True),
    StructField("complexity_source_type",      StringType(),    True),
    StructField("transformation_logic",        StringType(),    True),
    StructField("frequency",                   StringType(),    True),
])

request_row = [(
    request_id, datetime.now(timezone.utc),
    business_unit, request_date, requestor, business_justification,
    contains_phi, delete_handling, schema_stability, cdc_method,
    pipeline_name, float(source_gb), network_source_type,
    copy_interval, bool(include_egress), float(egress_gb), float(sla_time_hr), vm_type,
    data_distribution, delivery_pattern, partition_key_availability,
    complexity_source_type, transformation_logic, frequency,
)]

if save_results:
    df_request = spark.createDataFrame(request_row, request_schema)
    df_request.write.format("delta").mode("append").option("mergeSchema", "true") \
        .saveAsTable("edh.ingestion.edh_newsource_requests")
    print(f"Saved request to edh.ingestion.edh_newsource_requests - request_id={request_id}")

# COMMAND ----------

# ============================================================
# SECTION 12: SAVE FINAL RESULT TABLE FOR DASHBOARD (table 2 of 2)
# ============================================================

result_schema = StructType([
    StructField("request_id",                      StringType(),    True),
    StructField("estimation_timestamp",            TimestampType(), True),
    StructField("pipeline_name",                   StringType(),    True),
    StructField("business_unit",                   StringType(),    True),
    StructField("contains_phi",                    StringType(),    True),
    StructField("source_gb",                       DoubleType(),    True),
    StructField("vm_type",                         StringType(),    True),
    StructField("vm_per_node_throughput_gb_hr",    DoubleType(),    True),
    StructField("resolved_skew",                   DoubleType(),    True),
    StructField("resolved_small_files",            DoubleType(),    True),
    StructField("resolved_partitioning",           DoubleType(),    True),
    StructField("resolved_complexity_factor",      DoubleType(),    True),
    StructField("vm_cost_hr",                      DoubleType(),    True),
    StructField("network_cost_monthly",            DoubleType(),    True),
    StructField("storage_cost_monthly",            DoubleType(),    True),
    StructField("compute_cost_monthly",            DoubleType(),    True),
    StructField("total_cost_monthly",              DoubleType(),    True),
    StructField("total_cost_annual",               DoubleType(),    True),
    StructField("cost_per_gb_monthly",             DoubleType(),    True),
    StructField("worker_nodes_estimated",          IntegerType(),   True),
    StructField("total_nodes",                     IntegerType(),   True),
    StructField("estimated_runtime_hr",            DoubleType(),    True),
    StructField("meets_sla",                       BooleanType(),   True),
    StructField("complexity_score",                DoubleType(),    True),
    StructField("complexity_level",                StringType(),    True),
    StructField("derived_volume_tier",             StringType(),    True),
    StructField("discovery_days",                  DoubleType(),    True),
    StructField("design_days",                     DoubleType(),    True),
    StructField("build_ingestion_days",            DoubleType(),    True),
    StructField("build_transformation_days",       DoubleType(),    True),
    StructField("build_orchestration_days",        DoubleType(),    True),
    StructField("testing_days",                    DoubleType(),    True),
    StructField("deployment_days",                 DoubleType(),    True),
    StructField("documentation_days",              DoubleType(),    True),
    StructField("build_subtotal_days",             DoubleType(),    True),
    StructField("total_effort_days_min",           DoubleType(),    True),
    StructField("total_effort_days_estimate",      DoubleType(),    True),
    StructField("total_effort_days_max",           DoubleType(),    True),
])

p = effort["phases"]

result_row = [(
    request_id, datetime.now(timezone.utc),
    pipeline_name, business_unit, contains_phi, float(source_gb),
    vm_type, float(vm_specs["per_node_throughput_gb_hr"]), float(vm_specs["vm_cost_hr"]),
    float(resolved_skew), float(resolved_small_files), float(resolved_partitioning), float(resolved_complexity_factor),
    float(network["total_monthly_cost"]),
    float(storage["grand_total_monthly"]),
    float(compute["compute_cost_monthly"]),
    float(total_monthly_cost),
    float(total_annual_cost),
    float(cost_per_gb_monthly),
    int(sizing["worker_nodes_estimated"]),
    int(sizing["total_nodes"]),
    float(sizing["estimated_runtime_hr"]),
    bool(sizing["meets_sla"]),
    float(effort["complexity"]["total_score"]),
    effort["complexity_level"],
    derived_volume_tier,
    float(p["discovery"]["estimate"]),
    float(p["design"]["estimate"]),
    float(p["build_ingestion"]["estimate"]),
    float(p["build_transformation"]["estimate"]),
    float(p["build_orchestration"]["estimate"]),
    float(p["testing"]["estimate"]),
    float(p["deployment"]["estimate"]),
    float(p["documentation"]["estimate"]),
    float(effort["build_subtotal"]["estimate"]),
    float(effort["total_effort_days"]["min"]),
    float(effort["total_effort_days"]["estimate"]),
    float(effort["total_effort_days"]["max"]),
)]

if save_results:
    df_result = spark.createDataFrame(result_row, result_schema)
    df_result.write.format("delta").mode("append").option("mergeSchema", "true") \
        .saveAsTable("edh.ingestion.edh_newsource_estimations")
    print(f"Saved results to edh.ingestion.edh_newsource_estimations - request_id={request_id}")

# COMMAND ----------

# ============================================================
# SECTION 13: SAVE TO COMBINED RESULT TABLE (both ingestion types)
# ============================================================

VARIANCE_FACTOR = 0.10

def apply_variance(value, variance=VARIANCE_FACTOR):
    return round(value * (1 - variance), 2), round(value * (1 + variance), 2)

combined_compute_low,    combined_compute_high    = apply_variance(compute["compute_cost_monthly"])
combined_storage_low,    combined_storage_high    = apply_variance(storage["grand_total_monthly"])
combined_networking_low, combined_networking_high = apply_variance(network["total_monthly_cost"])
combined_total_low,      combined_total_high      = apply_variance(total_monthly_cost)
combined_annual_low,     combined_annual_high     = apply_variance(total_annual_cost)

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
    "New Source",
    business_unit,
    requestor,
    request_date,
    contains_phi,
    float(compute["compute_cost_monthly"]),     float(combined_compute_low),    float(combined_compute_high),
    float(storage["grand_total_monthly"]),      float(combined_storage_low),    float(combined_storage_high),
    float(network["total_monthly_cost"]),       float(combined_networking_low), float(combined_networking_high),
    float(total_monthly_cost),                  float(combined_total_low),      float(combined_total_high),
    float(total_annual_cost),                   float(combined_annual_low),     float(combined_annual_high),
)]

if save_results:
    df_combined = spark.createDataFrame(combined_row, combined_schema)
    df_combined.write \
      .format("delta") \
      .mode("append") \
      .option("mergeSchema", "true") \
      .saveAsTable("edh.ingestion.edh_combined_estimations")
    print(f"Saved to edh.ingestion.edh_combined_estimations - request_id={request_id}")
else:
    print("save_results=false - skipping all Delta table writes.")

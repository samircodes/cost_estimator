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

THROUGHPUT_GB_HR = {
    "SQL Server": 21.45,
    "Postgres":   1.83,
    "Sybase":     5.10,
    "S3":         28.38,
    "SFTP":       0.95
}

CLUSTER_CONFIG = {
    "SQL Server": {"type": "multi",  "dbu_driver": 16, "dbu_per_worker": 6.4, "max_workers": 5},
    "Postgres":   {"type": "multi",  "dbu_driver": 16, "dbu_per_worker": 6.4, "max_workers": 5},
    "S3":         {"type": "multi",  "dbu_driver": 16, "dbu_per_worker": 6.4, "max_workers": 5},
    "SFTP":       {"type": "multi",  "dbu_driver": 16, "dbu_per_worker": 6.4, "max_workers": 5},
    "Sybase":     {"type": "single", "dbu_fixed": 8}
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

VALID_FORMATS_BY_SOURCE = {
    "SQL Server": ["JDBC Tabular"],
    "Postgres":   ["JDBC Tabular"],
    "Sybase":     ["JDBC Tabular"],
    "S3":         ["Parquet"],
    "SFTP":       ["CSV", "XLS", "XLSB"]
}

TRANSFORMATION_PROPORTIONS = {
    "Silver":  40 / 238,
    "Gold":    34 / 238,
    "MDM":     40 / 238,
    "RT_Mart": 16 / 238
}

CDC_RUNTIME_FACTOR = 0.30

FREQUENCY_MAP = {
    "Daily":   30,
    "Weekly":  4,
    "Monthly": 1
}

COST_PER_DBU         = 0.30
STORAGE_PRICE_PER_GB = 0.023
ENDPOINT_COST_PER_GB = 0.01
VARIANCE_FACTOR      = 0.20

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
# SECTION 2: WORKER TIER LOGIC
# ============================================================

def get_worker_count(additional_gb):
    if additional_gb < 10:
        return 1
    elif additional_gb < 50:
        return 2
    elif additional_gb < 100:
        return 3
    elif additional_gb < 200:
        return 4
    else:
        return 5

def get_effective_dbu_hr(source_type, additional_gb):
    config = CLUSTER_CONFIG[source_type]
    if config["type"] == "single":
        return config["dbu_fixed"]
    else:
        workers = get_worker_count(additional_gb)
        return config["dbu_driver"] + (workers * config["dbu_per_worker"])

# COMMAND ----------

# ============================================================
# SECTION 3: COMPUTE COST CALCULATION
# ============================================================

def calculate_compute_cost(source_type, additional_gb, load_type, runs_per_month):
    throughput    = THROUGHPUT_GB_HR[source_type]
    effective_dbu = get_effective_dbu_hr(source_type, additional_gb)
    layers        = LAYER_CONFIG[source_type]["layers"]

    ingestion_runtime_hrs = additional_gb / throughput

    if load_type == "CDC":
        ingestion_runtime_hrs *= CDC_RUNTIME_FACTOR

    ingestion_cost = ingestion_runtime_hrs * effective_dbu * COST_PER_DBU * runs_per_month

    transformation_cost = 0
    if CLUSTER_CONFIG[source_type]["type"] == "multi" and len(layers) > 1:
        for stage, proportion in TRANSFORMATION_PROPORTIONS.items():
            if stage in ["Silver", "Gold"] and "Gold" in layers:
                stage_runtime_hrs = ingestion_runtime_hrs * proportion
                transformation_cost += stage_runtime_hrs * effective_dbu * COST_PER_DBU * runs_per_month

    total_compute = ingestion_cost + transformation_cost

    return {
        "ingestion_cost":      round(ingestion_cost, 4),
        "transformation_cost": round(transformation_cost, 4),
        "total_compute":       round(total_compute, 4),
        "runtime_hrs":         round(ingestion_runtime_hrs, 4),
        "effective_dbu_hr":    round(effective_dbu, 2),
        "workers":             get_worker_count(additional_gb) if CLUSTER_CONFIG[source_type]["type"] == "multi" else "N/A (Single Node)"
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
# SECTION 6: RANGE CALCULATION (+-20%)
# ============================================================

def apply_variance(value, variance=VARIANCE_FACTOR):
    return round(value * (1 - variance), 2), round(value * (1 + variance), 2)

# COMMAND ----------

# ============================================================
# SECTION 7: INPUT VALIDATION
# ============================================================

def validate_inputs(source_type, data_format, additional_gb, load_type,
                    ingestion_frequency, primary_key_available,
                    delete_handling, schema_stability, cdc_method):

    if source_type not in THROUGHPUT_GB_HR:
        raise ValueError(f"Invalid source type. Choose from: {list(THROUGHPUT_GB_HR.keys())}")
    if data_format not in VALID_FORMATS_BY_SOURCE.get(source_type, []):
        raise ValueError(f"Invalid format '{data_format}' for '{source_type}'. Valid: {VALID_FORMATS_BY_SOURCE[source_type]}")
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

# COMMAND ----------

# ============================================================
# SECTION 8: MAIN ESTIMATOR FUNCTION
# ============================================================

def estimate_cost(
    business_unit, request_date, requestor, business_justification,
    primary_key_available, delete_handling, schema_stability, cdc_method,
    source_type, data_format, additional_gb, load_type, ingestion_frequency
):
    validate_inputs(source_type, data_format, additional_gb, load_type,
                    ingestion_frequency, primary_key_available,
                    delete_handling, schema_stability, cdc_method)

    runs_per_month = FREQUENCY_MAP[ingestion_frequency]
    layers         = LAYER_CONFIG[source_type]["layers"]

    compute    = calculate_compute_cost(source_type, additional_gb, load_type, runs_per_month)
    storage    = calculate_storage_cost(source_type, data_format, additional_gb, runs_per_month)
    networking = calculate_networking_cost(source_type, data_format, additional_gb, runs_per_month)

    total_monthly_cost = round(
        compute["total_compute"] + storage["total_storage"] + networking["networking_cost"], 2
    )
    total_annual_cost = round(total_monthly_cost * 12, 2)

    compute_low,    compute_high    = apply_variance(compute["total_compute"])
    storage_low,    storage_high    = apply_variance(storage["total_storage"])
    networking_low, networking_high = apply_variance(networking["networking_cost"])
    total_low,      total_high      = apply_variance(total_monthly_cost)
    annual_low,     annual_high     = apply_variance(total_annual_cost)

    return {
        "business_unit":          business_unit,
        "request_date":           request_date,
        "requestor":              requestor,
        "business_justification": business_justification,
        "primary_key_available":  primary_key_available,
        "delete_handling":        delete_handling,
        "schema_stability":       schema_stability,
        "cdc_method":             cdc_method,
        "source_type":            source_type,
        "data_format":            data_format,
        "additional_gb":          additional_gb,
        "load_type":              load_type,
        "ingestion_frequency":    ingestion_frequency,
        "runs_per_month":         runs_per_month,
        "layers":                 ", ".join(layers),
        "workers":                str(compute["workers"]),
        "effective_dbu_hr":       compute["effective_dbu_hr"],
        "runtime_hrs":            compute["runtime_hrs"],
        "ingestion_cost":         compute["ingestion_cost"],
        "transformation_cost":    compute["transformation_cost"],
        "compute_cost":           compute["total_compute"],
        "compute_low":            compute_low,
        "compute_high":           compute_high,
        "compression_ratio":      storage["compression_ratio"],
        "compressed_gb":          storage["compressed_gb"],
        "data_storage_cost":      storage["data_storage_cost"],
        "transaction_cost":       storage["transaction_cost"],
        "storage_cost":           storage["total_storage"],
        "storage_low":            storage_low,
        "storage_high":           storage_high,
        "network_multiplier":     networking["network_multiplier"],
        "networking_cost":        networking["networking_cost"],
        "networking_low":         networking_low,
        "networking_high":        networking_high,
        "total_monthly_cost":     total_monthly_cost,
        "total_low":              total_low,
        "total_high":             total_high,
        "total_annual_cost":      total_annual_cost,
        "annual_low":             annual_low,
        "annual_high":            annual_high,
    }

# COMMAND ----------

# ============================================================
# SECTION 9: READ WIDGET INPUTS
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
source_type            = dbutils.widgets.get("source_type")
data_format            = dbutils.widgets.get("data_format")
additional_gb          = float(dbutils.widgets.get("additional_gb"))
load_type              = dbutils.widgets.get("load_type")
ingestion_frequency    = dbutils.widgets.get("ingestion_frequency")
save_results           = dbutils.widgets.get("save_results").lower() == "true"

# COMMAND ----------

# ============================================================
# SECTION 10: RUN ESTIMATOR
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
    source_type            = source_type,
    data_format            = data_format,
    additional_gb          = additional_gb,
    load_type              = load_type,
    ingestion_frequency    = ingestion_frequency,
)

# COMMAND ----------

# ============================================================
# SECTION 11: DISPLAY RESULTS
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
print(f"\n  CALCULATION INPUTS")
print(f"  Source Type      : {result['source_type']}")
print(f"  Data Format      : {result['data_format']}")
print(f"  Compression Ratio: {result['compression_ratio']}")
print(f"  Additional Volume: {result['additional_gb']} GB")
print(f"  Load Type        : {result['load_type']}")
print(f"  Frequency        : {result['ingestion_frequency']}")
print(f"  Runs Per Month   : {result['runs_per_month']}")
print(f"  Layers           : {result['layers']}")
print(f"\n{'─' * 65}")
print(f"  COMPUTE")
print(f"  Workers          : {result['workers']}")
print(f"  Effective DBU/hr : {result['effective_dbu_hr']}")
print(f"  Ingestion Runtime: {result['runtime_hrs']} hrs")
print(f"  Ingestion Cost   : ${result['ingestion_cost']}")
print(f"  Transform Cost   : ${result['transformation_cost']}")
print(f"  Total Compute    : ${result['compute_cost']}  (${result['compute_low']} – ${result['compute_high']})")
print(f"\n{'─' * 65}")
print(f"  STORAGE")
print(f"  Compressed GB    : {result['compressed_gb']} GB")
print(f"  Data Storage     : ${result['data_storage_cost']}")
print(f"  Transactions     : ${result['transaction_cost']}")
print(f"  Total Storage    : ${result['storage_cost']}  (${result['storage_low']} – ${result['storage_high']})")
print(f"\n{'─' * 65}")
print(f"  NETWORKING")
print(f"  Network Mult.    : {result['network_multiplier']}x")
print(f"  Total Networking : ${result['networking_cost']}  (${result['networking_low']} – ${result['networking_high']})")
print(f"\n{'=' * 65}")
print(f"  MONTHLY TOTAL    : ${result['total_monthly_cost']}  (${result['total_low']} – ${result['total_high']})")
print(f"  ANNUAL TOTAL     : ${result['total_annual_cost']}  (${result['annual_low']} – ${result['annual_high']})")
print(f"{'=' * 65}")

# COMMAND ----------

# ============================================================
# SECTION 12: SAVE TO DELTA TABLE
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
    # Calculation inputs
    StructField("source_type",            StringType(),    True),
    StructField("data_format",            StringType(),    True),
    StructField("additional_gb",          DoubleType(),    True),
    StructField("load_type",              StringType(),    True),
    StructField("ingestion_frequency",    StringType(),    True),
    StructField("runs_per_month",         IntegerType(),   True),
    StructField("layers",                 StringType(),    True),
    # Compute
    StructField("workers",                StringType(),    True),
    StructField("effective_dbu_hr",       DoubleType(),    True),
    StructField("runtime_hrs",            DoubleType(),    True),
    StructField("ingestion_cost",         DoubleType(),    True),
    StructField("transformation_cost",    DoubleType(),    True),
    StructField("compute_cost",           DoubleType(),    True),
    StructField("compute_low",            DoubleType(),    True),
    StructField("compute_high",           DoubleType(),    True),
    # Storage
    StructField("compression_ratio",      DoubleType(),    True),
    StructField("compressed_gb",          DoubleType(),    True),
    StructField("data_storage_cost",      DoubleType(),    True),
    StructField("transaction_cost",       DoubleType(),    True),
    StructField("storage_cost",           DoubleType(),    True),
    StructField("storage_low",            DoubleType(),    True),
    StructField("storage_high",           DoubleType(),    True),
    # Networking
    StructField("network_multiplier",     DoubleType(),    True),
    StructField("networking_cost",        DoubleType(),    True),
    StructField("networking_low",         DoubleType(),    True),
    StructField("networking_high",        DoubleType(),    True),
    # Totals
    StructField("total_monthly_cost",     DoubleType(),    True),
    StructField("total_low",              DoubleType(),    True),
    StructField("total_high",             DoubleType(),    True),
    StructField("total_annual_cost",      DoubleType(),    True),
    StructField("annual_low",             DoubleType(),    True),
    StructField("annual_high",            DoubleType(),    True),
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
    result["source_type"],
    result["data_format"],
    float(result["additional_gb"]),
    result["load_type"],
    result["ingestion_frequency"],
    int(result["runs_per_month"]),
    result["layers"],
    result["workers"],
    float(result["effective_dbu_hr"]),
    float(result["runtime_hrs"]),
    float(result["ingestion_cost"]),
    float(result["transformation_cost"]),
    float(result["compute_cost"]),
    float(result["compute_low"]),
    float(result["compute_high"]),
    float(result["compression_ratio"]),
    float(result["compressed_gb"]),
    float(result["data_storage_cost"]),
    float(result["transaction_cost"]),
    float(result["storage_cost"]),
    float(result["storage_low"]),
    float(result["storage_high"]),
    float(result["network_multiplier"]),
    float(result["networking_cost"]),
    float(result["networking_low"]),
    float(result["networking_high"]),
    float(result["total_monthly_cost"]),
    float(result["total_low"]),
    float(result["total_high"]),
    float(result["total_annual_cost"]),
    float(result["annual_low"]),
    float(result["annual_high"]),
)]

if save_results:
    df = spark.createDataFrame(row, schema)
    df.write \
      .format("delta") \
      .mode("append") \
      .option("mergeSchema", "true") \
      .saveAsTable("workspace.default.edh_cost_estimations")
    print(f"Saved to workspace.default.edh_cost_estimations — request_id={request_id}")
else:
    print("save_results=false — skipping Delta table write.")

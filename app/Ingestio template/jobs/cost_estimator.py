# Databricks notebook source
# EDH Incremental Cost Estimator
# Triggered by the Ingestio app — reads inputs via widgets,
# runs full cost calculation, and writes results to Delta table.

# COMMAND ----------

dbutils.widgets.text("request_id",    "")
dbutils.widgets.text("source_type",   "SQL Server")
dbutils.widgets.text("additional_gb", "10")
dbutils.widgets.text("load_type",     "Bulk")

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

COMPRESSION_RATIO = {
    "SQL Server": 0.30,
    "Postgres":   0.30,
    "Sybase":     0.30,
    "S3":         1.00,
    "SFTP":       0.30
}

TRANSFORMATION_PROPORTIONS = {
    "Silver":  40 / 238,
    "Gold":    34 / 238,
    "MDM":     40 / 238,
    "RT_Mart": 16 / 238
}

CDC_RUNTIME_FACTOR = 0.30

FREQUENCY = {
    "SQL Server": 30,
    "Postgres":   30,
    "Sybase":     30,
    "S3":         30,
    "SFTP":       30
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
# SECTION 3: COMPUTE COST
# ============================================================

def calculate_compute_cost(source_type, additional_gb, load_type):
    throughput      = THROUGHPUT_GB_HR[source_type]
    runs_per_month  = FREQUENCY[source_type]
    effective_dbu   = get_effective_dbu_hr(source_type, additional_gb)
    layers          = LAYER_CONFIG[source_type]["layers"]

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
# SECTION 4: STORAGE COST
# ============================================================

def calculate_storage_cost(source_type, additional_gb, runs_per_month):
    compression      = COMPRESSION_RATIO[source_type]
    layer_multiplier = LAYER_CONFIG[source_type]["layer_multiplier"]
    num_layers       = len(LAYER_CONFIG[source_type]["layers"])
    transaction_mult = TRANSACTION_MULTIPLIER[num_layers]
    compressed_gb    = additional_gb * compression

    data_storage_cost = compressed_gb * layer_multiplier * STORAGE_PRICE_PER_GB
    transaction_cost  = compressed_gb * transaction_mult * runs_per_month
    total_storage     = data_storage_cost + transaction_cost

    return {
        "compressed_gb":      round(compressed_gb, 4),
        "data_storage_cost":  round(data_storage_cost, 4),
        "transaction_cost":   round(transaction_cost, 6),
        "total_storage":      round(total_storage, 4)
    }

# COMMAND ----------

# ============================================================
# SECTION 5: NETWORKING COST
# ============================================================

def calculate_networking_cost(source_type, additional_gb, runs_per_month):
    compression   = COMPRESSION_RATIO[source_type]
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
# SECTION 6: VARIANCE
# ============================================================

def apply_variance(value, variance=VARIANCE_FACTOR):
    return round(value * (1 - variance), 2), round(value * (1 + variance), 2)

# COMMAND ----------

# ============================================================
# SECTION 7: MAIN ESTIMATOR
# ============================================================

def estimate_cost(source_type, additional_gb, load_type):
    valid_sources = list(THROUGHPUT_GB_HR.keys())
    if source_type not in valid_sources:
        raise ValueError(f"Invalid source type. Choose from: {valid_sources}")
    if load_type not in ["Bulk", "CDC"]:
        raise ValueError("Invalid load type. Choose from: ['Bulk', 'CDC']")
    if additional_gb <= 0:
        raise ValueError("Additional GB must be greater than 0")

    runs_per_month = FREQUENCY[source_type]
    layers         = LAYER_CONFIG[source_type]["layers"]

    compute    = calculate_compute_cost(source_type, additional_gb, load_type)
    storage    = calculate_storage_cost(source_type, additional_gb, runs_per_month)
    networking = calculate_networking_cost(source_type, additional_gb, runs_per_month)

    total_monthly_cost = round(compute["total_compute"] + storage["total_storage"] + networking["networking_cost"], 2)
    total_annual_cost  = round(total_monthly_cost * 12, 2)

    compute_low,    compute_high    = apply_variance(compute["total_compute"])
    storage_low,    storage_high    = apply_variance(storage["total_storage"])
    networking_low, networking_high = apply_variance(networking["networking_cost"])
    total_low,      total_high      = apply_variance(total_monthly_cost)
    annual_low,     annual_high     = apply_variance(total_annual_cost)

    return {
        "source_type":          source_type,
        "additional_gb":        additional_gb,
        "load_type":            load_type,
        "layers":               ", ".join(layers),
        "runs_per_month":       runs_per_month,
        "workers":              str(compute["workers"]),
        "effective_dbu_hr":     compute["effective_dbu_hr"],
        "runtime_hrs":          compute["runtime_hrs"],
        "ingestion_cost":       compute["ingestion_cost"],
        "transformation_cost":  compute["transformation_cost"],
        "compute_cost":         compute["total_compute"],
        "compute_low":          compute_low,
        "compute_high":         compute_high,
        "compressed_gb":        storage["compressed_gb"],
        "data_storage_cost":    storage["data_storage_cost"],
        "transaction_cost":     storage["transaction_cost"],
        "storage_cost":         storage["total_storage"],
        "storage_low":          storage_low,
        "storage_high":         storage_high,
        "network_multiplier":   networking["network_multiplier"],
        "networking_cost":      networking["networking_cost"],
        "networking_low":       networking_low,
        "networking_high":      networking_high,
        "total_monthly_cost":   total_monthly_cost,
        "total_low":            total_low,
        "total_high":           total_high,
        "total_annual_cost":    total_annual_cost,
        "annual_low":           annual_low,
        "annual_high":          annual_high,
    }

# COMMAND ----------

# ============================================================
# SECTION 8: READ WIDGET INPUTS
# ============================================================

request_id    = dbutils.widgets.get("request_id")
source_type   = dbutils.widgets.get("source_type")
additional_gb = float(dbutils.widgets.get("additional_gb"))
load_type     = dbutils.widgets.get("load_type")

# COMMAND ----------

# ============================================================
# SECTION 9: RUN ESTIMATOR
# ============================================================

result = estimate_cost(source_type, additional_gb, load_type)

# COMMAND ----------

# ============================================================
# SECTION 10: DISPLAY RESULTS (useful when run interactively)
# ============================================================

print("=" * 65)
print("       EDH INCREMENTAL COST ESTIMATOR")
print("=" * 65)
print(f"\n  Source Type       : {result['source_type']}")
print(f"  Additional Volume : {result['additional_gb']} GB")
print(f"  Load Type         : {result['load_type']}")
print(f"  Layers            : {result['layers']}")
print(f"  Runs Per Month    : {result['runs_per_month']}")
print(f"\n  COMPUTE")
print(f"  Workers           : {result['workers']}")
print(f"  Effective DBU/hr  : {result['effective_dbu_hr']}")
print(f"  Ingestion Runtime : {result['runtime_hrs']} hrs")
print(f"  Ingestion Cost    : ${result['ingestion_cost']}")
print(f"  Transform Cost    : ${result['transformation_cost']}")
print(f"  Total Compute     : ${result['compute_cost']}  (${result['compute_low']} – ${result['compute_high']})")
print(f"\n  STORAGE")
print(f"  Compressed GB     : {result['compressed_gb']} GB")
print(f"  Data Storage      : ${result['data_storage_cost']}")
print(f"  Transactions      : ${result['transaction_cost']}")
print(f"  Total Storage     : ${result['storage_cost']}  (${result['storage_low']} – ${result['storage_high']})")
print(f"\n  NETWORKING")
print(f"  Network Multiplier: {result['network_multiplier']}x")
print(f"  Total Networking  : ${result['networking_cost']}  (${result['networking_low']} – ${result['networking_high']})")
print(f"\n{'=' * 65}")
print(f"  MONTHLY TOTAL     : ${result['total_monthly_cost']}  (${result['total_low']} – ${result['total_high']})")
print(f"  ANNUAL TOTAL      : ${result['total_annual_cost']}  (${result['annual_low']} – ${result['annual_high']})")
print(f"{'=' * 65}")

# COMMAND ----------

# ============================================================
# SECTION 11: SAVE TO DELTA TABLE
# ============================================================

from pyspark.sql.types import (
    StructType, StructField,
    StringType, DoubleType, IntegerType, TimestampType
)
from datetime import datetime, timezone

schema = StructType([
    StructField("request_id",          StringType(),    True),
    StructField("estimation_timestamp",TimestampType(), True),
    StructField("source_type",         StringType(),    True),
    StructField("additional_gb",       DoubleType(),    True),
    StructField("load_type",           StringType(),    True),
    StructField("layers",              StringType(),    True),
    StructField("runs_per_month",      IntegerType(),   True),
    StructField("workers",             StringType(),    True),
    StructField("effective_dbu_hr",    DoubleType(),    True),
    StructField("runtime_hrs",         DoubleType(),    True),
    StructField("ingestion_cost",      DoubleType(),    True),
    StructField("transformation_cost", DoubleType(),    True),
    StructField("compute_cost",        DoubleType(),    True),
    StructField("compute_low",         DoubleType(),    True),
    StructField("compute_high",        DoubleType(),    True),
    StructField("compressed_gb",       DoubleType(),    True),
    StructField("data_storage_cost",   DoubleType(),    True),
    StructField("transaction_cost",    DoubleType(),    True),
    StructField("storage_cost",        DoubleType(),    True),
    StructField("storage_low",         DoubleType(),    True),
    StructField("storage_high",        DoubleType(),    True),
    StructField("network_multiplier",  DoubleType(),    True),
    StructField("networking_cost",     DoubleType(),    True),
    StructField("networking_low",      DoubleType(),    True),
    StructField("networking_high",     DoubleType(),    True),
    StructField("total_monthly_cost",  DoubleType(),    True),
    StructField("total_low",           DoubleType(),    True),
    StructField("total_high",          DoubleType(),    True),
    StructField("total_annual_cost",   DoubleType(),    True),
    StructField("annual_low",          DoubleType(),    True),
    StructField("annual_high",         DoubleType(),    True),
])

row = [(
    request_id,
    datetime.now(timezone.utc),
    result["source_type"],
    float(result["additional_gb"]),
    result["load_type"],
    result["layers"],
    int(result["runs_per_month"]),
    result["workers"],
    float(result["effective_dbu_hr"]),
    float(result["runtime_hrs"]),
    float(result["ingestion_cost"]),
    float(result["transformation_cost"]),
    float(result["compute_cost"]),
    float(result["compute_low"]),
    float(result["compute_high"]),
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

df = spark.createDataFrame(row, schema)

df.write \
  .format("delta") \
  .mode("append") \
  .option("mergeSchema", "true") \
  .saveAsTable("workspace.default.edh_cost_estimations")

print(f"Saved to workspace.default.edh_cost_estimations — request_id={request_id}")

# Databricks notebook source
# EDH Source System Cost & Effort Estimator
# Handles ingestion requests from known EDH source systems:
# Operational Database, File System, and API Endpoint System.
# Triggered via the dispatcher. Writes results to three Delta tables:
#   - edh_sourcesystem_requests  (raw request)
#   - edh_sourcesystem_estimations (detailed cost + effort breakdown)
#   - edh_combined_estimations  (dashboard summary, shared with other estimators)

# COMMAND ----------

# ============================================================
# SECTION 1: WIDGETS
# ============================================================

dbutils.widgets.text(    "request_id",             "")
dbutils.widgets.text(    "business_unit",           "")
dbutils.widgets.text(    "request_date",            "")
dbutils.widgets.text(    "requestor",               "")
dbutils.widgets.text(    "business_justification",  "")
dbutils.widgets.text(    "contains_phi",            "No")
dbutils.widgets.text(    "ingestion_method",        "Operational Database")
dbutils.widgets.text(    "source_system",           "")
dbutils.widgets.text(    "data_structure",          "")
dbutils.widgets.text(    "source_objects",          "")   # comma-separated list
dbutils.widgets.text(    "edh_table_names",         "")   # comma-separated list
dbutils.widgets.text(    "additional_gb",           "10")
dbutils.widgets.dropdown("ingestion_frequency",     "Daily",          ["Hourly", "Daily", "Weekly", "Monthly"])
dbutils.widgets.dropdown("load_type",               "Bulk",           ["Bulk", "Incremental"])
dbutils.widgets.dropdown("primary_key_available",   "Yes",            ["Yes", "No"])
dbutils.widgets.dropdown("delete_handling",         "Soft",           ["Hard", "Soft", "Ignore"])
dbutils.widgets.dropdown("schema_stability",        "Stable",         ["Stable", "Occasionally Changes", "Highly Dynamic"])
dbutils.widgets.dropdown("cdc_method",              "Not Applicable", ["Not Applicable", "Timestamp", "Log Based"])
dbutils.widgets.dropdown("save_results",            "true",           ["true", "false"])

# COMMAND ----------

# ============================================================
# SECTION 2: CATALOG / SCHEMA TARGET
# ============================================================

# Change CATALOG / SCHEMA when promoting to production.
CATALOG = "edh"
SCHEMA  = "ingestion"

# COMMAND ----------

# MAGIC %run ./EDH_Azure_Pricing_Utils

# COMMAND ----------

# ============================================================
# SECTION 3: ENGINEERING CONSTANTS
# ============================================================

# ── Compute ──────────────────────────────────────────────────────────────────

# Ingestion DBU rate per node per hour by ingestion method.
# Operational Database is highest due to JDBC connection overhead and
# row-level locking. API is high due to HTTP round-trips and retry logic.
INGESTION_DBU_BY_METHOD = {
    "Operational Database": 2.5,
    "File System":          1.5,
    "API Endpoint System":  2.0,
}

# Transformation DBU rate per node per hour (consistent across methods).
TRANSFORMATION_DBU_HR = 1.5

# Data throughput in GB per hour per worker node by data structure.
# Columnar formats (parquet) are fastest; JDBC and Excel formats are slowest.
THROUGHPUT_BY_STRUCTURE = {
    "Sql Server": 15.0,
    "Sybase":     10.0,   # Older protocol, lower throughput
    "Postgres":   18.0,
    "csv":        45.0,
    "parquet":    90.0,   # Already columnar and compressed
    "xlsb":        8.0,   # Excel binary parsing overhead
    "xls":         8.0,
    "API":         4.0,   # Rate-limited by API provider
}

# Typical number of worker nodes by ingestion method.
# These are non-integer because they represent averages across
# different cluster auto-scaling behaviours.
TYPICAL_WORKERS_BY_METHOD = {
    "Operational Database": 2.5,
    "File System":          1.8,
    "API Endpoint System":  1.2,
}

# Fetched live from Azure Retail Prices API; falls back to hardcoded if unavailable.
VM_RATE_PER_NODE_HR = fetch_vm_price("Standard_DS3_v2", fallback=0.38)
DBU_PRICE_HR        = 0.30   # Databricks Jobs Compute DBU price — not in Azure Retail API

# Each additional source object (table/file/endpoint) adds overhead:
# extra schema discovery, checkpoint management, and connection slots.
OBJECT_OVERHEAD_FACTOR = 0.15

# Fraction of total volume processed per run by load type.
# Incremental runs only process changed records (~15% of full volume on average).
LOAD_TYPE_FACTOR = {
    "Bulk":        1.0,
    "Incremental": 0.15,
}

# Runs per month by ingestion frequency.
RUNS_PER_MONTH = {
    "Hourly":  730,
    "Daily":    30,
    "Weekly":    4,
    "Monthly":   1,
}

# ── Storage ───────────────────────────────────────────────────────────────────

# Fetched live from Azure Retail Prices API; falls back to hardcoded if unavailable.
STORAGE_RATE_PER_GB     = fetch_adls_storage_price(fallback=0.023)
TRANSACTION_RATE_PER_GB = 0.004   # Delta transaction overhead — not in Azure Retail API

# Compression ratio: raw source GB → compressed Delta GB.
# Parquet is already compressed so ratio is lower.
COMPRESSION_RATIO_BY_STRUCTURE = {
    "Sql Server": 3.0,
    "Sybase":     2.8,
    "Postgres":   3.0,
    "csv":        2.5,
    "parquet":    1.5,
    "xlsb":       2.0,
    "xls":        2.5,
    "API":        2.0,
}

# ── Networking ────────────────────────────────────────────────────────────────

# Network cost per GB ingested per run by ingestion method.
# Operational Database: private link / ExpressRoute (contractual — not in Azure Retail API).
# File System: blended estimate for SFTP / internet file sources.
# API Endpoint System: full internet egress — fetched live from Azure Retail API.
_egress_rate = fetch_egress_price(fallback=0.087)
NETWORK_COST_PER_GB = {
    "Operational Database": 0.02,
    "File System":          0.05,
    "API Endpoint System":  _egress_rate,
}

# ── Effort ────────────────────────────────────────────────────────────────────

# Each factor contributes to an overall complexity score (higher = more complex).

INGESTION_METHOD_COMPLEXITY = {
    "Operational Database": 2.0,   # JDBC, connection pooling, locking concerns
    "File System":          1.0,   # Simpler file-based ingestion
    "API Endpoint System":  2.5,   # Auth, pagination, rate limits, variable schemas
}

DATA_STRUCTURE_COMPLEXITY = {
    "Sql Server": 1.5,
    "Sybase":     2.0,   # Legacy system, limited tooling
    "Postgres":   1.0,
    "csv":        0.5,
    "parquet":    0.5,
    "xlsb":       1.5,   # Excel binary format parsing is complex
    "xls":        1.5,
    "API":        1.5,
}

CDC_COMPLEXITY = {
    "Not Applicable": 0.0,
    "Timestamp":      1.0,
    "Log Based":      2.0,   # Requires log reader setup and maintenance
}

SCHEMA_STABILITY_COMPLEXITY = {
    "Stable":               0.0,
    "Occasionally Changes": 0.75,
    "Highly Dynamic":       1.5,   # Schema evolution logic needed
}

DELETE_COMPLEXITY = {
    "Ignore": 0.0,
    "Soft":   0.5,   # Flag column + filter logic
    "Hard":   1.0,   # Merge/delete logic required
}

LOAD_TYPE_COMPLEXITY = {
    "Bulk":        0.0,
    "Incremental": 1.0,   # Watermark / change detection logic
}

PRIMARY_KEY_COMPLEXITY = {
    "Yes": 0.0,
    "No":  0.5,   # No PK means deduplication logic needed
}

# Complexity thresholds for bucketing into Simple / Medium / Complex.
COMPLEXITY_THRESHOLD_SIMPLE  = 5.0
COMPLEXITY_THRESHOLD_MEDIUM  = 9.0

# Base effort days per phase for a single object at each complexity level.
EFFORT_BASE_DAYS = {
    "Simple": {
        "discovery":            0.5,
        "design":               0.5,
        "build_ingestion":      1.0,
        "build_transformation": 0.5,
        "testing":              0.5,
        "deployment":           0.5,
        "documentation":        0.5,
    },
    "Medium": {
        "discovery":            1.0,
        "design":               1.0,
        "build_ingestion":      2.0,
        "build_transformation": 1.0,
        "testing":              1.0,
        "deployment":           0.5,
        "documentation":        0.5,
    },
    "Complex": {
        "discovery":            1.5,
        "design":               1.5,
        "build_ingestion":      3.5,
        "build_transformation": 2.0,
        "testing":              2.0,
        "deployment":           1.0,
        "documentation":        1.0,
    },
}

# Each additional object adds this fraction of the base effort per phase.
# Diminishing returns: shared setup, connection, and deployment work is reused.
ADDITIONAL_OBJECT_EFFORT_FACTOR = 0.45

# ±10% variance applied to all cost and effort outputs.
VARIANCE_FACTOR = 0.10

# COMMAND ----------

# ============================================================
# SECTION 4: READ INPUTS
# ============================================================

request_id             = dbutils.widgets.get("request_id")
business_unit          = dbutils.widgets.get("business_unit")
request_date           = dbutils.widgets.get("request_date")
requestor              = dbutils.widgets.get("requestor")
business_justification = dbutils.widgets.get("business_justification")
contains_phi           = dbutils.widgets.get("contains_phi")
ingestion_method       = dbutils.widgets.get("ingestion_method")
source_system          = dbutils.widgets.get("source_system")
data_structure         = dbutils.widgets.get("data_structure")
source_objects_raw     = dbutils.widgets.get("source_objects")
edh_table_names_raw    = dbutils.widgets.get("edh_table_names")
additional_gb          = float(dbutils.widgets.get("additional_gb"))
ingestion_frequency    = dbutils.widgets.get("ingestion_frequency")
load_type              = dbutils.widgets.get("load_type")
primary_key_available  = dbutils.widgets.get("primary_key_available")
delete_handling        = dbutils.widgets.get("delete_handling")
schema_stability       = dbutils.widgets.get("schema_stability")
cdc_method             = dbutils.widgets.get("cdc_method")
save_results           = dbutils.widgets.get("save_results").lower() == "true"

source_objects_list  = [s.strip() for s in source_objects_raw.split(",")  if s.strip()]
edh_table_names_list = [s.strip() for s in edh_table_names_raw.split(",") if s.strip()]
n_objects = max(len(source_objects_list), 1)

print(f"request_id:         {request_id}")
print(f"ingestion_method:   {ingestion_method}")
print(f"source_system:      {source_system}")
print(f"data_structure:     {data_structure}")
print(f"n_objects:          {n_objects}  → {source_objects_list}")
print(f"additional_gb:      {additional_gb}")
print(f"ingestion_frequency:{ingestion_frequency}")
print(f"load_type:          {load_type}")
print(f"cdc_method:         {cdc_method}")

# COMMAND ----------

# ============================================================
# SECTION 5: COMPUTE COST
# ============================================================

runs_per_month   = RUNS_PER_MONTH[ingestion_frequency]
load_factor      = LOAD_TYPE_FACTOR[load_type]
typical_workers  = TYPICAL_WORKERS_BY_METHOD[ingestion_method]
ingestion_nodes  = int(1 + typical_workers)
throughput       = THROUGHPUT_BY_STRUCTURE.get(data_structure, 15.0)
ingestion_dbu_hr = INGESTION_DBU_BY_METHOD[ingestion_method]

# Effective volume processed per run (bulk = full, incremental = fraction)
effective_gb_per_run = additional_gb * load_factor

# Hours per run based on data volume and cluster throughput
runtime_hrs = effective_gb_per_run / (throughput * typical_workers)

# Additional objects multiply compute: first object is base,
# each extra object adds OBJECT_OVERHEAD_FACTOR of that base.
object_multiplier = 1.0 + (n_objects - 1) * OBJECT_OVERHEAD_FACTOR

total_dbu_cost      = (ingestion_dbu_hr + TRANSFORMATION_DBU_HR) * ingestion_nodes * runtime_hrs * runs_per_month * DBU_PRICE_HR * object_multiplier
total_vm_cost       = VM_RATE_PER_NODE_HR * ingestion_nodes * runtime_hrs * runs_per_month * object_multiplier
ingestion_cost      = ingestion_dbu_hr     * ingestion_nodes * runtime_hrs * runs_per_month * DBU_PRICE_HR * object_multiplier
transformation_cost = TRANSFORMATION_DBU_HR * ingestion_nodes * runtime_hrs * runs_per_month * DBU_PRICE_HR * object_multiplier
compute_cost        = total_dbu_cost + total_vm_cost

print(f"\n── Compute ──────────────────────────────")
print(f"  typical_workers:      {typical_workers}")
print(f"  ingestion_nodes:      {ingestion_nodes}")
print(f"  throughput (GB/hr):   {throughput}")
print(f"  effective_gb/run:     {effective_gb_per_run:.4f}")
print(f"  runtime_hrs/run:      {runtime_hrs:.4f}")
print(f"  runs/month:           {runs_per_month}")
print(f"  object_multiplier:    {object_multiplier:.2f}")
print(f"  compute_cost/mo:      ${compute_cost:.4f}")

# COMMAND ----------

# ============================================================
# SECTION 6: STORAGE COST
# ============================================================

compression_ratio = COMPRESSION_RATIO_BY_STRUCTURE.get(data_structure, 2.5)
compressed_gb     = additional_gb / compression_ratio
data_storage_cost = compressed_gb * STORAGE_RATE_PER_GB
transaction_cost  = compressed_gb * TRANSACTION_RATE_PER_GB
storage_cost      = data_storage_cost + transaction_cost

print(f"\n── Storage ──────────────────────────────")
print(f"  compression_ratio:    {compression_ratio}")
print(f"  compressed_gb:        {compressed_gb:.4f}")
print(f"  data_storage_cost:    ${data_storage_cost:.4f}")
print(f"  transaction_cost:     ${transaction_cost:.4f}")
print(f"  storage_cost/mo:      ${storage_cost:.4f}")

# COMMAND ----------

# ============================================================
# SECTION 7: NETWORKING COST
# ============================================================

network_rate_per_gb = NETWORK_COST_PER_GB[ingestion_method]
networking_cost     = additional_gb * network_rate_per_gb * runs_per_month * load_factor

print(f"\n── Networking ───────────────────────────")
print(f"  network_rate/GB:      ${network_rate_per_gb}")
print(f"  networking_cost/mo:   ${networking_cost:.4f}")

# COMMAND ----------

# ============================================================
# SECTION 8: TOTALS & VARIANCE
# ============================================================

total_monthly_cost = compute_cost + storage_cost + networking_cost
total_annual_cost  = total_monthly_cost * 12

def apply_variance(value: float) -> tuple:
    return value * (1 - VARIANCE_FACTOR), value * (1 + VARIANCE_FACTOR)

compute_low,    compute_high    = apply_variance(compute_cost)
storage_low,    storage_high    = apply_variance(storage_cost)
networking_low, networking_high = apply_variance(networking_cost)
total_low,      total_high      = apply_variance(total_monthly_cost)
annual_low,     annual_high     = apply_variance(total_annual_cost)

print(f"\n── Totals ───────────────────────────────")
print(f"  total_monthly:        ${total_monthly_cost:.4f}")
print(f"  total_monthly range:  ${total_low:.4f} – ${total_high:.4f}")
print(f"  total_annual:         ${total_annual_cost:.4f}")

# COMMAND ----------

# ============================================================
# SECTION 9: EFFORT ESTIMATION
# ============================================================

complexity_score = (
    INGESTION_METHOD_COMPLEXITY.get(ingestion_method, 2.0)
    + DATA_STRUCTURE_COMPLEXITY.get(data_structure,   1.0)
    + CDC_COMPLEXITY.get(cdc_method,                  0.0)
    + SCHEMA_STABILITY_COMPLEXITY.get(schema_stability, 0.0)
    + DELETE_COMPLEXITY.get(delete_handling,           0.0)
    + LOAD_TYPE_COMPLEXITY.get(load_type,              0.0)
    + PRIMARY_KEY_COMPLEXITY.get(primary_key_available, 0.0)
)

if complexity_score < COMPLEXITY_THRESHOLD_SIMPLE:
    complexity_level = "Simple"
elif complexity_score < COMPLEXITY_THRESHOLD_MEDIUM:
    complexity_level = "Medium"
else:
    complexity_level = "Complex"

base = EFFORT_BASE_DAYS[complexity_level]

# First object gets full base effort. Each extra object adds a fraction
# (diminishing returns: shared setup/deploy/docs work is reused).
object_effort_scale = 1.0 + (n_objects - 1) * ADDITIONAL_OBJECT_EFFORT_FACTOR

discovery_days            = base["discovery"]            * object_effort_scale
design_days               = base["design"]               * object_effort_scale
build_ingestion_days      = base["build_ingestion"]      * object_effort_scale
build_transformation_days = base["build_transformation"] * object_effort_scale
testing_days              = base["testing"]              * object_effort_scale
deployment_days           = base["deployment"]   # does not scale: one deploy per request
documentation_days        = base["documentation"]  # does not scale

total_effort_estimate = (
    discovery_days + design_days + build_ingestion_days +
    build_transformation_days + testing_days +
    deployment_days + documentation_days
)
total_effort_min = total_effort_estimate * (1 - VARIANCE_FACTOR)
total_effort_max = total_effort_estimate * (1 + VARIANCE_FACTOR)

print(f"\n── Effort ───────────────────────────────")
print(f"  complexity_score:     {complexity_score:.2f}")
print(f"  complexity_level:     {complexity_level}")
print(f"  n_objects:            {n_objects}")
print(f"  object_effort_scale:  {object_effort_scale:.2f}")
print(f"  total_effort (est):   {total_effort_estimate:.1f} days")
print(f"  range:                {total_effort_min:.1f} – {total_effort_max:.1f} days")

# COMMAND ----------

# ============================================================
# SECTION 10: SAVE RAW REQUEST
# ============================================================

from datetime import datetime, timezone
from pyspark.sql.types import (
    StructType, StructField, StringType, DoubleType, IntegerType, TimestampType,
)

request_schema = StructType([
    StructField("request_id",             StringType(),    True),
    StructField("submission_timestamp",   TimestampType(), True),
    StructField("business_unit",          StringType(),    True),
    StructField("request_date",           StringType(),    True),
    StructField("requestor",              StringType(),    True),
    StructField("business_justification", StringType(),    True),
    StructField("contains_phi",           StringType(),    True),
    StructField("ingestion_method",       StringType(),    True),
    StructField("source_system",          StringType(),    True),
    StructField("data_structure",         StringType(),    True),
    StructField("source_objects",         StringType(),    True),
    StructField("edh_table_names",        StringType(),    True),
    StructField("n_objects",              IntegerType(),   True),
    StructField("additional_gb",          DoubleType(),    True),
    StructField("ingestion_frequency",    StringType(),    True),
    StructField("load_type",              StringType(),    True),
    StructField("primary_key_available",  StringType(),    True),
    StructField("delete_handling",        StringType(),    True),
    StructField("schema_stability",       StringType(),    True),
    StructField("cdc_method",             StringType(),    True),
])

request_row = [(
    request_id,
    datetime.now(timezone.utc),
    business_unit,
    request_date,
    requestor,
    business_justification,
    contains_phi,
    ingestion_method,
    source_system,
    data_structure,
    source_objects_raw,
    edh_table_names_raw,
    n_objects,
    additional_gb,
    ingestion_frequency,
    load_type,
    primary_key_available,
    delete_handling,
    schema_stability,
    cdc_method,
)]

if save_results:
    df_req = spark.createDataFrame(request_row, request_schema)
    df_req.write \
        .format("delta") \
        .mode("append") \
        .option("mergeSchema", "true") \
        .saveAsTable(f"{CATALOG}.{SCHEMA}.edh_sourcesystem_requests")
    print(f"Saved to {CATALOG}.{SCHEMA}.edh_sourcesystem_requests — request_id={request_id}")

# COMMAND ----------

# ============================================================
# SECTION 11: SAVE DETAILED ESTIMATIONS
# ============================================================

estimation_schema = StructType([
    StructField("request_id",                 StringType(),    True),
    StructField("estimation_timestamp",       TimestampType(), True),
    StructField("ingestion_method",           StringType(),    True),
    StructField("source_system",              StringType(),    True),
    StructField("data_structure",             StringType(),    True),
    StructField("n_objects",                  IntegerType(),   True),
    StructField("additional_gb",              DoubleType(),    True),
    StructField("ingestion_frequency",        StringType(),    True),
    StructField("load_type",                  StringType(),    True),
    StructField("runs_per_month",             IntegerType(),   True),
    StructField("load_factor",                DoubleType(),    True),
    StructField("typical_workers",            DoubleType(),    True),
    StructField("ingestion_nodes",            IntegerType(),   True),
    StructField("throughput_gb_hr",           DoubleType(),    True),
    StructField("runtime_hrs",                DoubleType(),    True),
    StructField("object_multiplier",          DoubleType(),    True),
    StructField("ingestion_dbu_hr",           DoubleType(),    True),
    StructField("transformation_dbu_hr",      DoubleType(),    True),
    StructField("total_dbu_cost",             DoubleType(),    True),
    StructField("total_vm_cost",              DoubleType(),    True),
    StructField("ingestion_cost",             DoubleType(),    True),
    StructField("transformation_cost",        DoubleType(),    True),
    StructField("compute_cost",               DoubleType(),    True),
    StructField("compression_ratio",          DoubleType(),    True),
    StructField("compressed_gb",              DoubleType(),    True),
    StructField("data_storage_cost",          DoubleType(),    True),
    StructField("transaction_cost",           DoubleType(),    True),
    StructField("storage_cost",               DoubleType(),    True),
    StructField("network_rate_per_gb",        DoubleType(),    True),
    StructField("networking_cost",            DoubleType(),    True),
    StructField("total_monthly_cost",         DoubleType(),    True),
    StructField("total_annual_cost",          DoubleType(),    True),
    StructField("complexity_score",           DoubleType(),    True),
    StructField("complexity_level",           StringType(),    True),
    StructField("object_effort_scale",        DoubleType(),    True),
    StructField("discovery_days",             DoubleType(),    True),
    StructField("design_days",                DoubleType(),    True),
    StructField("build_ingestion_days",       DoubleType(),    True),
    StructField("build_transformation_days",  DoubleType(),    True),
    StructField("testing_days",               DoubleType(),    True),
    StructField("deployment_days",            DoubleType(),    True),
    StructField("documentation_days",         DoubleType(),    True),
    StructField("total_effort_days_min",      DoubleType(),    True),
    StructField("total_effort_days_estimate", DoubleType(),    True),
    StructField("total_effort_days_max",      DoubleType(),    True),
])

estimation_row = [(
    request_id,
    datetime.now(timezone.utc),
    ingestion_method,
    source_system,
    data_structure,
    n_objects,
    additional_gb,
    ingestion_frequency,
    load_type,
    runs_per_month,
    load_factor,
    typical_workers,
    ingestion_nodes,
    throughput,
    runtime_hrs,
    object_multiplier,
    ingestion_dbu_hr,
    TRANSFORMATION_DBU_HR,
    float(total_dbu_cost),
    float(total_vm_cost),
    float(ingestion_cost),
    float(transformation_cost),
    float(compute_cost),
    float(compression_ratio),
    float(compressed_gb),
    float(data_storage_cost),
    float(transaction_cost),
    float(storage_cost),
    float(network_rate_per_gb),
    float(networking_cost),
    float(total_monthly_cost),
    float(total_annual_cost),
    float(complexity_score),
    complexity_level,
    float(object_effort_scale),
    float(discovery_days),
    float(design_days),
    float(build_ingestion_days),
    float(build_transformation_days),
    float(testing_days),
    float(deployment_days),
    float(documentation_days),
    float(total_effort_min),
    float(total_effort_estimate),
    float(total_effort_max),
)]

if save_results:
    df_est = spark.createDataFrame(estimation_row, estimation_schema)
    df_est.write \
        .format("delta") \
        .mode("append") \
        .option("mergeSchema", "true") \
        .saveAsTable(f"{CATALOG}.{SCHEMA}.edh_sourcesystem_estimations")
    print(f"Saved to {CATALOG}.{SCHEMA}.edh_sourcesystem_estimations — request_id={request_id}")

# COMMAND ----------

# ============================================================
# SECTION 12: SAVE TO COMBINED TABLE (dashboard)
# ============================================================

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
    "Source System",
    business_unit,
    requestor,
    request_date,
    contains_phi,
    float(compute_cost),       float(compute_low),    float(compute_high),
    float(storage_cost),       float(storage_low),    float(storage_high),
    float(networking_cost),    float(networking_low), float(networking_high),
    float(total_monthly_cost), float(total_low),      float(total_high),
    float(total_annual_cost),  float(annual_low),     float(annual_high),
)]

if save_results:
    df_combined = spark.createDataFrame(combined_row, combined_schema)
    df_combined.write \
        .format("delta") \
        .mode("append") \
        .option("mergeSchema", "true") \
        .saveAsTable(f"{CATALOG}.{SCHEMA}.edh_combined_estimations")
    print(f"Saved to {CATALOG}.{SCHEMA}.edh_combined_estimations — request_id={request_id}")

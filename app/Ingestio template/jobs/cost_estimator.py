# Databricks notebook source

# COMMAND ----------

dbutils.widgets.text("request_id", "")
dbutils.widgets.text("data_volume_gb", "1.0")
dbutils.widgets.text("source_type", "")
dbutils.widgets.text("ingestion_mode", "")

# COMMAND ----------

request_id = dbutils.widgets.get("request_id")
data_volume_gb = float(dbutils.widgets.get("data_volume_gb"))
source_type = dbutils.widgets.get("source_type")
ingestion_mode = dbutils.widgets.get("ingestion_mode")

# Test logic: multiply volume by 5 to produce an estimated cost
estimated_cost_usd = round(data_volume_gb * 5, 2)
estimated_duration_days = max(1, int(data_volume_gb))

print(f"Inputs  : {data_volume_gb} GB | {source_type} | {ingestion_mode}")
print(f"Estimate: ${estimated_cost_usd} | {estimated_duration_days} day(s)")

# COMMAND ----------

from datetime import datetime, timezone
from pyspark.sql.types import (
    DoubleType, IntegerType, StringType, StructField, StructType, TimestampType
)

schema = StructType([
    StructField("request_id",             StringType(),    False),
    StructField("data_volume_gb",         DoubleType(),    False),
    StructField("source_type",            StringType(),    False),
    StructField("ingestion_mode",         StringType(),    False),
    StructField("estimated_cost_usd",     DoubleType(),    False),
    StructField("estimated_duration_days",IntegerType(),   False),
    StructField("submitted_at",           TimestampType(), False),
])

row = spark.createDataFrame(
    [(
        request_id,
        data_volume_gb,
        source_type,
        ingestion_mode,
        estimated_cost_usd,
        estimated_duration_days,
        datetime.now(timezone.utc),
    )],
    schema=schema,
)

row.write \
    .format("delta") \
    .mode("append") \
    .option("mergeSchema", "true") \
    .saveAsTable("workspace.default.cost_estimates")

print("Written to workspace.default.cost_estimates")

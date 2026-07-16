from datetime import datetime, timezone
from typing import Any

from databricks.connect import DatabricksSession
from pyspark.sql.types import (
    BooleanType, DoubleType, IntegerType, StringType, StructField, StructType, TimestampType,
)

from app_config import (
    ADMIN_USERS_TABLE,
    COMBINED_ESTIMATIONS_TABLE,
    NEW_SOURCE_ESTIMATIONS_TABLE,
    NEW_SOURCE_REQUESTS_TABLE,
    SOURCE_SYSTEM_ESTIMATIONS_TABLE,
    SOURCE_SYSTEM_REQUESTS_TABLE,
)
from estimators import new_source, schemas, source_system


def _spark() -> DatabricksSession:
    return DatabricksSession.builder.serverless(True).getOrCreate()


def _run_query(statement: str) -> list[list]:
    rows = _spark().sql(statement).collect()
    return [list(row) for row in rows]


def fetch_admin_emails() -> set[str]:
    """Empty set on an empty table or a failed lookup - callers should treat
    that the same as "no restriction", matching the old unset-ADMIN_USERS
    behaviour."""
    try:
        rows = _run_query(f"SELECT email FROM {ADMIN_USERS_TABLE}")
    except Exception:
        return set()
    return {row[0].strip().lower() for row in rows if row[0] and row[0].strip()}


# ── In-app estimation + Delta persistence ─────────────────────────────────────
# The cost/effort calculation used to run as a Databricks job; it now runs in
# the app via the estimators package, and we write the same Delta tables here so
# Request History keeps working.

_TYPE_MAP = {
    "str": StringType, "double": DoubleType, "int": IntegerType,
    "bool": BooleanType, "ts": TimestampType,
}


def _spark_schema(schema_meta) -> StructType:
    return StructType([StructField(name, _TYPE_MAP[code](), True) for name, code in schema_meta])


def _append(table: str, schema_meta, row: dict, ts_col: str, ts: datetime) -> None:
    values = tuple(ts if name == ts_col else row.get(name) for name, _ in schema_meta)
    df = _spark().createDataFrame([values], _spark_schema(schema_meta))
    df.write.format("delta").mode("append").option("mergeSchema", "true").saveAsTable(table)


def run_estimate(request_type: str, payload: dict) -> dict[str, Any]:
    """Compute the estimate in-app and persist request / estimation / combined
    rows to Delta. Returns the estimator result dict."""
    if request_type == "source_system":
        result = source_system.estimate(payload)
        request_table = SOURCE_SYSTEM_REQUESTS_TABLE
        estimation_table = SOURCE_SYSTEM_ESTIMATIONS_TABLE
        request_schema = schemas.SOURCE_SYSTEM_REQUEST
        estimation_schema = schemas.SOURCE_SYSTEM_ESTIMATION
    elif request_type == "new_source":
        result = new_source.estimate(payload)
        request_table = NEW_SOURCE_REQUESTS_TABLE
        estimation_table = NEW_SOURCE_ESTIMATIONS_TABLE
        request_schema = schemas.NEW_SOURCE_REQUEST
        estimation_schema = schemas.NEW_SOURCE_ESTIMATION
    else:
        raise ValueError(f"Unknown request_type '{request_type}'")

    now = datetime.now(timezone.utc)
    _append(request_table, request_schema, result["request"], schemas.REQUEST_TS, now)
    _append(estimation_table, estimation_schema, result["estimation"], schemas.ESTIMATION_TS, now)
    _append(COMBINED_ESTIMATIONS_TABLE, schemas.COMBINED, result["combined"], schemas.ESTIMATION_TS, now)
    return result


COMBINED_COLS = [
    "request_id",
    "estimation_timestamp",
    "ingestion_type",
    "business_unit",
    "requestor",
    "request_date",
    "contains_phi",
    "compute_cost_monthly",
    "compute_cost_low",
    "compute_cost_high",
    "storage_cost_monthly",
    "storage_cost_low",
    "storage_cost_high",
    "networking_cost_monthly",
    "networking_cost_low",
    "networking_cost_high",
    "total_cost_monthly",
    "total_cost_monthly_low",
    "total_cost_monthly_high",
    "total_cost_annual",
    "total_cost_annual_low",
    "total_cost_annual_high",
]

_SELECT = ", ".join(COMBINED_COLS)


def fetch_cost_estimate(request_id: str) -> dict[str, Any] | None:
    rows = _run_query(
        f"SELECT {_SELECT} FROM {COMBINED_ESTIMATIONS_TABLE} "
        f"WHERE request_id = '{request_id}' LIMIT 1"
    )
    return dict(zip(COMBINED_COLS, rows[0])) if rows else None


def fetch_all_estimates() -> list[dict[str, Any]]:
    rows = _run_query(
        f"SELECT {_SELECT} FROM {COMBINED_ESTIMATIONS_TABLE} "
        f"ORDER BY estimation_timestamp DESC"
    )
    return [dict(zip(COMBINED_COLS, row)) for row in rows]


NEW_SOURCE_DETAIL_COLS = [
    "request_id",
    "source_gb",
    "network_source_type",
    "copy_interval",
    "include_egress",
    "egress_gb",
    "sla_time_hr",
    "vm_type",
    "data_distribution",
    "delivery_pattern",
    "partition_key_availability",
    "complexity_source_type",
    "transformation_logic",
    "frequency",
    "delete_handling",
    "schema_stability",
    "cdc_method",
    "contains_phi",
]

NEW_SOURCE_EFFORT_COLS = [
    "request_id",
    "complexity_level",
    "total_effort_days_min",
    "total_effort_days_estimate",
    "total_effort_days_max",
]

SOURCE_SYSTEM_DETAIL_COLS = [
    "request_id",
    "ingestion_method",
    "source_system",
    "data_structure",
    "source_objects",
    "edh_table_names",
    "n_objects",
    "additional_gb",
    "sla_time_hr",
    "ingestion_frequency",
    "load_type",
    "primary_key_available",
    "delete_handling",
    "schema_stability",
    "cdc_method",
    "vm_type",
    "contains_phi",
]

SOURCE_SYSTEM_EFFORT_COLS = [
    "request_id",
    "complexity_level",
    "total_effort_days_min",
    "total_effort_days_estimate",
    "total_effort_days_max",
]


def fetch_all_request_details() -> tuple[dict[str, dict[str, Any]], list[str]]:
    """Returns (detail_map, errors). detail_map keyed by request_id."""
    result: dict[str, dict[str, Any]] = {}
    errors: list[str] = []

    try:
        sel  = ", ".join(NEW_SOURCE_DETAIL_COLS)
        rows = _run_query(f"SELECT {sel} FROM {NEW_SOURCE_REQUESTS_TABLE}")
        for row in rows:
            d = dict(zip(NEW_SOURCE_DETAIL_COLS, row))
            d["_source"] = "new_source"
            result[d["request_id"]] = d
    except Exception as exc:
        errors.append(f"Could not load new-source form details ({NEW_SOURCE_REQUESTS_TABLE}): {exc}")

    try:
        sel  = ", ".join(NEW_SOURCE_EFFORT_COLS)
        rows = _run_query(f"SELECT {sel} FROM {NEW_SOURCE_ESTIMATIONS_TABLE}")
        for row in rows:
            d   = dict(zip(NEW_SOURCE_EFFORT_COLS, row))
            rid = d["request_id"]
            if rid in result:
                result[rid]["effort_complexity_level"]    = d["complexity_level"]
                result[rid]["effort_total_days_min"]      = d["total_effort_days_min"]
                result[rid]["effort_total_days_estimate"] = d["total_effort_days_estimate"]
                result[rid]["effort_total_days_max"]      = d["total_effort_days_max"]
    except Exception as exc:
        errors.append(f"Could not load new-source effort data ({NEW_SOURCE_ESTIMATIONS_TABLE}): {exc}")

    try:
        sel  = ", ".join(SOURCE_SYSTEM_DETAIL_COLS)
        rows = _run_query(f"SELECT {sel} FROM {SOURCE_SYSTEM_REQUESTS_TABLE}")
        for row in rows:
            d = dict(zip(SOURCE_SYSTEM_DETAIL_COLS, row))
            d["_source"] = "source_system"
            result[d["request_id"]] = d
    except Exception as exc:
        errors.append(f"Could not load source-system form details ({SOURCE_SYSTEM_REQUESTS_TABLE}): {exc}")

    try:
        sel  = ", ".join(SOURCE_SYSTEM_EFFORT_COLS)
        rows = _run_query(f"SELECT {sel} FROM {SOURCE_SYSTEM_ESTIMATIONS_TABLE}")
        for row in rows:
            d   = dict(zip(SOURCE_SYSTEM_EFFORT_COLS, row))
            rid = d["request_id"]
            if rid in result:
                result[rid]["effort_complexity_level"]    = d["complexity_level"]
                result[rid]["effort_total_days_min"]      = d["total_effort_days_min"]
                result[rid]["effort_total_days_estimate"] = d["total_effort_days_estimate"]
                result[rid]["effort_total_days_max"]      = d["total_effort_days_max"]
    except Exception as exc:
        errors.append(f"Could not load source-system effort data ({SOURCE_SYSTEM_ESTIMATIONS_TABLE}): {exc}")

    return result, errors

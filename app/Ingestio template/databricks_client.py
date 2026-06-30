import json
import time
from typing import Any

from databricks.connect import DatabricksSession
from databricks.sdk import WorkspaceClient
from databricks.sdk.service.jobs import RunLifeCycleState, RunResultState

from app_config import (
    COMBINED_ESTIMATIONS_TABLE,
    COST_ESTIMATES_TABLE,
    ESTIMATOR_JOB_ID,
    NEW_SOURCE_ESTIMATIONS_TABLE,
    NEW_SOURCE_REQUESTS_TABLE,
)


def _client() -> WorkspaceClient:
    return WorkspaceClient()


def _spark() -> DatabricksSession:
    return DatabricksSession.builder.serverless(True).getOrCreate()


def _run_query(statement: str) -> list[list]:
    rows = _spark().sql(statement).collect()
    return [list(row) for row in rows]


def trigger_estimator_job(request_type: str, payload: dict) -> int:
    run = _client().jobs.run_now(
        job_id=ESTIMATOR_JOB_ID,
        job_parameters={
            "request_type": request_type,
            "payload":      json.dumps(payload),
        },
    )
    return run.run_id


def wait_for_run(run_id: int, timeout_seconds: int = 300) -> tuple[bool, str]:
    client   = _client()
    deadline = time.time() + timeout_seconds
    while time.time() < deadline:
        run   = client.jobs.get_run(run_id=run_id)
        state = run.state
        if state.life_cycle_state in (
            RunLifeCycleState.TERMINATED,
            RunLifeCycleState.SKIPPED,
            RunLifeCycleState.INTERNAL_ERROR,
        ):
            success = state.result_state == RunResultState.SUCCESS
            message = state.state_message or ("Completed" if success else "Job failed")
            return success, message
        time.sleep(3)
    return False, f"Timed out after {timeout_seconds}s"


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


EXISTING_SOURCE_DETAIL_COLS = [
    "request_id",
    "source_type",
    "data_format",
    "additional_gb",
    "load_type",
    "ingestion_frequency",
    "primary_key_available",
    "delete_handling",
    "schema_stability",
    "cdc_method",
    "contains_phi",
    "effort_complexity_level",
    "effort_total_days_min",
    "effort_total_days_estimate",
    "effort_total_days_max",
]

NEW_SOURCE_DETAIL_COLS = [
    "request_id",
    "pipeline_name",
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


def fetch_all_request_details() -> tuple[dict[str, dict[str, Any]], list[str]]:
    """Returns (detail_map, errors). detail_map keyed by request_id."""
    result: dict[str, dict[str, Any]] = {}
    errors: list[str] = []

    try:
        sel  = ", ".join(EXISTING_SOURCE_DETAIL_COLS)
        rows = _run_query(f"SELECT {sel} FROM {COST_ESTIMATES_TABLE}")
        for row in rows:
            d = dict(zip(EXISTING_SOURCE_DETAIL_COLS, row))
            d["_source"] = "existing"
            result[d["request_id"]] = d
    except Exception as exc:
        errors.append(f"Could not load existing-source form details ({COST_ESTIMATES_TABLE}): {exc}")

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

    return result, errors

import time
from typing import Any

from databricks.sdk import WorkspaceClient
from databricks.sdk.service.jobs import RunLifeCycleState, RunResultState
from databricks.sdk.service.sql import StatementState

import json

from app_config import (
    COMBINED_ESTIMATIONS_TABLE,
    COST_ESTIMATES_TABLE,
    ESTIMATOR_JOB_ID,
    NEW_SOURCE_ESTIMATIONS_TABLE,
    NEW_SOURCE_REQUESTS_TABLE,
)


def _client() -> WorkspaceClient:
    # Inside a Databricks App the SDK auto-authenticates via the environment.
    return WorkspaceClient()


def trigger_estimator_job(request_type: str, payload: dict) -> int:
    """
    Single entry point for both 'existing_source' and 'new_source' requests.
    Calls the dispatcher job which routes to the correct estimator notebook.
    payload must include a non-empty 'request_id' and all fields for that form.
    """
    run = _client().jobs.run_now(
        job_id=ESTIMATOR_JOB_ID,
        job_parameters={
            "request_type": request_type,
            "payload":      json.dumps(payload),
        },
    )
    return run.run_id


def wait_for_run(run_id: int, timeout_seconds: int = 300) -> tuple[bool, str]:
    """Poll until the run finishes. Returns (success, message)."""
    client = _client()
    deadline = time.time() + timeout_seconds
    while time.time() < deadline:
        run = client.jobs.get_run(run_id=run_id)
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


def _run_query(client: WorkspaceClient, statement: str) -> list[list]:
    response = client.statement_execution.execute_statement(
        warehouse_id=_get_warehouse_id(client),
        statement=statement,
        wait_timeout="50s",
    )
    state = response.status.state if response.status else None

    # If the warehouse was starting up the statement may still be running —
    # poll until it finishes (up to 70 more seconds, 120s total).
    if state in (StatementState.RUNNING, StatementState.PENDING):
        statement_id = response.statement_id
        for _ in range(14):
            time.sleep(5)
            response = client.statement_execution.get_statement(
                statement_id=statement_id
            )
            state = response.status.state if response.status else None
            if state not in (StatementState.RUNNING, StatementState.PENDING):
                break

    if state != StatementState.SUCCEEDED:
        error_detail = ""
        if response.status and response.status.error:
            error_detail = f" — {response.status.error.message}"
        raise RuntimeError(
            f"Query did not succeed (state={state})"
            f"{error_detail or '. The SQL warehouse may still be starting up — try refreshing.'}"
        )

    if response.result is None or response.result.data_array is None:
        return []
    return response.result.data_array


def fetch_cost_estimate(request_id: str) -> dict[str, Any] | None:
    client = _client()
    rows = _run_query(
        client,
        f"SELECT {_SELECT} FROM {COMBINED_ESTIMATIONS_TABLE} "
        f"WHERE request_id = '{request_id}' LIMIT 1",
    )
    return dict(zip(COMBINED_COLS, rows[0])) if rows else None


def fetch_all_estimates() -> list[dict[str, Any]]:
    client = _client()
    rows = _run_query(
        client,
        f"SELECT {_SELECT} FROM {COMBINED_ESTIMATIONS_TABLE} "
        f"ORDER BY estimation_timestamp DESC",
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
    """Returns (detail_map, errors). detail_map keyed by request_id; errors is a list of
    human-readable strings for any table that could not be queried."""
    client = _client()
    result: dict[str, dict[str, Any]] = {}
    errors: list[str] = []

    try:
        sel = ", ".join(EXISTING_SOURCE_DETAIL_COLS)
        rows = _run_query(client, f"SELECT {sel} FROM {COST_ESTIMATES_TABLE}")
        for row in rows:
            d = dict(zip(EXISTING_SOURCE_DETAIL_COLS, row))
            d["_source"] = "existing"
            result[d["request_id"]] = d
    except Exception as exc:
        errors.append(f"Could not load existing-source form details ({COST_ESTIMATES_TABLE}): {exc}")

    try:
        sel = ", ".join(NEW_SOURCE_DETAIL_COLS)
        rows = _run_query(client, f"SELECT {sel} FROM {NEW_SOURCE_REQUESTS_TABLE}")
        for row in rows:
            d = dict(zip(NEW_SOURCE_DETAIL_COLS, row))
            d["_source"] = "new_source"
            result[d["request_id"]] = d
    except Exception as exc:
        errors.append(f"Could not load new-source form details ({NEW_SOURCE_REQUESTS_TABLE}): {exc}")

    # Merge effort data for new-source requests (stored in a separate table with
    # different column names — normalise to the same keys used for existing source).
    try:
        sel = ", ".join(NEW_SOURCE_EFFORT_COLS)
        rows = _run_query(client, f"SELECT {sel} FROM {NEW_SOURCE_ESTIMATIONS_TABLE}")
        for row in rows:
            d = dict(zip(NEW_SOURCE_EFFORT_COLS, row))
            rid = d["request_id"]
            if rid in result:
                result[rid]["effort_complexity_level"]    = d["complexity_level"]
                result[rid]["effort_total_days_min"]      = d["total_effort_days_min"]
                result[rid]["effort_total_days_estimate"] = d["total_effort_days_estimate"]
                result[rid]["effort_total_days_max"]      = d["total_effort_days_max"]
    except Exception as exc:
        errors.append(f"Could not load new-source effort data ({NEW_SOURCE_ESTIMATIONS_TABLE}): {exc}")

    return result, errors


def _get_warehouse_id(client: WorkspaceClient) -> str:
    warehouses = list(client.warehouses.list())
    if not warehouses:
        raise RuntimeError("No SQL warehouse found in the workspace.")
    return warehouses[0].id

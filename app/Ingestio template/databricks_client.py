import time
from typing import Any

from databricks.sdk import WorkspaceClient
from databricks.sdk.service.jobs import RunLifeCycleState, RunResultState
from databricks.sdk.service.sql import StatementState

from app_config import COST_ESTIMATOR_JOB_ID, COST_ESTIMATES_TABLE


def _client() -> WorkspaceClient:
    # Inside a Databricks App the SDK auto-authenticates via the environment.
    return WorkspaceClient()


def trigger_cost_estimate_job(
    request_id: str,
    business_unit: str,
    request_date: str,
    requestor: str,
    business_justification: str,
    primary_key_available: str,
    delete_handling: str,
    schema_stability: str,
    cdc_method: str,
    source_type: str,
    data_format: str,
    additional_gb: float,
    load_type: str,
    ingestion_frequency: str,
) -> int:
    run = _client().jobs.run_now(
        job_id=COST_ESTIMATOR_JOB_ID,
        job_parameters={
            "request_id":             request_id,
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
            "additional_gb":          str(additional_gb),
            "load_type":              load_type,
            "ingestion_frequency":    ingestion_frequency,
            "save_results":           "true",
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


COLS = [
    "request_id",
    "estimation_timestamp",
    # Metadata
    "business_unit",
    "request_date",
    "requestor",
    "business_justification",
    "primary_key_available",
    "delete_handling",
    "schema_stability",
    "cdc_method",
    # Calculation inputs
    "source_type",
    "data_format",
    "additional_gb",
    "load_type",
    "ingestion_frequency",
    "layers",
    # Cost totals
    "compute_cost",
    "compute_low",
    "compute_high",
    "storage_cost",
    "storage_low",
    "storage_high",
    "networking_cost",
    "networking_low",
    "networking_high",
    "total_monthly_cost",
    "total_low",
    "total_high",
    "total_annual_cost",
    "annual_low",
    "annual_high",
]

_SELECT = ", ".join(COLS)


def _run_query(client: WorkspaceClient, statement: str) -> list[list]:
    response = client.statement_execution.execute_statement(
        warehouse_id=_get_warehouse_id(client),
        statement=statement,
        wait_timeout="30s",
    )
    if response.status is None or response.status.state != StatementState.SUCCEEDED:
        return []
    if response.result is None or response.result.data_array is None:
        return []
    return response.result.data_array


def fetch_cost_estimate(request_id: str) -> dict[str, Any] | None:
    client = _client()
    rows = _run_query(
        client,
        f"SELECT {_SELECT} FROM {COST_ESTIMATES_TABLE} "
        f"WHERE request_id = '{request_id}' LIMIT 1",
    )
    return dict(zip(COLS, rows[0])) if rows else None


def fetch_all_estimates() -> list[dict[str, Any]]:
    client = _client()
    rows = _run_query(
        client,
        f"SELECT {_SELECT} FROM {COST_ESTIMATES_TABLE} "
        f"ORDER BY estimation_timestamp DESC",
    )
    return [dict(zip(COLS, row)) for row in rows]


def _get_warehouse_id(client: WorkspaceClient) -> str:
    warehouses = list(client.warehouses.list())
    if not warehouses:
        raise RuntimeError("No SQL warehouse found in the workspace.")
    return warehouses[0].id

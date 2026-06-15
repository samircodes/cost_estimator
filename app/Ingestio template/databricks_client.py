import time
from typing import Any

from databricks.sdk import WorkspaceClient
from databricks.sdk.service.jobs import RunLifeCycleState, RunResultState

from app_config import COST_ESTIMATOR_JOB_ID, COST_ESTIMATES_TABLE


def _client() -> WorkspaceClient:
    # Inside a Databricks App the SDK auto-authenticates via the environment.
    return WorkspaceClient()


def trigger_cost_estimate_job(
    request_id: str,
    data_volume_gb: float,
    source_type: str,
    ingestion_mode: str,
) -> int:
    run = _client().jobs.run_now(
        job_id=COST_ESTIMATOR_JOB_ID,
        job_parameters={
            "request_id": request_id,
            "data_volume_gb": str(data_volume_gb),
            "source_type": source_type,
            "ingestion_mode": ingestion_mode,
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


def fetch_cost_estimate(request_id: str) -> dict[str, Any] | None:
    """Read the result row written by the job from the Delta table."""
    client = _client()
    rows = list(
        client.statement_execution.execute_statement(
            warehouse_id=_get_warehouse_id(client),
            statement=(
                f"SELECT * FROM {COST_ESTIMATES_TABLE} "
                f"WHERE request_id = '{request_id}' LIMIT 1"
            ),
            wait_timeout="30s",
        ).result.data_array
        or []
    )
    if not rows:
        return None
    # Column order must match the Delta table schema defined in cost_estimator.py
    cols = [
        "request_id",
        "data_volume_gb",
        "source_type",
        "ingestion_mode",
        "estimated_cost_usd",
        "estimated_duration_days",
        "submitted_at",
    ]
    return dict(zip(cols, rows[0]))


def fetch_all_estimates() -> list[dict[str, Any]]:
    """Return all rows from the cost estimates table for the history page."""
    client = _client()
    result = client.statement_execution.execute_statement(
        warehouse_id=_get_warehouse_id(client),
        statement=f"SELECT * FROM {COST_ESTIMATES_TABLE} ORDER BY submitted_at DESC",
        wait_timeout="30s",
    ).result
    cols = [
        "request_id",
        "data_volume_gb",
        "source_type",
        "ingestion_mode",
        "estimated_cost_usd",
        "estimated_duration_days",
        "submitted_at",
    ]
    rows = result.data_array or []
    return [dict(zip(cols, row)) for row in rows]


def _get_warehouse_id(client: WorkspaceClient) -> str:
    """Return the first running SQL warehouse, or raise if none found."""
    warehouses = list(client.warehouses.list())
    running = [w for w in warehouses if w.state and w.state.value == "RUNNING"]
    if not running:
        raise RuntimeError("No running SQL warehouse found in the workspace.")
    return running[0].id

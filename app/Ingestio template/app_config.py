import os
from dataclasses import dataclass


HOME_PAGE = "home"
NEW_INGESTION_PAGE = "new_ingestion"
EXISTING_SOURCE_PAGE = "existing_source"
REQUEST_HISTORY_PAGE = "request_history"

APP_TITLE = "Ryan | Ingestion Requests"

# Populate COST_ESTIMATOR_JOB_ID after running: databricks bundle deploy
COST_ESTIMATOR_JOB_ID: int = int(os.environ.get("COST_ESTIMATOR_JOB_ID", "571312722093562"))
COST_ESTIMATES_TABLE = os.environ.get("COST_ESTIMATES_TABLE", "workspace.default.cost_estimates")

SOURCE_TYPES = (
    "Amazon S3",
    "SFTP",
    "SQL (Postgres)",
    "SQL (SQL Server)",
    "Sybase",
)

INGESTION_MODES = ("CDC", "Bulk")


@dataclass(frozen=True)
class RequestType:
    number: str
    category: str
    title: str
    description: str
    button_label: str
    page: str
    primary: bool = False


REQUEST_TYPES = (
    RequestType(
        number="01",
        category="New source",
        title="New Ingestion Request",
        description=(
            "Bring a new data source into EDH and define how it should be "
            "delivered, processed, and maintained."
        ),
        button_label="Start a new request",
        page=NEW_INGESTION_PAGE,
        primary=True,
    ),
    RequestType(
        number="02",
        category="Existing source",
        title="Add Data to Existing EDH Sources",
        description=(
            "Extend an established source with new files, tables, fields, "
            "or delivery requirements."
        ),
        button_label="Choose an existing source",
        page=EXISTING_SOURCE_PAGE,
    ),
)

import os
from dataclasses import dataclass


HOME_PAGE = "home"
NEW_INGESTION_PAGE = "new_ingestion"
EXISTING_SOURCE_PAGE = "existing_source"
REQUEST_HISTORY_PAGE = "request_history"

APP_TITLE = "Ryan | Ingestion Requests"

# Populate COST_ESTIMATOR_JOB_ID after running: databricks bundle deploy
COST_ESTIMATOR_JOB_ID: int = int(os.environ.get("COST_ESTIMATOR_JOB_ID", "571312722093562"))
COST_ESTIMATES_TABLE = os.environ.get("COST_ESTIMATES_TABLE", "workspace.default.edh_cost_estimations")

SOURCE_TYPES = (
    "Amazon S3",
    "SFTP",
    "SQL (Postgres)",
    "SQL (SQL Server)",
    "Sybase",
)

# Maps the UI display names to the names expected by the cost estimator notebook
SOURCE_TYPE_MAP = {
    "Amazon S3":      "S3",
    "SFTP":           "SFTP",
    "SQL (Postgres)": "Postgres",
    "SQL (SQL Server)": "SQL Server",
    "Sybase":         "Sybase",
}

INGESTION_MODES = ("Bulk", "CDC")

DATA_FORMATS = ("CSV", "Parquet", "JDBC Tabular", "XLS", "XLSB", "JSON", "Avro", "ORC", "Other")

PRIMARY_KEY_OPTIONS      = ("Yes", "No")
DELETE_HANDLING_OPTIONS  = ("Hard", "Soft", "Ignore")
SCHEMA_STABILITY_OPTIONS = ("Stable", "Occasionally Changes", "Highly Dynamic")
CDC_METHOD_OPTIONS       = ("Not Applicable", "Timestamp", "Log Based")
INGESTION_FREQUENCIES    = ("Daily", "Weekly", "Monthly")

FILE_FORMATS = ("CSV", "JSON", "Parquet", "Delta", "Avro", "ORC", "XML")

FREQUENCIES = (
    "Every Hour",
    "Every 2 Hours",
    "Every 3 Hours",
    "Every 4 Hours",
    "Every 6 Hours",
    "Every 8 Hours",
    "Every 12 Hours",
    "Daily",
    "Weekly",
    "Monthly",
)

TRANSFORMATION_COMPLEXITIES = ("Low", "Medium", "High")

REGIONS = (
    "us-east-1",
    "us-east-2",
    "us-west-1",
    "us-west-2",
    "eu-west-1",
    "eu-central-1",
    "ap-southeast-1",
    "ap-northeast-1",
)

CLUSTER_TYPES = ("Job Cluster", "All Purpose", "Serverless")

VM_TYPES = (
    "General Purpose",
    "Memory Optimized",
    "Compute Optimized",
    "Storage Optimized",
)

NETWORK_CONNECTIONS = ("VPN", "Direct Connect", "Private Link", "Public Internet")


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

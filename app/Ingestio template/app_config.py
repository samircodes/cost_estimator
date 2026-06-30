import os
from dataclasses import dataclass


HOME_PAGE = "home"
NEW_INGESTION_PAGE = "new_ingestion"
EXISTING_SOURCE_PAGE = "existing_source"
REQUEST_HISTORY_PAGE = "request_history"

APP_TITLE = "Ryan | Ingestion Requests"

# ── Dispatcher job (handles both existing-source and new-source requests) ─────
# Populate after running: databricks bundle deploy
ESTIMATOR_JOB_ID: int = int(os.environ.get("ESTIMATOR_JOB_ID", "843013258339321"))

# ── Delta tables ──────────────────────────────────────────────────────────────
COST_ESTIMATES_TABLE         = os.environ.get("COST_ESTIMATES_TABLE",         "edh.ingestion.edh_cost_estimations")
NEW_SOURCE_REQUESTS_TABLE    = os.environ.get("NEW_SOURCE_REQUESTS_TABLE",    "edh.ingestion.edh_newsource_requests")
NEW_SOURCE_ESTIMATIONS_TABLE = os.environ.get("NEW_SOURCE_ESTIMATIONS_TABLE", "edh.ingestion.edh_newsource_estimations")
COMBINED_ESTIMATIONS_TABLE   = os.environ.get("COMBINED_ESTIMATIONS_TABLE",   "edh.ingestion.edh_combined_estimations")

# ── Existing-source form options ──────────────────────────────────────────────
SOURCE_TYPES = (
    "Amazon S3",
    "SFTP",
    "SQL (Postgres)",
    "SQL (SQL Server)",
    "Sybase",
)

# Maps the UI display names to the names expected by the cost estimator notebook
SOURCE_TYPE_MAP = {
    "Amazon S3":        "S3",
    "SFTP":             "SFTP",
    "SQL (Postgres)":   "Postgres",
    "SQL (SQL Server)": "SQL Server",
    "Sybase":           "Sybase",
}

INGESTION_MODES       = ("Bulk", "CDC")
DATA_FORMATS          = ("JDBC Tabular", "CSV", "XLS", "XLSB", "Parquet")
PRIMARY_KEY_OPTIONS   = ("Yes", "No")
DELETE_HANDLING_OPTIONS  = ("Hard", "Soft", "Ignore")
SCHEMA_STABILITY_OPTIONS = ("Stable", "Occasionally Changes", "Highly Dynamic")
CDC_METHOD_OPTIONS       = ("Not Applicable", "Timestamp", "Log Based")
INGESTION_FREQUENCIES    = ("Daily", "Weekly", "Monthly")

# ── New-source form options ───────────────────────────────────────────────────
NETWORK_SOURCE_TYPES = (
    "azure_same_region",
    "expressroute_metered",
    "expressroute_unlimited",
    "vpn",
    "aws_s3",
    "aws_rds",
    "gcp",
    "sftp",
    "api",
    "cross_region",
)

COPY_INTERVALS = ("bulk", "incremental")

VM_TYPES = ("Standard_DS3_v2", "Standard_DS5_v2")

DATA_DISTRIBUTIONS = (
    "Evenly distributed",
    "Some concentration in a few records",
    "Highly concentrated in a few records",
    "Not sure",
)

DELIVERY_PATTERNS = (
    "One large batch file/extract",
    "Many small files or frequent small batches",
    "Not sure",
)

PARTITION_KEY_AVAILABILITIES = (
    "Yes, a clear date/region/key field",
    "Somewhat",
    "No clear splitting field",
    "Not sure",
)

COMPLEXITY_SOURCE_TYPES = (
    "internal_sql",
    "internal_api",
    "azure_service",
    "external_sftp",
    "external_api",
    "aws_s3",
    "aws_rds",
    "gcp",
    "saas_connector",
    "legacy_mainframe",
    "multi_source",
)

VOLUME_TIERS = ("tiny", "small", "medium", "large", "very_large", "massive")

TRANSFORMATION_LOGICS = ("light", "medium", "heavy")

NEW_SOURCE_FREQUENCIES = (
    "adhoc",
    "weekly",
    "daily",
    "hourly",
    "near_real_time",
    "real_time",
)

DATA_QUALITY_RULES_OPTIONS = (
    "none",
    "basic_nulls",
    "standard_validation",
    "complex_cross_table",
    "regulatory_compliance",
    "full_reconciliation",
)

DEPENDENCIES_OPTIONS = (
    "standalone",
    "single_upstream",
    "few_dependencies",
    "moderate_dag",
    "complex_dag",
    "cross_team_multi_system",
)


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

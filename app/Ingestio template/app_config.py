import os
from dataclasses import dataclass


HOME_PAGE = "home"
NEW_INGESTION_PAGE = "new_ingestion"
EXISTING_SOURCE_PAGE = "existing_source"
SOURCE_SYSTEM_PAGE = "source_systems"
REQUEST_HISTORY_PAGE = "request_history"

APP_TITLE = "Ryan | Ingestion Requests"

# ── Dispatcher job (handles both existing-source and new-source requests) ─────
# Populate after running: databricks bundle deploy
ESTIMATOR_JOB_ID: int = int(os.environ["ESTIMATOR_JOB_ID"])

# ── Admin access (comma-separated emails allowed to view the dashboard) ───────
# e.g. ADMIN_USERS="alice@company.com,bob@company.com"
ADMIN_USERS: set[str] = {
    email.strip().lower()
    for email in os.environ.get("ADMIN_USERS", "").split(",")
    if email.strip()
}

# ── Delta tables ──────────────────────────────────────────────────────────────
NEW_SOURCE_REQUESTS_TABLE         = os.environ["NEW_SOURCE_REQUESTS"]
NEW_SOURCE_ESTIMATIONS_TABLE      = os.environ["NEW_SOURCE_ESTIMATIONS"]
SOURCE_SYSTEM_REQUESTS_TABLE      = os.environ["SOURCE_SYSTEM_REQUESTS"]
SOURCE_SYSTEM_ESTIMATIONS_TABLE   = os.environ["SOURCE_SYSTEM_ESTIMATIONS"]
COMBINED_ESTIMATIONS_TABLE        = os.environ["COMBINED_ESTIMATIONS"]

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


# ── EDH source system mapping (Ingestion Method → Source System → Data Structure) ─
INGESTION_SOURCE_MAP: dict[str, dict[str, str]] = {
    "Operational Database": {
        "IMS-RS":               "Sql Server",
        "VRU":                  "Sql Server",
        "DATALAYER":            "Sql Server",
        "AIM":                  "Sql Server",
        "SMITH":                "Sql Server",
        "AOP":                  "Sql Server",
        "C1-LOV":               "Sybase",
        "ACTUARIAL":            "Sql Server",
        "IMS-WKFC":             "Sql Server",
        "C1-RS":                "Sql Server",
        "ISA":                  "Sql Server",
        "TREASURY-HISTORICAL":  "Sql Server",
        "RYANRE-GRS-DM":        "Sql Server",
        "RYANRE-BITE":          "Sql Server",
        "CROUSE":               "Sql Server",
        "TMS":                  "Sql Server",
        "GRIFFIN":              "Sql Server",
        "CLAIMS":               "Sql Server",
        "LOCATION-SERVICES":    "Sql Server",
        "IMS-RTWPB":            "Sql Server",
        "IIM":                  "Sql Server",
        "POLCHAR":              "Sql Server",
        "RYANRE":               "Sql Server",
        "Connector":            "Postgres",
        "AIR-WKFC":             "Sql Server",
    },
    "File System": {
        "360 UW":                       "csv",
        "AIM":                          "csv",
        "Alis":                         "csv",
        "amazon_s3":                    "parquet",
        "Castel":                       "csv",
        "Ethos":                        "csv",
        "EverSports":                   "csv",
        "FXLoader":                     "csv",
        "Geo":                          "csv",
        "credit_ratings":               "csv",
        "Innovisk":                     "csv",
        "JMWilson":                     "csv",
        "SICS":                         "csv",
        "PricingModels":                "xlsb",
        "IMS_RS":                       "csv",
        "ManualDataFeeds":              "csv",
        "RSUM Manual Adjustment load":  "xls",
        "SSRU":                         "csv",
        "Trinity":                      "csv",
        "USAssure":                     "csv",
        "Velocity":                     "csv",
    },
    "API Endpoint System": {
        "KYRIBA":        "API",
        "FXLOADER-DAILY": "API",
    },
}


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
        category="Source systems",
        title="Add Data from Source Systems",
        description=(
            "Request ingestion from a known EDH source system — select your "
            "ingestion method, source system, and the objects you need."
        ),
        button_label="Select a source system",
        page=SOURCE_SYSTEM_PAGE,
    ),
)

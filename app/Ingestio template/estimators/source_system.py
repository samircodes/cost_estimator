"""Source System cost & effort estimator (pure Python).

Port of notebooks/EDH_Source_System_Estimator_Job.py. `estimate(payload)`
takes the same string-valued payload the app used to send to the Databricks
job and returns the rows for the request / estimation / combined Delta tables
(without timestamps — the caller adds those). Prices can be injected for
deterministic offline tests; otherwise they are fetched live (cached) with
hardcoded fallbacks.
"""

import math

from estimators import pricing

# ── Compute ───────────────────────────────────────────────────────────────────
INGESTION_DBU_BY_METHOD = {
    "Operational Database": 2.5,
    "File System":          1.5,
    "API Endpoint System":  2.0,
}
TRANSFORMATION_DBU_HR = 1.5
THROUGHPUT_BY_STRUCTURE = {
    "Sql Server": 15.0, "Sybase": 10.0, "Postgres": 18.0, "csv": 45.0,
    "parquet": 90.0, "xlsb": 8.0, "xls": 8.0, "API": 4.0,
}
DRIVER_NODES = 1
DBU_PRICE_HR = 0.30
OBJECT_OVERHEAD_FACTOR = 0.15
LOAD_TYPE_FACTOR = {"Bulk": 1.0, "Incremental": 0.15}
RUNS_PER_MONTH = {"Hourly": 730, "Daily": 30, "Weekly": 4, "Monthly": 1}

# Cluster sizing (see the validation notes in the notebook for the rationale).
CLUSTER_STARTUP_HR = 0.1
TARGET_GB_PER_WORKER = 50.0
MAX_WORKERS = 20
DEFAULT_VM_TYPE = "Standard_DS3_v2"

# VM throughput multiplier relative to DS3 (rate_hr resolved from live pricing).
VM_THROUGHPUT_MULTIPLIER = {"Standard_DS3_v2": 1.0, "Standard_DS5_v2": 4.0}
VM_FALLBACK_RATE = {"Standard_DS3_v2": 0.293, "Standard_DS5_v2": 1.17}

# ── Storage ───────────────────────────────────────────────────────────────────
STORAGE_FALLBACK_RATE = 0.023
TRANSACTION_RATE_PER_GB = 0.004
COMPRESSION_RATIO_BY_STRUCTURE = {
    "Sql Server": 3.0, "Sybase": 2.8, "Postgres": 3.0, "csv": 2.5,
    "parquet": 1.5, "xlsb": 2.0, "xls": 2.5, "API": 2.0,
}

# ── Networking ────────────────────────────────────────────────────────────────
EGRESS_FALLBACK_RATE = 0.087

# ── Effort ────────────────────────────────────────────────────────────────────
INGESTION_METHOD_COMPLEXITY = {
    "Operational Database": 2.0, "File System": 1.0, "API Endpoint System": 2.5,
}
DATA_STRUCTURE_COMPLEXITY = {
    "Sql Server": 1.5, "Sybase": 2.0, "Postgres": 1.0, "csv": 0.5,
    "parquet": 0.5, "xlsb": 1.5, "xls": 1.5, "API": 1.5,
}
CDC_COMPLEXITY = {"Not Applicable": 0.0, "Timestamp": 1.0, "Log Based": 2.0, "Not sure": 1.0}
SCHEMA_STABILITY_COMPLEXITY = {"Stable": 0.0, "Occasionally Changes": 0.75, "Highly Dynamic": 1.5}
DELETE_COMPLEXITY = {"Ignore": 0.0, "Soft": 0.5, "Hard": 1.0, "Not sure": 0.5}
LOAD_TYPE_COMPLEXITY = {"Bulk": 0.0, "Incremental": 1.0}
PRIMARY_KEY_COMPLEXITY = {"Yes": 0.0, "No": 0.5, "Not sure": 0.25}

COMPLEXITY_THRESHOLD_SIMPLE = 5.0
COMPLEXITY_THRESHOLD_MEDIUM = 9.0
EFFORT_BASE_DAYS = {
    "Simple": {"discovery": 0.5, "design": 0.5, "build_ingestion": 1.0,
               "build_transformation": 0.5, "testing": 0.5, "deployment": 0.5, "documentation": 0.5},
    "Medium": {"discovery": 1.0, "design": 1.0, "build_ingestion": 2.0,
               "build_transformation": 1.0, "testing": 1.0, "deployment": 0.5, "documentation": 0.5},
    "Complex": {"discovery": 1.5, "design": 1.5, "build_ingestion": 3.5,
                "build_transformation": 2.0, "testing": 2.0, "deployment": 1.0, "documentation": 1.0},
}
ADDITIONAL_OBJECT_EFFORT_FACTOR = 0.45
VARIANCE_FACTOR = 0.10

VALID_VM_TYPES = tuple(VM_THROUGHPUT_MULTIPLIER.keys())


def _resolve_prices(prices):
    if prices is not None:
        return prices
    return {
        "Standard_DS3_v2": pricing.fetch_vm_price("Standard_DS3_v2", VM_FALLBACK_RATE["Standard_DS3_v2"]),
        "Standard_DS5_v2": pricing.fetch_vm_price("Standard_DS5_v2", VM_FALLBACK_RATE["Standard_DS5_v2"]),
        "storage_per_gb":  pricing.fetch_adls_storage_price(fallback=STORAGE_FALLBACK_RATE),
        "egress_per_gb":   pricing.fetch_egress_price(fallback=EGRESS_FALLBACK_RATE),
    }


def estimate(payload, prices=None):
    """Compute the Source System estimate. Returns
    {"request_type", "request", "estimation", "combined"} with plain-typed
    dict rows (no timestamps)."""
    p = _resolve_prices(prices)

    # ── Inputs ────────────────────────────────────────────────────────────────
    request_id             = payload["request_id"]
    business_unit          = payload.get("business_unit", "")
    request_date           = payload.get("request_date", "")
    requestor              = payload.get("requestor", "")
    business_justification = payload.get("business_justification", "")
    contains_phi           = payload.get("contains_phi", "No")
    ingestion_method       = payload["ingestion_method"]
    source_system          = payload.get("source_system", "")
    data_structure         = payload.get("data_structure", "")
    source_objects_raw     = payload.get("source_objects", "")
    edh_table_names_raw    = payload.get("edh_table_names", "")
    additional_gb          = float(payload["additional_gb"])
    sla_raw                = payload.get("sla_time_hr", "Not sure")
    ingestion_frequency    = payload["ingestion_frequency"]
    load_type              = payload["load_type"]
    primary_key_available  = payload.get("primary_key_available", "Not sure")
    delete_handling        = payload.get("delete_handling", "Not sure")
    schema_stability       = payload.get("schema_stability", "Stable")
    cdc_method             = payload.get("cdc_method", "Not Applicable")
    vm_type                = payload.get("vm_type", "Not sure")

    # ── Resolve "Not sure" ─────────────────────────────────────────────────────
    vm_type_effective = DEFAULT_VM_TYPE if vm_type == "Not sure" else vm_type
    if vm_type_effective not in VM_THROUGHPUT_MULTIPLIER:
        raise ValueError(f"Invalid vm_type. Choose from: {list(VALID_VM_TYPES) + ['Not sure']}")

    sla_specified = str(sla_raw).strip().lower() != "not sure"
    sla_time_hr = float(sla_raw) if sla_specified else None
    if sla_specified and sla_time_hr <= 0:
        raise ValueError("sla_time_hr must be greater than 0")

    source_objects_list = [s.strip() for s in source_objects_raw.split(",") if s.strip()]
    n_objects = max(len(source_objects_list), 1)

    # ── Compute cost ───────────────────────────────────────────────────────────
    runs_per_month   = RUNS_PER_MONTH[ingestion_frequency]
    load_factor      = LOAD_TYPE_FACTOR[load_type]
    base_throughput  = THROUGHPUT_BY_STRUCTURE.get(data_structure, 15.0)
    ingestion_dbu_hr = INGESTION_DBU_BY_METHOD[ingestion_method]

    vm_rate_per_node_hr      = p[vm_type_effective]
    vm_throughput_multiplier = VM_THROUGHPUT_MULTIPLIER[vm_type_effective]
    throughput = base_throughput * vm_throughput_multiplier

    effective_gb_per_run = additional_gb * load_factor

    if sla_specified:
        usable_hr = sla_time_hr - CLUSTER_STARTUP_HR
        if usable_hr <= 0:
            worker_nodes_estimated = MAX_WORKERS
        else:
            worker_nodes_raw = effective_gb_per_run / (usable_hr * throughput)
            worker_nodes_estimated = max(1, min(MAX_WORKERS, math.ceil(worker_nodes_raw)))
    else:
        worker_nodes_estimated = max(
            1, min(MAX_WORKERS, math.ceil(effective_gb_per_run / TARGET_GB_PER_WORKER))
        )

    ingestion_nodes = worker_nodes_estimated + DRIVER_NODES
    runtime_hrs = CLUSTER_STARTUP_HR + effective_gb_per_run / (worker_nodes_estimated * throughput)
    meets_sla = (runtime_hrs <= sla_time_hr) if sla_specified else None

    object_multiplier = 1.0 + (n_objects - 1) * OBJECT_OVERHEAD_FACTOR

    total_dbu_cost      = (ingestion_dbu_hr + TRANSFORMATION_DBU_HR) * ingestion_nodes * runtime_hrs * runs_per_month * DBU_PRICE_HR * object_multiplier
    total_vm_cost       = vm_rate_per_node_hr * ingestion_nodes * runtime_hrs * runs_per_month * object_multiplier
    ingestion_cost      = ingestion_dbu_hr * ingestion_nodes * runtime_hrs * runs_per_month * DBU_PRICE_HR * object_multiplier
    transformation_cost = TRANSFORMATION_DBU_HR * ingestion_nodes * runtime_hrs * runs_per_month * DBU_PRICE_HR * object_multiplier
    compute_cost        = total_dbu_cost + total_vm_cost

    # ── Storage cost ───────────────────────────────────────────────────────────
    compression_ratio = COMPRESSION_RATIO_BY_STRUCTURE.get(data_structure, 2.5)
    compressed_gb     = additional_gb / compression_ratio
    data_storage_cost = compressed_gb * p["storage_per_gb"]
    transaction_cost  = compressed_gb * TRANSACTION_RATE_PER_GB
    storage_cost      = data_storage_cost + transaction_cost

    # ── Networking cost ────────────────────────────────────────────────────────
    network_cost_per_gb = {
        "Operational Database": 0.02,
        "File System":          0.05,
        "API Endpoint System":  p["egress_per_gb"],
    }
    network_rate_per_gb = network_cost_per_gb[ingestion_method]
    networking_cost     = additional_gb * network_rate_per_gb * runs_per_month * load_factor

    # ── Totals & variance ──────────────────────────────────────────────────────
    total_monthly_cost = compute_cost + storage_cost + networking_cost
    total_annual_cost  = total_monthly_cost * 12

    def variance(v):
        return v * (1 - VARIANCE_FACTOR), v * (1 + VARIANCE_FACTOR)

    compute_low, compute_high       = variance(compute_cost)
    storage_low, storage_high       = variance(storage_cost)
    networking_low, networking_high = variance(networking_cost)
    total_low, total_high           = variance(total_monthly_cost)
    annual_low, annual_high         = variance(total_annual_cost)

    # ── Effort ─────────────────────────────────────────────────────────────────
    complexity_score = (
        INGESTION_METHOD_COMPLEXITY.get(ingestion_method, 2.0)
        + DATA_STRUCTURE_COMPLEXITY.get(data_structure, 1.0)
        + CDC_COMPLEXITY.get(cdc_method, 0.0)
        + SCHEMA_STABILITY_COMPLEXITY.get(schema_stability, 0.0)
        + DELETE_COMPLEXITY.get(delete_handling, 0.0)
        + LOAD_TYPE_COMPLEXITY.get(load_type, 0.0)
        + PRIMARY_KEY_COMPLEXITY.get(primary_key_available, 0.0)
    )
    if complexity_score < COMPLEXITY_THRESHOLD_SIMPLE:
        complexity_level = "Simple"
    elif complexity_score < COMPLEXITY_THRESHOLD_MEDIUM:
        complexity_level = "Medium"
    else:
        complexity_level = "Complex"

    base = EFFORT_BASE_DAYS[complexity_level]
    object_effort_scale = 1.0 + (n_objects - 1) * ADDITIONAL_OBJECT_EFFORT_FACTOR

    discovery_days            = base["discovery"] * object_effort_scale
    design_days               = base["design"] * object_effort_scale
    build_ingestion_days      = base["build_ingestion"] * object_effort_scale
    build_transformation_days = base["build_transformation"] * object_effort_scale
    testing_days              = base["testing"] * object_effort_scale
    deployment_days           = base["deployment"]
    documentation_days        = base["documentation"]

    total_effort_estimate = (
        discovery_days + design_days + build_ingestion_days
        + build_transformation_days + testing_days + deployment_days + documentation_days
    )
    total_effort_min = total_effort_estimate * (1 - VARIANCE_FACTOR)
    total_effort_max = total_effort_estimate * (1 + VARIANCE_FACTOR)

    # ── Rows (no timestamps; caller adds them) ─────────────────────────────────
    request = {
        "request_id": request_id, "business_unit": business_unit, "request_date": request_date,
        "requestor": requestor, "business_justification": business_justification,
        "contains_phi": contains_phi, "ingestion_method": ingestion_method,
        "source_system": source_system, "data_structure": data_structure,
        "source_objects": source_objects_raw, "edh_table_names": edh_table_names_raw,
        "n_objects": n_objects, "additional_gb": additional_gb, "sla_time_hr": sla_time_hr,
        "ingestion_frequency": ingestion_frequency, "load_type": load_type,
        "primary_key_available": primary_key_available, "delete_handling": delete_handling,
        "schema_stability": schema_stability, "cdc_method": cdc_method, "vm_type": vm_type,
    }
    estimation = {
        "request_id": request_id, "ingestion_method": ingestion_method,
        "source_system": source_system, "data_structure": data_structure,
        "n_objects": n_objects, "additional_gb": additional_gb, "sla_time_hr": sla_time_hr,
        "ingestion_frequency": ingestion_frequency, "load_type": load_type,
        "runs_per_month": runs_per_month, "load_factor": load_factor,
        "vm_type": vm_type_effective, "worker_nodes_estimated": worker_nodes_estimated,
        "ingestion_nodes": ingestion_nodes, "throughput_gb_hr": throughput,
        "runtime_hrs": runtime_hrs, "meets_sla": meets_sla, "object_multiplier": object_multiplier,
        "ingestion_dbu_hr": ingestion_dbu_hr, "transformation_dbu_hr": TRANSFORMATION_DBU_HR,
        "total_dbu_cost": total_dbu_cost, "total_vm_cost": total_vm_cost,
        "ingestion_cost": ingestion_cost, "transformation_cost": transformation_cost,
        "compute_cost": compute_cost, "compression_ratio": compression_ratio,
        "compressed_gb": compressed_gb, "data_storage_cost": data_storage_cost,
        "transaction_cost": transaction_cost, "storage_cost": storage_cost,
        "network_rate_per_gb": network_rate_per_gb, "networking_cost": networking_cost,
        "total_monthly_cost": total_monthly_cost, "total_annual_cost": total_annual_cost,
        "complexity_score": complexity_score, "complexity_level": complexity_level,
        "object_effort_scale": object_effort_scale, "discovery_days": discovery_days,
        "design_days": design_days, "build_ingestion_days": build_ingestion_days,
        "build_transformation_days": build_transformation_days, "testing_days": testing_days,
        "deployment_days": deployment_days, "documentation_days": documentation_days,
        "total_effort_days_min": total_effort_min,
        "total_effort_days_estimate": total_effort_estimate,
        "total_effort_days_max": total_effort_max,
    }
    combined = {
        "request_id": request_id, "ingestion_type": "Source System",
        "business_unit": business_unit, "requestor": requestor, "request_date": request_date,
        "contains_phi": contains_phi,
        "compute_cost_monthly": compute_cost, "compute_cost_low": compute_low, "compute_cost_high": compute_high,
        "storage_cost_monthly": storage_cost, "storage_cost_low": storage_low, "storage_cost_high": storage_high,
        "networking_cost_monthly": networking_cost, "networking_cost_low": networking_low, "networking_cost_high": networking_high,
        "total_cost_monthly": total_monthly_cost, "total_cost_monthly_low": total_low, "total_cost_monthly_high": total_high,
        "total_cost_annual": total_annual_cost, "total_cost_annual_low": annual_low, "total_cost_annual_high": annual_high,
    }
    return {"request_type": "source_system", "request": request,
            "estimation": estimation, "combined": combined}

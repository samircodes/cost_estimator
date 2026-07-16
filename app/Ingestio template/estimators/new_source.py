"""New Source cost & effort estimator (pure Python).

Port of notebooks/EDH_New_Source_Estimator_Job.py. `estimate(payload)` takes
the same string-valued payload the app used to send to the Databricks job and
returns the rows for the request / estimation / combined Delta tables (without
timestamps). Prices can be injected for deterministic tests; otherwise fetched
live (cached) with hardcoded fallbacks.
"""

import math

from estimators import pricing

# ── Engineering constants ─────────────────────────────────────────────────────
SKEW_BY_PATTERN = {
    "Evenly distributed": 0.9,
    "Some concentration in a few records": 0.6,
    "Highly concentrated in a few records": 0.3,
    "Not sure": 0.6,
}
SMALL_FILES_BY_DELIVERY = {
    "One large batch file/extract": 0.8,
    "Many small files or frequent small batches": 0.4,
    "Not sure": 0.5,
}
PARTITIONING_BY_KEY_AVAILABILITY = {
    "Yes, a clear date/region/key field": 1.4,
    "Somewhat": 1.1,
    "No clear splitting field": 0.8,
    "Not sure": 1.2,
}
COMPLEXITY_FACTOR_BY_TRANSFORMATION = {"light": 1.2, "medium": 1.8, "heavy": 2.8}

DBU_COST_HR = 0.3
DRIVER_NODES = 1

VM_THROUGHPUT = {"Standard_DS3_v2": 4.5, "Standard_DS5_v2": 18.0}
VM_FALLBACK_RATE = {"Standard_DS3_v2": 0.293, "Standard_DS5_v2": 1.17}

ADLS_HOT_FALLBACK = 0.0208
ADLS_MANAGED_FALLBACK = 0.023
EGRESS_FALLBACK = 0.087

BRONZE_RATIO = 1.0
SILVER_RATIO = 0.022
GOLD_RATIO = 0.037

_RUNS_PER_MONTH = {
    "adhoc": 1, "weekly": 4, "daily": 30,
    "hourly": 730, "near_real_time": 4380, "real_time": 43800,
}

# ── Effort ────────────────────────────────────────────────────────────────────
COMPLEXITY_WEIGHTS = {
    "source_type": 0.286, "volume": 0.214, "transformation_logic": 0.357, "frequency": 0.143,
}
SCORING_RUBRICS = {
    "source_type": {
        "internal_sql": 10, "internal_api": 25, "azure_service": 20,
        "external_sftp": 40, "external_api": 55, "aws_s3": 45,
        "aws_rds": 50, "gcp": 55, "saas_connector": 60,
        "legacy_mainframe": 85, "multi_source": 90,
    },
    "volume": {"tiny": 10, "small": 25, "medium": 45, "large": 70, "very_large": 85, "massive": 95},
    "transformation_logic": {"light": 18, "medium": 50, "heavy": 83},
    "frequency": {"adhoc": 10, "weekly": 20, "daily": 35, "hourly": 60, "near_real_time": 80, "real_time": 95},
}
VOLUME_TIER_THRESHOLDS_GB = [(10, "tiny"), (50, "small"), (200, "medium"), (500, "large"), (2000, "very_large")]
VOLUME_TIER_DEFAULT_ABOVE = "massive"
PHASE_EFFORT = {
    "discovery":            {"Simple": {"min": 2, "max": 3}, "Medium": {"min": 4, "max": 6}, "Complex": {"min": 5, "max": 10}},
    "design":               {"Simple": {"min": 2, "max": 4}, "Medium": {"min": 5, "max": 8}, "Complex": {"min": 5, "max": 10}},
    "build_ingestion":      {"Simple": {"min": 2, "max": 3}, "Medium": {"min": 4, "max": 6}, "Complex": {"min": 5, "max": 10}},
    "build_transformation": {"Simple": {"min": 2, "max": 4}, "Medium": {"min": 5, "max": 8}, "Complex": {"min": 5, "max": 10}},
    "build_orchestration":  {"Simple": {"min": 2, "max": 3}, "Medium": {"min": 3, "max": 5}, "Complex": {"min": 5, "max": 8}},
    "deployment":           {"Simple": {"min": 2, "max": 4}, "Medium": {"min": 5, "max": 5}, "Complex": {"min": 5, "max": 7}},
    "documentation":        {"Simple": {"min": 2, "max": 3}, "Medium": {"min": 3, "max": 4}, "Complex": {"min": 4, "max": 5}},
}
TESTING_PERCENTAGE = 0.25


def get_volume_tier(source_gb):
    for threshold_gb, tier in VOLUME_TIER_THRESHOLDS_GB:
        if source_gb < threshold_gb:
            return tier
    return VOLUME_TIER_DEFAULT_ABOVE


def calculate_complexity_score(source_type, volume, transformation_logic, frequency):
    scores = {
        "source_type": SCORING_RUBRICS["source_type"].get(source_type, 50),
        "volume": SCORING_RUBRICS["volume"].get(volume, 50),
        "transformation_logic": SCORING_RUBRICS["transformation_logic"].get(transformation_logic, 50),
        "frequency": SCORING_RUBRICS["frequency"].get(frequency, 50),
    }
    weighted = {k: v * COMPLEXITY_WEIGHTS[k] for k, v in scores.items()}
    total = sum(weighted.values())
    level = "Simple" if total <= 30 else "Medium" if total <= 70 else "Complex"
    return {"total_score": round(total, 1), "complexity_level": level}


def estimate_effort(source_type, volume, transformation_logic, frequency):
    complexity = calculate_complexity_score(source_type, volume, transformation_logic, frequency)
    level = complexity["complexity_level"]
    phases = {}
    for phase, ranges in PHASE_EFFORT.items():
        r = ranges[level]
        phases[phase] = {"min": r["min"], "max": r["max"], "estimate": (r["min"] + r["max"]) / 2}
    build_min = phases["build_ingestion"]["min"] + phases["build_transformation"]["min"] + phases["build_orchestration"]["min"]
    build_max = phases["build_ingestion"]["max"] + phases["build_transformation"]["max"] + phases["build_orchestration"]["max"]
    build_est = phases["build_ingestion"]["estimate"] + phases["build_transformation"]["estimate"] + phases["build_orchestration"]["estimate"]
    phases["testing"] = {
        "min": math.ceil(build_min * TESTING_PERCENTAGE),
        "max": math.ceil(build_max * TESTING_PERCENTAGE),
        "estimate": round(build_est * TESTING_PERCENTAGE, 1),
    }
    total_min = sum(p["min"] for p in phases.values())
    total_max = sum(p["max"] for p in phases.values())
    total_est = sum(p["estimate"] for p in phases.values())
    return {
        "complexity": complexity, "complexity_level": level, "phases": phases,
        "build_subtotal": {"min": build_min, "max": build_max, "estimate": build_est},
        "total_effort_days": {"min": total_min, "max": total_max, "estimate": total_est},
    }


def calculate_network_cost(source_gb, source_type, include_egress, egress_gb, frequency, egress_rate):
    RATES = {
        "azure_same_region": 0.00, "expressroute_metered": 0.025, "expressroute_unlimited": 0.00,
        "vpn": 0.00, "aws_s3": egress_rate, "aws_rds": egress_rate, "gcp": 0.12,
        "sftp": 0.00, "api": 0.00, "private_endpoint": 0.01, "internet_egress": egress_rate,
        "cross_region": 0.02,
    }
    if source_type not in RATES:
        raise ValueError(f"Unknown source_type '{source_type}'. Valid: {list(RATES.keys())}")
    ingress_rate = RATES[source_type]
    stage1 = source_gb * ingress_rate
    stage2 = source_gb * BRONZE_RATIO * RATES["private_endpoint"]
    stage3 = source_gb * BRONZE_RATIO * RATES["private_endpoint"]
    stage4 = source_gb * SILVER_RATIO * RATES["private_endpoint"]
    stage5 = source_gb * SILVER_RATIO * RATES["private_endpoint"]
    stage6 = source_gb * GOLD_RATIO * RATES["private_endpoint"]
    egress_cost = egress_gb * RATES["internet_egress"] if include_egress else 0.0
    runs = _RUNS_PER_MONTH.get(frequency, 30)
    total_daily = stage1 + stage2 + stage3 + stage4 + stage5 + stage6 + egress_cost
    return {"total_monthly_cost": round(total_daily * runs, 2)}


def calculate_storage_cost(source_gb, copy_interval, frequency, adls_hot_rate, adls_managed_rate,
                           compression_factor=0.30):
    WRITE_RATE_PER_10K = 0.07
    READ_RATE_PER_10K = 0.0052
    LIST_RATE_PER_10K = 0.09
    runs = _RUNS_PER_MONTH.get(frequency, 30)
    stored_gb = source_gb * compression_factor
    bronze_gb = stored_gb * BRONZE_RATIO
    silver_gb = stored_gb * SILVER_RATIO
    gold_gb = stored_gb * GOLD_RATIO
    total_storage = bronze_gb * adls_hot_rate + silver_gb * adls_managed_rate + gold_gb * adls_managed_rate
    avg_files = max(1, int((stored_gb * 1024) / 128))
    daily_write = avg_files + 3
    daily_read = avg_files * 2 + 5 if copy_interval == "incremental" else 5
    daily_list = 4
    ops = ((daily_write * runs) / 10000) * WRITE_RATE_PER_10K \
        + ((daily_read * runs) / 10000) * READ_RATE_PER_10K \
        + ((daily_list * runs) / 10000) * LIST_RATE_PER_10K
    return {"grand_total_monthly": round(total_storage + ops, 4), "stored_gb": round(stored_gb, 4)}


def calculate_worker_sizing(data_volume_gb, sla_time_hr, partitioning, skew, small_files,
                            complexity_factor, per_node_throughput_gb_hr):
    combined_adjustment = partitioning * skew * small_files
    effective_per_node = per_node_throughput_gb_hr * combined_adjustment
    worker_nodes_raw = (data_volume_gb * complexity_factor) / (sla_time_hr * effective_per_node)
    worker_nodes_estimated = math.ceil(worker_nodes_raw)
    total_nodes = worker_nodes_estimated + DRIVER_NODES
    total_effective_throughput = worker_nodes_estimated * effective_per_node
    estimated_runtime = (data_volume_gb / total_effective_throughput) * complexity_factor
    return {
        "worker_nodes_estimated": worker_nodes_estimated, "total_nodes": total_nodes,
        "estimated_runtime_hr": round(estimated_runtime, 4), "meets_sla": estimated_runtime <= sla_time_hr,
        "resolved_skew": skew, "resolved_small_files": small_files,
        "resolved_partitioning": partitioning, "resolved_complexity_factor": complexity_factor,
    }


def calculate_compute_cost(sizing, vm_cost_hr, frequency):
    runs = _RUNS_PER_MONTH.get(frequency, 30)
    total_per_node_hr = vm_cost_hr + DBU_COST_HR
    cluster_cost_per_hr = sizing["total_nodes"] * total_per_node_hr
    daily = sizing["estimated_runtime_hr"] * cluster_cost_per_hr
    return {"compute_cost_monthly": round(daily * runs, 2)}


def validate_inputs(source_gb, copy_interval, sla_time_hr, contains_phi, delete_handling,
                    schema_stability, cdc_method, complexity_source_type, transformation_logic,
                    frequency, vm_type, data_distribution, delivery_pattern, partition_key_availability):
    if vm_type not in VM_THROUGHPUT:
        raise ValueError(f"Invalid vm_type. Choose from: {list(VM_THROUGHPUT.keys())}")
    if data_distribution not in SKEW_BY_PATTERN:
        raise ValueError(f"Invalid data_distribution. Choose from: {list(SKEW_BY_PATTERN.keys())}")
    if delivery_pattern not in SMALL_FILES_BY_DELIVERY:
        raise ValueError(f"Invalid delivery_pattern. Choose from: {list(SMALL_FILES_BY_DELIVERY.keys())}")
    if partition_key_availability not in PARTITIONING_BY_KEY_AVAILABILITY:
        raise ValueError(f"Invalid partition_key_availability. Choose from: {list(PARTITIONING_BY_KEY_AVAILABILITY.keys())}")
    if source_gb <= 0:
        raise ValueError("source_gb must be greater than 0")
    if copy_interval not in ["bulk", "incremental"]:
        raise ValueError("copy_interval must be 'bulk' or 'incremental'")
    if sla_time_hr <= 0:
        raise ValueError("sla_time_hr must be greater than 0")
    if contains_phi not in ["Yes", "No"]:
        raise ValueError("contains_phi must be Yes or No")
    if delete_handling not in ["Hard", "Soft", "Ignore"]:
        raise ValueError("Invalid delete handling.")
    if schema_stability not in ["Stable", "Occasionally Changes", "Highly Dynamic"]:
        raise ValueError("Invalid schema stability.")
    if cdc_method not in ["Timestamp", "Log Based", "Not Applicable"]:
        raise ValueError("Invalid CDC method.")
    if copy_interval == "incremental" and cdc_method == "Not Applicable":
        raise ValueError("CDC Method cannot be 'Not Applicable' when Copy Interval is 'incremental'")
    if copy_interval == "bulk" and cdc_method != "Not Applicable":
        raise ValueError("CDC Method should be 'Not Applicable' when Copy Interval is 'bulk'")
    if complexity_source_type not in SCORING_RUBRICS["source_type"]:
        raise ValueError(f"Invalid complexity_source_type. Choose from: {list(SCORING_RUBRICS['source_type'].keys())}")
    if transformation_logic not in SCORING_RUBRICS["transformation_logic"]:
        raise ValueError(f"Invalid transformation_logic. Choose from: {list(SCORING_RUBRICS['transformation_logic'].keys())}")
    if transformation_logic not in COMPLEXITY_FACTOR_BY_TRANSFORMATION:
        raise ValueError("transformation_logic has no matching COMPLEXITY_FACTOR entry.")
    if frequency not in SCORING_RUBRICS["frequency"]:
        raise ValueError(f"Invalid frequency. Choose from: {list(SCORING_RUBRICS['frequency'].keys())}")


def _resolve_prices(prices):
    if prices is not None:
        return prices
    return {
        "Standard_DS3_v2": pricing.fetch_vm_price("Standard_DS3_v2", VM_FALLBACK_RATE["Standard_DS3_v2"]),
        "Standard_DS5_v2": pricing.fetch_vm_price("Standard_DS5_v2", VM_FALLBACK_RATE["Standard_DS5_v2"]),
        "adls_hot":     pricing.fetch_adls_storage_price(fallback=ADLS_HOT_FALLBACK),
        "adls_managed": pricing.fetch_adls_storage_price(fallback=ADLS_MANAGED_FALLBACK),
        "egress":       pricing.fetch_egress_price(fallback=EGRESS_FALLBACK),
    }


def estimate(payload, prices=None):
    p = _resolve_prices(prices)

    request_id             = payload["request_id"]
    business_unit          = payload.get("business_unit", "")
    request_date           = payload.get("request_date", "")
    requestor              = payload.get("requestor", "")
    business_justification = payload.get("business_justification", "")
    contains_phi           = payload.get("contains_phi", "No")
    delete_handling        = payload["delete_handling"]
    schema_stability       = payload["schema_stability"]
    cdc_method             = payload["cdc_method"]
    source_gb              = float(payload["source_gb"])
    network_source_type    = payload["network_source_type"]
    copy_interval          = payload["copy_interval"]
    include_egress         = str(payload.get("include_egress", "false")).lower() == "true"
    egress_gb              = float(payload.get("egress_gb", 0))
    sla_time_hr            = float(payload["sla_time_hr"])
    vm_type                = payload["vm_type"]
    data_distribution      = payload["data_distribution"]
    delivery_pattern       = payload["delivery_pattern"]
    partition_key_availability = payload["partition_key_availability"]
    complexity_source_type = payload["complexity_source_type"]
    transformation_logic   = payload["transformation_logic"]
    frequency              = payload["frequency"]

    validate_inputs(source_gb, copy_interval, sla_time_hr, contains_phi, delete_handling,
                    schema_stability, cdc_method, complexity_source_type, transformation_logic,
                    frequency, vm_type, data_distribution, delivery_pattern, partition_key_availability)

    vm_throughput = VM_THROUGHPUT[vm_type]
    vm_cost_hr = p[vm_type]

    network = calculate_network_cost(source_gb, network_source_type, include_egress, egress_gb,
                                     frequency, egress_rate=p["egress"])
    storage = calculate_storage_cost(source_gb, copy_interval, frequency,
                                     adls_hot_rate=p["adls_hot"], adls_managed_rate=p["adls_managed"])

    resolved_skew         = SKEW_BY_PATTERN[data_distribution]
    resolved_small_files  = SMALL_FILES_BY_DELIVERY[delivery_pattern]
    resolved_partitioning = PARTITIONING_BY_KEY_AVAILABILITY[partition_key_availability]
    resolved_complexity_factor = COMPLEXITY_FACTOR_BY_TRANSFORMATION[transformation_logic]

    sizing = calculate_worker_sizing(source_gb, sla_time_hr, resolved_partitioning, resolved_skew,
                                     resolved_small_files, resolved_complexity_factor,
                                     per_node_throughput_gb_hr=vm_throughput)
    compute = calculate_compute_cost(sizing, vm_cost_hr=vm_cost_hr, frequency=frequency)

    total_monthly_cost = round(network["total_monthly_cost"] + storage["grand_total_monthly"] + compute["compute_cost_monthly"], 2)
    total_annual_cost = round(total_monthly_cost * 12, 2)
    cost_per_gb_monthly = round(total_monthly_cost / source_gb, 4) if source_gb > 0 else 0

    derived_volume_tier = get_volume_tier(source_gb)
    effort = estimate_effort(complexity_source_type, derived_volume_tier, transformation_logic, frequency)
    ph = effort["phases"]

    def variance(v):
        return round(v * 0.9, 2), round(v * 1.1, 2)

    compute_low, compute_high       = variance(compute["compute_cost_monthly"])
    storage_low, storage_high       = variance(storage["grand_total_monthly"])
    networking_low, networking_high = variance(network["total_monthly_cost"])
    total_low, total_high           = variance(total_monthly_cost)
    annual_low, annual_high         = variance(total_annual_cost)

    request = {
        "request_id": request_id, "business_unit": business_unit, "request_date": request_date,
        "requestor": requestor, "business_justification": business_justification,
        "contains_phi": contains_phi, "delete_handling": delete_handling,
        "schema_stability": schema_stability, "cdc_method": cdc_method, "source_gb": source_gb,
        "network_source_type": network_source_type, "copy_interval": copy_interval,
        "include_egress": include_egress, "egress_gb": egress_gb, "sla_time_hr": sla_time_hr,
        "vm_type": vm_type, "data_distribution": data_distribution, "delivery_pattern": delivery_pattern,
        "partition_key_availability": partition_key_availability,
        "complexity_source_type": complexity_source_type, "transformation_logic": transformation_logic,
        "frequency": frequency,
    }
    estimation = {
        "request_id": request_id, "business_unit": business_unit, "contains_phi": contains_phi,
        "source_gb": source_gb, "vm_type": vm_type, "vm_per_node_throughput_gb_hr": vm_throughput,
        "resolved_skew": resolved_skew, "resolved_small_files": resolved_small_files,
        "resolved_partitioning": resolved_partitioning, "resolved_complexity_factor": resolved_complexity_factor,
        "vm_cost_hr": vm_cost_hr, "network_cost_monthly": network["total_monthly_cost"],
        "storage_cost_monthly": storage["grand_total_monthly"], "compute_cost_monthly": compute["compute_cost_monthly"],
        "total_cost_monthly": total_monthly_cost, "total_cost_annual": total_annual_cost,
        "cost_per_gb_monthly": cost_per_gb_monthly,
        "worker_nodes_estimated": sizing["worker_nodes_estimated"], "total_nodes": sizing["total_nodes"],
        "estimated_runtime_hr": sizing["estimated_runtime_hr"], "meets_sla": sizing["meets_sla"],
        "complexity_score": effort["complexity"]["total_score"], "complexity_level": effort["complexity_level"],
        "derived_volume_tier": derived_volume_tier,
        "discovery_days": ph["discovery"]["estimate"], "design_days": ph["design"]["estimate"],
        "build_ingestion_days": ph["build_ingestion"]["estimate"],
        "build_transformation_days": ph["build_transformation"]["estimate"],
        "build_orchestration_days": ph["build_orchestration"]["estimate"],
        "testing_days": ph["testing"]["estimate"], "deployment_days": ph["deployment"]["estimate"],
        "documentation_days": ph["documentation"]["estimate"],
        "build_subtotal_days": effort["build_subtotal"]["estimate"],
        "total_effort_days_min": effort["total_effort_days"]["min"],
        "total_effort_days_estimate": effort["total_effort_days"]["estimate"],
        "total_effort_days_max": effort["total_effort_days"]["max"],
    }
    combined = {
        "request_id": request_id, "ingestion_type": "New Source",
        "business_unit": business_unit, "requestor": requestor, "request_date": request_date,
        "contains_phi": contains_phi,
        "compute_cost_monthly": compute["compute_cost_monthly"], "compute_cost_low": compute_low, "compute_cost_high": compute_high,
        "storage_cost_monthly": storage["grand_total_monthly"], "storage_cost_low": storage_low, "storage_cost_high": storage_high,
        "networking_cost_monthly": network["total_monthly_cost"], "networking_cost_low": networking_low, "networking_cost_high": networking_high,
        "total_cost_monthly": total_monthly_cost, "total_cost_monthly_low": total_low, "total_cost_monthly_high": total_high,
        "total_cost_annual": total_annual_cost, "total_cost_annual_low": annual_low, "total_cost_annual_high": annual_high,
    }
    return {"request_type": "new_source", "request": request,
            "estimation": estimation, "combined": combined}

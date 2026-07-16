"""Ordered Delta table schemas for the estimator outputs.

Each schema is a list of (column_name, type_code) in table-column order,
including the timestamp column the client fills in. Kept pyspark-free so the
estimator modules stay pure; databricks_client maps the type codes to Spark
types when it writes. These mirror the schemas in the reference notebooks.

Type codes: "str", "double", "int", "bool", "ts".
"""

SOURCE_SYSTEM_REQUEST = [
    ("request_id", "str"), ("submission_timestamp", "ts"), ("business_unit", "str"),
    ("request_date", "str"), ("requestor", "str"), ("business_justification", "str"),
    ("contains_phi", "str"), ("ingestion_method", "str"), ("source_system", "str"),
    ("data_structure", "str"), ("source_objects", "str"), ("edh_table_names", "str"),
    ("n_objects", "int"), ("additional_gb", "double"), ("sla_time_hr", "double"),
    ("ingestion_frequency", "str"), ("load_type", "str"),
    ("bulk_table_count", "int"), ("incremental_table_count", "int"),
    ("primary_key_available", "str"), ("delete_handling", "str"), ("schema_stability", "str"),
    ("cdc_method", "str"), ("vm_type", "str"),
]

SOURCE_SYSTEM_ESTIMATION = [
    ("request_id", "str"), ("estimation_timestamp", "ts"), ("ingestion_method", "str"),
    ("source_system", "str"), ("data_structure", "str"), ("n_objects", "int"),
    ("additional_gb", "double"), ("sla_time_hr", "double"), ("ingestion_frequency", "str"),
    ("load_type", "str"), ("runs_per_month", "int"), ("load_factor", "double"),
    ("vm_type", "str"), ("worker_nodes_estimated", "int"), ("ingestion_nodes", "int"),
    ("throughput_gb_hr", "double"), ("runtime_hrs", "double"), ("meets_sla", "bool"),
    ("object_multiplier", "double"), ("ingestion_dbu_hr", "double"),
    ("transformation_dbu_hr", "double"), ("total_dbu_cost", "double"), ("total_vm_cost", "double"),
    ("ingestion_cost", "double"), ("transformation_cost", "double"), ("compute_cost", "double"),
    ("compression_ratio", "double"), ("compressed_gb", "double"), ("data_storage_cost", "double"),
    ("transaction_cost", "double"), ("storage_cost", "double"), ("network_rate_per_gb", "double"),
    ("networking_cost", "double"), ("total_monthly_cost", "double"), ("total_annual_cost", "double"),
    ("complexity_score", "double"), ("complexity_level", "str"), ("object_effort_scale", "double"),
    ("discovery_days", "double"), ("design_days", "double"), ("build_ingestion_days", "double"),
    ("build_transformation_days", "double"), ("testing_days", "double"), ("deployment_days", "double"),
    ("documentation_days", "double"), ("total_effort_days_min", "double"),
    ("total_effort_days_estimate", "double"), ("total_effort_days_max", "double"),
]

NEW_SOURCE_REQUEST = [
    ("request_id", "str"), ("submission_timestamp", "ts"), ("business_unit", "str"),
    ("request_date", "str"), ("requestor", "str"), ("business_justification", "str"),
    ("contains_phi", "str"), ("delete_handling", "str"), ("schema_stability", "str"),
    ("cdc_method", "str"), ("source_gb", "double"), ("network_source_type", "str"),
    ("copy_interval", "str"), ("include_egress", "bool"), ("egress_gb", "double"),
    ("sla_time_hr", "double"), ("vm_type", "str"), ("data_distribution", "str"),
    ("delivery_pattern", "str"), ("partition_key_availability", "str"),
    ("complexity_source_type", "str"), ("transformation_logic", "str"), ("frequency", "str"),
]

NEW_SOURCE_ESTIMATION = [
    ("request_id", "str"), ("estimation_timestamp", "ts"), ("business_unit", "str"),
    ("contains_phi", "str"), ("source_gb", "double"), ("vm_type", "str"),
    ("vm_per_node_throughput_gb_hr", "double"), ("resolved_skew", "double"),
    ("resolved_small_files", "double"), ("resolved_partitioning", "double"),
    ("resolved_complexity_factor", "double"), ("vm_cost_hr", "double"),
    ("network_cost_monthly", "double"), ("storage_cost_monthly", "double"),
    ("compute_cost_monthly", "double"), ("total_cost_monthly", "double"),
    ("total_cost_annual", "double"), ("cost_per_gb_monthly", "double"),
    ("worker_nodes_estimated", "int"), ("total_nodes", "int"), ("estimated_runtime_hr", "double"),
    ("meets_sla", "bool"), ("complexity_score", "double"), ("complexity_level", "str"),
    ("derived_volume_tier", "str"), ("discovery_days", "double"), ("design_days", "double"),
    ("build_ingestion_days", "double"), ("build_transformation_days", "double"),
    ("build_orchestration_days", "double"), ("testing_days", "double"), ("deployment_days", "double"),
    ("documentation_days", "double"), ("build_subtotal_days", "double"),
    ("total_effort_days_min", "double"), ("total_effort_days_estimate", "double"),
    ("total_effort_days_max", "double"),
]

COMBINED = [
    ("request_id", "str"), ("estimation_timestamp", "ts"), ("ingestion_type", "str"),
    ("business_unit", "str"), ("requestor", "str"), ("request_date", "str"), ("contains_phi", "str"),
    ("compute_cost_monthly", "double"), ("compute_cost_low", "double"), ("compute_cost_high", "double"),
    ("storage_cost_monthly", "double"), ("storage_cost_low", "double"), ("storage_cost_high", "double"),
    ("networking_cost_monthly", "double"), ("networking_cost_low", "double"), ("networking_cost_high", "double"),
    ("total_cost_monthly", "double"), ("total_cost_monthly_low", "double"), ("total_cost_monthly_high", "double"),
    ("total_cost_annual", "double"), ("total_cost_annual_low", "double"), ("total_cost_annual_high", "double"),
]

# Which timestamp column each schema fills, and the estimator attribute it maps from.
REQUEST_TS = "submission_timestamp"
ESTIMATION_TS = "estimation_timestamp"

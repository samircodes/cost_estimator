import uuid
from datetime import date

import streamlit as st

from app_config import (
    CDC_METHOD_OPTIONS,
    COMPLEXITY_SOURCE_TYPES,
    COPY_INTERVALS,
    DATA_DISTRIBUTIONS,
    DELETE_HANDLING_OPTIONS,
    DELIVERY_PATTERNS,
    NETWORK_SOURCE_TYPES,
    NEW_SOURCE_FREQUENCIES,
    PARTITION_KEY_AVAILABILITIES,
    SCHEMA_STABILITY_OPTIONS,
    TRANSFORMATION_LOGICS,
    VM_TYPES,
)
from databricks_client import trigger_estimator_job
from ui import render_back_button, render_field_intro, render_form_heading, render_page_intro

YES_NO = ("Yes", "No")


def render_new_ingestion_page() -> None:
    render_back_button("back_from_new")

    render_page_intro(
        "New request",
        "New Ingestion Request",
        "Provide the details needed to register and onboard a new data source into EDH.",
    )

    render_form_heading("New Source Details", 22)

    with st.form("new_ingestion_request"):

        # ── Section 0: Request metadata ───────────────────────────────────────
        col_a, col_b = st.columns(2, gap="large")
        with col_a:
            render_field_intro(1, "Business unit", "Please fill the name for your business unit")
            business_unit = st.text_input(
                "Business unit", placeholder="e.g. Finance", label_visibility="collapsed"
            )
        with col_b:
            render_field_intro(2, "Requestor", "Please fill your name")
            requestor = st.text_input(
                "Requestor", placeholder="e.g. John Smith", label_visibility="collapsed"
            )

        st.markdown('<div class="form-divider"></div>', unsafe_allow_html=True)
        col_c, col_d = st.columns(2, gap="large")
        with col_c:
            render_field_intro(3, "Request date", "Please select the date of this request")
            request_date = st.date_input(
                "Request date", value=date.today(), label_visibility="collapsed"
            )
        with col_d:
            render_field_intro(4, "Business justification", "Please describe why this data source is needed")
            business_justification = st.text_input(
                "Business justification",
                placeholder="e.g. Required for monthly reporting",
                label_visibility="collapsed",
            )

        # ── Section 1: Data governance ────────────────────────────────────────
        st.markdown('<div class="form-divider"></div>', unsafe_allow_html=True)
        col_e, col_f = st.columns(2, gap="large")
        with col_e:
            render_field_intro(5, "Contains PHI", "Please select Yes if this data includes Protected Health Information, otherwise No")
            contains_phi = st.selectbox(
                "Contains PHI",
                options=YES_NO,
                index=None,
                placeholder="Select",
                label_visibility="collapsed",
            )
        with col_f:
            render_field_intro(6, "Delete handling", "Please select how deleted records from the source should be handled — Hard (physically removed), Soft (flagged but kept), or Ignore (deletes not tracked)")
            delete_handling = st.selectbox(
                "Delete handling",
                options=DELETE_HANDLING_OPTIONS,
                index=None,
                placeholder="Select",
                label_visibility="collapsed",
            )

        st.markdown('<div class="form-divider"></div>', unsafe_allow_html=True)
        col_g, col_h = st.columns(2, gap="large")
        with col_g:
            render_field_intro(7, "Schema stability", "Please select how often you expect the structure of this data to change")
            schema_stability = st.selectbox(
                "Schema stability",
                options=SCHEMA_STABILITY_OPTIONS,
                index=None,
                placeholder="Select",
                label_visibility="collapsed",
            )
        with col_h:
            render_field_intro(8, "CDC method", "Please select how changes will be tracked for this source — Timestamp, Log Based, or Not Applicable if this is a full reload each time")
            cdc_method = st.selectbox(
                "CDC method",
                options=CDC_METHOD_OPTIONS,
                index=None,
                placeholder="Select",
                label_visibility="collapsed",
            )

        # ── Section 2: Source details ─────────────────────────────────────────
        st.markdown('<div class="form-divider"></div>', unsafe_allow_html=True)
        col_i, col_j = st.columns(2, gap="large")
        with col_i:
            render_field_intro(9, "Pipeline name", "Please fill a name to identify this new pipeline")
            pipeline_name = st.text_input(
                "Pipeline name",
                placeholder="e.g. Claims Feed — Vendor ABC",
                label_visibility="collapsed",
            )
        with col_j:
            render_field_intro(10, "Data volume (GB)", "Please fill the expected size of this data source in GB")
            source_gb = st.number_input(
                "Data volume (GB)",
                min_value=0.01,
                value=1.0,
                step=0.5,
                format="%.2f",
                label_visibility="collapsed",
            )

        st.markdown('<div class="form-divider"></div>', unsafe_allow_html=True)
        col_k, col_l = st.columns(2, gap="large")
        with col_k:
            render_field_intro(11, "SLA (hours)", "Please fill how many hours this data needs to be ready within, after each run starts")
            sla_time_hr = st.number_input(
                "SLA (hours)",
                min_value=0.5,
                value=2.0,
                step=0.5,
                format="%.1f",
                label_visibility="collapsed",
            )
        with col_l:
            render_field_intro(12, "Network source type", "Please select how this data connects to Azure")
            network_source_type = st.selectbox(
                "Network source type",
                options=NETWORK_SOURCE_TYPES,
                index=None,
                placeholder="Select connection type",
                label_visibility="collapsed",
            )

        # ── Section 3: Load configuration ─────────────────────────────────────
        st.markdown('<div class="form-divider"></div>', unsafe_allow_html=True)
        col_m, col_n = st.columns(2, gap="large")
        with col_m:
            render_field_intro(13, "Copy interval", "Please select whether this is a full reload each time (Bulk) or only changed records (Incremental)")
            copy_interval = st.selectbox(
                "Copy interval",
                options=COPY_INTERVALS,
                index=None,
                placeholder="Select",
                label_visibility="collapsed",
            )
        with col_n:
            render_field_intro(14, "VM type", "Please select the virtual machine type this pipeline should run on")
            vm_type = st.selectbox(
                "VM type",
                options=VM_TYPES,
                index=None,
                placeholder="Select VM type",
                label_visibility="collapsed",
            )

        st.markdown('<div class="form-divider"></div>', unsafe_allow_html=True)
        col_o, col_p = st.columns(2, gap="large")
        with col_o:
            render_field_intro(15, "Egress", "Please select Yes if this data will be sent to a destination outside Azure, otherwise No")
            include_egress_raw = st.selectbox(
                "Egress",
                options=YES_NO,
                index=None,
                placeholder="Select",
                label_visibility="collapsed",
            )
        with col_p:
            render_field_intro(16, "Egress volume (GB)", "Please fill how much data, in GB, will be sent outside Azure (only if the above is Yes)")
            egress_gb = st.number_input(
                "Egress volume (GB)",
                min_value=0.0,
                value=0.0,
                step=1.0,
                format="%.1f",
                label_visibility="collapsed",
            )

        # ── Section 4: Data characteristics ──────────────────────────────────
        st.markdown('<div class="form-divider"></div>', unsafe_allow_html=True)
        render_field_intro(17, "Data distribution", "Please select whether most of this data is spread evenly, or comes mostly from a small number of records, customers, or categories")
        data_distribution = st.selectbox(
            "Data distribution",
            options=DATA_DISTRIBUTIONS,
            index=None,
            placeholder="Select",
            label_visibility="collapsed",
        )

        st.markdown('<div class="form-divider"></div>', unsafe_allow_html=True)
        render_field_intro(18, "Delivery pattern", "Please select whether this data typically arrives as one large file, or as many small files or frequent small batches")
        delivery_pattern = st.selectbox(
            "Delivery pattern",
            options=DELIVERY_PATTERNS,
            index=None,
            placeholder="Select",
            label_visibility="collapsed",
        )

        st.markdown('<div class="form-divider"></div>', unsafe_allow_html=True)
        render_field_intro(19, "Partition key", "Please select whether this data has a clear field like date, region, or account that can be used to split it into smaller groups")
        partition_key_availability = st.selectbox(
            "Partition key",
            options=PARTITION_KEY_AVAILABILITIES,
            index=None,
            placeholder="Select",
            label_visibility="collapsed",
        )

        # ── Section 5: Effort estimation ──────────────────────────────────────
        st.markdown('<div class="form-divider"></div>', unsafe_allow_html=True)
        col_t, col_u, col_v = st.columns(3, gap="large")
        with col_t:
            render_field_intro(20, "Source complexity", "Please select the type of system this data is coming from")
            complexity_source_type = st.selectbox(
                "Source complexity",
                options=COMPLEXITY_SOURCE_TYPES,
                index=None,
                placeholder="Select",
                label_visibility="collapsed",
            )
        with col_u:
            render_field_intro(21, "Transformation logic", "Please select how much transformation or business logic needs to be applied to this data — light, medium, or heavy")
            transformation_logic = st.selectbox(
                "Transformation logic",
                options=TRANSFORMATION_LOGICS,
                index=None,
                placeholder="Select",
                label_visibility="collapsed",
            )
        with col_v:
            render_field_intro(22, "Frequency", "Please select how often this data needs to be refreshed")
            frequency = st.selectbox(
                "Frequency",
                options=NEW_SOURCE_FREQUENCIES,
                index=None,
                placeholder="Select",
                label_visibility="collapsed",
            )

        st.markdown('<div class="form-divider"></div>', unsafe_allow_html=True)
        submitted = st.form_submit_button(
            "Submit request", type="primary", use_container_width=False
        )

    if submitted:
        missing = [
            name for name, val in [
                ("Business unit", business_unit),
                ("Contains PHI", contains_phi),
                ("Delete handling", delete_handling),
                ("Schema stability", schema_stability),
                ("CDC method", cdc_method),
                ("Pipeline name", pipeline_name),
                ("Network source type", network_source_type),
                ("Copy interval", copy_interval),
                ("VM type", vm_type),
                ("Egress", include_egress_raw),
                ("Data distribution", data_distribution),
                ("Delivery pattern", delivery_pattern),
                ("Partition key", partition_key_availability),
                ("Source complexity", complexity_source_type),
                ("Transformation logic", transformation_logic),
                ("Frequency", frequency),
            ] if not val
        ]
        if missing:
            st.error(f"Please fill in: {', '.join(missing)}")
            return

        if copy_interval == "incremental" and cdc_method == "Not Applicable":
            st.error("CDC Method cannot be 'Not Applicable' when Copy Interval is 'incremental'.")
            return
        if copy_interval == "bulk" and cdc_method != "Not Applicable":
            st.error("CDC Method should be 'Not Applicable' when Copy Interval is 'bulk'.")
            return

        request_id = str(uuid.uuid4())
        include_egress = "true" if include_egress_raw == "Yes" else "false"

        try:
            trigger_estimator_job(
                request_type="new_source",
                payload={
                    "request_id":                  request_id,
                    "business_unit":               business_unit,
                    "request_date":                str(request_date),
                    "requestor":                   requestor,
                    "business_justification":      business_justification or "",
                    "contains_phi":                contains_phi,
                    "delete_handling":             delete_handling,
                    "schema_stability":            schema_stability,
                    "cdc_method":                  cdc_method,
                    "pipeline_name":               pipeline_name,
                    "source_gb":                   str(source_gb),
                    "network_source_type":         network_source_type,
                    "copy_interval":               copy_interval,
                    "include_egress":              include_egress,
                    "egress_gb":                   str(egress_gb),
                    "sla_time_hr":                 str(sla_time_hr),
                    "vm_type":                     vm_type,
                    "data_distribution":           data_distribution,
                    "delivery_pattern":            delivery_pattern,
                    "partition_key_availability":  partition_key_availability,
                    "complexity_source_type":      complexity_source_type,
                    "transformation_logic":        transformation_logic,
                    "frequency":                   frequency,
                    "save_results":                "true",
                },
            )
        except Exception as exc:
            st.error(f"Failed to submit request: {exc}")
            return

        st.success(
            "Your request has been submitted! "
            "Cost and effort estimates will appear in Request History & Costs once processed."
        )

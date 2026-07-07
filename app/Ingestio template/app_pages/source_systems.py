import uuid
from datetime import date

import streamlit as st

from app_config import (
    CDC_METHOD_OPTIONS,
    DELETE_HANDLING_OPTIONS,
    INGESTION_FREQUENCIES,
    INGESTION_SOURCE_MAP,
    PRIMARY_KEY_OPTIONS,
    SCHEMA_STABILITY_OPTIONS,
)
from databricks_client import trigger_estimator_job
from ui import render_back_button, render_field_intro, render_form_heading, render_page_intro

YES_NO = ("Yes", "No")
LOAD_TYPES = ("Bulk", "Incremental")
INGESTION_METHODS = tuple(INGESTION_SOURCE_MAP.keys())


def render_source_systems_page() -> None:
    render_back_button("back_from_source_systems")

    render_page_intro(
        "Source systems",
        "Add Data from Source Systems",
        "Request ingestion from a known EDH source system. Select your ingestion method, "
        "source system, and the objects you need.",
    )

    render_form_heading("Source Systems Request", 14)

    # ── Cascade: Ingestion Method → Source System → Data Structure ────────────
    # These sit outside the form so they react to each other immediately.
    st.markdown("##### Source Identification")

    col1, col2 = st.columns(2, gap="large")
    with col1:
        render_field_intro(1, "Ingestion method", "Select how this data is ingested into EDH")
        ingestion_method = st.selectbox(
            "Ingestion method",
            options=INGESTION_METHODS,
            index=None,
            placeholder="Select ingestion method",
            label_visibility="collapsed",
            key="ss_ingestion_method",
        )

    source_systems = list(INGESTION_SOURCE_MAP.get(ingestion_method or "", {}).keys())

    with col2:
        render_field_intro(2, "Source system", "Select the source system this data comes from")
        source_system = st.selectbox(
            "Source system",
            options=source_systems,
            index=None,
            placeholder="Select source system" if ingestion_method else "Select ingestion method first",
            disabled=not ingestion_method,
            label_visibility="collapsed",
            key="ss_source_system",
        )

    data_structure = INGESTION_SOURCE_MAP.get(ingestion_method or "", {}).get(source_system or "", None)

    if data_structure:
        st.markdown(
            f"<div style='background:#f0fdf4;border:1px solid #bbf7d0;border-radius:8px;"
            f"padding:10px 16px;margin:8px 0 16px'>"
            f"<span style='font-size:0.78rem;color:#166534;font-weight:700;"
            f"text-transform:uppercase;letter-spacing:0.06em'>Source Data Structure</span>"
            f"<span style='font-size:1rem;font-weight:700;color:#14532d;"
            f"margin-left:12px'>{data_structure}</span></div>",
            unsafe_allow_html=True,
        )

    # ── Source Objects & EDH Table Names ─────────────────────────────────────
    st.markdown('<div class="form-divider"></div>', unsafe_allow_html=True)
    st.markdown("##### Source Objects")
    st.caption("Paste or type one value per line. Both columns must have the same number of entries.")

    col_obj, col_edh = st.columns(2, gap="large")
    with col_obj:
        source_objects_input = st.text_area(
            "Source Objects (one per line)",
            placeholder="e.g.\nclaims_header\nclaims_detail\npolicy_master",
            height=160,
            key="ss_source_objects",
        )
    with col_edh:
        edh_table_names_input = st.text_area(
            "EDH Table Names (one per line)",
            placeholder="e.g.\nedh_claims_header\nedh_claims_detail\nedh_policy_master",
            height=160,
            key="ss_edh_table_names",
        )

    # ── Main form ─────────────────────────────────────────────────────────────
    st.markdown('<div class="form-divider"></div>', unsafe_allow_html=True)
    st.markdown("##### Request Details")

    with st.form("source_systems_request"):

        # Section: Request metadata
        col_a, col_b = st.columns(2, gap="large")
        with col_a:
            render_field_intro(3, "Business unit", "Please fill the name for your business unit")
            business_unit = st.text_input(
                "Business unit", placeholder="e.g. Finance", label_visibility="collapsed"
            )
        with col_b:
            render_field_intro(4, "Requestor", "Please fill your name")
            requestor = st.text_input(
                "Requestor", placeholder="e.g. John Smith", label_visibility="collapsed"
            )

        st.markdown('<div class="form-divider"></div>', unsafe_allow_html=True)
        col_c, col_d = st.columns(2, gap="large")
        with col_c:
            render_field_intro(5, "Request date", "Automatically set to today's date")
            request_date = date.today()
            st.markdown(f"**{request_date.strftime('%d %B %Y')}**")
        with col_d:
            render_field_intro(6, "Business justification", "Please describe why this data is needed")
            business_justification = st.text_input(
                "Business justification",
                placeholder="e.g. Required for monthly reporting",
                label_visibility="collapsed",
            )

        # Section: Load configuration
        st.markdown('<div class="form-divider"></div>', unsafe_allow_html=True)
        col_e, col_f = st.columns(2, gap="large")
        with col_e:
            render_field_intro(7, "Data volume (GB)", "Please fill the estimated total size of the data in GB")
            additional_gb = st.number_input(
                "Data volume (GB)",
                min_value=0.01,
                value=1.0,
                step=0.5,
                format="%.2f",
                label_visibility="collapsed",
            )
        with col_f:
            render_field_intro(8, "Ingestion frequency", "Please select how often this data should be loaded")
            ingestion_frequency = st.selectbox(
                "Ingestion frequency",
                options=INGESTION_FREQUENCIES,
                index=None,
                placeholder="Select frequency",
                label_visibility="collapsed",
            )

        st.markdown('<div class="form-divider"></div>', unsafe_allow_html=True)
        render_field_intro(9, "Load type", "Please select whether this is a full reload (Bulk) or only changed records (Incremental)")
        load_type = st.radio(
            "Load type",
            options=LOAD_TYPES,
            horizontal=True,
            label_visibility="collapsed",
        )

        # Section: Governance
        st.markdown('<div class="form-divider"></div>', unsafe_allow_html=True)
        col_g, col_h = st.columns(2, gap="large")
        with col_g:
            render_field_intro(10, "Primary key available", "Please select whether this source has a unique identifier per record")
            primary_key_available = st.selectbox(
                "Primary key available",
                options=PRIMARY_KEY_OPTIONS,
                index=None,
                placeholder="Select",
                label_visibility="collapsed",
            )
        with col_h:
            render_field_intro(11, "Delete handling", "Please select how deleted records should be handled")
            delete_handling = st.selectbox(
                "Delete handling",
                options=DELETE_HANDLING_OPTIONS,
                index=None,
                placeholder="Select",
                label_visibility="collapsed",
            )

        st.markdown('<div class="form-divider"></div>', unsafe_allow_html=True)
        col_i, col_j = st.columns(2, gap="large")
        with col_i:
            render_field_intro(12, "Schema stability", "Please select how often you expect the structure of this data to change")
            schema_stability = st.selectbox(
                "Schema stability",
                options=SCHEMA_STABILITY_OPTIONS,
                index=None,
                placeholder="Select",
                label_visibility="collapsed",
            )
        with col_j:
            render_field_intro(13, "CDC method", "Please select how changes will be tracked for this source")
            cdc_method = st.selectbox(
                "CDC method",
                options=CDC_METHOD_OPTIONS,
                index=None,
                placeholder="Select",
                label_visibility="collapsed",
            )

        st.markdown('<div class="form-divider"></div>', unsafe_allow_html=True)
        render_field_intro(14, "Contains PHI", "Please select Yes if this data includes Protected Health Information")
        contains_phi = st.radio(
            "Contains PHI",
            options=YES_NO,
            horizontal=True,
            label_visibility="collapsed",
        )

        st.markdown('<div class="form-divider"></div>', unsafe_allow_html=True)
        st.caption("All fields are required. Please ensure everything is filled in before submitting.")
        submitted = st.form_submit_button("Submit request", type="primary")

    if submitted:
        # Validate cascade fields
        if not ingestion_method:
            st.error("Please select an Ingestion Method.")
            return
        if not source_system:
            st.error("Please select a Source System.")
            return

        # Validate source objects text areas
        source_objects_list = [v.strip() for v in source_objects_input.splitlines() if v.strip()]
        edh_table_names_list = [v.strip() for v in edh_table_names_input.splitlines() if v.strip()]

        if not source_objects_list:
            st.error("Please enter at least one Source Object.")
            return
        if not edh_table_names_list:
            st.error("Please enter at least one EDH Table Name.")
            return
        if len(source_objects_list) != len(edh_table_names_list):
            st.error(
                f"Source Objects ({len(source_objects_list)}) and EDH Table Names "
                f"({len(edh_table_names_list)}) must have the same number of entries."
            )
            return

        # Validate form fields
        missing = [
            name for name, val in [
                ("Business unit",       business_unit),
                ("Requestor",           requestor),
                ("Ingestion frequency", ingestion_frequency),
                ("Primary key",         primary_key_available),
                ("Delete handling",     delete_handling),
                ("Schema stability",    schema_stability),
                ("CDC method",          cdc_method),
            ] if not val
        ]
        if missing:
            st.error(f"Please fill in: {', '.join(missing)}")
            return

        if load_type == "Incremental" and cdc_method == "Not Applicable":
            st.error("CDC Method cannot be 'Not Applicable' when Load Type is Incremental.")
            return
        if load_type == "Bulk" and cdc_method != "Not Applicable":
            st.error("CDC Method should be 'Not Applicable' when Load Type is Bulk.")
            return

        request_id = str(uuid.uuid4())

        try:
            trigger_estimator_job(
                request_type="source_system",
                payload={
                    "request_id":             request_id,
                    "business_unit":          business_unit,
                    "request_date":           str(request_date),
                    "requestor":              requestor,
                    "business_justification": business_justification or "",
                    "ingestion_method":       ingestion_method,
                    "source_system":          source_system,
                    "data_structure":         data_structure,
                    "source_objects":         ",".join(source_objects_list),
                    "edh_table_names":        ",".join(edh_table_names_list),
                    "additional_gb":          str(additional_gb),
                    "ingestion_frequency":    ingestion_frequency,
                    "load_type":              load_type,
                    "primary_key_available":  primary_key_available,
                    "delete_handling":        delete_handling,
                    "schema_stability":       schema_stability,
                    "cdc_method":             cdc_method,
                    "contains_phi":           contains_phi,
                    "save_results":           "true",
                },
            )
        except Exception as exc:
            st.error(f"Failed to submit request: {exc}")
            return

        st.success(
            "Your request has been submitted! "
            "Cost and effort estimates will appear in Request History & Costs once processed."
        )

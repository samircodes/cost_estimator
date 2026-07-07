import uuid
from datetime import date

import pandas as pd
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

ALL_DATA_STRUCTURES = (
    "Sql Server", "Sybase", "Postgres", "csv", "parquet", "xlsb", "xls", "API", "Other",
)

_EMPTY_OBJECTS = pd.DataFrame({"Source Object": [""], "EDH Table Name": [""]})


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

    source_systems = list(INGESTION_SOURCE_MAP.get(ingestion_method or "", {}).keys()) + ["Other (not in list)"]

    with col2:
        render_field_intro(2, "Source system", "Select the source system, or choose 'Other' to type a custom value")
        source_system_select = st.selectbox(
            "Source system",
            options=source_systems,
            index=None,
            placeholder="Select source system" if ingestion_method else "Select ingestion method first",
            disabled=not ingestion_method,
            label_visibility="collapsed",
            key="ss_source_system",
        )

    is_custom_source = source_system_select == "Other (not in list)"

    if is_custom_source:
        source_system_custom = st.text_input(
            "Custom source system name",
            placeholder="e.g. MYAPP-PROD",
            key="ss_source_system_custom",
        )
        source_system = source_system_custom.strip() or None
    else:
        source_system = source_system_select

    # Auto-detect data structure from mapping; None if custom source selected
    auto_data_structure = INGESTION_SOURCE_MAP.get(ingestion_method or "", {}).get(source_system or "", None)

    # Always show data structure selector — pre-filled when auto-detected, editable always
    if ingestion_method:
        st.markdown(
            "<div class='field-intro'><span>&#8594;</span><div>"
            "<strong>Source data structure</strong>"
            "<small>Auto-filled from your source system — change if needed</small>"
            "</div></div>",
            unsafe_allow_html=True,
        )
        auto_index = list(ALL_DATA_STRUCTURES).index(auto_data_structure) if auto_data_structure in ALL_DATA_STRUCTURES else None
        data_structure = st.selectbox(
            "Source data structure",
            options=ALL_DATA_STRUCTURES,
            index=auto_index,
            placeholder="Select data structure",
            label_visibility="collapsed",
            key="ss_data_structure",
        )
    else:
        data_structure = None

    # ── Source Objects & EDH Table Names ─────────────────────────────────────
    st.markdown('<div class="form-divider"></div>', unsafe_allow_html=True)
    st.markdown("##### Source Objects")

    st.caption(
        "Add one row per object. Both Source Object and EDH Table Name are required for each row."
    )

    edited_df = st.data_editor(
        _EMPTY_OBJECTS,
        num_rows="dynamic",
        use_container_width=True,
        hide_index=True,
        column_config={
            "Source Object":   st.column_config.TextColumn("Source Object",   required=True),
            "EDH Table Name":  st.column_config.TextColumn("EDH Table Name",  required=True),
        },
        key="ss_objects_editor",
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
        if not source_system_select:
            st.error("Please select a Source System.")
            return
        if is_custom_source and not source_system:
            st.error("Please type a name for your custom source system.")
            return
        if not data_structure:
            st.error("Please select a Source Data Structure.")
            return

        # Validate source objects table
        valid_rows = edited_df.dropna(subset=["Source Object", "EDH Table Name"])
        valid_rows = valid_rows[
            (valid_rows["Source Object"].str.strip() != "") &
            (valid_rows["EDH Table Name"].str.strip() != "")
        ]
        if valid_rows.empty:
            st.error("Please add at least one Source Object and EDH Table Name.")
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
        source_objects = valid_rows["Source Object"].str.strip().tolist()
        edh_table_names = valid_rows["EDH Table Name"].str.strip().tolist()

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
                    "source_objects":         ",".join(source_objects),
                    "edh_table_names":        ",".join(edh_table_names),
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

import uuid
from datetime import date

import streamlit as st

from app_config import (
    CDC_METHOD_OPTIONS,
    DATA_FORMAT_BY_SOURCE,
    DELETE_HANDLING_OPTIONS,
    INGESTION_FREQUENCIES,
    INGESTION_MODES,
    PRIMARY_KEY_OPTIONS,
    SCHEMA_STABILITY_OPTIONS,
    SOURCE_TYPE_MAP,
    SOURCE_TYPES,
)
from databricks_client import trigger_cost_estimate_job
from ui import render_back_button, render_field_intro, render_form_heading, render_page_intro


def render_existing_source_page() -> None:
    render_back_button("back_from_existing")

    render_page_intro(
        "Existing source",
        "Add Data to Existing EDH Sources",
        "Extend an established source with new files, tables, fields, or delivery requirements.",
    )

    render_form_heading("Request Details", 13)

    with st.form("existing_source_request"):

        # ── Section 1: Request metadata ──────────────────────────────────────
        col_a, col_b = st.columns(2, gap="large")
        with col_a:
            render_field_intro(1, "Business unit", "Which department is making this request?")
            business_unit = st.text_input(
                "Business unit", placeholder="e.g. Finance", label_visibility="collapsed"
            )
        with col_b:
            render_field_intro(2, "Requestor", "Who is submitting this request?")
            requestor = st.text_input(
                "Requestor", placeholder="e.g. John Smith", label_visibility="collapsed"
            )

        st.markdown('<div class="form-divider"></div>', unsafe_allow_html=True)
        col_c, col_d = st.columns(2, gap="large")
        with col_c:
            render_field_intro(3, "Request date", "Date of this request.")
            request_date = st.date_input(
                "Request date", value=date.today(), label_visibility="collapsed"
            )
        with col_d:
            render_field_intro(4, "Business justification", "Why is this data needed?")
            business_justification = st.text_input(
                "Business justification",
                placeholder="e.g. Required for monthly reporting",
                label_visibility="collapsed",
            )

        # ── Section 2: Source & volume ────────────────────────────────────────
        st.markdown('<div class="form-divider"></div>', unsafe_allow_html=True)
        col_e, col_f = st.columns(2, gap="large")
        with col_e:
            render_field_intro(5, "Source type", "Where will the data come from?")
            source_type = st.selectbox(
                "Source type",
                options=SOURCE_TYPES,
                index=None,
                placeholder="Select a source type",
                label_visibility="collapsed",
            )
        with col_f:
            render_field_intro(6, "Data format", "Format of the source data.")
            format_options = DATA_FORMAT_BY_SOURCE.get(source_type, ()) if source_type else ()
            data_format = st.selectbox(
                "Data format",
                options=format_options,
                index=None,
                placeholder="Select source type first" if not source_type else "Select a format",
                label_visibility="collapsed",
                disabled=not source_type,
            )

        st.markdown('<div class="form-divider"></div>', unsafe_allow_html=True)
        col_g, col_h = st.columns(2, gap="large")
        with col_g:
            render_field_intro(7, "Estimated volume", "How much data will be added (GB)?")
            additional_gb = st.number_input(
                "Data volume (GB)",
                min_value=0.01,
                value=1.0,
                step=0.5,
                format="%.2f",
                label_visibility="collapsed",
            )
        with col_h:
            render_field_intro(8, "Ingestion frequency", "How often will data be loaded?")
            ingestion_frequency = st.selectbox(
                "Ingestion frequency",
                options=INGESTION_FREQUENCIES,
                index=None,
                placeholder="Select frequency",
                label_visibility="collapsed",
            )

        st.markdown('<div class="form-divider"></div>', unsafe_allow_html=True)
        render_field_intro(9, "Load type", "How should this data be loaded into EDH?")
        load_type = st.radio(
            "Load type",
            options=INGESTION_MODES,
            horizontal=True,
            label_visibility="collapsed",
        )

        # ── Section 3: Technical metadata ─────────────────────────────────────
        st.markdown('<div class="form-divider"></div>', unsafe_allow_html=True)
        col_i, col_j = st.columns(2, gap="large")
        with col_i:
            render_field_intro(10, "Primary key available", "Does this source have a primary key?")
            primary_key_available = st.selectbox(
                "Primary key",
                options=PRIMARY_KEY_OPTIONS,
                index=None,
                placeholder="Select",
                label_visibility="collapsed",
            )
        with col_j:
            render_field_intro(11, "Delete handling", "How should deleted records be handled?")
            delete_handling = st.selectbox(
                "Delete handling",
                options=DELETE_HANDLING_OPTIONS,
                index=None,
                placeholder="Select",
                label_visibility="collapsed",
            )

        st.markdown('<div class="form-divider"></div>', unsafe_allow_html=True)
        col_k, col_l = st.columns(2, gap="large")
        with col_k:
            render_field_intro(12, "Schema stability", "How often does the source schema change?")
            schema_stability = st.selectbox(
                "Schema stability",
                options=SCHEMA_STABILITY_OPTIONS,
                index=None,
                placeholder="Select",
                label_visibility="collapsed",
            )
        with col_l:
            render_field_intro(13, "CDC method", "How are changes captured in the source?")
            cdc_method = st.selectbox(
                "CDC method",
                options=CDC_METHOD_OPTIONS,
                index=None,
                placeholder="Select",
                label_visibility="collapsed",
            )

        st.markdown('<div class="form-divider"></div>', unsafe_allow_html=True)
        submitted = st.form_submit_button(
            "Submit request", type="primary", use_container_width=False
        )

    if submitted:
        # Validate required fields
        missing = [
            name for name, val in [
                ("Source type", source_type),
                ("Data format", data_format),
                ("Ingestion frequency", ingestion_frequency),
                ("Primary key available", primary_key_available),
                ("Delete handling", delete_handling),
                ("Schema stability", schema_stability),
                ("CDC method", cdc_method),
                ("Business unit", business_unit),
                ("Requestor", requestor),
            ] if not val
        ]
        if missing:
            st.error(f"Please fill in: {', '.join(missing)}")
            return

        request_id = str(uuid.uuid4())

        try:
            trigger_cost_estimate_job(
                request_id=request_id,
                business_unit=business_unit,
                request_date=str(request_date),
                requestor=requestor,
                business_justification=business_justification or "",
                primary_key_available=primary_key_available,
                delete_handling=delete_handling,
                schema_stability=schema_stability,
                cdc_method=cdc_method,
                source_type=SOURCE_TYPE_MAP[source_type],
                data_format=data_format,
                additional_gb=additional_gb,
                load_type=load_type,
                ingestion_frequency=ingestion_frequency,
            )
        except Exception as exc:
            st.error(f"Failed to submit request: {exc}")
            return

        st.success(
            "Your request has been submitted! "
            "Cost estimates will appear in Request History & Costs once processed."
        )

import uuid

import streamlit as st

from app_config import INGESTION_MODES, SOURCE_TYPE_MAP, SOURCE_TYPES
from databricks_client import trigger_cost_estimate_job
from ui import render_back_button, render_field_intro


def render_existing_source_page() -> None:
    render_back_button("back_from_existing")

    with st.form("existing_source_request"):
        volume_column, source_column = st.columns(2, gap="large")

        with volume_column:
            render_field_intro(
                1,
                "Estimated volume",
                "How much data will be added?",
            )
            data_volume_gb = st.number_input(
                "Data volume (GB)",
                min_value=0.01,
                value=1.0,
                step=0.5,
                format="%.2f",
                help="Enter the estimated volume of data to be added.",
            )

        with source_column:
            render_field_intro(
                2,
                "Source technology",
                "Where will the data come from?",
            )
            source_type = st.selectbox(
                "Source type",
                options=SOURCE_TYPES,
                index=None,
                placeholder="Select a source type",
            )

        st.markdown('<div class="form-divider"></div>', unsafe_allow_html=True)
        render_field_intro(
            3,
            "Delivery pattern",
            "How should this data be loaded into EDH?",
        )
        ingestion_mode = st.radio(
            "Ingestion mode",
            options=INGESTION_MODES,
            horizontal=True,
            help="Choose whether data arrives as ongoing changes or as a full load.",
            label_visibility="collapsed",
        )

        st.markdown(
            """
            <div class="mode-help">
                <span><strong>Incremental</strong> adds new or changed records.</span>
                <span><strong>Bulk</strong> loads the complete dataset.</span>
            </div>
            """,
            unsafe_allow_html=True,
        )

        submitted = st.form_submit_button(
            "Continue with request",
            type="primary",
            use_container_width=False,
        )

    if submitted:
        if source_type is None:
            st.error("Select a source type before continuing.")
            return

        request_id = str(uuid.uuid4())

        try:
            trigger_cost_estimate_job(
                request_id=request_id,
                data_volume_gb=data_volume_gb,
                source_type=SOURCE_TYPE_MAP[source_type],
                load_type=ingestion_mode,
            )
        except Exception as exc:
            st.error(f"Failed to submit request: {exc}")
            return

        st.success(
            "Your request has been submitted! "
            "Cost estimates will appear in Request History & Costs once processed."
        )

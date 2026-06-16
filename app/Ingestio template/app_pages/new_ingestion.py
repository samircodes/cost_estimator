import streamlit as st

from app_config import (
    CLUSTER_TYPES,
    FILE_FORMATS,
    FREQUENCIES,
    NETWORK_CONNECTIONS,
    REGIONS,
    TRANSFORMATION_COMPLEXITIES,
    VM_TYPES,
)
from ui import render_back_button, render_field_intro, render_form_heading, render_page_intro

YES_NO = ("Yes", "No")


def render_new_ingestion_page() -> None:
    render_back_button("back_from_new")

    render_page_intro(
        "New request",
        "New Ingestion Request",
        "Provide the details needed to register and onboard a new data source into EDH.",
    )

    render_form_heading("New Source Details", 16)

    with st.form("new_ingestion_request"):

        # ── Section 1: Source details ────────────────────────────────────────
        col_a, col_b = st.columns(2, gap="large")

        with col_a:
            render_field_intro(1, "Source name", "What is the name of this data source?")
            source_name = st.text_input(
                "Source name",
                placeholder="e.g. Claims Feed — Vendor ABC",
                label_visibility="collapsed",
            )

        with col_b:
            render_field_intro(2, "Data volume", "How much data is expected per load (GB)?")
            data_volume_gb = st.number_input(
                "Data volume (GB)",
                min_value=0.01,
                value=1.0,
                step=0.5,
                format="%.2f",
                label_visibility="collapsed",
            )

        st.markdown('<div class="form-divider"></div>', unsafe_allow_html=True)
        col_c, col_d = st.columns(2, gap="large")

        with col_c:
            render_field_intro(3, "SLA time", "Required delivery time from source arrival (hours).")
            sla_hours = st.number_input(
                "SLA time (hours)",
                min_value=1,
                value=24,
                step=1,
                label_visibility="collapsed",
            )

        with col_d:
            render_field_intro(4, "File format", "What format will the source files arrive in?")
            file_format = st.selectbox(
                "File format",
                options=FILE_FORMATS,
                index=None,
                placeholder="Select a format",
                label_visibility="collapsed",
            )

        # ── Section 2: Data characteristics ─────────────────────────────────
        st.markdown('<div class="form-divider"></div>', unsafe_allow_html=True)
        col_e, col_f, col_g = st.columns(3, gap="large")

        with col_e:
            render_field_intro(5, "Partitioning", "Is the source data partitioned?")
            partitioning = st.selectbox(
                "Partitioning",
                options=YES_NO,
                index=None,
                placeholder="Select",
                label_visibility="collapsed",
            )

        with col_f:
            render_field_intro(6, "Data skew", "Does the data have significant skew?")
            data_skew = st.selectbox(
                "Data skew",
                options=YES_NO,
                index=None,
                placeholder="Select",
                label_visibility="collapsed",
            )

        with col_g:
            render_field_intro(7, "Small files problem", "Will this source produce many small files?")
            small_files = st.selectbox(
                "Small files problem",
                options=YES_NO,
                index=None,
                placeholder="Select",
                label_visibility="collapsed",
            )

        st.markdown('<div class="form-divider"></div>', unsafe_allow_html=True)
        render_field_intro(8, "Frequency", "How often will data be loaded?")
        frequency = st.selectbox(
            "Frequency",
            options=FREQUENCIES,
            index=None,
            placeholder="Select a frequency",
            label_visibility="collapsed",
        )

        # ── Section 3: Network & complexity ──────────────────────────────────
        st.markdown('<div class="form-divider"></div>', unsafe_allow_html=True)
        col_h, col_i = st.columns(2, gap="large")

        with col_h:
            render_field_intro(9, "Egress", "Will data cross network boundaries (egress charges apply)?")
            egress = st.selectbox(
                "Egress",
                options=YES_NO,
                index=None,
                placeholder="Select",
                label_visibility="collapsed",
            )

        with col_i:
            render_field_intro(10, "Network connection", "How is the source system connected?")
            network_connection = st.selectbox(
                "Network connection",
                options=NETWORK_CONNECTIONS,
                index=None,
                placeholder="Select connection type",
                label_visibility="collapsed",
            )

        st.markdown('<div class="form-divider"></div>', unsafe_allow_html=True)
        render_field_intro(
            11,
            "Transformation complexity",
            "How complex are the transformations required for this source?",
        )
        transformation_complexity = st.radio(
            "Transformation complexity",
            options=TRANSFORMATION_COMPLEXITIES,
            horizontal=True,
            label_visibility="collapsed",
        )

        # ── Section 4: Infrastructure ─────────────────────────────────────────
        st.markdown('<div class="form-divider"></div>', unsafe_allow_html=True)
        col_j, col_k = st.columns(2, gap="large")

        with col_j:
            render_field_intro(12, "Region", "Which cloud region will this source be ingested in?")
            region = st.selectbox(
                "Region",
                options=REGIONS,
                index=None,
                placeholder="Select a region",
                label_visibility="collapsed",
            )

        with col_k:
            render_field_intro(13, "Cluster type", "What type of Databricks cluster will be used?")
            cluster_type = st.selectbox(
                "Cluster type",
                options=CLUSTER_TYPES,
                index=None,
                placeholder="Select cluster type",
                label_visibility="collapsed",
            )

        st.markdown('<div class="form-divider"></div>', unsafe_allow_html=True)
        col_l, col_m, col_n = st.columns(3, gap="large")

        with col_l:
            render_field_intro(14, "VM type", "What VM category will the cluster use?")
            vm_type = st.selectbox(
                "VM type",
                options=VM_TYPES,
                index=None,
                placeholder="Select VM type",
                label_visibility="collapsed",
            )

        with col_m:
            render_field_intro(15, "Nodes", "How many worker nodes will the cluster have?")
            nodes = st.number_input(
                "Nodes",
                min_value=1,
                value=2,
                step=1,
                label_visibility="collapsed",
            )

        with col_n:
            render_field_intro(16, "Runtime", "Expected job runtime in hours.")
            runtime_hours = st.number_input(
                "Runtime (hours)",
                min_value=1,
                value=1,
                step=1,
                label_visibility="collapsed",
            )

        st.markdown('<div class="form-divider"></div>', unsafe_allow_html=True)
        st.form_submit_button(
            "Submit request",
            type="primary",
            use_container_width=False,
        )

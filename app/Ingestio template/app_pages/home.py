import streamlit as st

from app_config import REQUEST_TYPES
from ui import render_request_type


def render_home_page() -> None:
    st.markdown(
        """
        <h1 class="hero-title">
            Data Onboarding <span class="accent">Tool</span>
        </h1>
        """,
        unsafe_allow_html=True,
    )
    st.markdown(
        """
        <p class="hero-copy">
            Register a new data source or expand an existing one in Ryan's
            Enterprise Data Hub.
        </p>
        """,
        unsafe_allow_html=True,
    )

    st.markdown(
        '<div class="choice-heading"><span>Choose a request type</span></div>',
        unsafe_allow_html=True,
    )

    columns = st.columns(len(REQUEST_TYPES), gap="large")
    for column, request_type in zip(columns, REQUEST_TYPES):
        with column:
            render_request_type(request_type)

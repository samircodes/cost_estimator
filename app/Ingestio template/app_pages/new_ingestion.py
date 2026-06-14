import streamlit as st

from ui import render_back_button, render_page_intro


def render_new_ingestion_page() -> None:
    render_back_button("back_from_new")

    render_page_intro(
        "New request",
        "New Ingestion Request",
        "Provide the details needed to register and onboard a new data source into EDH.",
    )

    st.markdown(
        """
        <div class="status-panel">
            <strong>Ready for the request form</strong>
            This page is separated from the landing page and ready for the
            fields, validation, and submission workflow you define next.
        </div>
        """,
        unsafe_allow_html=True,
    )

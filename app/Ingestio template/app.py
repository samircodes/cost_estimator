import streamlit as st

from app_config import (
    APP_TITLE,
    EXISTING_SOURCE_PAGE,
    HOME_PAGE,
    NEW_INGESTION_PAGE,
    REQUEST_HISTORY_PAGE,
)
from app_pages.existing_source import render_existing_source_page
from app_pages.home import render_home_page
from app_pages.new_ingestion import render_new_ingestion_page
from app_pages.request_history import render_request_history_page
from ui import apply_theme, render_header


st.set_page_config(
    page_title=APP_TITLE,
    page_icon="R",
    layout="wide",
    initial_sidebar_state="collapsed",
)

PAGES = {
    HOME_PAGE: render_home_page,
    NEW_INGESTION_PAGE: render_new_ingestion_page,
    EXISTING_SOURCE_PAGE: render_existing_source_page,
    REQUEST_HISTORY_PAGE: render_request_history_page,
}

apply_theme()
render_header()

page = st.session_state.get("page", HOME_PAGE)
PAGES.get(page, render_home_page)()

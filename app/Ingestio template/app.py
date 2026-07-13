import streamlit as st

from app_config import (
    APP_TITLE,
    HOME_PAGE,
    NEW_INGESTION_PAGE,
    REQUEST_HISTORY_PAGE,
    SOURCE_SYSTEM_PAGE,
)
from app_pages.home import render_home_page
from app_pages.new_ingestion import render_new_ingestion_page
from app_pages.request_history import render_request_history_page
from app_pages.source_systems import render_source_systems_page
from ui import apply_theme, render_admin_action, render_header


st.set_page_config(
    page_title=APP_TITLE,
    page_icon="R",
    layout="wide",
    initial_sidebar_state="collapsed",
)

PAGES = {
    HOME_PAGE: render_home_page,
    NEW_INGESTION_PAGE: render_new_ingestion_page,
    SOURCE_SYSTEM_PAGE: render_source_systems_page,
    REQUEST_HISTORY_PAGE: render_request_history_page,
}

apply_theme()
admin_action_placeholder = render_header()

page = st.session_state.get("page", HOME_PAGE)
PAGES.get(page, render_home_page)()

render_admin_action(admin_action_placeholder)

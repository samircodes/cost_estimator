from html import escape
from pathlib import Path

import streamlit as st

from app_config import HOME_PAGE, REQUEST_HISTORY_PAGE, RequestType


STYLES_PATH = Path(__file__).parent / "assets" / "styles.css"


def apply_theme() -> None:
    styles = STYLES_PATH.read_text(encoding="utf-8")
    st.markdown(f"<style>{styles}</style>", unsafe_allow_html=True)


def navigate_to(page: str) -> None:
    st.session_state.page = page
    st.rerun()


def render_header() -> None:
    brand_column, action_column = st.columns([2, 1])

    with brand_column:
        st.markdown('<div class="brand-marker"></div>', unsafe_allow_html=True)
        st.markdown('<span class="brand-name">Ryan Specialty</span>', unsafe_allow_html=True)

    with action_column:
        if st.button("Request History & Costs", key="open_request_history"):
            navigate_to(REQUEST_HISTORY_PAGE)


def render_back_button(key: str) -> None:
    if st.button("Back to request types", key=key):
        navigate_to(HOME_PAGE)


def render_page_intro(eyebrow: str, title: str, description: str) -> None:
    st.markdown(
        (
            f'<div class="eyebrow">{escape(eyebrow)}</div>'
            f'<h1 class="page-title">{escape(title)}</h1>'
            f'<p class="page-copy">{escape(description)}</p>'
        ),
        unsafe_allow_html=True,
    )


def render_request_type(request_type: RequestType) -> None:
    st.markdown(
        f"""
        <div class="request-choice">
            <div class="choice-number">
                {escape(request_type.number)} / {escape(request_type.category)}
            </div>
            <div class="card-title">{escape(request_type.title)}</div>
            <div class="card-copy">{escape(request_type.description)}</div>
        </div>
        """,
        unsafe_allow_html=True,
    )

    if st.button(
        request_type.button_label,
        type="primary" if request_type.primary else "secondary",
        key=f"open_{request_type.page}",
    ):
        navigate_to(request_type.page)


def render_form_heading(title: str, field_count: int) -> None:
    st.markdown(
        f"""
        <div class="form-heading">
            <div>
                <span class="form-kicker">Request details</span>
                <h2>{escape(title)}</h2>
            </div>
            <div class="form-step">{field_count} fields</div>
        </div>
        """,
        unsafe_allow_html=True,
    )


def render_field_intro(number: int, title: str, description: str) -> None:
    st.markdown(
        f"""
        <div class="field-intro">
            <span>{number:02d}</span>
            <div>
                <strong>{escape(title)}</strong>
                <small>{escape(description)}</small>
            </div>
        </div>
        """,
        unsafe_allow_html=True,
    )


def render_empty_state(title: str, description: str, icon: str = "DB") -> None:
    st.markdown(
        f"""
        <div class="history-empty">
            <div class="history-empty-icon">{escape(icon)}</div>
            <div>
                <strong>{escape(title)}</strong>
                <p>{escape(description)}</p>
            </div>
        </div>
        """,
        unsafe_allow_html=True,
    )

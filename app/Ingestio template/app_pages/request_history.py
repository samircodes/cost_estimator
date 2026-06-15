import streamlit as st

from databricks_client import fetch_all_estimates
from ui import render_back_button, render_empty_state, render_page_intro


def render_request_history_page() -> None:
    render_back_button("back_from_history")

    render_page_intro(
        "Dashboard",
        "Request History & Costs",
        "Review historical ingestion requests, estimated costs, and delivery status.",
    )

    try:
        rows = fetch_all_estimates()
    except Exception as exc:
        st.error(f"Could not load request history: {exc}")
        return

    if not rows:
        render_empty_state(
            "No requests yet",
            "Submitted cost estimates will appear here once the first request is processed.",
        )
        return

    total_cost = sum(r["estimated_cost_usd"] for r in rows)
    col_count, col_total = st.columns(2)
    col_count.metric("Total Requests", len(rows))
    col_total.metric("Total Estimated Cost", f"${total_cost:,.2f}")

    st.markdown("---")

    for row in rows:
        with st.container():
            left, right = st.columns([3, 1])
            with left:
                st.markdown(
                    f"**{row['source_type']}** — "
                    f"{row['data_volume_gb']:,.2f} GB — "
                    f"{row['ingestion_mode']} ingestion"
                )
                st.caption(f"Request ID: {row['request_id']}  |  Submitted: {row['submitted_at']}")
            with right:
                st.metric(
                    "Cost",
                    f"${row['estimated_cost_usd']:,.2f}",
                    delta=f"{row['estimated_duration_days']}d",
                    delta_color="off",
                )
            st.markdown('<div class="form-divider"></div>', unsafe_allow_html=True)

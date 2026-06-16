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

    # Summary metrics
    total_monthly = sum(float(r["total_monthly_cost"] or 0) for r in rows)
    total_annual  = sum(float(r["total_annual_cost"]  or 0) for r in rows)
    c1, c2, c3 = st.columns(3)
    c1.metric("Total Requests",        len(rows))
    c2.metric("Total Monthly (est.)",  f"${total_monthly:,.2f}")
    c3.metric("Total Annual (est.)",   f"${total_annual:,.2f}")

    st.markdown("---")

    for row in rows:
        with st.expander(
            f"**{row['source_type']}** — {row['additional_gb']} GB — "
            f"{row['load_type']} — ${float(row['total_monthly_cost'] or 0):,.2f}/mo",
            expanded=False,
        ):
            st.caption(
                f"Request ID: {row['request_id']}  |  "
                f"Submitted: {row['estimation_timestamp']}  |  "
                f"Layers: {row['layers']}"
            )

            st.markdown("##### Cost Breakdown")
            col_compute, col_storage, col_network = st.columns(3)

            with col_compute:
                st.metric(
                    "Compute",
                    f"${float(row['compute_cost'] or 0):,.4f}",
                    help=f"Range: ${row['compute_low']} – ${row['compute_high']}",
                )
                st.caption(f"Range: ${row['compute_low']} – ${row['compute_high']}")

            with col_storage:
                st.metric(
                    "Storage",
                    f"${float(row['storage_cost'] or 0):,.4f}",
                    help=f"Range: ${row['storage_low']} – ${row['storage_high']}",
                )
                st.caption(f"Range: ${row['storage_low']} – ${row['storage_high']}")

            with col_network:
                st.metric(
                    "Networking",
                    f"${float(row['networking_cost'] or 0):,.4f}",
                    help=f"Range: ${row['networking_low']} – ${row['networking_high']}",
                )
                st.caption(f"Range: ${row['networking_low']} – ${row['networking_high']}")

            st.markdown("##### Total")
            col_monthly, col_annual = st.columns(2)
            with col_monthly:
                st.metric(
                    "Monthly Estimate",
                    f"${float(row['total_monthly_cost'] or 0):,.2f}",
                )
                st.caption(f"Range: ${row['total_low']} – ${row['total_high']}")
            with col_annual:
                st.metric(
                    "Annual Estimate",
                    f"${float(row['total_annual_cost'] or 0):,.2f}",
                )
                st.caption(f"Range: ${row['annual_low']} – ${row['annual_high']}")

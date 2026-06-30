import streamlit as st

from databricks_client import fetch_all_estimates, fetch_all_request_details
from ui import render_back_button, render_empty_state, render_page_intro


def _fmt(val) -> str:
    try:
        f = float(val)
        if abs(f) >= 1:
            return f"${f:,.2f}"
        elif abs(f) >= 0.1:
            return f"${f:,.3f}"
        else:
            return f"${f:,.4f}"
    except (TypeError, ValueError):
        return "—"


def _cost_metric(col, label: str, value: str, low: str, high: str) -> None:
    col.markdown(
        f"<p style='margin:0 0 3px;font-size:0.72rem;color:#6b7280;font-weight:600;"
        f"text-transform:uppercase;letter-spacing:0.05em'>{label}</p>"
        f"<p style='margin:0 0 4px;font-size:1.25rem;font-weight:700;color:#111827'>{value}</p>"
        f"<p style='margin:0;font-size:0.75rem;color:#9ca3af'>{low} – {high}</p>",
        unsafe_allow_html=True,
    )


def _effort_badge(level: str) -> str:
    colours = {
        "Simple":  ("dbeafe", "1e40af"),
        "Medium":  ("fef3c7", "92400e"),
        "Complex": ("fee2e2", "991b1b"),
    }
    bg, fg = colours.get(level, ("f3f4f6", "374151"))
    return (
        f"<span style='background:#{bg};color:#{fg};padding:2px 10px;"
        f"border-radius:4px;font-size:0.78rem;font-weight:600'>{level}</span>"
    )


def _divider() -> None:
    st.markdown(
        "<hr style='margin:10px 0;border:none;border-top:1px solid #e5e7eb'>",
        unsafe_allow_html=True,
    )


def _render_existing_details(detail: dict) -> None:
    st.markdown("##### Source Details")
    c1, c2, c3 = st.columns(3)
    c1.markdown(f"**Source Type**  \n{detail.get('source_type') or '—'}")
    c2.markdown(f"**Data Format**  \n{detail.get('data_format') or '—'}")
    c3.markdown(f"**Volume**  \n{detail.get('additional_gb') or '—'} GB")

    c4, c5, c6 = st.columns(3)
    c4.markdown(f"**Load Type**  \n{detail.get('load_type') or '—'}")
    c5.markdown(f"**Frequency**  \n{detail.get('ingestion_frequency') or '—'}")
    c6.markdown(f"**CDC Method**  \n{detail.get('cdc_method') or '—'}")

    st.markdown("##### Governance")
    g1, g2, g3, g4 = st.columns(4)
    g1.markdown(f"**Primary Key**  \n{detail.get('primary_key_available') or '—'}")
    g2.markdown(f"**Delete Handling**  \n{detail.get('delete_handling') or '—'}")
    g3.markdown(f"**Schema Stability**  \n{detail.get('schema_stability') or '—'}")
    g4.markdown(f"**Contains PHI**  \n{detail.get('contains_phi') or '—'}")


def _render_new_source_details(detail: dict) -> None:
    st.markdown("##### Pipeline Details")
    p1, p2 = st.columns(2)
    p1.markdown(f"**Pipeline Name**  \n{detail.get('pipeline_name') or '—'}")
    p2.markdown(f"**Data Volume**  \n{detail.get('source_gb') or '—'} GB")

    st.markdown("##### Connection")
    c1, c2, c3 = st.columns(3)
    c1.markdown(f"**Network Source Type**  \n{detail.get('network_source_type') or '—'}")
    c2.markdown(f"**Copy Interval**  \n{detail.get('copy_interval') or '—'}")
    c3.markdown(f"**VM Type**  \n{detail.get('vm_type') or '—'}")

    egress_val = "Yes" if str(detail.get("include_egress", "")).lower() == "true" else "No"
    c4, c5, c6 = st.columns(3)
    c4.markdown(f"**Egress**  \n{egress_val}")
    c5.markdown(f"**Egress Volume**  \n{detail.get('egress_gb') or '0'} GB")
    c6.markdown(f"**SLA**  \n{detail.get('sla_time_hr') or '—'} hr")

    st.markdown("##### Data Characteristics")
    st.columns(1)[0].markdown(
        f"**Data Distribution**  \n{detail.get('data_distribution') or '—'}"
    )
    d2, d3 = st.columns(2)
    d2.markdown(f"**Delivery Pattern**  \n{detail.get('delivery_pattern') or '—'}")
    d3.markdown(f"**Partition Key**  \n{detail.get('partition_key_availability') or '—'}")

    st.markdown("##### Effort Estimation Inputs")
    e1, e2, e3 = st.columns(3)
    e1.markdown(f"**Source Complexity**  \n{detail.get('complexity_source_type') or '—'}")
    e2.markdown(f"**Transformation Logic**  \n{detail.get('transformation_logic') or '—'}")
    e3.markdown(f"**Frequency**  \n{detail.get('frequency') or '—'}")

    st.markdown("##### Governance")
    g1, g2, g3 = st.columns(3)
    g1.markdown(f"**Delete Handling**  \n{detail.get('delete_handling') or '—'}")
    g2.markdown(f"**Schema Stability**  \n{detail.get('schema_stability') or '—'}")
    g3.markdown(f"**CDC Method**  \n{detail.get('cdc_method') or '—'}")


def render_request_history_page() -> None:
    render_back_button("back_from_history")

    render_page_intro(
        "Dashboard",
        "Request History & Costs",
        "Review historical ingestion requests, estimated costs, and delivery status.",
    )

    try:
        rows = fetch_all_estimates()
        detail_map, detail_errors = fetch_all_request_details()
    except Exception as exc:
        st.error(f"Could not load request history: {exc}")
        return

    for err in detail_errors:
        st.warning(err)

    if not rows:
        render_empty_state(
            "No requests yet",
            "Submitted cost estimates will appear here once the first request is processed.",
        )
        return

    # ── Summary bar ───────────────────────────────────────────────────────────
    total_monthly = sum(float(r["total_cost_monthly"] or 0) for r in rows)
    total_annual  = sum(float(r["total_cost_annual"]  or 0) for r in rows)
    s1, s2, s3 = st.columns(3)
    s1.metric("Total Requests",       len(rows))
    s2.metric("Total Monthly (est.)", f"${total_monthly:,.2f}")
    s3.metric("Total Annual (est.)",  f"${total_annual:,.2f}")

    st.markdown("---")

    # ── Request cards ─────────────────────────────────────────────────────────
    for row in rows:
        with st.container(border=True):

            # Header: ingestion type · requestor · business unit · date
            h1, h2, h3, h4 = st.columns([1.5, 2, 2, 2])
            h1.markdown(f"**{row['ingestion_type'] or '—'}**")
            h2.markdown(f"**{row['requestor'] or '—'}**")
            h3.markdown(f"{row['business_unit'] or '—'}")
            h4.markdown(f"{row['request_date'] or '—'}")

            _divider()

            # Cost columns — midpoint value + low–high range below
            c1, c2, c3, c4 = st.columns(4)
            _cost_metric(
                c1, "Compute /mo",
                _fmt(row["compute_cost_monthly"]),
                _fmt(row["compute_cost_low"]),
                _fmt(row["compute_cost_high"]),
            )
            _cost_metric(
                c2, "Storage /mo",
                _fmt(row["storage_cost_monthly"]),
                _fmt(row["storage_cost_low"]),
                _fmt(row["storage_cost_high"]),
            )
            _cost_metric(
                c3, "Network /mo",
                _fmt(row["networking_cost_monthly"]),
                _fmt(row["networking_cost_low"]),
                _fmt(row["networking_cost_high"]),
            )
            _cost_metric(
                c4, "Total /mo",
                _fmt(row["total_cost_monthly"]),
                _fmt(row["total_cost_monthly_low"]),
                _fmt(row["total_cost_monthly_high"]),
            )

            # Effort row (populated for both existing and new source via detail_map)
            detail = detail_map.get(row["request_id"])
            effort_level = (detail or {}).get("effort_complexity_level")
            effort_est   = (detail or {}).get("effort_total_days_estimate")
            effort_min   = (detail or {}).get("effort_total_days_min")
            effort_max   = (detail or {}).get("effort_total_days_max")

            if effort_level and effort_est is not None:
                _divider()
                st.markdown(
                    f"<p style='margin:0;font-size:0.85rem;color:#374151'>"
                    f"<span style='font-weight:600;margin-right:10px'>Effort</span>"
                    f"{_effort_badge(effort_level)}"
                    f"&nbsp;&nbsp;"
                    f"<span>{effort_est} days estimated</span>"
                    f"&nbsp;&nbsp;"
                    f"<span style='color:#9ca3af'>({effort_min} – {effort_max} day range)</span>"
                    f"</p>",
                    unsafe_allow_html=True,
                )

            # Form detail expander
            with st.expander("View Submitted Form"):
                if not detail:
                    st.info(
                        "Form details not yet available — "
                        "the estimator job may still be processing."
                    )
                else:
                    st.caption(
                        f"Request ID: {row['request_id']}  |  "
                        f"Submitted: {row['estimation_timestamp']}  |  "
                        f"Contains PHI: {row['contains_phi'] or '—'}"
                    )
                    if detail["_source"] == "existing":
                        _render_existing_details(detail)
                    else:
                        _render_new_source_details(detail)

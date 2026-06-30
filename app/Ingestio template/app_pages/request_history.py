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


def _cost_cell(label: str, low: str, high: str, accent: bool = False) -> str:
    label_color = "#9ca3af"
    value_color = "#1e40af" if accent else "#111827"
    weight      = "800"    if accent else "700"
    return (
        f"<div style='padding:0 8px'>"
        f"<p style='margin:0 0 5px;font-size:0.67rem;color:{label_color};font-weight:600;"
        f"text-transform:uppercase;letter-spacing:0.07em'>{label}</p>"
        f"<p style='margin:0;font-size:0.93rem;font-weight:{weight};"
        f"color:{value_color};font-variant-numeric:tabular-nums;white-space:nowrap'>"
        f"{low}&thinsp;–&thinsp;{high}</p>"
        f"</div>"
    )


def _type_badge(ingestion_type: str) -> str:
    if (ingestion_type or "").lower().startswith("new"):
        bg, fg = "dbeafe", "1e40af"
    else:
        bg, fg = "d1fae5", "065f46"
    return (
        f"<span style='background:#{bg};color:#{fg};padding:3px 12px;"
        f"border-radius:5px;font-size:0.76rem;font-weight:700;white-space:nowrap;"
        f"letter-spacing:0.02em'>{ingestion_type or '—'}</span>"
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
        f"border-radius:4px;font-size:0.78rem;font-weight:700'>{level}</span>"
    )


def _sep() -> None:
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

    # ── Sort control ───────────────────────────────────────────────────────────
    count_col, sort_col = st.columns([3, 2])
    count_col.markdown(
        f"<p style='margin:6px 0 0;font-size:0.88rem;color:#6b7280'>"
        f"{len(rows)} request{'s' if len(rows) != 1 else ''}</p>",
        unsafe_allow_html=True,
    )
    with sort_col:
        sort_order = st.radio(
            "Sort by date",
            options=["Newest first", "Oldest first"],
            horizontal=True,
        )

    sorted_rows = rows if sort_order == "Newest first" else list(reversed(rows))

    st.markdown("")

    # ── Request cards ─────────────────────────────────────────────────────────
    for row in sorted_rows:
        with st.container(border=True):

            # Header
            h1, h2, h3 = st.columns([2, 3, 2])
            h1.markdown(_type_badge(row["ingestion_type"]), unsafe_allow_html=True)
            h2.markdown(
                f"<p style='margin:2px 0 0;font-size:0.92rem;line-height:1.5'>"
                f"<strong style='color:#111827'>{row['requestor'] or '—'}</strong>"
                f"<span style='color:#e5e7eb'> · </span>"
                f"<span style='color:#6b7280'>{row['business_unit'] or '—'}</span>"
                f"</p>",
                unsafe_allow_html=True,
            )
            h3.markdown(
                f"<p style='margin:2px 0 0;font-size:0.83rem;color:#9ca3af;"
                f"text-align:right;line-height:1.5'>{row['request_date'] or '—'}</p>",
                unsafe_allow_html=True,
            )

            _sep()

            # Cost ranges — shown as a CSS grid for even spacing and no gaps
            st.markdown(
                f"<div style='display:grid;grid-template-columns:repeat(4,1fr);"
                f"background:#f8fafc;border-radius:8px;padding:14px 8px;gap:4px'>"
                f"{_cost_cell('Compute /mo', _fmt(row['compute_cost_low']),      _fmt(row['compute_cost_high']))}"
                f"{_cost_cell('Storage /mo', _fmt(row['storage_cost_low']),      _fmt(row['storage_cost_high']))}"
                f"{_cost_cell('Network /mo', _fmt(row['networking_cost_low']),   _fmt(row['networking_cost_high']))}"
                f"{_cost_cell('Total /mo',   _fmt(row['total_cost_monthly_low']),_fmt(row['total_cost_monthly_high']), accent=True)}"
                f"</div>",
                unsafe_allow_html=True,
            )

            # Effort row
            detail = detail_map.get(row["request_id"])
            effort_level = (detail or {}).get("effort_complexity_level")
            effort_est   = (detail or {}).get("effort_total_days_estimate")
            effort_min   = (detail or {}).get("effort_total_days_min")
            effort_max   = (detail or {}).get("effort_total_days_max")

            if effort_level and effort_est is not None:
                _sep()
                st.markdown(
                    f"<p style='margin:0;font-size:0.85rem;color:#374151'>"
                    f"<span style='font-weight:600;margin-right:10px'>Effort</span>"
                    f"{_effort_badge(effort_level)}"
                    f"&nbsp;&nbsp;"
                    f"<span style='color:#374151'>{effort_est} days estimated</span>"
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

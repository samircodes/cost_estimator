from itertools import zip_longest

import streamlit as st

from databricks_client import fetch_all_estimates, fetch_all_request_details
from ui import is_admin, render_back_button, render_empty_state, render_page_intro


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


_TILE_STYLES = {
    "compute": ("eff6ff", "bfdbfe", "1e40af"),
    "storage": ("fdf4ff", "e9d5ff", "7e22ce"),
    "network": ("f0fdf4", "bbf7d0", "166534"),
    "total":   ("eef2ff", "c7d2fe", "3730a3"),
}


def _cost_cell(label: str, low: str, high: str, tile: str = "total") -> str:
    bg, border, fg = _TILE_STYLES.get(tile, _TILE_STYLES["total"])
    return (
        f"<div style='background:#{bg};border:1px solid #{border};"
        f"border-radius:10px;padding:14px 16px'>"
        f"<p style='margin:0 0 6px;font-size:0.67rem;color:#{fg};font-weight:700;"
        f"text-transform:uppercase;letter-spacing:0.07em;opacity:0.75'>{label}</p>"
        f"<p style='margin:0;font-size:0.95rem;font-weight:800;"
        f"color:#{fg};font-variant-numeric:tabular-nums;white-space:nowrap'>"
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


def _meta_line(detail: dict) -> str:
    """Small descriptive row (Ingestion Method / Source System) shown under the
    card header for non-admin users. Only fields present in the detail render."""
    fields = [
        ("Ingestion Method", detail.get("ingestion_method")),
        ("Source System", detail.get("source_system")),
        ("Source Complexity", detail.get("complexity_source_type")),
    ]
    pills = [
        (
            f"<span style='font-size:0.82rem;color:#6b7280'>"
            f"<strong style='color:#374151;font-weight:600'>{label}:</strong> {value}"
            f"</span>"
        )
        for label, value in fields
        if value
    ]
    if not pills:
        return ""
    return (
        f"<div style='display:flex;gap:20px;flex-wrap:wrap;margin:-6px 0 12px'>"
        f"{''.join(pills)}"
        f"</div>"
    )


def _render_additional_details(detail: dict) -> None:
    text = (detail.get("additional_details") or "").strip()
    if not text:
        return
    st.markdown("##### Additional Details")
    st.markdown(text)


def _render_new_source_details(detail: dict) -> None:
    st.markdown("##### Connection")
    c1, c2, c3, c4 = st.columns(4)
    c1.markdown(f"**Data Volume**  \n{detail.get('source_gb') or '—'} GB")
    c2.markdown(f"**Network Source Type**  \n{detail.get('network_source_type') or '—'}")
    c3.markdown(f"**Load Type**  \n{detail.get('copy_interval') or '—'}")
    c4.markdown(f"**VM Type**  \n{detail.get('vm_type') or '—'}")

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

    _render_additional_details(detail)


def _render_source_system_details(detail: dict) -> None:
    st.markdown("##### Source Identification")
    s1, s2, s3 = st.columns(3)
    s1.markdown(f"**Ingestion Method**  \n{detail.get('ingestion_method') or '—'}")
    s2.markdown(f"**Source System**  \n{detail.get('source_system') or '—'}")
    s3.markdown(f"**Data Structure**  \n{detail.get('data_structure') or '—'}")

    st.markdown("##### Source Objects")
    source_objects   = (detail.get("source_objects")      or "").split(",")
    edh_table_names  = (detail.get("edh_table_names")     or "").split(",")
    primary_key_cols = (detail.get("primary_key_columns") or "").split(",")
    for src, edh, pk in zip_longest(source_objects, edh_table_names, primary_key_cols, fillvalue=""):
        if src.strip():
            pk_suffix = f"  ·  PK: `{pk.strip()}`" if pk.strip() else ""
            st.markdown(f"- **{src.strip()}** → `{edh.strip()}`{pk_suffix}")

    st.markdown("##### Load Configuration")
    l1, l2, l3, l4 = st.columns(4)
    l1.markdown(f"**Volume**  \n{detail.get('additional_gb') or '—'} GB")
    l2.markdown(f"**Frequency**  \n{detail.get('ingestion_frequency') or '—'}")
    load_type = detail.get("load_type") or "—"
    if load_type == "Mix":
        load_type = f"Mix ({detail.get('bulk_table_count') or 0} bulk / {detail.get('incremental_table_count') or 0} incr)"
    l3.markdown(f"**Load Type**  \n{load_type}")
    l4.markdown(f"**VM Type**  \n{detail.get('vm_type') or '—'}")

    st.markdown("##### Governance")
    g1, g2, g3, g4 = st.columns(4)
    g1.markdown(f"**Primary Key**  \n{detail.get('primary_key_available') or '—'}")
    g2.markdown(f"**Delete Handling**  \n{detail.get('delete_handling') or '—'}")
    g3.markdown(f"**Schema Stability**  \n{detail.get('schema_stability') or '—'}")
    g4.markdown(f"**CDC Method**  \n{detail.get('cdc_method') or '—'}")

    _render_additional_details(detail)


def render_request_history_page() -> None:
    admin = is_admin()

    render_back_button("back_from_history")

    if admin:
        render_page_intro(
            "Dashboard",
            "Request History & Costs",
            "Review historical ingestion requests, estimated costs, and delivery status.",
        )
    else:
        render_page_intro(
            "Dashboard",
            "Request History",
            "Review historical ingestion requests and their submitted details.",
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
        ingestion_type = row["ingestion_type"] or "—"
        is_new  = ingestion_type.lower().startswith("new")
        accent  = "1e40af" if is_new else "065f46"

        detail       = detail_map.get(row["request_id"])
        effort_level = (detail or {}).get("effort_complexity_level")
        effort_est   = (detail or {}).get("effort_total_days_estimate")
        effort_min   = (detail or {}).get("effort_total_days_min")
        effort_max   = (detail or {}).get("effort_total_days_max")

        # Header HTML
        header_html = (
            f"<div style='display:flex;justify-content:space-between;"
            f"align-items:flex-start;margin-bottom:14px'>"
            f"  <div style='display:flex;align-items:center;gap:10px;flex-wrap:wrap'>"
            f"    {_type_badge(ingestion_type)}"
            f"    <span style='font-size:0.92rem;color:#111827'>"
            f"      <strong>{row['requestor'] or '—'}</strong>"
            f"      <span style='color:#d1d5db'> · </span>"
            f"      <span style='color:#6b7280'>{row['business_unit'] or '—'}</span>"
            f"    </span>"
            f"  </div>"
            f"  <span style='font-size:0.98rem;font-weight:600;color:#6b7280;"
            f"  white-space:nowrap;padding-top:1px'>{row['request_date'] or '—'}</span>"
            f"</div>"
        )

        meta_html = "" if admin or not detail else _meta_line(detail)

        sep = "<hr style='margin:12px 0;border:none;border-top:1px solid #e5e7eb'>"

        cost_html = ""
        if admin:
            cost_html = (
                f"<div style='display:grid;grid-template-columns:repeat(4,1fr);gap:10px'>"
                f"{_cost_cell('Compute /mo', _fmt(row['compute_cost_low']),       _fmt(row['compute_cost_high']),       'compute')}"
                f"{_cost_cell('Storage /mo', _fmt(row['storage_cost_low']),       _fmt(row['storage_cost_high']),       'storage')}"
                f"{_cost_cell('Network /mo', _fmt(row['networking_cost_low']),    _fmt(row['networking_cost_high']),    'network')}"
                f"{_cost_cell('Total /mo',   _fmt(row['total_cost_monthly_low']), _fmt(row['total_cost_monthly_high']), 'total')}"
                f"</div>"
            )

        effort_html = ""
        if admin and effort_level and effort_est is not None:
            effort_html = (
                f"{sep}"
                f"<p style='margin:0;font-size:0.85rem;color:#374151'>"
                f"<span style='font-weight:600;margin-right:10px'>Effort</span>"
                f"{_effort_badge(effort_level)}"
                f"&nbsp;&nbsp;"
                f"<span>{effort_est} days estimated</span>"
                f"&nbsp;&nbsp;"
                f"<span style='color:#9ca3af'>({effort_min} – {effort_max} day range)</span>"
                f"</p>"
            )

        with st.container(border=True):
            # Left accent bar + card body side by side
            st.markdown(
                f"<div style='display:flex;gap:16px;align-items:stretch'>"
                f"  <div style='width:5px;min-height:90px;background:#{accent};"
                f"  border-radius:4px;flex-shrink:0'></div>"
                f"  <div style='flex:1;min-width:0'>"
                f"    {header_html}"
                f"    {meta_html}"
                f"    {sep if cost_html else ''}"
                f"    {cost_html}"
                f"    {effort_html}"
                f"  </div>"
                f"</div>",
                unsafe_allow_html=True,
            )

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
                    if detail["_source"] == "source_system":
                        _render_source_system_details(detail)
                    else:
                        _render_new_source_details(detail)

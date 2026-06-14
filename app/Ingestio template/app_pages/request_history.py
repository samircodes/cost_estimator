from ui import render_back_button, render_empty_state, render_page_intro


def render_request_history_page() -> None:
    render_back_button("back_from_history")

    render_page_intro(
        "Dashboard",
        "Request History & Costs",
        "Review historical ingestion requests, estimated costs, and delivery status.",
    )

    render_empty_state(
        "Request history is not connected yet",
        (
            "Historical requests and cost information will appear here after "
            "this dashboard is connected to the Databricks catalog."
        ),
    )

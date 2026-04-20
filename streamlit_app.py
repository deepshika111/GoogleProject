from __future__ import annotations

from pathlib import Path
import sys

import pandas as pd
import streamlit as st

BASE_DIR = Path(__file__).resolve().parent
sys.path.insert(0, str(BASE_DIR / "src"))

from google_merch_store_analysis.dashboard_data import (
    filter_sessions,
    load_dashboard_dataset,
    summarize_channel_table,
    summarize_daily_table,
    summarize_funnel,
    summarize_kpis,
)

DATA_DIR = BASE_DIR / "data" / "processed"
REPORTS_DIR = BASE_DIR / "reports"


@st.cache_data(show_spinner=False)
def _load_dataset() -> tuple[pd.DataFrame, pd.DataFrame, pd.DataFrame, str, str, str | None]:
    dataset = load_dashboard_dataset(DATA_DIR, REPORTS_DIR)
    report_path = str(dataset.report_path) if dataset.report_path else None
    return dataset.sessions, dataset.channel_summary, dataset.daily_summary, dataset.mode, dataset.message, report_path


def _format_number(value: float, decimals: int = 0, prefix: str = "", suffix: str = "") -> str:
    if decimals == 0:
        return f"{prefix}{value:,.0f}{suffix}"
    return f"{prefix}{value:,.{decimals}f}{suffix}"


def _format_percent(value: float, decimals: int = 2) -> str:
    return f"{value:.{decimals}%}"


def _metric_card(title: str, value: str, subtitle: str) -> str:
    return f"""
    <div class="metric-card">
      <div class="metric-title">{title}</div>
      <div class="metric-value">{value}</div>
      <div class="metric-subtitle">{subtitle}</div>
    </div>
    """


def _inject_styles() -> None:
    st.markdown(
        """
        <style>
        .stApp {
            background:
                radial-gradient(circle at top left, rgba(15, 118, 110, 0.16), transparent 28%),
                radial-gradient(circle at top right, rgba(180, 83, 9, 0.14), transparent 24%),
                linear-gradient(180deg, #f7f3ea 0%, #f9f6ef 52%, #efe7db 100%);
        }
        .block-container {
            padding-top: 2.2rem;
            padding-bottom: 2.4rem;
        }
        .hero {
            padding: 1.4rem 1.5rem;
            border: 1px solid rgba(31, 41, 55, 0.08);
            background: rgba(255, 252, 245, 0.8);
            border-radius: 22px;
            box-shadow: 0 18px 50px rgba(31, 41, 55, 0.08);
            margin-bottom: 1rem;
        }
        .eyebrow {
            letter-spacing: 0.12em;
            text-transform: uppercase;
            font-size: 0.72rem;
            color: #0f766e;
            font-weight: 700;
        }
        .hero h1 {
            font-size: 2.1rem;
            line-height: 1.1;
            margin: 0.3rem 0 0.7rem 0;
            color: #111827;
        }
        .hero p {
            margin: 0;
            color: #374151;
            font-size: 1rem;
        }
        .metric-card {
            background: rgba(255, 252, 245, 0.88);
            border: 1px solid rgba(31, 41, 55, 0.08);
            border-radius: 18px;
            padding: 1rem 1rem 0.95rem 1rem;
            min-height: 132px;
            box-shadow: 0 12px 34px rgba(31, 41, 55, 0.07);
        }
        .metric-title {
            color: #6b7280;
            font-size: 0.82rem;
            text-transform: uppercase;
            letter-spacing: 0.08em;
            font-weight: 700;
        }
        .metric-value {
            color: #111827;
            font-size: 1.8rem;
            font-weight: 700;
            margin-top: 0.55rem;
        }
        .metric-subtitle {
            color: #4b5563;
            font-size: 0.9rem;
            margin-top: 0.35rem;
        }
        .section-card {
            background: rgba(255, 252, 245, 0.82);
            border: 1px solid rgba(31, 41, 55, 0.08);
            border-radius: 20px;
            padding: 1rem 1.1rem 1.15rem 1.1rem;
            box-shadow: 0 12px 34px rgba(31, 41, 55, 0.06);
            margin-bottom: 1rem;
        }
        .insight-note {
            padding: 0.9rem 1rem;
            border-left: 4px solid #0f766e;
            background: rgba(15, 118, 110, 0.08);
            border-radius: 12px;
            color: #134e4a;
            margin-top: 0.75rem;
        }
        .demo-badge {
            display: inline-block;
            padding: 0.3rem 0.55rem;
            border-radius: 999px;
            font-size: 0.78rem;
            font-weight: 700;
            letter-spacing: 0.05em;
            margin-bottom: 0.9rem;
        }
        .demo {
            background: rgba(180, 83, 9, 0.12);
            color: #9a3412;
        }
        .real {
            background: rgba(15, 118, 110, 0.12);
            color: #115e59;
        }
        </style>
        """,
        unsafe_allow_html=True,
    )


def _build_insights(channel_table: pd.DataFrame, sessions: pd.DataFrame) -> list[str]:
    insights: list[str] = []
    eligible = channel_table[channel_table["sessions"] >= 10].copy()
    if not eligible.empty:
        best = eligible.sort_values(["conversion_rate", "sessions"], ascending=[False, False]).iloc[0]
        worst = eligible.sort_values(["conversion_rate", "sessions"], ascending=[True, False]).iloc[0]
        insights.append(
            f"{best['channel_group']} is the strongest filtered channel at {best['conversion_rate']:.2%} conversion across "
            f"{int(best['sessions']):,} sessions."
        )
        if best["channel_group"] != worst["channel_group"]:
            insights.append(
                f"{worst['channel_group']} trails at {worst['conversion_rate']:.2%}, which is a "
                f"{(best['conversion_rate'] - worst['conversion_rate']):.2%} gap versus the leader."
            )

    if not sessions.empty:
        device_view = (
            sessions.groupby("device_category", as_index=False)
            .agg(sessions=("session_key", "count"), purchasers=("converted", "sum"))
            .sort_values("sessions", ascending=False)
        )
        device_view["conversion_rate"] = device_view["purchasers"] / device_view["sessions"]
        if len(device_view) >= 2:
            top_device = device_view.sort_values("conversion_rate", ascending=False).iloc[0]
            bottom_device = device_view.sort_values("conversion_rate", ascending=True).iloc[0]
            insights.append(
                f"{top_device['device_category'].title()} converts best in the current slice at {top_device['conversion_rate']:.2%}; "
                f"{bottom_device['device_category'].title()} is the lowest-converting device."
            )

    return insights


def main() -> None:
    st.set_page_config(
        page_title="Google Merch Funnel Dashboard",
        page_icon=":bar_chart:",
        layout="wide",
        initial_sidebar_state="expanded",
    )
    _inject_styles()

    sessions, _, _, mode, message, report_path = _load_dataset()

    st.markdown(
        f"""
        <div class="hero">
          <div class="eyebrow">Google Merchandise Store</div>
          <h1>Conversion Funnel And Channel Experiment Dashboard</h1>
          <p>Explore funnel performance, channel efficiency, device behavior, and daily trend movement from the processed GA4 outputs.</p>
        </div>
        """,
        unsafe_allow_html=True,
    )

    badge_class = "real" if mode in {"real", "sample"} else "demo"
    badge_label = "FULL REAL DATA" if mode == "real" else "REAL DATA SAMPLE" if mode == "sample" else "DEMO MODE"
    st.markdown(
        f'<div class="demo-badge {badge_class}">{badge_label}</div>',
        unsafe_allow_html=True,
    )
    st.info(message)

    min_date = sessions["session_date"].min()
    max_date = sessions["session_date"].max()

    with st.sidebar:
        st.header("Filters")
        st.caption(f"Data directory: `{DATA_DIR}`")
        channels = sorted(sessions["channel_group"].dropna().unique().tolist())
        devices = sorted(sessions["device_category"].dropna().unique().tolist())
        selected_channels = st.multiselect("Channel group", channels, default=channels)
        selected_devices = st.multiselect("Device", devices, default=devices)
        selected_dates = st.date_input(
            "Date range",
            value=(min_date, max_date),
            min_value=min_date,
            max_value=max_date,
        )
        if isinstance(selected_dates, tuple) and len(selected_dates) == 2:
            start_date, end_date = selected_dates
        else:
            start_date, end_date = min_date, max_date

        st.divider()
        st.caption("Launch locally")
        st.code("streamlit run streamlit_app.py", language="bash")
        if mode == "demo":
            st.caption("Build real outputs first with `merch-analysis build ...` to replace the demo dataset.")
        if mode == "sample":
            st.caption("Cloud is using a small real session sample. The full local dataset remains excluded from GitHub.")

    filtered_sessions = filter_sessions(
        sessions,
        channels=selected_channels,
        devices=selected_devices,
        start_date=start_date,
        end_date=end_date,
    )

    if filtered_sessions.empty:
        st.warning("No sessions match the current filter combination.")
        return

    kpis = summarize_kpis(filtered_sessions)
    funnel = summarize_funnel(filtered_sessions)
    channel_table = summarize_channel_table(filtered_sessions)
    daily_table = summarize_daily_table(filtered_sessions)
    device_matrix = (
        filtered_sessions.groupby(["channel_group", "device_category"], as_index=False)
        .agg(sessions=("session_key", "count"), purchasers=("converted", "sum"), revenue_usd=("purchase_revenue_usd", "sum"))
    )
    device_matrix["conversion_rate"] = device_matrix["purchasers"] / device_matrix["sessions"]
    insights = _build_insights(channel_table, filtered_sessions)

    metric_columns = st.columns(5)
    metric_payload = [
        ("Sessions", _format_number(kpis["sessions"]), f"{len(selected_channels)} channels selected"),
        ("Purchasers", _format_number(kpis["purchasers"]), f"{_format_percent(kpis['conversion_rate'])} of sessions"),
        ("Revenue", _format_number(kpis["revenue_usd"], 0, prefix="$"), _format_number(kpis["revenue_per_session"], 2, prefix="$", suffix=" per session")),
        ("Avg Session", _format_number(kpis["avg_session_duration_seconds"], 1, suffix="s"), "Average session duration"),
        ("Date Window", f"{start_date:%b %d} to {end_date:%b %d}", f"{len(filtered_sessions):,} filtered rows"),
    ]
    for column, payload in zip(metric_columns, metric_payload):
        with column:
            st.markdown(_metric_card(*payload), unsafe_allow_html=True)

    overview_tab, trends_tab, detail_tab = st.tabs(["Overview", "Trends", "Data Explorer"])

    with overview_tab:
        col1, col2 = st.columns([1.05, 1.2])
        with col1:
            st.markdown('<div class="section-card">', unsafe_allow_html=True)
            st.subheader("Funnel Progression")
            st.dataframe(
                funnel.style.format(
                    {
                        "sessions": "{:,.0f}",
                        "conversion_from_start": "{:.1%}",
                        "drop_off_from_previous": "{:.1%}",
                    }
                ),
                use_container_width=True,
                hide_index=True,
            )
            st.vega_lite_chart(
                funnel,
                {
                    "mark": {"type": "bar", "cornerRadiusEnd": 6},
                    "encoding": {
                        "x": {"field": "sessions", "type": "quantitative", "title": "Sessions"},
                        "y": {"field": "stage", "type": "ordinal", "sort": None, "title": None},
                        "color": {"value": "#0f766e"},
                        "tooltip": [
                            {"field": "stage", "type": "nominal"},
                            {"field": "sessions", "type": "quantitative", "format": ","},
                            {"field": "conversion_from_start", "type": "quantitative", "format": ".1%"},
                            {"field": "drop_off_from_previous", "type": "quantitative", "format": ".1%"},
                        ],
                    },
                    "height": 260,
                },
                use_container_width=True,
            )
            st.markdown("</div>", unsafe_allow_html=True)

        with col2:
            st.markdown('<div class="section-card">', unsafe_allow_html=True)
            st.subheader("Channel Leaderboard")
            st.vega_lite_chart(
                channel_table,
                {
                    "mark": {"type": "bar", "cornerRadiusEnd": 6},
                    "encoding": {
                        "x": {"field": "conversion_rate", "type": "quantitative", "axis": {"format": "%"}, "title": "Conversion Rate"},
                        "y": {"field": "channel_group", "type": "ordinal", "sort": "-x", "title": None},
                        "color": {
                            "field": "channel_group",
                            "type": "nominal",
                            "legend": None,
                            "scale": {
                                "range": ["#0f766e", "#1d4ed8", "#c2410c", "#7c3aed", "#4d7c0f", "#b91c1c"]
                            },
                        },
                        "tooltip": [
                            {"field": "channel_group", "type": "nominal"},
                            {"field": "sessions", "type": "quantitative", "format": ","},
                            {"field": "purchasers", "type": "quantitative", "format": ","},
                            {"field": "conversion_rate", "type": "quantitative", "format": ".2%"},
                            {"field": "revenue_per_session", "type": "quantitative", "format": "$.2f"},
                        ],
                    },
                    "height": 300,
                },
                use_container_width=True,
            )
            st.dataframe(
                channel_table.style.format(
                    {
                        "sessions": "{:,.0f}",
                        "purchasers": "{:,.0f}",
                        "conversion_rate": "{:.2%}",
                        "revenue_usd": "${:,.0f}",
                        "revenue_per_session": "${:,.2f}",
                        "avg_session_duration_seconds": "{:,.1f}",
                    }
                ),
                use_container_width=True,
                hide_index=True,
            )
            st.markdown("</div>", unsafe_allow_html=True)

        if insights:
            for insight in insights:
                st.markdown(f'<div class="insight-note">{insight}</div>', unsafe_allow_html=True)

        if report_path:
            with st.expander("Statistical report path"):
                st.write(f"Markdown report detected at `{report_path}`.")

    with trends_tab:
        left, right = st.columns([1.2, 1.0])
        with left:
            st.markdown('<div class="section-card">', unsafe_allow_html=True)
            st.subheader("Daily Conversion Trend")
            st.vega_lite_chart(
                daily_table,
                {
                    "mark": {"type": "line", "point": True, "strokeWidth": 3},
                    "encoding": {
                        "x": {"field": "session_date", "type": "temporal", "title": "Date"},
                        "y": {"field": "conversion_rate", "type": "quantitative", "axis": {"format": "%"}, "title": "Conversion Rate"},
                        "color": {"field": "channel_group", "type": "nominal", "title": "Channel"},
                        "tooltip": [
                            {"field": "session_date", "type": "temporal"},
                            {"field": "channel_group", "type": "nominal"},
                            {"field": "sessions", "type": "quantitative", "format": ","},
                            {"field": "purchasers", "type": "quantitative", "format": ","},
                            {"field": "conversion_rate", "type": "quantitative", "format": ".2%"},
                        ],
                    },
                    "height": 340,
                },
                use_container_width=True,
            )
            st.markdown("</div>", unsafe_allow_html=True)

        with right:
            st.markdown('<div class="section-card">', unsafe_allow_html=True)
            st.subheader("Volume Vs Efficiency")
            st.vega_lite_chart(
                channel_table,
                {
                    "mark": {"type": "circle", "opacity": 0.9, "stroke": "#111827", "strokeWidth": 0.4},
                    "encoding": {
                        "x": {"field": "sessions", "type": "quantitative", "title": "Sessions"},
                        "y": {"field": "conversion_rate", "type": "quantitative", "axis": {"format": "%"}, "title": "Conversion Rate"},
                        "size": {"field": "revenue_usd", "type": "quantitative", "title": "Revenue"},
                        "color": {"field": "channel_group", "type": "nominal", "title": "Channel"},
                        "tooltip": [
                            {"field": "channel_group", "type": "nominal"},
                            {"field": "sessions", "type": "quantitative", "format": ","},
                            {"field": "conversion_rate", "type": "quantitative", "format": ".2%"},
                            {"field": "revenue_usd", "type": "quantitative", "format": "$,.0f"},
                            {"field": "revenue_per_session", "type": "quantitative", "format": "$.2f"},
                        ],
                    },
                    "height": 340,
                },
                use_container_width=True,
            )
            st.markdown("</div>", unsafe_allow_html=True)

    with detail_tab:
        left, right = st.columns([1.05, 1.15])
        with left:
            st.markdown('<div class="section-card">', unsafe_allow_html=True)
            st.subheader("Channel x Device Matrix")
            pivot = device_matrix.pivot(index="channel_group", columns="device_category", values="conversion_rate").fillna(0)
            st.dataframe(pivot.style.format("{:.2%}"), use_container_width=True)
            st.markdown("</div>", unsafe_allow_html=True)

        with right:
            st.markdown('<div class="section-card">', unsafe_allow_html=True)
            st.subheader("Filtered Session Export Preview")
            preview_columns = [
                "session_date",
                "channel_group",
                "device_category",
                "converted",
                "purchase_revenue_usd",
                "session_duration_seconds",
                "funnel_stage",
            ]
            st.dataframe(filtered_sessions[preview_columns], use_container_width=True, hide_index=True)
            st.markdown("</div>", unsafe_allow_html=True)


if __name__ == "__main__":
    main()

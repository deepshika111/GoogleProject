from __future__ import annotations

from dataclasses import dataclass
from datetime import date, timedelta
from pathlib import Path

import pandas as pd

from google_merch_store_analysis.transform import build_channel_summary, build_daily_channel_summary


@dataclass(frozen=True)
class DashboardDataset:
    sessions: pd.DataFrame
    channel_summary: pd.DataFrame
    daily_summary: pd.DataFrame
    mode: str
    message: str
    report_path: Path | None


def load_dashboard_dataset(data_dir: Path, reports_dir: Path | None = None) -> DashboardDataset:
    sessions_path = _first_existing_path(
        [
            data_dir / "ga4_sessions.csv",
            data_dir.parent / "sample" / "ga4_sessions_sample.csv",
        ]
    )
    channel_summary_path = data_dir / "ga4_channel_summary.csv"
    daily_summary_path = data_dir / "ga4_daily_channel_summary.csv"
    report_path = None

    if reports_dir is not None:
        candidate = reports_dir / "channel_experiment_report.md"
        if candidate.exists():
            report_path = candidate

    if sessions_path is not None:
        sessions = pd.read_csv(sessions_path)
        sessions["session_date"] = pd.to_datetime(sessions["session_date"], errors="coerce").dt.date
        for column in ["session_start_ts", "session_end_ts"]:
            if column in sessions.columns:
                sessions[column] = pd.to_datetime(sessions[column], errors="coerce", utc=True)
        numeric_columns = [
            "ga_session_id",
            "ga_session_number",
            "session_duration_seconds",
            "event_count",
            "page_view_events",
            "total_product_views",
            "total_add_to_cart_events",
            "total_begin_checkout_events",
            "total_purchase_events",
            "transaction_count",
            "purchase_revenue_usd",
            "session_engaged_flag",
            "returning_user_flag",
            "bounce_like_session",
            "reached_product_view",
            "reached_add_to_cart",
            "reached_begin_checkout",
            "reached_purchase",
            "converted",
        ]
        for column in numeric_columns:
            if column in sessions.columns:
                sessions[column] = pd.to_numeric(sessions[column], errors="coerce").fillna(0)

        is_sample_data = sessions_path.name == "ga4_sessions_sample.csv"
        if channel_summary_path.exists():
            channel_summary = pd.read_csv(channel_summary_path)
        else:
            channel_summary = build_channel_summary(sessions)

        if daily_summary_path.exists():
            daily_summary = pd.read_csv(daily_summary_path)
            daily_summary["session_date"] = pd.to_datetime(daily_summary["session_date"], errors="coerce").dt.date
        else:
            daily_summary = build_daily_channel_summary(sessions)

        return DashboardDataset(
            sessions=sessions,
            channel_summary=channel_summary,
            daily_summary=daily_summary,
            mode="sample" if is_sample_data else "real",
            message=(
                "Loaded a commit-safe real GA4 session sample for Streamlit Cloud. "
                "The full processed session file is used automatically when present locally."
                if is_sample_data
                else "Loaded full processed GA4 outputs from data/processed."
            ),
            report_path=report_path,
        )

    sessions = build_demo_sessions()
    return DashboardDataset(
        sessions=sessions,
        channel_summary=build_channel_summary(sessions),
        daily_summary=build_daily_channel_summary(sessions),
        mode="demo",
        message=(
            "Processed CSVs were not found, so the dashboard is showing deterministic demo data "
            "that mirrors the real schema and layout."
        ),
        report_path=report_path,
    )


def _first_existing_path(paths: list[Path]) -> Path | None:
    for path in paths:
        if path.exists():
            return path
    return None


def build_demo_sessions() -> pd.DataFrame:
    base_date = date(2021, 1, 3)
    channel_profiles = {
        "Organic Search": {"volume": 5, "product": 0.82, "cart": 0.32, "checkout": 0.19, "purchase": 0.12, "revenue": 82},
        "Paid Search": {"volume": 6, "product": 0.78, "cart": 0.29, "checkout": 0.18, "purchase": 0.10, "revenue": 91},
        "Direct": {"volume": 4, "product": 0.63, "cart": 0.21, "checkout": 0.11, "purchase": 0.07, "revenue": 74},
        "Referral": {"volume": 3, "product": 0.58, "cart": 0.18, "checkout": 0.09, "purchase": 0.05, "revenue": 68},
    }
    device_profiles = {
        "desktop": {"multiplier": 1.12, "duration_boost": 45},
        "mobile": {"multiplier": 0.82, "duration_boost": 12},
        "tablet": {"multiplier": 0.95, "duration_boost": 28},
    }

    rows: list[dict[str, object]] = []
    session_counter = 1
    for day_index in range(14):
        session_date = base_date + timedelta(days=day_index)
        for channel_index, (channel, profile) in enumerate(channel_profiles.items()):
            for device_index, (device, device_profile) in enumerate(device_profiles.items()):
                session_volume = profile["volume"] + (1 if device == "desktop" else 0)
                for repeat in range(session_volume):
                    score = ((day_index + 2) * 17 + (channel_index + 1) * 11 + (device_index + 1) * 7 + repeat * 13) % 100
                    product_cutoff = int(profile["product"] * 100)
                    cart_cutoff = int(profile["cart"] * 100 * device_profile["multiplier"])
                    checkout_cutoff = int(profile["checkout"] * 100 * device_profile["multiplier"])
                    purchase_cutoff = int(profile["purchase"] * 100 * device_profile["multiplier"])

                    reached_product = int(score < product_cutoff)
                    reached_cart = int(score < cart_cutoff)
                    reached_checkout = int(score < checkout_cutoff)
                    converted = int(score < purchase_cutoff)

                    funnel_stage = "session_start"
                    if reached_product:
                        funnel_stage = "product_view"
                    if reached_cart:
                        funnel_stage = "add_to_cart"
                    if reached_checkout:
                        funnel_stage = "begin_checkout"
                    if converted:
                        funnel_stage = "purchase"

                    duration = 18 + day_index * 5 + device_profile["duration_boost"] + repeat * 9
                    revenue = float(profile["revenue"] * device_profile["multiplier"]) if converted else 0.0
                    returning_flag = int((day_index + repeat + channel_index) % 3 == 0)

                    rows.append(
                        {
                            "session_key": f"demo_{session_counter}",
                            "session_date": session_date,
                            "user_pseudo_id": f"user_{(session_counter % 45) + 1}",
                            "ga_session_id": session_counter,
                            "ga_session_number": 2 if returning_flag else 1,
                            "channel_group": channel,
                            "source": channel.lower().replace(" ", "_"),
                            "medium": "organic" if channel == "Organic Search" else "cpc" if channel == "Paid Search" else "referral" if channel == "Referral" else "(none)",
                            "device_category": device,
                            "country": "United States",
                            "session_start_ts": pd.Timestamp(session_date),
                            "session_end_ts": pd.Timestamp(session_date) + pd.Timedelta(seconds=duration),
                            "session_duration_seconds": float(duration),
                            "event_count": 2 + reached_product + reached_cart + reached_checkout + converted,
                            "page_view_events": 1 + reached_product,
                            "total_product_views": reached_product,
                            "total_add_to_cart_events": reached_cart,
                            "total_begin_checkout_events": reached_checkout,
                            "total_purchase_events": converted,
                            "transaction_count": converted,
                            "purchase_revenue_usd": revenue,
                            "session_engaged_flag": int(reached_product or duration > 30),
                            "returning_user_flag": returning_flag,
                            "bounce_like_session": int(not reached_product and duration < 35),
                            "reached_product_view": reached_product,
                            "reached_add_to_cart": reached_cart,
                            "reached_begin_checkout": reached_checkout,
                            "reached_purchase": converted,
                            "converted": converted,
                            "funnel_stage": funnel_stage,
                        }
                    )
                    session_counter += 1

    return pd.DataFrame(rows)


def filter_sessions(
    sessions: pd.DataFrame,
    channels: list[str] | None = None,
    devices: list[str] | None = None,
    start_date: date | None = None,
    end_date: date | None = None,
) -> pd.DataFrame:
    filtered = sessions.copy()

    if channels:
        filtered = filtered[filtered["channel_group"].isin(channels)]
    if devices:
        filtered = filtered[filtered["device_category"].isin(devices)]
    if start_date:
        filtered = filtered[filtered["session_date"] >= start_date]
    if end_date:
        filtered = filtered[filtered["session_date"] <= end_date]

    return filtered.reset_index(drop=True)


def summarize_funnel(sessions: pd.DataFrame) -> pd.DataFrame:
    stages = [
        ("Sessions", len(sessions)),
        ("Product Views", int(sessions["reached_product_view"].sum())),
        ("Add To Cart", int(sessions["reached_add_to_cart"].sum())),
        ("Checkout", int(sessions["reached_begin_checkout"].sum())),
        ("Purchases", int(sessions["reached_purchase"].sum())),
    ]

    funnel = pd.DataFrame(stages, columns=["stage", "sessions"])
    funnel["conversion_from_start"] = funnel["sessions"] / max(len(sessions), 1)
    funnel["drop_off_from_previous"] = 0.0
    for index in range(1, len(funnel)):
        previous_value = funnel.loc[index - 1, "sessions"]
        current_value = funnel.loc[index, "sessions"]
        funnel.loc[index, "drop_off_from_previous"] = 1 - (current_value / previous_value if previous_value else 0)
    return funnel


def summarize_kpis(sessions: pd.DataFrame) -> dict[str, float]:
    session_count = float(len(sessions))
    purchasers = float(sessions["converted"].sum())
    revenue = float(sessions["purchase_revenue_usd"].sum())
    return {
        "sessions": session_count,
        "purchasers": purchasers,
        "conversion_rate": purchasers / session_count if session_count else 0.0,
        "revenue_usd": revenue,
        "revenue_per_session": revenue / session_count if session_count else 0.0,
        "avg_session_duration_seconds": float(sessions["session_duration_seconds"].mean()) if session_count else 0.0,
    }


def summarize_channel_table(sessions: pd.DataFrame) -> pd.DataFrame:
    if sessions.empty:
        return build_channel_summary(sessions)

    channel_table = (
        sessions.groupby("channel_group", as_index=False)
        .agg(
            sessions=("session_key", "count"),
            purchasers=("converted", "sum"),
            revenue_usd=("purchase_revenue_usd", "sum"),
            avg_session_duration_seconds=("session_duration_seconds", "mean"),
        )
        .sort_values(["sessions", "revenue_usd"], ascending=[False, False])
        .reset_index(drop=True)
    )
    channel_table["conversion_rate"] = channel_table["purchasers"] / channel_table["sessions"]
    channel_table["revenue_per_session"] = channel_table["revenue_usd"] / channel_table["sessions"]
    return channel_table[
        [
            "channel_group",
            "sessions",
            "purchasers",
            "conversion_rate",
            "revenue_usd",
            "revenue_per_session",
            "avg_session_duration_seconds",
        ]
    ]


def summarize_daily_table(sessions: pd.DataFrame) -> pd.DataFrame:
    if sessions.empty:
        return build_daily_channel_summary(sessions)

    daily_table = (
        sessions.groupby(["session_date", "channel_group"], as_index=False)
        .agg(
            sessions=("session_key", "count"),
            purchasers=("converted", "sum"),
            revenue_usd=("purchase_revenue_usd", "sum"),
        )
        .sort_values(["session_date", "sessions"], ascending=[True, False])
        .reset_index(drop=True)
    )
    daily_table["conversion_rate"] = daily_table["purchasers"] / daily_table["sessions"]
    return daily_table

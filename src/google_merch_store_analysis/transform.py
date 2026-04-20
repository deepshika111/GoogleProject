from __future__ import annotations

from pathlib import Path

import pandas as pd

GA4_REQUIRED_COLUMNS = {
    "event_date",
    "event_timestamp",
    "user_pseudo_id",
    "ga_session_id",
    "session_key",
    "ga_session_number",
    "event_name",
    "device_category",
    "source",
    "medium",
    "channel_group",
    "session_engaged",
    "transaction_id",
    "purchase_revenue_usd",
}

UA_REQUIRED_COLUMNS = {
    "session_date",
    "session_key",
    "channel_group",
    "source",
    "medium",
    "device_category",
    "pageviews",
    "transactions",
    "revenue_usd",
}

TRUE_VALUES = {"1", "true", "t", "yes", "y"}


def _require_columns(dataframe: pd.DataFrame, required_columns: set[str], context: str) -> None:
    missing_columns = sorted(required_columns - set(dataframe.columns))
    if missing_columns:
        raise ValueError(f"{context} is missing required columns: {', '.join(missing_columns)}")


def _normalize_dimension(series: pd.Series, fallback: str) -> pd.Series:
    return (
        series.fillna("")
        .astype(str)
        .str.strip()
        .replace({"": fallback, "(not set)": fallback, "<Other>": fallback, "nan": fallback})
    )


def _column_or_default(dataframe: pd.DataFrame, column_name: str, default_value: str | int | float) -> pd.Series:
    if column_name in dataframe.columns:
        return dataframe[column_name]
    return pd.Series(default_value, index=dataframe.index)


def _parse_timestamp(series: pd.Series) -> pd.Series:
    parsed = pd.to_datetime(series, errors="coerce", utc=True)
    numeric_values = pd.to_numeric(series, errors="coerce")
    needs_numeric_parse = parsed.isna() & numeric_values.notna()
    if needs_numeric_parse.any():
        parsed.loc[needs_numeric_parse] = pd.to_datetime(
            numeric_values.loc[needs_numeric_parse],
            unit="us",
            errors="coerce",
            utc=True,
        )
    return parsed


def _truthy_to_int(series: pd.Series) -> pd.Series:
    normalized = series.fillna("").astype(str).str.strip().str.lower()
    return normalized.isin(TRUE_VALUES).astype(int)


def _count_distinct_strings(series: pd.Series) -> int:
    cleaned = (
        series.fillna("")
        .astype(str)
        .str.strip()
        .replace({"": pd.NA, "nan": pd.NA})
        .dropna()
    )
    return int(cleaned.nunique())


def load_ga4_events(csv_path: Path) -> pd.DataFrame:
    dataframe = pd.read_csv(csv_path)
    _require_columns(dataframe, GA4_REQUIRED_COLUMNS, "GA4 export")

    dataframe = dataframe.copy()
    dataframe["event_date"] = pd.to_datetime(dataframe["event_date"], errors="coerce").dt.date
    dataframe["event_timestamp"] = _parse_timestamp(dataframe["event_timestamp"])
    dataframe["ga_session_id"] = pd.to_numeric(dataframe["ga_session_id"], errors="coerce").astype("Int64")
    dataframe["ga_session_number"] = (
        pd.to_numeric(dataframe["ga_session_number"], errors="coerce").fillna(1).astype("Int64")
    )
    dataframe["purchase_revenue_usd"] = (
        pd.to_numeric(dataframe["purchase_revenue_usd"], errors="coerce").fillna(0.0)
    )
    dataframe["event_name"] = dataframe["event_name"].fillna("").astype(str).str.strip().str.lower()
    dataframe["session_key"] = _normalize_dimension(dataframe["session_key"], "missing_session")
    dataframe["device_category"] = _normalize_dimension(dataframe["device_category"], "unknown")
    dataframe["channel_group"] = _normalize_dimension(dataframe["channel_group"], "Other")
    dataframe["source"] = _normalize_dimension(dataframe["source"], "(direct)")
    dataframe["medium"] = _normalize_dimension(dataframe["medium"], "(none)")
    dataframe["country"] = _normalize_dimension(_column_or_default(dataframe, "country", ""), "Unknown")
    dataframe["transaction_id"] = (
        _column_or_default(dataframe, "transaction_id", "").fillna("").astype(str).str.strip()
    )
    dataframe["session_engaged_flag"] = _truthy_to_int(dataframe["session_engaged"])

    return dataframe.sort_values(["session_key", "event_timestamp", "event_name"]).reset_index(drop=True)


def build_session_summary(events: pd.DataFrame) -> pd.DataFrame:
    if events.empty:
        return pd.DataFrame(
            columns=[
                "session_key",
                "session_date",
                "user_pseudo_id",
                "ga_session_id",
                "ga_session_number",
                "channel_group",
                "source",
                "medium",
                "device_category",
                "country",
                "session_start_ts",
                "session_end_ts",
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
                "funnel_stage",
            ]
        )

    working = events.copy()
    working["event_date"] = pd.to_datetime(working["event_date"], errors="coerce").dt.date
    working["event_timestamp"] = _parse_timestamp(working["event_timestamp"])
    working["ga_session_number"] = (
        pd.to_numeric(working["ga_session_number"], errors="coerce").fillna(1).astype("Int64")
    )
    working["purchase_revenue_usd"] = (
        pd.to_numeric(working["purchase_revenue_usd"], errors="coerce").fillna(0.0)
    )
    if "session_engaged_flag" not in working.columns:
        working["session_engaged_flag"] = _truthy_to_int(_column_or_default(working, "session_engaged", "0"))
    if "country" not in working.columns:
        working["country"] = "Unknown"
    working["is_page_view"] = (working["event_name"] == "page_view").astype(int)
    working["is_product_view"] = (working["event_name"] == "view_item").astype(int)
    working["is_add_to_cart"] = (working["event_name"] == "add_to_cart").astype(int)
    working["is_begin_checkout"] = (working["event_name"] == "begin_checkout").astype(int)
    working["is_purchase"] = (working["event_name"] == "purchase").astype(int)

    sessions = (
        working.groupby("session_key", as_index=False)
        .agg(
            session_date=("event_date", "min"),
            user_pseudo_id=("user_pseudo_id", "first"),
            ga_session_id=("ga_session_id", "first"),
            ga_session_number=("ga_session_number", "max"),
            channel_group=("channel_group", "first"),
            source=("source", "first"),
            medium=("medium", "first"),
            device_category=("device_category", "first"),
            country=("country", "first"),
            session_start_ts=("event_timestamp", "min"),
            session_end_ts=("event_timestamp", "max"),
            event_count=("event_name", "size"),
            page_view_events=("is_page_view", "sum"),
            total_product_views=("is_product_view", "sum"),
            total_add_to_cart_events=("is_add_to_cart", "sum"),
            total_begin_checkout_events=("is_begin_checkout", "sum"),
            total_purchase_events=("is_purchase", "sum"),
            transaction_count=("transaction_id", _count_distinct_strings),
            purchase_revenue_usd=("purchase_revenue_usd", "sum"),
            session_engaged_flag=("session_engaged_flag", "max"),
        )
        .sort_values("session_date")
        .reset_index(drop=True)
    )

    sessions["session_duration_seconds"] = (
        sessions["session_end_ts"] - sessions["session_start_ts"]
    ).dt.total_seconds().fillna(0).clip(lower=0)
    sessions["returning_user_flag"] = (sessions["ga_session_number"].fillna(1) > 1).astype(int)
    sessions["bounce_like_session"] = (
        (sessions["session_engaged_flag"] == 0) & (sessions["page_view_events"] <= 1)
    ).astype(int)
    sessions["reached_product_view"] = (sessions["total_product_views"] > 0).astype(int)
    sessions["reached_add_to_cart"] = (sessions["total_add_to_cart_events"] > 0).astype(int)
    sessions["reached_begin_checkout"] = (sessions["total_begin_checkout_events"] > 0).astype(int)
    sessions["reached_purchase"] = (
        (sessions["total_purchase_events"] > 0)
        | (sessions["transaction_count"] > 0)
        | (sessions["purchase_revenue_usd"] > 0)
    ).astype(int)
    sessions["converted"] = sessions["reached_purchase"]
    sessions["funnel_stage"] = "session_start"
    sessions.loc[sessions["reached_product_view"] == 1, "funnel_stage"] = "product_view"
    sessions.loc[sessions["reached_add_to_cart"] == 1, "funnel_stage"] = "add_to_cart"
    sessions.loc[sessions["reached_begin_checkout"] == 1, "funnel_stage"] = "begin_checkout"
    sessions.loc[sessions["reached_purchase"] == 1, "funnel_stage"] = "purchase"

    return sessions[
        [
            "session_key",
            "session_date",
            "user_pseudo_id",
            "ga_session_id",
            "ga_session_number",
            "channel_group",
            "source",
            "medium",
            "device_category",
            "country",
            "session_start_ts",
            "session_end_ts",
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
            "funnel_stage",
        ]
    ]


def build_channel_summary(sessions: pd.DataFrame) -> pd.DataFrame:
    if sessions.empty:
        return pd.DataFrame(
            columns=[
                "channel_group",
                "device_category",
                "sessions",
                "purchasers",
                "conversion_rate",
                "product_view_rate",
                "add_to_cart_rate",
                "checkout_rate",
                "bounce_like_rate",
                "avg_session_duration_seconds",
                "revenue_usd",
                "revenue_per_session",
            ]
        )

    summary = (
        sessions.groupby(["channel_group", "device_category"], as_index=False)
        .agg(
            sessions=("session_key", "count"),
            purchasers=("converted", "sum"),
            product_view_sessions=("reached_product_view", "sum"),
            add_to_cart_sessions=("reached_add_to_cart", "sum"),
            checkout_sessions=("reached_begin_checkout", "sum"),
            bounce_like_sessions=("bounce_like_session", "sum"),
            avg_session_duration_seconds=("session_duration_seconds", "mean"),
            revenue_usd=("purchase_revenue_usd", "sum"),
        )
        .sort_values(["sessions", "revenue_usd"], ascending=[False, False])
        .reset_index(drop=True)
    )

    summary["conversion_rate"] = summary["purchasers"] / summary["sessions"]
    summary["product_view_rate"] = summary["product_view_sessions"] / summary["sessions"]
    summary["add_to_cart_rate"] = summary["add_to_cart_sessions"] / summary["sessions"]
    summary["checkout_rate"] = summary["checkout_sessions"] / summary["sessions"]
    summary["bounce_like_rate"] = summary["bounce_like_sessions"] / summary["sessions"]
    summary["revenue_per_session"] = summary["revenue_usd"] / summary["sessions"]

    return summary[
        [
            "channel_group",
            "device_category",
            "sessions",
            "purchasers",
            "conversion_rate",
            "product_view_rate",
            "add_to_cart_rate",
            "checkout_rate",
            "bounce_like_rate",
            "avg_session_duration_seconds",
            "revenue_usd",
            "revenue_per_session",
        ]
    ]


def build_daily_channel_summary(sessions: pd.DataFrame) -> pd.DataFrame:
    if sessions.empty:
        return pd.DataFrame(
            columns=[
                "session_date",
                "channel_group",
                "device_category",
                "sessions",
                "purchasers",
                "conversion_rate",
                "revenue_usd",
            ]
        )

    daily = (
        sessions.groupby(["session_date", "channel_group", "device_category"], as_index=False)
        .agg(
            sessions=("session_key", "count"),
            purchasers=("converted", "sum"),
            revenue_usd=("purchase_revenue_usd", "sum"),
        )
        .sort_values(["session_date", "sessions"], ascending=[True, False])
        .reset_index(drop=True)
    )
    daily["conversion_rate"] = daily["purchasers"] / daily["sessions"]
    return daily[
        [
            "session_date",
            "channel_group",
            "device_category",
            "sessions",
            "purchasers",
            "conversion_rate",
            "revenue_usd",
        ]
    ]


def load_ua_sessions(csv_path: Path) -> pd.DataFrame:
    dataframe = pd.read_csv(csv_path)
    _require_columns(dataframe, UA_REQUIRED_COLUMNS, "UA export")

    dataframe = dataframe.copy()
    dataframe["session_date"] = pd.to_datetime(dataframe["session_date"], errors="coerce").dt.date
    dataframe["channel_group"] = _normalize_dimension(dataframe["channel_group"], "Other")
    dataframe["source"] = _normalize_dimension(dataframe["source"], "(direct)")
    dataframe["medium"] = _normalize_dimension(dataframe["medium"], "(none)")
    dataframe["device_category"] = _normalize_dimension(dataframe["device_category"], "unknown")
    dataframe["country"] = _normalize_dimension(_column_or_default(dataframe, "country", ""), "Unknown")
    dataframe["pageviews"] = pd.to_numeric(dataframe["pageviews"], errors="coerce").fillna(0).astype(int)
    dataframe["transactions"] = pd.to_numeric(dataframe["transactions"], errors="coerce").fillna(0).astype(int)
    dataframe["revenue_usd"] = pd.to_numeric(dataframe["revenue_usd"], errors="coerce").fillna(0.0)
    dataframe["time_on_site_seconds"] = (
        pd.to_numeric(_column_or_default(dataframe, "time_on_site_seconds", 0), errors="coerce").fillna(0.0)
    )

    return dataframe.sort_values(["session_date", "session_key"]).reset_index(drop=True)


def build_ua_daily_summary(ua_sessions: pd.DataFrame) -> pd.DataFrame:
    if ua_sessions.empty:
        return pd.DataFrame(
            columns=[
                "session_date",
                "channel_group",
                "device_category",
                "sessions",
                "transactions",
                "conversion_rate",
                "revenue_usd",
            ]
        )

    summary = (
        ua_sessions.groupby(["session_date", "channel_group", "device_category"], as_index=False)
        .agg(
            sessions=("session_key", "count"),
            transactions=("transactions", "sum"),
            revenue_usd=("revenue_usd", "sum"),
        )
        .sort_values(["session_date", "sessions"], ascending=[True, False])
        .reset_index(drop=True)
    )
    summary["conversion_rate"] = summary["transactions"] / summary["sessions"]
    return summary[
        [
            "session_date",
            "channel_group",
            "device_category",
            "sessions",
            "transactions",
            "conversion_rate",
            "revenue_usd",
        ]
    ]

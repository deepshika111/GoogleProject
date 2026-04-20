from pathlib import Path
import sys

import pandas as pd

sys.path.insert(0, str(Path(__file__).resolve().parents[1] / "src"))

from google_merch_store_analysis.transform import build_channel_summary, build_session_summary


def _sample_events() -> pd.DataFrame:
    return pd.DataFrame(
        [
            {
                "event_date": "2021-01-01",
                "event_timestamp": "2021-01-01T00:00:00Z",
                "user_pseudo_id": "user_1",
                "ga_session_id": 1,
                "session_key": "user_1.1",
                "ga_session_number": 1,
                "event_name": "view_item",
                "device_category": "mobile",
                "country": "United States",
                "source": "google",
                "medium": "cpc",
                "channel_group": "Paid Search",
                "session_engaged": "1",
                "transaction_id": "",
                "purchase_revenue_usd": 0.0,
            },
            {
                "event_date": "2021-01-01",
                "event_timestamp": "2021-01-01T00:01:00Z",
                "user_pseudo_id": "user_1",
                "ga_session_id": 1,
                "session_key": "user_1.1",
                "ga_session_number": 1,
                "event_name": "add_to_cart",
                "device_category": "mobile",
                "country": "United States",
                "source": "google",
                "medium": "cpc",
                "channel_group": "Paid Search",
                "session_engaged": "1",
                "transaction_id": "",
                "purchase_revenue_usd": 0.0,
            },
            {
                "event_date": "2021-01-01",
                "event_timestamp": "2021-01-01T00:02:00Z",
                "user_pseudo_id": "user_1",
                "ga_session_id": 1,
                "session_key": "user_1.1",
                "ga_session_number": 1,
                "event_name": "purchase",
                "device_category": "mobile",
                "country": "United States",
                "source": "google",
                "medium": "cpc",
                "channel_group": "Paid Search",
                "session_engaged": "1",
                "transaction_id": "txn_1",
                "purchase_revenue_usd": 99.0,
            },
            {
                "event_date": "2021-01-02",
                "event_timestamp": "2021-01-02T03:00:00Z",
                "user_pseudo_id": "user_2",
                "ga_session_id": 2,
                "session_key": "user_2.2",
                "ga_session_number": 2,
                "event_name": "page_view",
                "device_category": "desktop",
                "country": "United States",
                "source": "(direct)",
                "medium": "(none)",
                "channel_group": "Direct",
                "session_engaged": "0",
                "transaction_id": "",
                "purchase_revenue_usd": 0.0,
            },
        ]
    )


def test_build_session_summary_identifies_funnel_stage() -> None:
    sessions = build_session_summary(_sample_events())

    assert len(sessions) == 2

    purchase_session = sessions.loc[sessions["session_key"] == "user_1.1"].iloc[0]
    direct_session = sessions.loc[sessions["session_key"] == "user_2.2"].iloc[0]

    assert purchase_session["converted"] == 1
    assert purchase_session["funnel_stage"] == "purchase"
    assert purchase_session["transaction_count"] == 1
    assert direct_session["bounce_like_session"] == 1
    assert direct_session["returning_user_flag"] == 1


def test_build_channel_summary_calculates_rates() -> None:
    sessions = build_session_summary(_sample_events())
    summary = build_channel_summary(sessions)

    paid_search = summary.loc[summary["channel_group"] == "Paid Search"].iloc[0]
    direct = summary.loc[summary["channel_group"] == "Direct"].iloc[0]

    assert paid_search["sessions"] == 1
    assert paid_search["conversion_rate"] == 1.0
    assert paid_search["revenue_per_session"] == 99.0
    assert direct["bounce_like_rate"] == 1.0

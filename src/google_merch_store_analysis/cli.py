from __future__ import annotations

import argparse
from pathlib import Path

from google_merch_store_analysis.storage import write_dataframes, write_sqlite_database
from google_merch_store_analysis.transform import (
    build_channel_summary,
    build_daily_channel_summary,
    build_session_summary,
    build_ua_daily_summary,
    load_ga4_events,
    load_ua_sessions,
)


def _build_command(args: argparse.Namespace) -> None:
    output_dir = Path(args.output_dir)
    output_dir.mkdir(parents=True, exist_ok=True)

    ga4_events = load_ga4_events(Path(args.events_csv))
    ga4_sessions = build_session_summary(ga4_events)
    ga4_channel_summary = build_channel_summary(ga4_sessions)
    ga4_daily_channel_summary = build_daily_channel_summary(ga4_sessions)

    outputs = {
        "ga4_events_clean": ga4_events,
        "ga4_sessions": ga4_sessions,
        "ga4_channel_summary": ga4_channel_summary,
        "ga4_daily_channel_summary": ga4_daily_channel_summary,
    }

    if args.ua_sessions_csv:
        ua_sessions = load_ua_sessions(Path(args.ua_sessions_csv))
        outputs["ua_sessions_clean"] = ua_sessions
        outputs["ua_daily_channel_summary"] = build_ua_daily_summary(ua_sessions)

    write_dataframes(output_dir, outputs)
    write_sqlite_database(output_dir / args.sqlite_name, outputs)

    print(f"Wrote {len(outputs)} tables to {output_dir}")
    print(f"SQLite database: {output_dir / args.sqlite_name}")
    print(f"GA4 sessions analyzed: {len(ga4_sessions):,}")
    print(f"GA4 purchases: {int(ga4_sessions['converted'].sum()):,}")


def _make_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        prog="merch-analysis",
        description="Build local analysis tables from Google Merchandise Store exports.",
    )
    subparsers = parser.add_subparsers(dest="command", required=True)

    build_parser = subparsers.add_parser(
        "build",
        help="Create cleaned CSVs, summary tables, and a SQLite database.",
    )
    build_parser.add_argument(
        "--events-csv",
        required=True,
        help="Path to the GA4 event export CSV created from sql/ga4_events_export.sql.",
    )
    build_parser.add_argument(
        "--ua-sessions-csv",
        help="Optional path to the UA sessions export CSV created from sql/ua_sessions_export.sql.",
    )
    build_parser.add_argument(
        "--output-dir",
        default="data/processed",
        help="Directory for CSV outputs and SQLite database.",
    )
    build_parser.add_argument(
        "--sqlite-name",
        default="google_merch_store.sqlite",
        help="SQLite database filename to create inside the output directory.",
    )
    build_parser.set_defaults(func=_build_command)

    return parser


def main() -> None:
    parser = _make_parser()
    args = parser.parse_args()
    args.func(args)

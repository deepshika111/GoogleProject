from pathlib import Path
import sys

sys.path.insert(0, str(Path(__file__).resolve().parents[1] / "src"))

from google_merch_store_analysis.dashboard_data import (
    build_demo_sessions,
    filter_sessions,
    load_dashboard_dataset,
    summarize_funnel,
    summarize_kpis,
)


def test_build_demo_sessions_has_expected_shape() -> None:
    sessions = build_demo_sessions()

    assert not sessions.empty
    assert {"session_date", "channel_group", "device_category", "converted", "funnel_stage"} <= set(sessions.columns)
    assert sessions["channel_group"].nunique() >= 4
    assert sessions["device_category"].nunique() >= 3


def test_load_dashboard_dataset_falls_back_to_demo_when_processed_files_missing(tmp_path: Path) -> None:
    reports_dir = tmp_path / "reports"
    reports_dir.mkdir()

    dataset = load_dashboard_dataset(tmp_path / "processed", reports_dir)

    assert dataset.mode == "demo"
    assert not dataset.sessions.empty
    assert "Processed CSVs were not found" in dataset.message


def test_load_dashboard_dataset_uses_real_sample_when_full_processed_file_missing(tmp_path: Path) -> None:
    sample_dir = tmp_path / "sample"
    processed_dir = tmp_path / "processed"
    reports_dir = tmp_path / "reports"
    sample_dir.mkdir()
    processed_dir.mkdir()
    reports_dir.mkdir()
    build_demo_sessions().head(12).to_csv(sample_dir / "ga4_sessions_sample.csv", index=False)

    dataset = load_dashboard_dataset(processed_dir, reports_dir)

    assert dataset.mode == "sample"
    assert len(dataset.sessions) == 12
    assert "real GA4 session sample" in dataset.message


def test_filter_and_summary_functions_work_together() -> None:
    sessions = build_demo_sessions()
    filtered = filter_sessions(sessions, channels=["Organic Search"], devices=["desktop"])
    funnel = summarize_funnel(filtered)
    kpis = summarize_kpis(filtered)

    assert not filtered.empty
    assert set(filtered["channel_group"]) == {"Organic Search"}
    assert set(filtered["device_category"]) == {"desktop"}
    assert funnel.iloc[0]["sessions"] == len(filtered)
    assert kpis["sessions"] == float(len(filtered))

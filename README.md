# Google Merchandise Store Conversion Funnel Analysis

Portfolio-ready analytics project built around Google's public ecommerce sample data. The workflow matches the project brief you shared: pull event/session data from BigQuery, clean it locally with Python, test channel-level conversion differences in R, and hand the results off in a business-friendly reporting format.

## What this repo includes

- `sql/`: BigQuery SQL for GA4 event exports, UA session exports, and a daily channel rollup.
- `src/google_merch_store_analysis/`: Python pipeline that turns exported CSVs into cleaned session tables, summary tables, and a local SQLite database.
- `streamlit_app.py`: Streamlit dashboard for funnel, channel, device, and daily performance views.
- `r/`: R script that runs proportion tests, a chi-square test, and a logistic regression against the cleaned session-level data.
- `docs/`: dashboard/reporting guidance plus resume-ready bullet templates.
- `tests/`: lightweight Python tests around the funnel transformation logic.

## Data sources

- GA4 public ecommerce dataset: `bigquery-public-data.ga4_obfuscated_sample_ecommerce.events_*`
- UA sample sessions dataset: `bigquery-public-data.google_analytics_sample.ga_sessions_*`

Official references:

- [GA4 sample ecommerce dataset](https://developers.google.com/analytics/bigquery/web-ecommerce-demo-dataset?hl=en)
- [BigQuery Sandbox](https://cloud.google.com/bigquery/docs/sandbox)

## Quick start

1. Create a Google Cloud project with BigQuery enabled.
2. Run the SQL in [sql/ga4_events_export.sql](/Users/deepshikathakur/Documents/Google/sql/ga4_events_export.sql) and export the result as `data/raw/ga4_events_export.csv`.
3. Optional: run [sql/ua_sessions_export.sql](/Users/deepshikathakur/Documents/Google/sql/ua_sessions_export.sql) and export the result as `data/raw/ua_sessions_export.csv`.
4. Create a virtual environment and install the package:

```bash
python3 -m venv .venv
source .venv/bin/activate
python -m pip install --upgrade pip
python -m pip install -e .[dev,app]
```

5. Build the local analysis tables:

```bash
merch-analysis build \
  --events-csv data/raw/ga4_events_export.csv \
  --ua-sessions-csv data/raw/ua_sessions_export.csv \
  --output-dir data/processed
```

6. Run the statistical layer in R:

```bash
Rscript r/channel_experiment_analysis.R \
  data/processed/ga4_sessions.csv \
  reports/channel_experiment_report.md
```

7. Launch the Streamlit dashboard:

```bash
streamlit run streamlit_app.py
```

## Outputs

Running the Python pipeline writes these files into `data/processed/`:

- `ga4_events_clean.csv`
- `ga4_sessions.csv`
- `ga4_channel_summary.csv`
- `ga4_daily_channel_summary.csv`
- `google_merch_store.sqlite`
- `ua_sessions_clean.csv` and `ua_daily_channel_summary.csv` if the UA export is provided

The R script writes a Markdown report to `reports/channel_experiment_report.md`.

The Streamlit app reads the processed CSVs from `data/processed/`. If those files are not there yet, it opens in a clearly labeled demo mode so you can still review the dashboard layout.

## Suggested storytelling angle

Use channels as pseudo-experimental groups:

- Frame the question as: "Which acquisition channels convert significantly better once we control for device and session behavior?"
- Use `prop.test` for pairwise conversion-rate comparisons.
- Use `chisq.test` to test whether purchase outcomes differ by channel.
- Use logistic regression to separate channel effects from device, engagement, session duration, and returning-user behavior.

That gives you both stakeholder-friendly findings and a more defensible statistical layer than a simple dashboard-only project.

## Validation

```bash
pytest
python3 -m py_compile src/google_merch_store_analysis/*.py tests/test_transform.py
```

## Notes

- The repo does not include exported data because the public dataset is already hosted by Google and the raw exports can be large.
- The GA4 sample data is obfuscated, so you should expect some placeholder values and imperfect internal consistency.

# FastFlowTransform project scaffold

This project was created with `fft init`.
Next steps:
1. Update `profiles.yml` with real connection details (docs/Profiles.md).
2. Add sources in `sources.yml` and author models under `models/` (docs/Config_and_Macros.md).
3. Seed sample data with `fft seed` and execute models with `fft run` (docs/Quickstart.md).

## Engines

- DuckDB/Postgres/Databricks Spark are pre-wired. Use `make demo ENGINE=duckdb|postgres|databricks_spark`.
- BigQuery (pandas or BigFrames) mirrors the basic demo setup. Set `ENGINE=bigquery` and optionally `BQ_FRAME=pandas|bigframes` (default bigframes), then run `make demo ENGINE=bigquery BQ_FRAME=bigframes`.
- Sample env files: `.env.dev_bigquery_bigframes` and `.env.dev_bigquery_pandas` contain the required `FF_BQ_*` variables and `GOOGLE_APPLICATION_CREDENTIALS` hint.

# Hooks demo

Small FFT project that demonstrates run- and model-level hooks:

- `on_run_start` / `on_run_end` lifecycle hooks for audit logging and notifications.
- `before_model` / `after_model` hooks that record per-model audit events based on selectors.

The data model is intentionally tiny: a single staging model (`events_clean`) and a daily mart
(`mart_events_daily`). Hooks write into two audit tables:

- `_ff_run_audit`  – one row per fft run (start/end, status)
- `_ff_model_audit` – one row per model event (model start/end, status, row counts)

See `project.yml` for the hook configuration and `hooks/notify.py` for the example Python hooks.

You can run the demo on DuckDB, Postgres, Databricks Spark, or BigQuery:

  make demo ENGINE=duckdb
  make demo ENGINE=postgres
  make demo ENGINE=databricks_spark
  make demo ENGINE=bigquery BQ_FRAME=bigframes

Inspect the DAG under `site/dag/index.html` and the audit tables in your engine.

# Materializations demo

FastFlowTransform example highlighting materialized views/tables, incremental models, and Python emitters.
See `docs/examples/Materializations_Demo.md` for a full walkthrough.

## Engines

- DuckDB/Postgres/Databricks Spark are wired via the Makefile: `make demo ENGINE=duckdb|postgres|databricks_spark`.
- BigQuery supports both pandas and BigFrames clients. Copy `.env.dev_bigquery_pandas` (or `_bigframes`),
  set `GOOGLE_APPLICATION_CREDENTIALS`, and run `make demo ENGINE=bigquery BQ_FRAME=pandas|bigframes`.
- Snowflake Snowpark mirrors the basic demo setup. Copy `.env.dev_snowflake`, install `fastflowtransform[snowflake]`,
  then run `make demo ENGINE=snowflake_snowpark`.

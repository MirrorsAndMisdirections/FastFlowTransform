# Macros demo

FastFlowTransform example that highlights SQL & Python macros. See `docs/examples/Macros_Demo.md`
for a full walkthrough.

## Engines

- DuckDB/Postgres/Databricks Spark are pre-wired. Run `make demo ENGINE=duckdb|postgres|databricks_spark`.
- BigQuery (pandas or BigFrames) mirrors the basic demo setup. Set `ENGINE=bigquery` and optionally
  `BQ_FRAME=pandas|bigframes` (default `bigframes`), then run `make demo ENGINE=bigquery BQ_FRAME=<frame>`.
- Snowflake Snowpark is available via `ENGINE=snowflake_snowpark`. Copy `.env.dev_snowflake` and install
  `fastflowtransform[snowflake]` before running `make demo ENGINE=snowflake_snowpark`.

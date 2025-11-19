# Basic demo

Minimal FFT pipeline that runs unchanged on DuckDB, Postgres, Databricks Spark, BigQuery, and Snowflake (Snowpark).

## How to use
- See the full walkthrough (env setup, Makefile targets, engine notes, DQ tests) in `docs/examples/Basic_Demo.md`.
- From this directory: set the desired `.env.dev_*` (for BigQuery choose `.env.dev_bigquery_pandas` or `.env.dev_bigquery_bigframes`), then run `make demo ENGINE=<duckdb|postgres|databricks_spark|bigquery|snowflake_snowpark>` (set `BQ_FRAME` to switch BigQuery client) to seed → run → dag → test.
- To inspect results, open `site/dag/index.html` after a run or query the mart tables via your engine client.

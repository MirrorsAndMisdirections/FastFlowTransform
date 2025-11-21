# Incremental demo

Small FFT example that showcases incremental models and Delta/Iceberg-style merges
across DuckDB, Postgres, Databricks Spark, BigQuery (pandas or BigFrames), and Snowflake Snowpark.

## How to use
- Fill an `.env.dev_*` for your engine (DuckDB/Postgres/Databricks/BigQuery/Snowflake). For Databricks pick the format-specific files:
  - `.env.dev_databricks_parquet`
  - `.env.dev_databricks_delta`
  - `.env.dev_databricks_iceberg`
  Each profile uses its own managed database/warehouse so switching formats never reuses stale tables.
  For BigQuery use `.env.dev_bigquery_pandas` or `.env.dev_bigquery_bigframes`; for Snowflake use `.env.dev_snowflake`.
- From this directory run `make demo ENGINE=<duckdb|postgres|databricks_spark|bigquery|snowflake_snowpark>` (set `BQ_FRAME` for BigQuery, `DBR_TABLE_FORMAT` for Spark).
- Artifacts: DAG HTML in `site/dag/index.html`, FFT metadata in `.fastflowtransform/target/`.
- See `docs/examples/Incremental_Demo.md` for a full walkthrough of the models and incremental configs.

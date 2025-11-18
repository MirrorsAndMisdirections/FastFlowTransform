# Incremental demo

Small FFT example that showcases incremental models and Delta/Iceberg-style merges
across DuckDB, Postgres, Databricks Spark, and BigQuery (pandas or BigFrames).

## How to use
- Fill an `.env.dev_*` for your engine (DuckDB/Postgres/Databricks/BigQuery). For BigQuery use `.env.dev_bigquery_pandas` or `.env.dev_bigquery_bigframes` plus a service-account key in `secrets/`.
- From this directory run `make demo ENGINE=<duckdb|postgres|databricks_spark|bigquery>` (set `BQ_FRAME` to switch BigQuery client; set `DBR_TABLE_FORMAT` for Spark).
- Artifacts: DAG HTML in `site/dag/index.html`, FFT metadata in `.fastflowtransform/target/`.
- See `docs/examples/Incremental_Demo.md` for a full walkthrough of the models and incremental configs.

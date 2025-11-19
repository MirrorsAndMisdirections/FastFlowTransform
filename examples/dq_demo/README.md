# Data Quality Demo

Run the complete DQ demo (seeds → models → DAG → tests) on DuckDB, Postgres, Databricks Spark, BigQuery (pandas or BigFrames), or Snowflake Snowpark.

## Quickstart
From this directory:

1) Pick an engine and copy the matching `.env.dev_*` to `.env` (edit project/dataset if needed):
   - DuckDB: `.env.dev_duckdb`
   - Postgres: `.env.dev_postgres`
   - Databricks Spark: `.env.dev_databricks`
   - BigQuery (pandas): `.env.dev_bigquery_pandas`
   - BigQuery (BigFrames): `.env.dev_bigquery_bigframes`
   - Snowflake Snowpark: `.env.dev_snowflake`

2) Run the demo (set `BQ_FRAME` when using BigQuery):
   ```sh
   make demo ENGINE=duckdb
   make demo ENGINE=postgres
   make demo ENGINE=databricks_spark
   make demo ENGINE=bigquery BQ_FRAME=pandas      # or bigframes
   make demo ENGINE=snowflake_snowpark            # install fastflowtransform[snowflake]
   ```

Artifacts:
- Target metadata: `.fastflowtransform/target/{manifest.json,run_results.json,catalog.xml}`
- DAG HTML: `site/dag/index.html`

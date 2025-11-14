## Local Engine Setup

### DuckDB

- Copy `.env.dev_duckdb` and adjust `FF_DUCKDB_PATH` if you want a different location (default: `.local/api_demo.duckdb`).  
  Optionally set `FF_DUCKDB_SCHEMA` (default schema for models/seeds) and `FF_DUCKDB_CATALOG` (catalog alias) if you need to isolate namespaces.
- Create the target directory once: `mkdir -p examples/api_demo/.local`.
- Run `make ENGINE=duckdb seed run` to build the seeds and models inside the DuckDB file.

### Postgres

- Start a local database, e.g. via Docker:  
  `docker run --name fft-postgres -e POSTGRES_PASSWORD=postgres -p 5432:5432 -d postgres:15`.
- Set `FF_PG_DSN` in `.env.dev_postgres` (for example `postgresql+psycopg://postgres:postgres@localhost:5432/fft`) and optionally override `FF_PG_SCHEMA` (defaults to `api_demo`).  
  The executor ensures the schema exists via `CREATE SCHEMA IF NOT EXISTS` on first connect.
- Execute `make ENGINE=postgres seed run` to materialize seeds and models in Postgres.

### Databricks Spark (local)

- Install Java (JDK ≥ 17) and declare `JAVA_HOME`, for example:  
  `brew install openjdk@17`  
  `echo 'JAVA_HOME=/opt/homebrew/opt/openjdk@17' >> examples/api_demo/.env.dev_databricks`.
- Optionally tweak `FF_SPARK_MASTER` / `FF_SPARK_APP_NAME` in `.env.dev_databricks` (default: `local[*]`).
- To persist tables across separate `seed`/`run` sessions, enable the bundled Hive metastore defaults:  
  `FF_DBR_ENABLE_HIVE=1`, `FF_DBR_WAREHOUSE_DIR=examples/api_demo/spark-warehouse`, `FF_DBR_DATABASE=api_demo`.
- Switch the physical format by setting `FF_DBR_TABLE_FORMAT` (e.g. `delta`, requires the Delta Lake runtime); extra writer options can be supplied via `profiles.yml → databricks_spark.table_options`.
- Ensure your shell loads `.env.dev_databricks` (via `make`, `direnv`, or manual export) and run `make ENGINE=databricks_spark seed run`.

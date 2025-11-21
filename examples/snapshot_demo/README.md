# Snapshot demo

`examples/snapshot_demo` reuses the basic users pipeline and adds snapshot models that keep
slowly-changing history tables. It now mirrors the incremental demo by supporting Spark parquet,
Delta Lake, and Iceberg targets through the Databricks/Spark executor.

## Environment files

Copy one of the `.env.dev_*` files and export it before running `make`:

| File | Purpose |
| --- | --- |
| `.env.dev_duckdb` | Local DuckDB file for the demo |
| `.env.dev_postgres` | Postgres DSN/schema |
| `.env.dev_databricks_parquet` | Local Spark defaults for managed parquet tables |
| `.env.dev_databricks_delta` | Local Spark defaults for Delta Lake tables |
| `.env.dev_databricks_iceberg` | Spark 4+/Databricks configuration with the Iceberg catalog wired in |

Each Databricks profile uses its own managed database/warehouse (`snapshot_demo_parquet`,
`snapshot_demo_delta`, `snapshot_demo_iceberg`) so switching formats never reuses stale metadata.

`FF_DBR_TABLE_FORMAT` can always override the physical Spark table format (`parquet`, `delta`,
`iceberg`) even if the profile defaults differ.

## Running the demo

```bash
# DuckDB / Postgres
make snapshot_demo ENGINE=duckdb
make snapshot_demo ENGINE=postgres

# Databricks / Spark: switch table format via DBR_TABLE_FORMAT
make snapshot_demo ENGINE=databricks_spark DBR_TABLE_FORMAT=parquet
make snapshot_demo ENGINE=databricks_spark DBR_TABLE_FORMAT=delta
make snapshot_demo ENGINE=databricks_spark DBR_TABLE_FORMAT=iceberg
```

Under the hood `make snapshot_demo` executes `fft seed`, `fft run`, `fft snapshot run`, `fft dag`,
and `fft test` for the models tagged with `example:snapshot_demo`. Use `make snapshot ENGINE=...`
if you only want to update the snapshot materializations after a regular `fft run`.

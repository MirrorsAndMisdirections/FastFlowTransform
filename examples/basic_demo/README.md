# Basic demo

This project is a minimal FastFlowTransform pipeline that works unchanged on DuckDB, Postgres, and Databricks Spark. It ships with:
- `seeds/seed_users.csv` – three sample users that bootstrap the project.
- `models/staging/users_clean.ff.sql` – normalizes emails and signup timestamps.
- `models/marts/mart_users_by_domain.ff.sql` – aggregates users by email domain.
- `models/engines/*/mart_latest_signup.ff.py` – engine-scoped Python models (pandas for DuckDB/Postgres, PySpark for Databricks) that grab the latest signup per domain from the staging view.

## Quickstart

1. Install the package and CLI (see repository root instructions).
2. `cd examples/basic_demo` (this folder) so relative paths line up.
3. Load one of the provided engine environments, then seed and run the project.

> ⚠️ `make clean` (or direct calls to `cleanup_env.py`) rely on the same environment variables as the run commands. Always export the `.env.dev_*` file for the engine you are cleaning so paths, schemas, and credentials are available.

### DuckDB

```bash
cp .env.dev_duckdb .env.local            # optional convenience copy
set -a; source .env.dev_duckdb; set +a    # export FF_DUCKDB_PATH
fft seed basic_demo --env dev_duckdb
fft run basic_demo --env dev_duckdb
fft show basic_demo.mart_users_by_domain --env dev_duckdb
fft show basic_demo.mart_latest_signup --env dev_duckdb
```

### Postgres

```bash
cp .env.dev_postgres .env.local           # fill in FF_PG_DSN with your credentials
set -a; source .env.dev_postgres; set +a
fft seed basic_demo --env dev_postgres
fft run basic_demo --env dev_postgres
fft show basic_demo.mart_users_by_domain --env dev_postgres
fft show basic_demo.mart_latest_signup --env dev_postgres
```

### Databricks Spark (local or hosted)

```bash
cp .env.dev_databricks .env.local         # adjust Spark master / credentials as needed
set -a; source .env.dev_databricks; set +a
fft seed basic_demo --env dev_databricks
fft run basic_demo --env dev_databricks
fft show basic_demo.mart_users_by_domain --env dev_databricks
fft show basic_demo.mart_latest_signup --env dev_databricks
```

The resulting tables report user counts per email domain and spotlight the most recent signup per domain. Extend any of the CSV, SQL, or Python assets to explore more complex scenarios.

Further background is documented in [`docs/examples/Basic_Demo.md`](../../docs/examples/Basic_Demo.md).

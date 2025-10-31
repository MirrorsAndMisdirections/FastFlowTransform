# Models directory

This demo ships with:
- `staging/users_clean.ff.sql` – normalizes the seeded users table.
- `marts/mart_users_by_domain.ff.sql` – aggregates signups per email domain.
- `engines/*/mart_latest_signup.ff.py` – engine-scoped Python models (pandas for DuckDB/Postgres, PySpark for Databricks) that select the most recent signup per domain using the staging view as input.

Add further SQL (`*.ff.sql`) or Python (`*.ff.py`) models alongside them to grow the pipeline.

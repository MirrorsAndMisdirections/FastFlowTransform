# packages_demo main project

This is a normal FastFlowTransform project that **consumes** models
and macros from:

- a **local path package** at `../shared_package`
- optionally, a **git-based package** declared in `packages.yml`

Key pieces:

- `packages.yml` – declares `shared_package` (local path) and shows an example
  git package entry.
- `profiles.yml` – DuckDB connection profile.
- `seeds/seed_users.csv` + `sources.yml` – seed + source for CRM users.
- `models/marts/mart_users_from_package.ff.sql` – uses `ref('users_base.ff')`
  where `users_base.ff` lives in the shared package.

Typical workflow:

```bash
cd examples/packages_demo/main_project

# Configure env (DuckDB path, engine)
set -a; source env.dev_duckdb; set +a

# Run the full demo on DuckDB
make demo ENGINE=duckdb

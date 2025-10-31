# Basic Demo Project

The `examples/basic_demo` project shows the smallest end-to-end FastFlowTransform pipeline. It combines one seed, a staging model, and a final mart while staying portable across DuckDB, Postgres, and Databricks Spark.

## Why it exists
- **Start small** – demonstrate the minimum folder structure (`seeds/`, `models/`, `profiles.yml`) needed to run `fft`.
- **Engine parity** – prove that a single project can target multiple engines by swapping profiles.
- **Understand outputs** – show where documentation and manifests land after a run.

Use it as a sandbox before adding your own sources, macros, or Python models.

## Project layout

| Path | Purpose |
|------|---------|
| `seeds/seed_users.csv` | Sample CRM-style user data. `fft seed` materializes it as `crm.users`. |
| `models/staging/users_clean.ff.sql` | Normalizes emails, casts types, and tags the model for all engines. |
| `models/marts/mart_users_by_domain.ff.sql` | Aggregates users per email domain and records the first/last signup dates. |
| `models/engines/*/mart_latest_signup.ff.py` | Engine-specific Python models (pandas for DuckDB/Postgres, PySpark for Databricks) selecting the most recent signup per domain from the staging view. |
| `profiles.yml` | Declares `dev_duckdb`, `dev_postgres`, and `dev_databricks` profiles driven by environment variables. |
| `.env.dev_*` | Template environment files you can `source` per engine. |
| `Makefile` | One command (`make demo ENGINE=…`) to seed, run, document, test, and preview results. |

## Running the demo

1. `cd examples/basic_demo`
2. Choose an engine and export its environment variables:
   ```bash
   set -a; source .env.dev_duckdb; set +a
   # swap to .env.dev_postgres or .env.dev_databricks for other engines
   ```
3. Execute the full flow:
   ```bash
   make demo ENGINE=duckdb
   ```
   The Makefile runs `fft seed`, `fft run`, `fft dag`, `fft test`, and `fft show basic_demo.mart_users_by_domain`. To preview the Python mart, run `make show ENGINE=duckdb SHOW_MODEL=mart_latest_signup` (or swap `ENGINE` as needed).
4. Inspect artifacts:
   - `.fastflowtransform/target/manifest.json` and `run_results.json`
   - `site/dag/index.html` for the rendered model graph
   - CLI output from `fft show` displaying the aggregated mart

The demo also enables baseline data quality checks in `project.yml`. Running `fft test` (or `make test`) verifies that primary keys remain unique/not-null across `seed_users`, `users_clean`, `mart_users_by_domain`, and the Python mart, while ensuring aggregate metrics such as `user_count` never drop below zero and each domain appears only once in `mart_latest_signup`.

## Next steps

- Add more CSVs under `seeds/` and declare them in `sources.yml`.
- Create additional staging models so marts can reuse normalized data.
- Introduce Python models or macros mirroring how the API demo scales up.
- Update `.env.dev_*` with real credentials once you connect to shared databases.

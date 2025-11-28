# Basic Demo Project

The `examples/basic_demo` project shows the smallest end-to-end FastFlowTransform pipeline. It combines one seed, a staging model, and a final mart while staying portable across DuckDB, Postgres, Databricks Spark, and BigQuery.

## Why it exists

- **Start small** – demonstrate the minimum folder structure (`seeds/`, `models/`, `profiles.yml`) needed to run `fft`.
- **Engine parity** – prove that a single project can target multiple engines by swapping profiles.
- **Cloud & local** – show that the same project runs both on local engines (DuckDB/Postgres/Spark) and in a cloud warehouse (BigQuery).
- **Understand outputs** – show where documentation and manifests land after a run.

Use it as a sandbox before adding your own sources, macros, or Python models.

## Project layout

| Path | Purpose |
|------|---------|
| `seeds/seed_users.csv` | Sample CRM-style user data. `fft seed` materializes it as a physical `seed_users` table in the active engine (schema/dataset depends on the profile). |
| `models/staging/users_clean.ff.sql` | Normalizes emails, casts types, and tags the model for all engines. |
| `models/marts/mart_users_by_domain.ff.sql` | Aggregates users per email domain and records the first/last signup dates. |
| `models/engines/*/mart_latest_signup.ff.py` | Engine-specific Python models selecting the most recent signup per domain from the staging view:<br>• pandas for DuckDB/Postgres<br>• PySpark for Databricks<br>• BigQuery DataFrames (BigFrames) for BigQuery. |
| `tests/unit/*.yml` | Model unit-test specs for the demo models (`users_clean`, `mart_users_by_domain`, `mart_latest_signup`), used by `fft utest` and `make utest ENGINE=…`. |
| `profiles.yml` | Declares `dev_duckdb`, `dev_postgres`, `dev_databricks`, and `dev_bigquery` profiles driven by environment variables. |
| `.env.dev_*` | Template environment files you can `source` per engine (`.env.dev_duckdb`, `.env.dev_postgres`, `.env.dev_databricks`, `.env.dev_bigquery`). |
| `Makefile` | One command (`make demo ENGINE=…`) to seed, run, unit-test, document, test, and preview results. |

## Running the demo

1. `cd examples/basic_demo`
2. Choose an engine and export its environment variables:
   ```bash
   # DuckDB
   set -a; source .env.dev_duckdb; set +a

   # Postgres
   # set -a; source .env.dev_postgres; set +a

   # Databricks Spark
   # set -a; source .env.dev_databricks; set +a

   # BigQuery (choose one)
   # set -a; source .env.dev_bigquery_pandas; set +a      # pandas client
   # set -a; source .env.dev_bigquery_bigframes; set +a   # BigFrames
   ```

3. Execute the full flow for the selected engine:

   ```bash
   # DuckDB / Postgres / Databricks
   make demo ENGINE=duckdb
   # make demo ENGINE=postgres
   # make demo ENGINE=databricks_spark

   # BigQuery (set BQ_FRAME to choose pandas vs bigframes)
   # builds into <FF_BQ_PROJECT>.<FF_BQ_DATASET>.*
   # requires a GCP project, dataset, and credentials (see BigQuery setup docs)
   # set profiles.yml → bigquery.allow_create_dataset: true if the dataset should be auto-created
   # make demo ENGINE=bigquery BQ_FRAME=bigframes
   # make demo ENGINE=bigquery BQ_FRAME=pandas
   ```

   The Makefile runs `fft seed`, `fft run`, `fft dag`, `fft utest`, and `fft test`.

   To open the rendered DAG site after a run:

   ```bash
   make show ENGINE=duckdb
   make show ENGINE=bigquery
   ```
4. Inspect artifacts:

   * `.fastflowtransform/target/manifest.json` and `run_results.json`
   * `site/dag/index.html` for the rendered model graph
   * Use your engine’s client (or `fft run` logs) to inspect the mart outputs

## Data quality tests

The demo enables baseline data quality checks in `project.yml`. Running `fft test` (or `make test ENGINE=…`) verifies that:

* Primary keys remain unique/not-null across:

  * `seed_users`
  * `users_clean`
  * `mart_users_by_domain`
  * the Python mart `mart_latest_signup`
* Aggregate metrics such as `user_count` never drop below zero.
* Each email domain appears only once in `mart_latest_signup`.

These tests run against whatever engine/profile is active — including BigQuery, where they execute as standard SQL queries on the configured dataset.

## Model unit tests (`fft utest`)

The basic demo also includes **model-level unit tests** under `tests/unit/`. They exercise:

- `users_clean` (staging)
- `mart_users_by_domain` (mart)
- the engine-specific `mart_latest_signup` Python model

Each YAML spec defines small input fixtures (inline `rows` or external CSVs) and the expected
output rows. To run the unit tests for the active engine:

```bash
make utest ENGINE=duckdb
# or, equivalent:
fft utest . --env dev_duckdb
```

You can swap engines the same way as for the main demo:

```bash
make utest ENGINE=postgres
make utest ENGINE=databricks_spark
make utest ENGINE=bigquery BQ_FRAME=bigframes
```

`fft utest` only builds the target model for each spec and compares the result to the expected
rows, which makes these tests fast and self-contained while still running against the real
warehouse/engine.

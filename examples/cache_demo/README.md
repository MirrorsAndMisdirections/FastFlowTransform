# Cache Demo

This demo shows:
- Build cache skip/hit via fingerprints
- Downstream invalidation (seed → staging → mart)
- Environment-driven invalidation (only `FF_*`)
- Parallelism within levels (`--jobs`)
- HTTP response cache + offline mode
- Bytes-based cost monitoring (per-query guards + run-level budgets)

## Quickstart

```bash
# pick your engine (duckdb, postgres, databricks_spark, bigquery, or snowflake_snowpark); defaults to duckdb
cp .env.dev_duckdb .env
# or: cp .env.dev_postgres .env  (then edit DSN/schema)
# or: cp .env.dev_databricks .env
# or: cp .env.dev_bigquery_pandas .env   # or .env.dev_bigquery_bigframes
# or: cp .env.dev_snowflake .env

cd examples/cache_demo
make cache_first      ENGINE=duckdb   # builds and writes cache
make cache_second     ENGINE=duckdb   # should SKIP everything
make change_sql       ENGINE=duckdb   # touch SQL → mart rebuilds
make change_seed      ENGINE=duckdb   # seed with base + patches/seed_users_patch.csv (no tracked edits)
make change_env       ENGINE=duckdb   # FF_* env change → full rebuild
make change_py        ENGINE=duckdb   # edit constant in py_constants.ff.py → it rebuilds

make http_first       ENGINE=duckdb   # warms HTTP cache
make http_offline     ENGINE=duckdb   # reuses HTTP cache without network
make http_cache_clear                  # clears HTTP response cache
#
# Seeds stay immutable: change_seed builds a temporary combined copy in .local/seeds using
# patches/seed_users_patch.csv so the repo doesn’t become dirty.
Inspect:

site/dag/index.html

.fastflowtransform/target/run_results.json (HTTP stats, results)

## Cost & budgets (bytes-based)

This example also showcases the **bytes-based cost features**:

1. **Per-query cost guard** (`budgets.yml → query_limits`, overridable with env vars)
2. **Per-run budgets** (via `budgets.yml`)
3. **Per-model stats** in `run_results.json`

### 1. Per-query guard demo

Each engine now reads its default per-query limit from `budgets.yml` (`query_limits.<engine>.max_bytes`),
but you can override it ad-hoc via the legacy env vars:

| Engine              | Env var              |
|---------------------|----------------------|
| DuckDB              | `FF_DUCKDB_MAX_BYTES`|
| Postgres            | `FF_PG_MAX_BYTES`    |
| Databricks / Spark  | `FF_SPK_MAX_BYTES`   |
| BigQuery            | `FF_BQ_MAX_BYTES`    |
| Snowflake Snowpark  | `FF_SF_MAX_BYTES`    |

The Makefile wraps a tiny demo that intentionally sets the env-var override so low that the query is blocked:

```bash
# Pick an engine (defaults to duckdb)
make cost_guard_example ENGINE=duckdb
# or:
make cost_guard_example ENGINE=postgres
make cost_guard_example ENGINE=bigquery BQ_FRAME=bigframes
```

Under the hood this runs:

- `FF_*_MAX_BYTES=1` → the executor estimates bytes before running SQL
- If the estimate is > 1 byte, the query is **aborted** with a clear RuntimeError
- The `make` target uses `|| true` so you can still see the error without the overall demo failing

Unset the env var (or set it to a large value) to disable the guard again.

### 2. Run-level budgets (`budgets.yml`)

`examples/cache_demo/budgets.yml` defines the **project-level budgets** and the per-query limits:

- `query_limits.<engine>.max_bytes` – aborts a single query before execution
- `total.bytes_scanned` – aggregate over all models (driven by run stats)
- `total.query_duration_ms` – aggregate query time
- `models.mart_user_orders.ff.bytes_scanned` – per-model example

By default all budgets are using `on_exceed: "warn"`, so they **never break** the main demo. If a run exceeds any threshold, FFT will print a warning after the run finishes (based on `run_results.json`).

If you want to see a *failing* run:

```yaml
# In budgets.yml, temporarily tighten one budget, e.g.:
per_model:
  mart_user_orders.ff:
    bytes_scanned:
      warn_after: "1KB"
      error_after: "2KB"   # will likely fail even this tiny demo
```

Then run:

```bash
make run ENGINE=duckdb
```

and the CLI will exit with code `1` once the budget is exceeded.

### 3. Inspecting bytes & timing stats

After any run, `run_results.json` includes per-model stats (best effort per engine):

- `bytes_scanned`
- `rows`
- `query_duration_ms`

Example inspection:

```bash
jq '.results[] | {name, status, bytes_scanned, query_duration_ms}' \
  .fastflowtransform/target/run_results.json
```

You can use this to build your own cost dashboards or to tune `budgets.yml` for your real project.

---

To run everything on Postgres, set `ENGINE=postgres` and copy/edit `.env.dev_postgres`, e.g. `make demo ENGINE=postgres`.
To run on Databricks/Spark locally, set `ENGINE=databricks_spark` and copy/edit `.env.dev_databricks`, e.g. `make demo ENGINE=databricks_spark`.
To run on BigQuery, set `ENGINE=bigquery` and copy/edit `.env.dev_bigquery_pandas` (or `.env.dev_bigquery_bigframes`), e.g. `make demo ENGINE=bigquery BQ_FRAME=bigframes` (default) or `BQ_FRAME=pandas`.
To run on Snowflake Snowpark, install `fastflowtransform[snowflake]`, set `ENGINE=snowflake_snowpark`, copy/edit `.env.dev_snowflake`, and run e.g. `make demo ENGINE=snowflake_snowpark`.

## What this demo proves (in a minute)

- **Cache hit/skip:** `make cache_second` should skip everything (if nothing changed).
- **Upstream invalidation:** `make change_seed` rebuilds staging **and** the mart.
- **Env invalidation:** `make change_env` (because `FF_*` is part of the fingerprint).
- **Python source sensitivity:** `py_constants` rebuilds only when its code changes.
- **HTTP cache:** `http_first` fetches; `http_offline` runs fully offline using cached responses.

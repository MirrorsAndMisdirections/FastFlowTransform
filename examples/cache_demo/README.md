# Cache Demo

This demo shows:
- Build cache skip/hit via fingerprints
- Downstream invalidation (seed → staging → mart)
- Environment-driven invalidation (only `FF_*`)
- Parallelism within levels (`--jobs`)
- HTTP response cache + offline mode

## Quickstart

```bash
# pick your engine (duckdb, postgres, databricks_spark, or bigquery); defaults to duckdb
cp .env.dev_duckdb .env
# or: cp .env.dev_postgres .env  (then edit DSN/schema)
# or: cp .env.dev_databricks .env
# or: cp .env.dev_bigquery_pandas .env   # or .env.dev_bigquery_bigframes

cd examples/cache_demo
make cache_first      ENGINE=duckdb   # builds and writes cache
make cache_second     ENGINE=duckdb   # should SKIP everything
make change_sql       ENGINE=duckdb   # touch SQL → mart rebuilds
make change_seed      ENGINE=duckdb   # add a seed row → staging + mart rebuild
make change_env       ENGINE=duckdb   # FF_* env change → full rebuild
make change_py        ENGINE=duckdb   # edit constant in py_constants.ff.py → it rebuilds

make http_first       ENGINE=duckdb   # warms HTTP cache
make http_offline     ENGINE=duckdb   # reuses HTTP cache without network
make http_cache_clear                  # clears HTTP response cache
Inspect:

site/dag/index.html

.fastflowtransform/target/run_results.json (HTTP stats, results)

markdown
Code kopieren

---

To run everything on Postgres, set `ENGINE=postgres` and copy/edit `.env.dev_postgres`, e.g. `make demo ENGINE=postgres`.
To run on Databricks/Spark locally, set `ENGINE=databricks_spark` and copy/edit `.env.dev_databricks`, e.g. `make demo ENGINE=databricks_spark`.
To run on BigQuery, set `ENGINE=bigquery` and copy/edit `.env.dev_bigquery_pandas` (or `.env.dev_bigquery_bigframes`), e.g. `make demo ENGINE=bigquery BQ_FRAME=bigframes` (default) or `BQ_FRAME=pandas`.

## What this demo proves (in a minute)

- **Cache hit/skip:** `make cache_second` should skip everything (if nothing changed).
- **Upstream invalidation:** `make change_seed` rebuilds staging **and** the mart.
- **Env invalidation:** `make change_env` (because `FF_*` is part of the fingerprint).
- **Python source sensitivity:** `py_constants` rebuilds only when its code changes.
- **HTTP cache:** `http_first` fetches; `http_offline` runs fully offline using cached responses.

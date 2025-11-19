# 🧠 Cache & Parallelism Demo

This example demonstrates FastFlowTransform’s **build cache**, **fingerprint logic**, **parallel scheduler**, and **HTTP response caching**.
It’s a compact playground to visualize **when nodes are skipped**, **what triggers rebuilds**, and **how caching accelerates iterative runs**.

---

## 🗂 Directory Structure

```text
cache_demo/
  .env.dev_duckdb
  Makefile
  profiles.yml
  project.yml
  sources.yml
  models/
    seeds_consumers/
      stg_users.ff.sql
      stg_orders.ff.sql
    marts/
      mart_user_orders.ff.sql
    python/
      py_constants.ff.py
    http/
      http_users.ff.py
  seeds/
    seed_users.csv
    seed_orders.csv
  README.md
```

---

## ⚙️ Overview

This demo showcases several FastFlowTransform features:

| Feature                    | Demonstrated by                                 |
| -------------------------- | ----------------------------------------------- |
| Level-wise parallelism     | Multiple models running concurrently (`--jobs`) |
| Deterministic fingerprints | Build cache skipping unchanged nodes            |
| Upstream invalidation      | Seed → staging → mart rebuilds                  |
| Environment invalidation   | Any `FF_*` change triggers rebuild              |
| Python model caching       | Fingerprints derived from function source       |
| HTTP response caching      | Persistent API result cache with offline mode   |

---

## ⚡ Quickstart

```bash
cd examples/cache_demo
make cache_first       # builds all nodes, writes cache
make cache_second      # no-op run (everything skipped)
make change_sql        # touch a model -> rebuilds dependent mart
make change_seed       # use patches/seed_users_patch.csv -> rebuilds staging + mart (no tracked edits)
make change_env        # set FF_* env -> invalidates cache globally
make change_py         # edit py_constants.ff.py -> rebuilds that model
make run_parallel      # runs entire DAG with 4 workers per level
```

> Engines: set `ENGINE=<duckdb|postgres|databricks_spark|bigquery|snowflake_snowpark>` and copy the matching `.env.dev_*` file (`.env.dev_snowflake` for Snowflake; install `fastflowtransform[snowflake]`).

Seeds stay immutable: `change_seed` assembles a temporary combined copy in `.local/seeds` using
`patches/seed_users_patch.csv`, so the repo stays clean while fingerprints still change.

Inspect results:

* `.fastflowtransform/target/run_results.json` – fingerprints, results, timings, HTTP stats
* `site/dag/index.html` – DAG visualization
* `.local/http-cache/` – persisted API responses

---

## 🧩 Model Summary

| Model                     | Kind   | Purpose                     | Notes                                |
| ------------------------- | ------ | --------------------------- | ------------------------------------ |
| `stg_users.ff.sql`        | SQL    | Load & normalize users seed | Rebuilds if seed changes             |
| `stg_orders.ff.sql`       | SQL    | Load orders seed            | Builds as a view                     |
| `mart_user_orders.ff.sql` | SQL    | Join staging tables         | Rebuilds if any staging changes      |
| `py_constants.ff.py`      | Python | Simple constant DataFrame   | Fingerprint based on function source |
| `http_users.ff.py`        | Python | HTTP fetch with cache       | Uses `get_df()` and offline cache    |

---

## 🌐 HTTP Response Cache

The `http_users.ff.py` model demonstrates the built-in HTTP cache:

* **First run:** downloads `https://jsonplaceholder.typicode.com/users`
* **Subsequent runs:** reuse cached responses from `.local/http-cache`
* **Offline mode:** works with `FF_HTTP_OFFLINE=1`

```bash
make http_first        # warms HTTP cache
make http_offline      # reuses cached response, no network access
make http_cache_clear  # deletes cache directory
```

You can inspect HTTP usage in the `run_results.json` file:

```bash
jq -r '.results[] | select(.http!=null)
  | "\(.name): requests=\(.http.requests) cache_hits=\(.http.cache_hits) offline=\(.http.used_offline)"' \
  .fastflowtransform/target/run_results.json
```

---

## ⚙️ Cache Logic Recap

FastFlowTransform caches model fingerprints and skips nodes when:

1. **Fingerprints match** (SQL text, Python source, vars, engine, env, deps).
2. The **physical relation exists** in the database.

Changing *any* of the following invalidates the cache:

* SQL/Jinja content
* Python model code
* `sources.yml`
* `FF_*` environment variables
* Seed file contents
* Engine or profile name

You can control cache behavior via CLI:

```bash
--cache=off   # always build
--cache=rw    # default; skip on match; write cache
--cache=ro    # read-only; skip on hit, build on miss
--cache=wo    # always build, always write
```

---

## 🧮 Parallel Scheduler

FastFlowTransform executes models **level-wise**:

* Each level contains nodes whose dependencies are fully satisfied.
* Up to `--jobs` nodes per level run concurrently.
* Logs are serialized for clean output.

Example:

```bash
fft run . --env dev_duckdb --jobs 4
```

---

## 🧪 Example Experiments

| Scenario                  | Command                                | Expected behavior               |
| ------------------------- | -------------------------------------- | ------------------------------- |
| First full run            | `make cache_first`                     | All models build, cache written |
| No-op run                 | `make cache_second`                    | All skipped (no rebuilds)       |
| Modify SQL                | `make change_sql`                      | Downstream mart rebuilds        |
| Add seed row              | `make change_seed`                     | Staging + mart rebuild (temp combined seed from patches/) |
| Change env                | `make change_env`                      | All nodes rebuild               |
| Edit Python constant      | `make change_py`                       | Only that Python model rebuilds |
| Warm & offline HTTP cache | `make http_first && make http_offline` | HTTP cache reused, no network   |

---

## 🧩 DAG Example

After the first run, generate the DAG visualization:

```bash
make dag
open site/dag/index.html
```

You’ll see:

```
seed_users   → stg_users.ff
seed_orders  → stg_orders.ff
(stg_users + stg_orders) → mart_user_orders.ff
py_constants
http_users
```

* `py_constants` runs independently (parallel)
* `mart_user_orders.ff` depends on both staging nodes

---

## 🧰 Tips

* **Inspect fingerprints:** stored in `.fastflowtransform/target/manifest.json`
* **Audit table:** `_ff_meta` table in the engine stores build metadata
* **Clear cache:** delete `.fastflowtransform/` or use `make clean`
* **Parallel debugging:** use `--keep-going` to continue unaffected levels

---

## ✅ Takeaways

* FFT’s build cache uses stable fingerprints to skip unchanged nodes.
* Fingerprints propagate downstream, ensuring correctness.
* The HTTP cache supports deterministic, offline API pipelines.
* Parallel execution accelerates runs without breaking dependencies.

Together, these features make iterative development **fast, reliable, and reproducible**.

# Cost monitoring, query stats & budgets

This document describes the new **cost & observability** features in
`fastflowtransform`:

- Per-query **cost guards** via environment variables  
- Per-model **query statistics** in `run_results.json`  
- Optional **project-level budgets** via `budgets.yml`

All of these are *opt-in* and backwards-compatible: if you do nothing, existing
projects keep working exactly as before.

---

## 1. Per-query cost guards (budgets.yml or FF_*_MAX_BYTES)

For engines that can estimate query size, FFT can now abort a query **before**
running it if it would scan more data than you allow.

### Supported engines & env vars

| Engine                 | Env var               | Applies to                            |
|------------------------|-----------------------|---------------------------------------|
| BigQuery               | `FF_BQ_MAX_BYTES`     | All model-driven SQL queries          |
| DuckDB                 | `FF_DUCKDB_MAX_BYTES` | All model-driven SQL queries          |
| Postgres               | `FF_PG_MAX_BYTES`     | All model-driven SQL queries          |
| Databricks / Spark     | `FF_SPK_MAX_BYTES`    | All `spark.sql(...)` queries          |
| Snowflake (Snowpark)   | `FF_SF_MAX_BYTES`     | All `session.sql(...)` queries        |

If the env var is **unset or ≤ 0**, the guard is **disabled** for that engine.

### Value format

All `FF_*_MAX_BYTES` use the same parser as `fastflowtransform.executors.budget.parse_max_bytes_env`:

- Plain integers: `5000000000`
- With unit suffix (case-insensitive, `_` and `,` ignored):

  - `10GB`, `5G`
  - `750MB`, `100M`
  - `42KB`, `10K`
  - Units are powers of 1024

Examples:

```bash
# Limit BigQuery queries to ~1 GB
export FF_BQ_MAX_BYTES=1GB

# Limit DuckDB queries to ~2 GB
export FF_DUCKDB_MAX_BYTES=2GB

# Limit Snowflake queries to ~100 MB
export FF_SF_MAX_BYTES=100mb

# Limit Postgres queries to ~500 MB
export FF_PG_MAX_BYTES=500M

# Limit Spark queries to ~10 GB
export FF_SPK_MAX_BYTES=10g
````

### Behaviour

1. **Before running a query**, the executor asks the engine for a best-effort
   size estimate:

   * BigQuery: dry-run load/query job reports logical bytes.
   * DuckDB: `EXPLAIN (FORMAT JSON)` stats (cardinality × row width), best-effort.
   * Postgres: `EXPLAIN (FORMAT JSON)` → `Plan Rows × Plan Width`.
   * Spark/Databricks: `DataFrame._jdf.queryExecution().optimizedPlan().stats().sizeInBytes`.
   * Snowflake Snowpark: `EXPLAIN USING JSON` and `GlobalStats.bytesAssigned`.

2. If the estimate is **`None`** or **≤ 0** (cannot estimate), the query runs
   as usual and is **not blocked**.

3. If the estimate is **> limit**, FFT raises a `RuntimeError` *before* the
   query is submitted:

   > Aborting <Engine> query: estimated scanned bytes 123456789 (117 MB)
   > exceed FF_XY_MAX_BYTES=100000000 (95.4 MB).
   > Increase FF_XY_MAX_BYTES or unset it to allow this query.

The CLI surfaces this error as a **failed model** like any other engine error.

### Configuring limits in budgets.yml

You can define the same per-query limits directly in `budgets.yml` so that
every engineer sees a single source of truth:

```yaml
version: 1

query_limits:
  duckdb:
    max_bytes: 2_000_000_000
  postgres:
    max_bytes: 500_000_000
```

When `query_limits.<engine>.max_bytes` is set, FFT automatically enforces it for
that engine. Environment variables still work and explicitly override the YAML
value, which is useful for quick experiments in CI or ad-hoc runs.

---

## 2. Query statistics per model (run_results.json)

FFT now collects **per-model query statistics** and writes them into
`.fastflowtransform/target/run_results.json` alongside the usual run
information.

### What is recorded?

For each **model** in a run, FFT now aggregates (best effort):

* `bytes_scanned`: total bytes processed by all SQL queries in that model
* `rows`: total rows affected/returned (where the engine reports it)
* `query_duration_ms`: total time spent *inside the engine* for SQL queries
  (excluding Python overhead)

These are stored on each `RunNodeResult`:

```jsonc
{
  "results": [
    {
      "name": "fct_events",
      "status": "success",
      "duration_ms": 1234,
      "bytes_scanned": 987654321,
      "rows": 123456,
      "query_duration_ms": 1100
    },
    {
      "name": "dim_users",
      "status": "success",
      "duration_ms": 250,
      "bytes_scanned": 1048576,
      "rows": 1000,
      "query_duration_ms": 200
    }
  ]
}
```

Engines that don’t support a metric simply leave it as `null`/`0`; all
statistics are **best effort** and must never break a run.

### Where does it come from?

* Each executor implements a private `_execute_sql(...)` method that is used for
  all model-driven SQL.
* After each query, the executor updates its internal “current node stats”.
* At the end of a model run, `_RunEngine` calls `executor.get_node_stats()` and
  stores the aggregated numbers in `run_results.json`.

This design means:

* **All SQL** executed by FFT for a model contributes to these stats.
* Python models now contribute metrics on engines that can inspect their DataFrames:
  * Postgres (pandas): uses in-memory dataframe size / row count.
  * BigQuery (pandas): `load_table_from_dataframe` duration + pandas memory usage.
  * Databricks/Spark: optimized plan stats via Snowpark/Spark query execution.
  * Snowflake Snowpark: `EXPLAIN USING JSON` for the Snowpark plan.
  Engines without support fall back to `None` for bytes/rows.

### How to use it

You can post-process `run_results.json` to:

* Identify the **most expensive models** (by bytes scanned or query time)
* Build dashboards / cost reports in your own tooling
* Feed data into a documentation UI (e.g. show last run cost per model)

Example (pseudo-Python):

```python
import json
from pathlib import Path

path = Path("my_project/.fastflowtransform/target/run_results.json")
data = json.loads(path.read_text())

for node in data["results"]:
    print(
        node["name"],
        node.get("bytes_scanned"),
        node.get("query_duration_ms"),
    )
```

---

## 3. Project-level budgets (budgets.yml)

In addition to per-query env guards, you can define **project-level budgets** in
a `budgets.yml` file at the project root:

* Env vars (`FF_*_MAX_BYTES`) protect **individual queries**
* `budgets.yml` protects the **entire run** (and optional per-engine / per-model
  slices) based on the aggregated stats in `run_results.json`

> ⚠️ If you don’t create `budgets.yml`, nothing changes – budgets are fully
> opt-in.

### File location

Place the file next to `project.yml`:

```text
my_project/
  project.yml
  budgets.yml   # ← new
  models/
  ...
```

### Basic example

```yaml
version: 1

# Per-engine query limits (applied before executing individual queries)
query_limits:
  duckdb:
    max_bytes: 5_000_000
  postgres:
    max_bytes: 10_000_000
  bigquery:
    max_bytes: 50_000_000
  databricks_spark:
    max_bytes: 50_000_000
  snowflake_snowpark:
    max_bytes: 50_000_000

# Global limits across the entire fft run
total:
  bytes_scanned:
    # ~10 MB – adjust down if you want to force a warning
    warn: 100
    # ~100 MB – adjust down if you want to force an error
    error: 100_000_000

  # Optional: total query time across all queries in the run
  query_duration_ms:
    warn: "30s"   # human-friendly duration, parsed to ms
    error: "2m"

# Per-model limits (keys must match node names: stg_users.ff, mart_user_orders.ff, http_users, ...)
models:
  stg_users.ff:
    bytes_scanned:
      # keep this fairly low so you can see a warn if you want
      warn: 100
      error: 10_000_000

  stg_orders.ff:
    bytes_scanned:
      warn: 1_000_000
      error: 10_000_000

  mart_user_orders.ff:
    bytes_scanned:
      warn: 1_000_000
      error: 100_000_000

  http_users:
    # HTTP model → mainly interesting on engines that can report bytes_scanned
    bytes_scanned:
      warn: 5_000_000
      error: 50_000_000

  py_constants:
    bytes_scanned:
      warn: 5_000_000
      error: 50_000_000

# Per-tag budgets (aggregated over all models with that tag)
tags:
  "example:cache_demo":
    bytes_scanned:
      warn: 10_000_000
```

#### Value syntax

* `warn` / `error` for **bytes** use the same notation as
  `FF_*_MAX_BYTES`:

  ```yaml
  warn: "5GB"
  error: "500_000_000"
  ```

* Durations are currently in **milliseconds** (plain integers).

### When are budgets evaluated?

1. You run `fft run` as usual.
2. FFT executes models and writes `run_results.json` with the new stats.
3. After the run completes, FFT:

   * Aggregates `bytes_scanned` / `query_duration_ms` across all `RunNodeResult`
   * Compares the totals to the budgets defined in `budgets.yml`
   * Emits **warnings** and/or fails the run depending on your config

### What happens on exceed?

* If a **warn** budget is exceeded, FFT prints a warning but the CLI exit code
  stays `0` (unless there were other errors).
* If an **error** budget is exceeded, FFT treats it like a failed run and exits
  with `1`.

If both `arn` and `error` are defined and exceeded, the **error**
behaviour wins.

### Interaction with env-level guards

* Env vars (`FF_*_MAX_BYTES`) are **hard per-query gates**. If an estimate is
  above the limit, the query is never sent to the engine.
* Budgets (`budgets.yml`) are **soft run-level gates**. They look at what was
  actually executed and may warn or fail after the fact.

You can happily combine both:

* Env vars to avoid accidentally launching a single “monster query”
* Budgets to catch an overall run that’s becoming too expensive over time

---

## 4. Backwards compatibility

* All new features are **opt-in**:

  * No env var → no per-query guard
  * No `budgets.yml` → no run-level budgets
* Existing `run_results.json` readers remain valid; they simply gain more fields
  (`bytes_scanned`, `rows`, `query_duration_ms`).
* Engines that can’t provide certain metrics just leave them empty/`null`. No
  behaviour changes for those engines.

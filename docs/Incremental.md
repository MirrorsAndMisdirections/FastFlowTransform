# Incremental models

Incremental models let you **reuse existing data** and only process **new or changed rows** instead of rebuilding a table from scratch on every run. This is essential for larger datasets or frequently running pipelines.

This page explains the **concepts and configuration** of incremental models in FastFlowTransform (FFT) independently of any specific example project.

---

## Why incremental models?

By default, a model is built with a **full refresh**:

* Read all sources
* Recompute all transformations
* Overwrite the target table

For small tables this is fine. For anything medium-sized or larger, this quickly becomes:

* slow,
* expensive (especially on cloud warehouses / Spark),
* and unnecessary if only a small portion of rows changed.

Incremental models solve this by:

1. Reusing existing target data.
2. Processing only **new / changed** rows.
3. Applying an **incremental strategy** (append or merge).

---

## High-level architecture

Incremental behaviour is coordinated between three layers:

1. **Model configuration**

   You declare that a model is incremental and provide hints:

   * Does it append or upsert?
   * What is the **unique key**?
   * Which column(s) indicate freshness (e.g. `updated_at`)?

   This lives in the model’s `config(...)` (SQL) or `meta` (Python) and is validated against a strict schema.

2. **Planner / Core**

   FFT looks at:

   * the model’s incremental config (`incremental={...}`),
   * whether the physical table already exists,
   * CLI flags like `--full-refresh`,

   and decides whether to:

   * run a **full rebuild**, or
   * run an **incremental update** using engine hooks.

3. **Engine executors** (DuckDB, Postgres, Databricks/Spark, …)

   Each engine implements a small incremental API:

   * `exists_relation(relation)`
   * `create_table_as(relation, select_sql)`       – initial full build
   * `full_refresh_table(relation, select_sql)`    – forced rebuild
   * `incremental_insert(relation, select_sql)`    – append-only
   * `incremental_merge(relation, select_sql, unique_key)` – upsert / merge
   * `alter_table_sync_schema(relation, select_sql, mode=...)` – optional schema evolution

   The planner calls these methods – you just configure the model.

---

## Enabling incremental mode

You enable incremental mode **per model** via the model config.

### SQL models

Inside the Jinja `config` block you use a structured `incremental` dictionary:

```sql
{{ config(
    materialized='incremental',
    tags=['example:incremental', 'engine:duckdb'],
    incremental={
        "enabled": true,
        "strategy": "merge",          # or "append", "insert", "full_refresh"
        "unique_key": ["event_id"],
        "updated_at_column": "updated_at"
    }
) }}

select
  event_id,
  updated_at,
  value
from some_source
````

Key points:

* `materialized='incremental'` tells FFT to use the incremental pipeline.
* `incremental.enabled: true` declares that this model supports incremental processing.
* `unique_key` declares one or more columns that uniquely identify a row in the target.
* `strategy` is a hint for how deltas should be applied (append vs merge etc.).
* `updated_at_column` (or `delta_columns`/`updated_at_columns`) tells FFT which column is used for “new vs old” comparisons (usually a timestamp or monotonically increasing surrogate).

There is **no extra `meta={...}` wrapper** anymore – the fields of `config(...)` are validated directly.

### Python engine models

For `@engine_model` functions you pass the same information via the `meta` parameter – but again with **top-level incremental config**, not inside another `meta` key:

```python
from fastflowtransform import engine_model

@engine_model(
    only="duckdb",
    name="fct_events_py_incremental",
    deps=["events_base.ff"],
    tags=["incremental", "engine:duckdb"],
    meta={
        "materialized": "incremental",
        "incremental": {
            "enabled": True,
            "strategy": "merge",
            "unique_key": ["event_id"],
            "updated_at_column": "updated_at",
        },
    },
)
def build(df):
    # Return a frame with event_id, updated_at, value, ...
    return df
```

The **frame you return** (pandas, Spark, etc.) is treated as the *delta dataset* for incremental processing – FFT does not care how you compute it, only about the columns and the meta.

---

## Incremental strategies

The core supports at least two conceptual strategies:

### 1. Append / insert-only (`strategy: "append"` / `"insert"`)

Use this when:

* data is immutable once written, and
* new rows have strictly increasing `updated_at` / timestamp or surrogate key.

Behaviour:

* For the **first run**, FFT calls `create_table_as(relation, SELECT ...)`.
* For **subsequent runs**:

  * Only rows considered “new” are included in the SELECT (using your configured watermark columns).
  * The executor calls `incremental_insert(relation, SELECT ...)` which typically becomes:

    ```sql
    INSERT INTO target_table
    SELECT ...
    ```

Good for:

* log/event style tables
* audit trails
* many ingestion pipelines

### 2. Merge / upsert (`strategy: "merge"`)

Use this when:

* rows may change later,
* you want the target table to always reflect the **latest version** per `unique_key`.

Behaviour:

* For the **first run**, same as full refresh: `create_table_as`.
* For **later runs**:

  * The SELECT (or delta query, see below) produces a *delta* frame with new/updated rows.
  * Executor tries `incremental_merge(relation, select_sql, unique_key)`.

Engine-specific behaviour:

* **Databricks / Spark (Delta)**
  The executor attempts a native Delta MERGE:

  ```sql
  MERGE INTO target AS t
  USING (SELECT ...) AS s
  ON t.key1 = s.key1 AND ...
  WHEN MATCHED THEN UPDATE SET *
  WHEN NOT MATCHED THEN INSERT *
  ```

  If MERGE is not supported (non-Delta table), it falls back to a safe full rebuild.

* **Other engines (DuckDB, Postgres, …)**
  The executor can implement merge using:

  * `INSERT ... ON CONFLICT ... DO UPDATE` (Postgres),
  * a **full-refresh emulation**: build a new version by combining old rows and delta rows and overwrite.

In all cases, the `unique_key` list is used to match rows between existing table and delta frame.

---

## Watermark / delta SQL and default behaviour

To decide **which rows are “new enough”** for an incremental run, FFT uses the configuration you provide (for example `updated_at_column` or `delta_columns`) plus the existing table.

A typical default pattern is:

```sql
where updated_at > (
  select coalesce(max(updated_at), timestamp '1970-01-01 00:00:00')
  from {{ this }}
)
```

The exact SQL will vary by engine, but the core idea is:

* Read the current maximum of your watermark column in the target.
* Select only rows strictly newer than that.

### Overriding the delta logic

If the default “`updated_at > max(updated_at)`” is not enough, you have a few options:

1. **Additional delta columns**

   Use `delta_columns` / `updated_at_columns` in `incremental={...}` to indicate multiple fields that drive change detection (especially for Python incremental).

2. **Inline delta SQL (`delta_sql`)**

   Provide a custom **delta SELECT** that FFT should use on incremental runs:

   ```sql
   {{ config(
       materialized='incremental',
       incremental={
         "enabled": true,
         "strategy": "merge",
         "unique_key": ["event_id"],
         "updated_at_column": "updated_at",
         "delta_sql": "
           with base as (
             select event_id, updated_at, value
             from {{ ref('events_base.ff') }}
           )
           select *
           from base
           where updated_at > (
             select coalesce(max(updated_at), timestamp '1970-01-01 00:00:00')
             from {{ this }}
           )
         "
       }
   ) }}
   ```

3. **External delta config (`delta_config`)**

   Keep the base query in the model, but put the delta SQL into a separate YAML file and reference it via `delta_config: "config/incremental/my_model.delta.yml"`.

In all cases, FFT still delegates the **merge/insert mechanics** to the executor; you only control what qualifies as “delta”.

---

## Full refresh vs incremental

You can always force a full rebuild:

```bash
fft run . --env dev --full-refresh
```

The logic is:

* If `--full-refresh` is set → **ignore incremental** and call `full_refresh_table`.

* Otherwise, if the model has `incremental.enabled` and the target exists:

  * attempt incremental path (`incremental_insert` / `incremental_merge`),

* Otherwise:

  * do initial full build via `create_table_as`.

---

## Schema evolution for incremental models

Real tables evolve. To avoid incremental runs failing when the output schema changes, executors can implement:

```python
alter_table_sync_schema(relation: str, select_sql: str, mode: str = "append_new_columns")
```

Typical behaviour (Spark example):

1. Run the SELECT with `LIMIT 0` to infer the **output schema**.
2. Compare it to the existing table schema.
3. For any **new columns**:

   * issue `ALTER TABLE ... ADD COLUMNS (...)`,
   * map complex types to reasonable SQL types (often defaulting to `STRING` in Spark for safety).

Modes:

* `"append_new_columns"` – only new columns are added; existing columns are left untouched.
* `"sync_all_columns"` – more aggressive sync, may also adjust types (implementation-specific).

For DuckDB/Postgres, the simplest implementation may be a no-op initially; more advanced engines (or future versions) can support automatic `ALTER TABLE` statements.

---

## Storage overrides and Delta Lake integration

Incremental models work with both:

1. **Managed / catalog tables**, and
2. **Storage overrides** via `project.yml` / model config, e.g.:

   ```yaml
   models:
     storage:
       fct_events:
         path: ".local/spark/fct_events"
         format: delta
   ```

The storage layer (`fastflowtransform.storage`) provides helpers like:

* `get_model_storage(name)` – resolve per-model `path`/`format`/`options`
* `spark_write_to_path(spark, identifier, df, storage=..., default_format=...)`

For Spark/Delta:

* Incremental models can be backed by **Delta files** at a fixed path.

* The executor writes the DataFrame to a temporary directory, then atomically renames it into place and wires up:

  ```sql
  CREATE TABLE `db`.`tbl`
  USING DELTA
  LOCATION '/path/to/model'
  ```

* Incremental MERGE (`incremental_merge`) then runs against this Delta table.

This keeps:

* a stable location on disk / in the lake,
* and a proper table in the metastore/catalog.

When the Databricks/Spark executor's `table_format` (or `FF_DBR_TABLE_FORMAT`) resolves to `delta`,
FastFlowTransform automatically pulls in `delta-spark` and configures both
`spark.sql.extensions=io.delta.sql.DeltaSparkSessionExtension` and
`spark.sql.catalog.spark_catalog=org.apache.spark.sql.delta.catalog.DeltaCatalog` (unless you
already provided custom values). Install `delta-spark >= 4.0` and you can seed/run Delta-backed
models without manually adding Spark CLI flags.

---

## Interaction with metadata and DAG selection

After each successful build, executors call:

```python
on_node_built(node, relation, fingerprint)
```

which uses the meta helpers:

* `ensure_meta_table(executor)`
* `upsert_meta(executor, node_name, relation, fingerprint, engine_name)`

The `_ff_meta` table records, for each model and engine:

* the relation name,
* the last fingerprint/hash,
* timestamps, etc.

While this metadata is **not strictly required** for incremental mechanics, it is used for advanced features such as:

* **state-based selection** (`--select state:modified`, etc.),
* change-aware DAG runs.

Incremental models work together with these features: you can, for example, run only models whose source files changed and let the incremental planner update them efficiently.

---

## Best practices & recommendations

* **Always define a `unique_key`** for merge strategies.
  Without a stable key, upserts can behave unpredictably.

* **Use timestamps or monotonically increasing columns** for delta selection.
  Avoid non-deterministic expressions (e.g. `now()` in your model SQL) in incremental filters.

* **Start simple**:

  * Begin with `strategy: "append"` and a single `updated_at_column`.
  * Move to `strategy: "merge"` only when you truly need updates.

* **Test both fresh and incremental runs**:

  * First run with an empty database (initial full build).
  * Then run again with new rows and verify the target grew as expected.
  * Add automated tests that run the same model twice and assert row counts / contents.

* **Use `--full-refresh` when semantics change**:
  If you change the business logic of a model in a way that invalidates old rows, do a full rebuild at least once.

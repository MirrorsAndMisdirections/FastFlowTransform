````markdown
# Incremental & Delta Demo

This example project shows how to use **incremental models** and **Delta-style merges** in FastFlowTransform across DuckDB, Postgres and Databricks Spark.

It is intentionally small and self-contained so you can copy/paste patterns into your own project.

---

## Location & Layout

The example lives under:

```text
examples/incremental_demo/
````

Suggested directory structure (mirrors the `api_demo` example):

```text
incremental_demo/
  .env
  .env.dev_duckdb
  .env.dev_postgres
  .env.dev_databricks
  Makefile
  profiles.yml
  project.yml
  sources.yml

  seeds/
    seed_events.csv

  models/
    common/
      events_base.ff.sql
      fct_events_sql_inline.ff.sql
      fct_events_sql_yaml.ff.sql
    engines/
      duckdb/
        fct_events_py_incremental.ff.py
      postgres/
        fct_events_py_incremental.ff.py
      databricks_spark/
        fct_events_py_incremental.ff.py

  config/
    incremental/
      fct_events_sql_yaml.delta.yml
```

*Your actual filenames may differ slightly; the concepts are the same.*

---

## What the demo shows

The demo revolves around a tiny `events` dataset and three different ways to build an incremental fact table:

1. **SQL incremental model with inline delta SQL**

   * `models/common/fct_events_sql_inline.ff.sql`
   * All incremental logic (how to find “new/changed” rows) is defined directly in the model’s `config(meta=...)` block.

2. **SQL incremental model with external YAML delta config**

   * `models/common/fct_events_sql_yaml.ff.sql`
   * The base SELECT lives in the model, but the incremental *delta* SELECT is stored in a separate YAML file under `config/incremental/…`.

3. **Python incremental model**

   * `models/engines/*/fct_events_py_incremental.ff.py`
   * A Python model that returns a DataFrame; the executor applies incremental behaviour based on model `meta` (unique key + updated-at timestamp) and the target engine:

     * DuckDB / Postgres: incremental insert/merge in SQL
     * Databricks Spark: `MERGE INTO` for Delta where available, with a fallback full-refresh strategy

---

## Seed data

The demo uses a simple seed file:

```text
examples/incremental_demo/seeds/seed_events.csv
```

Example contents (conceptually):

```csv
event_id,updated_at,value
1,2024-01-01T10:00:00,10
2,2024-01-01T10:05:00,20
3,2024-01-01T10:10:00,30
```

Running:

```bash
fft seed examples/incremental_demo --env dev_duckdb
```

(or with your engine/env of choice) will materialize this seed into the warehouse (e.g. a DuckDB table or Postgres table).

---

## Base model: `events_base`

The base staging model simply exposes the events from the seed:

```text
models/common/events_base.ff.sql
```

Conceptually:

```sql
{{ config(
    materialized='table',
    tags=[
        'example:incremental_demo',
        'scope:common',
        'engine:duckdb',
        'engine:postgres',
        'engine:databricks_spark',
    ],
) }}

select
  event_id,
  updated_at,
  value
from {{ source('raw', 'events') }};
```

All incremental models build on top of this base table.

---

## Incremental configuration (high-level)

All three incremental models share the same core idea:

* Mark the model as **incremental**
* Provide a **unique key** (e.g. `event_id`)
* Provide an **updated-at / timestamp column** (e.g. `updated_at`)
* Optionally specify a **delta strategy**:

  * **Inline SQL** (in the model)
  * **External YAML** (referenced from the model)
  * **Python** (engine-specific model that returns the delta dataset)

The exact `meta` keys depend on your implementation, but conceptually they look like:

```jinja
{{ config(
    materialized='table',
    tags=['example:incremental_demo'],
    meta={
      'incremental': True,
      'unique_key': ['event_id'],
      'updated_at': 'updated_at',
      # plus engine- or strategy-specific options
    },
) }}
```

The incremental engine then uses these `meta` fields to decide whether to:

* create the table (`create_table_as`) for the **first run**
* perform an **incremental insert** or **merge** for subsequent runs

---

## 1) SQL incremental with inline delta SQL

File:

```text
models/common/fct_events_sql_inline.ff.sql
```

In this variant, the *delta* (the rows to insert/merge) is defined as SQL directly in the model `config(...)` block. The model body usually just defines the **full canonical SELECT**, while the `meta.delta.sql` (or similar) tells the engine how to restrict it to “new” rows.

Conceptually:

```jinja
{{ config(
    materialized='table',
    tags=[
        'example:incremental_demo',
        'kind:incremental',
        'engine:duckdb',
        'engine:postgres',
        'engine:databricks_spark',
    ],
    meta={
        'incremental': True,
        'unique_key': ['event_id'],
        'updated_at': 'updated_at',
        'delta': {
            'sql': "
              with base as (
                select event_id, updated_at, value
                from {{ ref('events_base') }}
              )
              select
                event_id,
                updated_at,
                value
              from base
              where updated_at > (
                select coalesce(max(updated_at), timestamp '1970-01-01 00:00:00')
                from {{ this }}
              )
            "
        },
    },
) }}

-- canonical full-select form (used for docs / full-refresh fallbacks)
select
  event_id,
  updated_at,
  value
from {{ ref('events_base') }};
```

On the **first run**, the engine sees no existing relation, so it materializes the full `select ... from events_base`.

On subsequent runs, the engine evaluates the `delta.sql` snippet and:

* **DuckDB / Postgres**: inserts or merges the resulting rows into the target table
* **Databricks Spark**: tries a `MERGE INTO` (Delta) and falls back to a full-refresh if necessary

---

## 2) SQL incremental with YAML delta config

File:

* Model: `models/common/fct_events_sql_yaml.ff.sql`
* Delta config: `config/incremental/fct_events_sql_yaml.delta.yml`

This variant keeps the model body minimal and moves the delta logic out into a YAML file to keep SQL cleaner or to share the same delta definition across environments.

Example model (conceptually):

```jinja
{{ config(
    materialized='table',
    tags=[
        'example:incremental_demo',
        'kind:incremental',
        'engine:duckdb',
        'engine:postgres',
        'engine:databricks_spark',
    ],
    meta={
        'incremental': True,
        'unique_key': ['event_id'],
        'updated_at': 'updated_at',
        'delta_config': 'config/incremental/fct_events_sql_yaml.delta.yml',
    },
) }}

select
  event_id,
  updated_at,
  value
from {{ ref('events_base') }};
```

The corresponding YAML might hold the same delta query as above, e.g.:

```yaml
# config/incremental/fct_events_sql_yaml.delta.yml
sql: |
  with base as (
    select event_id, updated_at, value
    from {{ ref('events_base') }}
  )
  select
    event_id,
    updated_at,
    value
  from base
  where updated_at > (
    select coalesce(max(updated_at), timestamp '1970-01-01 00:00:00')
    from {{ this }}
  )
```

The engine reads this file, extracts `sql`, and uses it as the **delta SELECT**.

---

## 3) Python incremental model

Files:

```text
models/engines/duckdb/fct_events_py_incremental.ff.py
models/engines/postgres/fct_events_py_incremental.ff.py
models/engines/databricks_spark/fct_events_py_incremental.ff.py
```

Each engine variant uses the same logical signature:

```python
from fastflowtransform import engine_model
import pandas as pd  # or pyspark.sql.DataFrame for Databricks Spark


@engine_model(
    only="duckdb",  # or "postgres" / "databricks_spark"
    name="fct_events_py_incremental",
    deps=["events_base.ff"],
    tags=[
        "example:incremental_demo",
        "scope:engine",
        "engine:duckdb",  # or engine-specific
    ],
    meta={
        "incremental": True,
        "unique_key": ["event_id"],
        "updated_at": "updated_at",
    },
)
def build(df_events):
    # 'df_events' is either a pandas.DataFrame or Spark DataFrame
    # depending on the engine.
    # The function returns either:
    #   - a full canonical result, or
    #   - only the delta rows, depending on your design.
    #
    # In the simplest version, you just return the full dataset and let the
    # executor handle incremental logic based on meta.
    return df_events[["event_id", "updated_at", "value"]]
```

The executor uses the `meta.incremental` / `meta.unique_key` / `meta.updated_at` hints to run:

* A **full-refresh** on the first run
* A **delta merge** on subsequent runs:

  * For DuckDB / Postgres: insert/merge SQL
  * For Databricks Spark:

    * `MERGE INTO` for Delta tables, or
    * a full-refresh fallback strategy that rewrites the table based on the union of existing + delta rows

---

## Delta Lake integration (Databricks Spark)

When running on Databricks Spark, you can configure the executor to use Delta:

```env
# examples/incremental_demo/.env.dev_databricks
FF_DBR_TABLE_FORMAT=delta
FF_DBR_DATABASE=incremental_demo
```

With this set:

* The Databricks Spark executor writes Delta tables by default.
* The incremental merge path uses `MERGE INTO ... USING (...)` where possible.
* If `MERGE` is not supported for a table (or fails for other reasons), it falls back to a safe full-refresh strategy.

---

## Running the demo

From the project root:

```bash
cd examples/incremental_demo
```

### DuckDB

```bash
# Seed
FFT_ACTIVE_ENV=dev_duckdb fft seed . 

# Initial full run
FFT_ACTIVE_ENV=dev_duckdb fft run . \
  --select tag:example:incremental_demo --select tag:engine:duckdb

# Incremental run (after modifying seed_events.csv to add later events)
FFT_ACTIVE_ENV=dev_duckdb fft run . \
  --select tag:example:incremental_demo --select tag:engine:duckdb \
  --cache rw

# Data-quality tests (if configured in project.yml / schema YAML)
FFT_ACTIVE_ENV=dev_duckdb fft test . \
  --select tag:example:incremental_demo
```

### Postgres

```bash
FFT_ACTIVE_ENV=dev_postgres fft seed .
FFT_ACTIVE_ENV=dev_postgres fft run . \
  --select tag:example:incremental_demo --select tag:engine:postgres
FFT_ACTIVE_ENV=dev_postgres fft test . \
  --select tag:example:incremental_demo
```

### Databricks Spark

```bash
FFT_ACTIVE_ENV=dev_databricks fft seed .
FFT_ACTIVE_ENV=dev_databricks fft run . \
  --select tag:example:incremental_demo --select tag:engine:databricks_spark
FFT_ACTIVE_ENV=dev_databricks fft test . \
  --select tag:example:incremental_demo
```

Adjust environment names to match your `profiles.yml`.

---

## How to link this page into your docs

### 1. MkDocs (`mkdocs.yml`)

If you use MkDocs, place this file under e.g.:

```text
docs/examples/incremental_demo.md
```

and add it to your `mkdocs.yml` nav:

```yaml
nav:
  - Overview: index.md
  - Examples:
      - API demo: examples/api_demo.md
      - Incremental & Delta demo: examples/incremental_demo.md
```

### 2. Sphinx (`index.rst` + Markdown)

If you use Sphinx with MyST or Markdown support, put the file under:

```text
docs/examples/incremental_demo.md
```

and reference it from your main `index.rst`:

```rst
Welcome to FastFlowTransform's documentation!
=============================================

.. toctree::
   :maxdepth: 2

   overview
   examples/api_demo
   examples/incremental_demo
```

(Adjust paths to match your actual layout.)

### 3. Top-level `index.md` (Markdown-only docs)

If your docs use a pure Markdown index, just add a link:

```markdown
## Examples

- [API demo](examples/api_demo.md)
- [Incremental & Delta demo](examples/incremental_demo.md)
```

This way, the incremental demo appears alongside your existing API demo and other examples in your global documentation navigation.

```
```

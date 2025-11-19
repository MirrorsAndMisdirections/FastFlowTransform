#  Incremental, Delta & Iceberg Demo

This example project shows how to use **incremental models** and **Delta-/Iceberg-style merges** in FastFlowTransform across DuckDB, Postgres, Databricks Spark (Parquet, Delta & Iceberg), BigQuery (pandas or BigFrames), and Snowflake Snowpark.


It is intentionally small and self-contained so you can copy/paste patterns into your own project.

---

## Location & Layout

The example lives under:

```text
examples/incremental_demo/
````

Directory structure:

```text
incremental_demo/
  .env
  .env.dev_duckdb
  .env.dev_postgres
  .env.dev_databricks_delta
  .env.dev_databricks_iceberg
  .env.dev_bigquery_pandas
  .env.dev_bigquery_bigframes
  .env.dev_snowflake
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
      bigquery/
        pandas/
          fct_events_py_incremental.ff.py
        bigframes/
          fct_events_py_incremental.ff.py
      snowflake_snowpark/
        fct_events_py_incremental.ff.py
```

*Your actual filenames may differ slightly; the concepts are the same.*

---

## What the demo shows

The demo revolves around a tiny `events` dataset and three different ways to build an incremental fact table:

1. **SQL incremental model with inline delta SQL**

   * `models/common/fct_events_sql_inline.ff.sql`
   * All incremental logic (how to find “new/changed” rows) is defined directly in the model’s `config(meta=...)` block.

2. **SQL incremental model with YAML config in `project.yml`**

   * `models/common/fct_events_sql_yaml.ff.sql`
   * The base SELECT lives in the model, but all incremental hints (`incremental.enabled`, `unique_key`, `updated_at_column`, …) are configured in `project.yml → models.incremental`.

3. **Python incremental model**

   * `models/engines/*/fct_events_py_incremental.ff.py`
   * A Python model that returns a DataFrame; the executor applies incremental behaviour based on model `meta` (unique key + updated-at timestamp) and the target engine:

     * DuckDB / Postgres: incremental insert/merge in SQL
     * Databricks Spark: `MERGE INTO` for Delta or Iceberg where available (Spark 4), with a fallback full-refresh strategy for other formats
     * BigQuery: pandas- or BigFrames-backed DataFrame models with incremental merge logic handled by the BigQuery executor
     * Snowflake Snowpark: Snowpark DataFrame operations with merges handled by the Snowflake executor

4. **Iceberg profile for Spark 4**

   * Optional Databricks/Spark profile that uses the built-in **Iceberg catalog**.
   * Seeds and models are materialized as Iceberg tables in a local warehouse directory.
   * `ref()` and `source()` automatically point to the Iceberg catalog when the `databricks_spark.table_format` is set to `iceberg`.

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
        'engine:bigquery',
        'engine:snowflake_snowpark'
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

There are two ways to express this in the demo:

1. **Inline on the model** (used by `fct_events_sql_inline.ff.sql`), via `config(...)`:

```jinja
{{ config(
    materialized='incremental',
    unique_key='event_id',
    incremental={'updated_at_column': 'updated_at'},
    tags=['example:incremental_demo'],
) }}
```

2. **As an overlay in `project.yml`** (used by `fct_events_sql_yaml.ff.sql` and the Python model):

```yaml
models:
  incremental:
    fct_events_sql_yaml.ff:
      unique_key: "event_id"
      incremental:
        enabled: true
        updated_at_column: "updated_at"

    fct_events_py_incremental.ff:
      unique_key: "event_id"
      incremental:
        enabled: true
        updated_at_column: "updated_at"
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

In this variant, both *incremental configuration* and the *delta filter* live directly in the model:

```jinja
{{ config(
    materialized='incremental',
    unique_key='event_id',
    incremental={'updated_at_column': 'updated_at'},
    tags=[
        'example:incremental_demo',
        'scope:common',
        'kind:incremental',
        'inc:type:inline-sql',
        'engine:duckdb',
        'engine:postgres',
        'engine:databricks_spark',
    ],
) }}

with base as (
  select *
  from {{ ref('events_base.ff') }}
)
select
  event_id,
  updated_at,
  value
from base
{% if is_incremental() %}
where updated_at > (
  select coalesce(max(updated_at), timestamp '1970-01-01 00:00:00')
  from {{ this }}
)
{% endif %};
```

On the **first run**, the engine sees no existing relation, so it materializes the full `select ... from events_base`.

On subsequent runs, the engine evaluates the `delta.sql` snippet and:

* **DuckDB / Postgres**: inserts or merges the resulting rows into the target table
* **Databricks Spark**: tries a `MERGE INTO` (Delta) and falls back to a full-refresh if necessary
* **BigQuery**: applies incremental insert/merge logic in SQL via the BigQuery executor

---

## 2) SQL incremental with YAML delta config

File:

```text
models/common/fct_events_sql_yaml.ff.sql
```

Here the model body only defines the **canonical SELECT** and does *not* contain any incremental hints:

```jinja
{{ config(
    materialized='incremental',
    tags=[
        'example:incremental_demo',
        'scope:common',
        'kind:incremental',
        'inc:type:yaml-config',
        'engine:duckdb',
        'engine:postgres',
        'engine:databricks_spark',
        'engine:bigquery',
        'engine:snowflake_snowpark',
    ],
) }}

with base as (
  select *
  from {{ ref('events_base.ff') }}
)
select
  event_id,
  updated_at,
  value
from base;
```

All incremental behaviour for this model is driven by `project.yml`:

```yaml
models:
  incremental:
    fct_events_sql_yaml.ff:
      unique_key: "event_id"
      incremental:
        enabled: true
        updated_at_column: "updated_at"
```

The registry merges this overlay into the model at load time, so the incremental runtime
sees effectively the same config as for the inline model (`unique_key` + `updated_at_column`) –
only the **source of truth** is different.

---

### Inline vs YAML config at a glance

| Model                       | Where is incremental configured?        | What lives in the SQL file?                    |
|----------------------------|-----------------------------------------|-----------------------------------------------|
| `fct_events_sql_inline.ff` | Inline in `config(...)` on the model   | Full SELECT **+** `is_incremental()` filter   |
| `fct_events_sql_yaml.ff`   | `project.yml → models.incremental`     | Full SELECT only (no incremental hints)       |

Both end up with the same runtime meta, only the **location of config** differs.

## 3) Python incremental model

Files:

```text
models/engines/duckdb/fct_events_py_incremental.ff.py
models/engines/postgres/fct_events_py_incremental.ff.py
models/engines/databricks_spark/fct_events_py_incremental.ff.py
models/engines/bigquery/pandas/fct_events_py_incremental.ff.py
models/engines/bigquery/bigframes/fct_events_py_incremental.ff.py
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

## Delta & Iceberg variants (Databricks / Spark)

In addition to the “regular” incremental models, the demo also includes **Delta Lake** and **Iceberg** variants
that shows how to:

- route a model to **Delta tables** via `project.yml`  
- reuse the same incremental pattern, but with a **Delta-backed** table on Databricks/Spark  
- keep Parquet and Delta models side-by-side in the same project

This is optional and only relevant for the `databricks_spark` engine.

---

### Storage configuration for the Delta / Iceberg models

In `project.yml`, the Delta variant gets its own storage entry, separate from the Parquet fact table:

```yaml
models:
  storage:
    # Existing Parquet fact table
    fct_events_sql_inline:
      path: ".local/spark/fct_events_sql_inline"
      format: parquet

    # 🔹 Delta-based fact table (Spark/Databricks only)
    fct_events_sql_inline_delta:
      path: ".local/spark_delta/fct_events_sql_inline"
      format: delta

    # ❄️ Iceberg-based fact table (Spark 4 / Databricks only)
    fct_events_sql_inline_iceberg:
      # Points into the Iceberg warehouse; must match your Iceberg catalog config
      path: ".local/iceberg_warehouse/incremental_demo/fct_events_sql_inline"
      format: iceberg
````

Notes:

* The key `fct_events_sql_inline_delta` must match the **model name**.
* `format: delta` tells the Databricks/Spark executor to create `USING DELTA LOCATION ...`.
* The path is different from the Parquet path so artifacts don’t clash.

---

### Delta fact model

The Delta fact model is a close sibling of `fct_events_sql_inline.ff.sql`, but:

* is tagged only for the Databricks/Spark engine
* is configured for incremental **merge** with a `unique_key` + `updated_at` column

Example (conceptual) model:

```sql
-- models/common/fct_events_sql_inline_delta.ff.sql

{{ config(
    materialized='table',
    tags=[
        'example:incremental_demo',
        'kind:incremental',
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
                from {{ ref('events_base.ff') }}
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

-- canonical full-select (used for docs / full-refresh)
select
  event_id,
  updated_at,
  value
from {{ ref('events_base.ff') }};
```

What happens:

* On the **first run**, the engine sees no existing table and does a full materialization
  (a Delta table at `.local/spark_delta/fct_events_sql_inline`).
* On **subsequent runs**, the executor uses the `delta.sql` query as the **incremental delta** and:

  * attempts a `MERGE INTO` for Delta tables, or
  * falls back to a full-refresh strategy if MERGE is not supported.

---

### Running the Delta variant

Once your Databricks/Spark profile is configured (e.g. `dev_databricks` in `profiles.yml` and `.env.dev_databricks`),
you can run the Delta model like any other:

```bash
# From the repo root
cd examples/incremental_demo

# Seed
FFT_ACTIVE_ENV=dev_databricks fft seed .

# Run only the Delta variant
FFT_ACTIVE_ENV=dev_databricks fft run . \
  --select fct_events_sql_inline_delta.ff \
  --select tag:engine:databricks_spark

# Or include it in the general incremental demo selection
FFT_ACTIVE_ENV=dev_databricks fft run . \
  --select tag:example:incremental_demo \
  --select tag:engine:databricks_spark
```

Optionally, you can add a small `not_null` test to `project.yml` to verify the Delta model:

```yaml
tests:
  - type: not_null
    table: fct_events_sql_inline_delta
    column: event_id
    tags: [batch, delta]
```

Then run:

```bash
FFT_ACTIVE_ENV=dev_databricks fft test . --select tag:delta
```

to validate the Delta-backed incremental table specifically.

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

### BigQuery

```bash
# pandas
FF_ENGINE=bigquery FF_ENGINE_VARIANT=pandas FFT_ACTIVE_ENV=dev_bigquery_pandas fft seed .
FF_ENGINE=bigquery FF_ENGINE_VARIANT=pandas FFT_ACTIVE_ENV=dev_bigquery_pandas fft run . \
  --select tag:example:incremental_demo --select tag:engine:bigquery --cache rw
FF_ENGINE=bigquery FF_ENGINE_VARIANT=pandas FFT_ACTIVE_ENV=dev_bigquery_pandas fft test . \
  --select tag:example:incremental_demo

# BigFrames
FF_ENGINE=bigquery FF_ENGINE_VARIANT=bigframes FFT_ACTIVE_ENV=dev_bigquery_bigframes fft seed .
FF_ENGINE=bigquery FF_ENGINE_VARIANT=bigframes FFT_ACTIVE_ENV=dev_bigquery_bigframes fft run . \
  --select tag:example:incremental_demo --select tag:engine:bigquery --cache rw
FF_ENGINE=bigquery FF_ENGINE_VARIANT=bigframes FFT_ACTIVE_ENV=dev_bigquery_bigframes fft test . \
  --select tag:example:incremental_demo
```

Ensure the service account credentials pointed to by `GOOGLE_APPLICATION_CREDENTIALS` can create/drop tables in the target dataset.

### Snowflake Snowpark

```bash
# Seed / run / test (Snowflake profile)
FFT_ACTIVE_ENV=dev_snowflake FF_ENGINE=snowflake_snowpark fft seed .
FFT_ACTIVE_ENV=dev_snowflake FF_ENGINE=snowflake_snowpark fft run . \
  --select tag:example:incremental_demo --select tag:engine:snowflake_snowpark --cache rw
FFT_ACTIVE_ENV=dev_snowflake FF_ENGINE=snowflake_snowpark fft test . \
  --select tag:example:incremental_demo
```

Make sure `.env.dev_snowflake` sets the required `FF_SF_*` variables and install `fastflowtransform[snowflake]` so the Snowpark executor and client libraries are available.

### Databricks Spark

```bash
FFT_ACTIVE_ENV=dev_databricks fft seed .
FFT_ACTIVE_ENV=dev_databricks fft run . \
  --select tag:example:incremental_demo --select tag:engine:databricks_spark
FFT_ACTIVE_ENV=dev_databricks fft test . \
  --select tag:example:incremental_demo
````

### Databricks Spark (parquet vs Delta)

You can run the incremental demo on Databricks/Spark against either **parquet** or **Delta** tables.

FFT reads the desired table format from the `FF_DBR_TABLE_FORMAT` environment variable, which overrides
`databricks_spark.table_format` from `profiles.yml`.

When `FF_DBR_TABLE_FORMAT=delta`, the Databricks/Spark executor automatically wires Delta Lake into the
SparkSession (downloads the Maven artifact via `delta-spark`, adds
`spark.sql.extensions=io.delta.sql.DeltaSparkSessionExtension`, and sets
`spark.sql.catalog.spark_catalog=org.apache.spark.sql.delta.catalog.DeltaCatalog` unless you already
overrode those settings). No extra `spark-submit --conf` flags are needed—just ensure the
`delta-spark >= 4.0` Python package is installed.

From the repo root:

```bash
cd examples/incremental_demo
````

Run with **parquet** tables (default):

```bash
FF_DBR_TABLE_FORMAT=parquet FFT_ACTIVE_ENV=dev_databricks fft seed .
FF_DBR_TABLE_FORMAT=parquet FFT_ACTIVE_ENV=dev_databricks fft run . \
  --select tag:example:incremental_demo --select tag:engine:databricks_spark
FF_DBR_TABLE_FORMAT=parquet FFT_ACTIVE_ENV=dev_databricks fft test . \
  --select tag:example:incremental_demo
```

Run with **Delta** tables:

```bash
FF_DBR_TABLE_FORMAT=delta FFT_ACTIVE_ENV=dev_databricks fft seed .
FF_DBR_TABLE_FORMAT=delta FFT_ACTIVE_ENV=dev_databricks fft run . \
  --select tag:example:incremental_demo --select tag:engine:databricks_spark
FF_DBR_TABLE_FORMAT=delta FFT_ACTIVE_ENV=dev_databricks fft test . \
  --select tag:example:incremental_demo
```

This way you can switch between parquet and Delta just by changing the `FF_DBR_TABLE_FORMAT`
environment variable, without touching the models or project.yml.

Adjust environment names to match your `profiles.yml`.

### Databricks Spark (Iceberg / Spark 4+)

If you are on Spark 4 / Databricks with Iceberg support, you can also run the incremental demo
purely against Iceberg tables using a dedicated profile (for example `dev_databricks_iceberg`).

That profile typically:

* uses `engine: databricks_spark`
* sets `databricks_spark.table_format: iceberg`
* configures an Iceberg catalog via `extra_conf`, for example:

  models:
    storage:
      # Example warehouse location, adjust as needed
      fct_events_sql_inline_iceberg:
        path: ".local/iceberg_warehouse/incremental_demo/fct_events_sql_inline"
        format: iceberg

and in the profile (profiles.yml) something like:

  dev_databricks_iceberg:
    engine: databricks_spark
    databricks_spark:
      master: "local[*]"
      app_name: "incremental_demo"
      warehouse_dir: "{{ project_dir() }}/.local/spark_warehouse"
      extra_conf:
        spark.sql.catalog.iceberg: org.apache.iceberg.spark.SparkCatalog
        spark.sql.catalog.iceberg.type: hadoop
        spark.sql.catalog.iceberg.warehouse: "file:///{{ project_dir() }}/.local/iceberg_warehouse"

From the repo root:

  cd examples/incremental_demo

Run seeds and models against Iceberg:

  FFT_ACTIVE_ENV=dev_databricks_iceberg fft seed .

  FFT_ACTIVE_ENV=dev_databricks_iceberg fft run . \
    --select tag:example:incremental_demo --select tag:engine:databricks_spark

  FFT_ACTIVE_ENV=dev_databricks_iceberg fft test . \
    --select tag:example:incremental_demo

Under this profile, all `ref()` / `source()` calls in Spark SQL and Python models are resolved
against the Iceberg catalog, so seeds and incremental models operate purely on Iceberg tables.

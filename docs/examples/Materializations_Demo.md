# Materializations Demo

> This example shows how different **materializations** (`view`, `table`, `incremental`, `ephemeral`) behave in FastFlowTransform.

The demo models are located under:
```

examples/materializations_demo/models/

````

Each model type demonstrates how FastFlowTransform builds, caches, or executes models differently depending on its `materialized:` configuration.

---

## 🧩 1. View Models

A **view** model is always re-created from scratch each run.  
It defines a virtual relation that doesn’t store data permanently — ideal for lightweight transformations.

```sql
{{ config(materialized='view') }}

select
    order_id,
    customer_id,
    total_amount,
    order_date
from {{ ref('stg_orders') }}
````

**Characteristics**

* Rebuilt each run (no persisted data)
* Useful for staging, joins, and intermediate logic
* Fast and always up-to-date with upstreams
* Cannot store or cache incremental state

---

## 🧱 2. Table Models

A **table** model materializes into a physical table on the target engine.

```sql
{{ config(materialized='table') }}

select *
from {{ ref('fct_orders_view') }}
```

**Characteristics**

* Fully rebuilt every run
* Good for final curated datasets or small tables
* Overwrites previous contents (atomic replace)
* Compatible with all engines (DuckDB, Postgres, BigQuery, etc.)

---

## ⚡ 3. Incremental Models

An **incremental** model stores state and only updates changed records on subsequent runs.

```sql
{{ config(
    materialized='incremental',
    incremental={
        "enabled": true,
        "unique_key": "order_id",
        "updated_at_column": "updated_at",
        "delta_sql": "select * from {{ ref('stg_orders') }} where updated_at > (select max(updated_at) from {{ this }})"
    }
) }}
```

**Characteristics**

* Persists data between runs
* Only merges new or changed rows
* Significantly faster for large tables
* Requires `unique_key` and (optionally) an `updated_at_column`
* Schema changes can be managed via:

  * `on_schema_change: "ignore"`
  * `on_schema_change: "append_new_columns"`
  * `on_schema_change: "sync_all_columns"`

**Behavior example:**

| Run | Operation   | Rows affected |
| --- | ----------- | ------------- |
| 1   | full load   | 10,000        |
| 2   | merge delta | 120           |
| 3   | merge delta | 45            |

---

## 🧮 4. Ephemeral Models

An **ephemeral** model exists only during query compilation.
It never creates a physical table or view — it’s inlined wherever it’s referenced.

```sql
{{ config(materialized='ephemeral') }}

select
    order_id,
    total_amount * 0.1 as tax_amount
from {{ ref('fct_orders_inc') }}
```

**Characteristics**

* Inlined into parent queries
* Reduces I/O overhead (no temporary tables)
* Ideal for lightweight reusable SQL snippets
* Not visible in the warehouse after execution

---

## 🔗 5. Combined Example DAG

In the demo, these models are connected as follows:

```text
stg_orders
   ↓
fct_orders_view (view)
   ↓
fct_orders_tbl (table)
   ↓
fct_orders_inc (incremental)
   ↓
fct_orders_ephemeral (ephemeral)
```

This DAG demonstrates:

* How **data flows** between materializations
* Which ones persist or recompute data
* How incremental models can feed downstream table or ephemeral models

---

## 🧭 When to Use Each Type

| Materialization | Persists? | Performance         | Recommended Use Case                      |
| --------------- | --------- | ------------------- | ----------------------------------------- |
| `view`          | ❌ No      | ⚡ Fast rebuild      | Intermediate or temporary transformations |
| `table`         | ✅ Yes     | ⚖️ Moderate         | Final outputs or smaller datasets         |
| `incremental`   | ✅ Yes     | 🚀 High (on deltas) | Large, frequently updated fact tables     |
| `ephemeral`     | ❌ No      | ⚡ Fast inline       | Reusable SQL snippets or shared logic     |

---

## 🧠 Tips

* You can set default materializations in `project.yml` under `models.materialized`.
* Override per model using `{{ config(materialized='...') }}`.
* For incremental models, ensure **unique keys** and **delta logic** are consistent across runs.
* Test behavior locally using the DuckDB engine before deploying to a warehouse.

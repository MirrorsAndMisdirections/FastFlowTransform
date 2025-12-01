# Macros Demo

**Goal:** Showcase

- **SQL Jinja macros** (`models/macros/*.sql`)
- **Python render-time macros** (`models/macros_py/*.py`)
- **Engine-aware stdlib helpers** (`fastflowtransform.stdlib.*` wired in as `ff_*` Jinja globals)

working together across engines:

- DuckDB
- Postgres
- Databricks Spark
- BigQuery (pandas & BigFrames)
- Snowflake Snowpark

You’ll see reusable SQL helpers, engine-aware SQL generation, and Python functions exposed as Jinja globals/filters.

---

## Directory structure

```text
examples/macros_demo/
  .env.dev_bigquery_bigframes
  .env.dev_bigquery_pandas
  .env.dev_databricks
  .env.dev_duckdb
  .env.dev_postgres
  .env.dev_snowflake
  Makefile
  profiles.yml
  project.yml
  sources.yml
  seeds/
    seed_users.csv
    seed_orders.csv
  models/
    macros/
      utils.sql
      star.sql
    macros_py/
      helpers.py
    common/
      stg_users.ff.sql
      stg_orders.ff.sql
      dim_users.ff.sql
      fct_user_sales.ff.sql
      fct_user_sales_by_country.ff.sql
      fct_user_sales_partitioned.ff.sql
    engines/
      duckdb/
        py_example.ff.py
      postgres/
        py_example.ff.py
      databricks_spark/
        py_example.ff.py
      bigquery/
        bigframes/
          py_example.ff.py
        pandas/
          py_example.ff.py
      snowflake_snowpark/
        py_example.ff.py
  tests/
    unit/
      ...
```

*(Exact engine folders may vary slightly depending on your checkout, but conceptually this is the layout.)*

---

## What this demo shows

### 1. SQL Jinja macros (`models/macros/*.sql`)

#### `utils.sql`

High-level helpers used by `stg_users` / `stg_orders`:

* **`email_domain(expr)`**
  Engine-aware extraction of the domain part of an email address.

  * BigQuery: uses `split(...)[SAFE_OFFSET(1)]`
  * Other engines: uses `split_part(..., '@', 2)`

* **`safe_cast_amount(expr)`**
  A convenience wrapper around the stdlib **`ff_safe_cast`** to cast arbitrary expressions into a numeric type safely and consistently across engines.

* **`coalesce_any(expr, default)`**
  Tiny convenience macro wrapping `coalesce(...)`.

* **`default_country()`**
  Reads a default country from `project.yml → vars.default_country`.

#### `star.sql`

* **`star_except(relation, exclude_cols)`**
  Selects all columns of `relation` except a list of exclusions.

  * Uses `adapter_columns(relation)` if the executor can describe the table.
  * Falls back to `*` if columns are unknown.

---

### 2. Python macros (`models/macros_py/helpers.py`)

Render-time Python helpers exposed as Jinja globals/filters:

* **`slugify(value: str) -> str`**
  Simple URL-friendly slug: lower-case, replace non-alphanumerics with `-`, squash duplicates.

* **`mask_email(email: str) -> str`**
  Redacts the local part of the email, e.g. `a@example.com` → `a***@example.com`.

* **`csv_values(rows: list[dict[str, Any]], cols: list[str]) -> str`**
  Renders a `VALUES(...)` list for small inline lookup tables in SQL.

These run at **render time** (inside the Jinja phase), not as SQL UDFs.

---

### 3. Stdlib helpers (`fastflowtransform.stdlib` → `ff_*` Jinja globals)

The demo also showcases the built-in, engine-aware SQL stdlib. These are wired into Jinja as globals like `ff_safe_cast(...)`.

Key helpers used in this demo:

* **`ff_engine()` / `ff_engine_family()`**
  Return the normalized engine name / family (`"duckdb"`, `"postgres"`, `"bigquery"`, `"snowflake"`, `"spark"`, `"generic"`).

* **`ff_is_engine(*candidates)`**
  Convenience predicate to branch on engine in Jinja:

  ```jinja
  {% if ff_is_engine('bigquery') %}
    ...
  {% endif %}
  ```

* **`ff_safe_cast(expr, target_type, default=None)`**
  Engine-aware safe casting, used in this demo to safely cast `amount` to a numeric type:

  * DuckDB: `try_cast(expr AS NUMERIC)`
  * BigQuery: `SAFE_CAST(expr AS NUMERIC)`
  * Spark: `TRY_CAST(expr AS NUMERIC)`
  * Snowflake / Postgres / others: `CAST(expr AS NUMERIC)`
  * If `default` is given → wrapped as `COALESCE(..., default)`.

* **`ff_date_trunc(expr, part="day")`**
  Engine-aware `DATE_TRUNC`, used to derive `order_day` from `order_ts`:

  * DuckDB/Postgres/Snowflake/Spark: `date_trunc('<part>', CAST(expr AS TIMESTAMP))`
  * BigQuery: `DATE_TRUNC(CAST(expr AS TIMESTAMP), PART)`

* **`ff_date_add(expr, part, amount)`**
  Engine-aware date/timestamp arithmetic, used to get `order_ts_plus_1d`:

  * DuckDB/Postgres: `CAST(expr AS TIMESTAMP) + INTERVAL 'amount part'`
  * Snowflake: `DATEADD(PART, amount, TO_TIMESTAMP(expr))`
  * BigQuery: `DATE_ADD(CAST(expr AS TIMESTAMP), INTERVAL amount PART)`
  * Spark: `date_add(expr, amount)` when `part == "day"`, else falls back to `INTERVAL`.

* **`ff_partition_filter(column, start, end)`**
  Builds a range predicate for partitions; demo uses it in `fct_user_sales_partitioned`:

  * `start` & `end`: `column BETWEEN <lit(start)> AND <lit(end)>`
  * Only `start`: `column >= <lit(start)>`
  * Only `end`: `column <= <lit(end)>`
  * Both `None`: `1=1` (no-op)

* **`ff_partition_in(column, values)`**
  Builds an `IN (...)` predicate from Python values (`date`, `datetime`, strings, ints, etc.) via the stdlib’s `sql_literal`:

  * Empty values → `1=0` (safe “match nothing” guard).
  * Non-empty → `column IN (<lit1>, <lit2>, ...)`.

Even if only some of these are used in the demo models, they are all available to your own models and macros.

---

## Prerequisites

* A working FastFlowTransform installation (CLI `fft` available).
* For Postgres / Databricks / BigQuery / Snowflake: drivers and credentials configured via the `.env.dev_*` files.
* The FFT core already wires Jinja with:

  * `var(name, default)`
  * `env(name, default)`
  * `engine(default)` (legacy in macros; new code uses `ff_engine` / `ff_is_engine` from stdlib).

---

## Seeds

Two tiny CSVs materialized via `fft seed`:

* `seeds/seed_users.csv` — `id,email,country`
* `seeds/seed_orders.csv` — `order_id,customer_id,amount,order_ts`

`profiles.yml` and `project.yml` provide minimal connection config; `.env.dev_*` files bind environment variables like `FF_DUCKDB_PATH`, `FF_PG_DSN`, `FF_BQ_PROJECT`, `FF_SF_*`, etc.

---

## How to run

From the repo root:

```bash
cd examples/macros_demo

# Choose engine: duckdb (default) | postgres | databricks_spark | bigquery | snowflake_snowpark
make ENGINE=duckdb demo
# or
make ENGINE=postgres demo
# or
make ENGINE=databricks_spark demo
# or
make ENGINE=bigquery BQ_FRAME=pandas demo     # or bigframes
# or
make ENGINE=snowflake_snowpark demo
```

The `demo` target:

1. **`fft seed`** — loads the CSV seeds.
2. **`fft run`** — builds all tagged models using macros & stdlib.
3. **`fft dag --html`** — renders a DAG HTML to `site/dag/index.html`.
4. **`fft test`** — executes example tests from `project.yml`.
5. Prints artifact paths and (if possible) opens the DAG in your browser.

> For Snowflake, copy `.env.dev_snowflake` to `.env` or export the `FF_SF_*` variables yourself, and install `fastflowtransform[snowflake]` so the Snowpark executor is available.

---

## Key files (highlights)

### SQL macros – `models/macros/utils.sql`

Conceptually:

```jinja
{# Engine-aware email domain extraction using stdlib helpers #}
{%- macro email_domain(expr) -%}
  {%- if ff_is_engine('bigquery') -%}
    lower(split({{ expr }}, '@')[SAFE_OFFSET(1)])
  {%- else -%}
    lower(split_part({{ expr }}, '@', 2))
  {%- endif -%}
{%- endmacro %}

{# Convenience wrapper on top of ff_safe_cast #}
{%- macro safe_cast_amount(expr) -%}
  {{ ff_safe_cast(expr, "numeric", default="0") }}
{%- endmacro %}

{%- macro coalesce_any(expr, default) -%}
  coalesce({{ expr }}, {{ default }})
{%- endmacro %}

{%- macro default_country() -%}
  '{{ var("default_country", "DE") }}'
{%- endmacro %}
```

> Exact implementation may differ slightly in your tree, but the *idea* is:
>
> * Use stdlib (`ff_safe_cast`, `ff_is_engine`) for the heavy lifting.
> * Keep project macros thin and readable.

### SQL macros – `models/macros/star.sql`

```jinja
{# Select * except some columns. Works across engines. #}
{%- macro star_except(relation, exclude_cols) -%}
  {%- set excl = exclude_cols | map('lower') | list -%}
  {%- set cols = adapter_columns(relation) -%}
  {# adapter_columns is provided by FFT executors' catalog/describe (if available).
     To keep demo simple, fall back to literal star if unknown. #}
  {%- if cols and cols|length > 0 -%}
    {{- (cols | reject('in', excl) | map('string') | join(', ')) -}}
  {%- else -%}
    *
  {%- endif -%}
{%- endmacro %}
```

---

## Models using macros & stdlib

### `stg_users.ff.sql` — SQL + Python macros

* Uses `coalesce_any(...)` + `default_country()` to fill missing countries.
* Uses `email_domain(...)` for engine-aware domain extraction.
* Injects a Python macro result via `slugify(var("site_name", "My Site"))`.

Conceptually:

```jinja
{{ config(
    materialized='view',
    tags=[
      'example:macros_demo',
      'scope:common',
      'engine:duckdb',
      'engine:postgres',
      'engine:databricks_spark',
      'engine:bigquery',
      'engine:snowflake_snowpark'
    ]
) }}

with src as (
  select
    cast(id as int) as user_id,
    lower(email)     as email,
    {{ coalesce_any("country", default_country()) }} as country
  from {{ source('crm', 'users') }}
)
select
  user_id,
  email,
  {{ email_domain("email") }} as email_domain,
  country,
  -- Render-time Python macro usage (literal in SQL)
  '{{ slugify(var("site_name", "My Site")) }}' as site_slug
from src;
```

### `stg_orders.ff.sql` — stdlib date & cast helpers

This model shows:

* `safe_cast_amount(...)` and `ff_safe_cast(...)` for robust numeric casting.
* `ff_date_trunc(...)` for engine-aware `DATE_TRUNC`.
* `ff_date_add(...)` for portable “+ 1 day” logic.

Conceptually:

```jinja
{{ config(
    materialized='view',
    tags=[
      'example:macros_demo',
      'scope:common',
      'engine:duckdb',
      'engine:postgres',
      'engine:databricks_spark',
      'engine:bigquery',
      'engine:snowflake_snowpark'
    ]
) }}

select
  cast(order_id as int)     as order_id,
  cast(customer_id as int)  as user_id,
  {{ safe_cast_amount("amount") }} as amount,
  {{ ff_safe_cast("amount", "numeric", default="0") }} as amount_safe,
  {{ ff_date_trunc("order_ts", "day") }} as order_day,
  {{ ff_date_add("order_ts", "day", 1) }} as order_ts_plus_1d,
  cast(order_ts as timestamp) as order_ts
from {{ source('sales', 'orders') }};
```

### `dim_users.ff.sql` — inline lookup (can use Python or stdlib)

The demo joins staged users with a tiny lookup table that maps domains to labels (e.g. `"example.com" → "internal"`). In your current project this may be expressed either:

* via a Python macro (`csv_values(...)` → `VALUES(...)`), or
* via an explicit SQL snippet using literal `VALUES`.

Either way, it shows how to generate small lookups at render time and join them in the model.

### `fct_user_sales.ff.sql` — base fact table

Joins `stg_orders` with `dim_users` and aggregates:

* `order_count`
* `total_amount`
* `first_order_ts`
* `last_order_ts`

This is the “plain” fact model without partitioning.

### `fct_user_sales_by_country.ff.sql` — grouping example

Demonstrates using the staged and dim models to build a simple grouped fact table, usually aggregating at `(country, user_segment)` or similar.

### `fct_user_sales_partitioned.ff.sql` — partition range filter with stdlib

This model demonstrates **`ff_partition_filter`** in action. It filters a date/timestamp column with a portable predicate built from variables:

```jinja
{{ config(
    materialized='table',
    tags=[
      'example:macros_demo',
      'scope:common',
      'engine:duckdb',
      'engine:postgres',
      'engine:databricks_spark',
      'engine:bigquery',
      'engine:snowflake_snowpark'
    ]
) }}

with sales as (
  select
    u.user_id,
    u.user_segment,
    o.order_ts,
    o.amount
  from {{ ref('stg_orders.ff') }} as o
  join {{ ref('dim_users.ff') }} as u
    on u.user_id = o.user_id
  where
    -- demo: engine-aware partition predicate using stdlib
    {{ ff_partition_filter(
        'o.order_ts',
        var('from_date', '2025-10-01'),
        var('to_date', '2025-10-31')
    ) }}
)
select
  user_id,
  user_segment,
  count(*) as order_count,
  sum(amount) as total_amount
from sales
group by user_id, user_segment;
```

You can use the same pattern with **`ff_partition_in`** if you prefer a discrete partition list driven by `var("partitions", [...])`.

---

## Tests (examples)

In `project.yml` you’ll see example tests like:

```yaml
tests:
  - type: not_null
    table: dim_users
    column: user_id
    tags: [batch]

  - type: row_count_between
    table: fct_user_sales
    min_rows: 1
    tags: [batch]

  - type: not_null
    table: fct_user_sales_by_country
    column: user_id
    tags: [batch]

  - type: row_count_between
    table: fct_user_sales_by_country
    min_rows: 1
    tags: [batch]

  - type: not_null
    table: fct_user_sales_partitioned
    column: user_id
    tags: [batch]

  - type: row_count_between
    table: fct_user_sales_partitioned
    min_rows: 1
    tags: [batch]

  - type: not_null
    table: stg_orders
    column: user_id
    tags: [batch]

  - type: row_count_between
    table: stg_orders
    min_rows: 1
    tags: [batch]

  - type: not_null
    table: stg_users
    column: user_id
    tags: [batch]

  - type: row_count_between
    table: stg_users
    min_rows: 1
    tags: [batch]
```

Run them using the appropriate profile, e.g.:

```bash
fft test examples/macros_demo --env dev_duckdb --select tag:example:macros_demo
```

---

## Troubleshooting

* **`jinja2.exceptions.UndefinedError: 'var'/'env'/'engine' is undefined`**

  Ensure your FFT core registers these Jinja globals before rendering models:

  ```python
  env.globals.update(var=..., env=..., engine=...)
  ```

  and that `fastflowtransform.stdlib.register_jinja(...)` is called to inject `ff_*` helpers.

* **Engine differences (types & functions)**

  Use `ff_engine()` / `ff_is_engine(...)` or your own macros to branch on engine where syntax differs (e.g. `split_part` vs `split()[SAFE_OFFSET(...)]`, `SAFE_CAST` vs `TRY_CAST`).

* **`adapter_columns(...)` returns None**

  In that case `star_except` falls back to `*`. If you need strict column lists for some engines, replace that macro with explicit column sets or configure your executor to provide catalog metadata.

---

## Extending this demo

* Add more helpers to `helpers.py` (e.g. JSON formatting, list formatting).
* Create more engine-aware macros for date handling or SCDs, potentially layered on top of stdlib (`ff_date_trunc`, `ff_date_add`).
* Add new models that use `ff_partition_in` or more elaborate `ff_safe_cast` combinations.
* Use `var(...)` to parameterize from/to dates, partition lists, or feature flags per environment.

---

Happy macro-ing 🚀

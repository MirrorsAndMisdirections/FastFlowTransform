# Macros Demo

**Goal:** Showcase **SQL Jinja macros** and **Python render-time macros** working together across engines (DuckDB, Postgres, Databricks Spark).
You’ll see reusable SQL helpers, engine-aware SQL generation, and Python functions exposed as Jinja globals/filters.

---

## Directory structure

```text
examples/macros_demo/
  .env
  .env.dev_databricks
  .env.dev_duckdb
  .env.dev_postgres
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
    engines/
      duckdb/
        py_example.ff.py
      postgres/
        py_example.ff.py
      databricks_spark/
        py_example.ff.py
```

---

## What this demo shows

* **SQL Jinja macros** (`models/macros/*.sql`)

  * `email_domain(expr)` – derive email domain
  * `safe_cast_amount(expr)` – engine-aware numeric cast
  * `coalesce_any(expr, default)` – small convenience
  * `default_country()` – pull a default from `project.yml → vars`
  * `star_except(relation, exclude_cols)` – select all except listed columns (falls back to `*` if columns unknown)
* **Python macros** (`models/macros_py/helpers.py`)

  * `slugify(str)` – URL-friendly slug
  * `mask_email(email)` – redact local part
  * `csv_values(rows, cols)` – inline small lookup tables via SQL `VALUES(...)`
* **Usage from models**

  * `stg_users` uses SQL + Python macros at render time
  * `stg_orders` uses engine-aware casting
  * `dim_users` builds a tiny inline lookup via `csv_values(...)`
  * `fct_user_sales` aggregates across staged models

---

## Prerequisites

* A working FFT installation (CLI `fft` available)
* For Postgres/Databricks: valid local env and drivers
* The core must expose these Jinja globals (already done in the FFT core):

  * `var(name, default)`, `env(name, default)`, `engine(default)`
    (Used by profiles/macros to read vars and detect engine.)

---

## Seeds

Two tiny CSVs materialized via `fft seed`:

* `seed_users.csv` — `id,email,country`
* `seed_orders.csv` — `order_id,customer_id,amount,order_ts`

`profiles.yml` and `project.yml` give minimal storage and connection configs.

---

## How to run

From repo root:

```bash
cd examples/macros_demo

# Choose engine: duckdb (default) | postgres | databricks_spark
make ENGINE=duckdb demo
# or
make ENGINE=postgres demo
# or
make ENGINE=databricks_spark demo
```

The `demo` target runs:

1. `fft seed` — loads CSVs
2. `fft run` — builds models using macros
3. `fft dag --html` — writes DAG HTML to `site/dag/index.html`
4. `fft test` — runs example tests
5. Prints artifact paths and tries to open the DAG

---

## Key files (highlights)

### SQL macros – `models/macros/utils.sql`

```jinja
{%- macro email_domain(expr) -%}
  lower(split_part({{ expr }}, '@', 2))
{%- endmacro -%}

{%- macro safe_cast_amount(expr) -%}
{%- set e = engine('duckdb') -%}
{%- if e in ['duckdb', 'postgres', 'databricks_spark'] -%}
  cast({{ expr }} as double)
{%- else -%}
  cast({{ expr }} as double)
{%- endif -%}
{%- endmacro -%}

{%- macro coalesce_any(expr, default) -%}
  coalesce({{ expr }}, {{ default }})
{%- endmacro -%}

{%- macro default_country() -%}
  '{{ var("default_country", "DE") }}'
{%- endmacro -%}
```

### SQL macros – `models/macros/star.sql`

```jinja
{%- macro star_except(relation, exclude_cols) -%}
{%- set excl = exclude_cols | map('lower') | list -%}
{%- set cols = adapter_columns(relation) -%}
{%- if cols and cols|length > 0 -%}
  {{- (cols | reject('in', excl) | map('string') | join(', ')) -}}
{%- else -%}
  *
{%- endif -%}
{%- endmacro -%}
```

> Note: If the executor can’t describe columns for `relation`, this macro falls back to `*`.

### Python macros – `models/macros_py/helpers.py`

```python
def slugify(value: str) -> str: ...
def mask_email(email: str) -> str: ...
def csv_values(rows: list[dict], cols: list[str]) -> str: ...
```

Exposed as Jinja globals/filters at **render time** (not runtime SQL UDFs).

---

## Models using macros

### `stg_users.ff.sql` (Jinja + Python macro usage)

* Coalesces missing country with `default_country()`
* Adds `email_domain(...)`
* Embeds a `slugify(var('site_name', ...))` literal into SQL

```jinja
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
  '{{ slugify(var("site_name", "My Site")) }}' as site_slug
from src;
```

### `stg_orders.ff.sql` (engine-aware types)

```jinja
select
  cast(order_id as int)     as order_id,
  cast(customer_id as int)  as user_id,
  {{ safe_cast_amount("amount") }} as amount,
  cast(order_ts as timestamp) as order_ts
from {{ source('sales', 'orders') }};
```

### `dim_users.ff.sql` (inline lookup via Python macro)

```jinja
labels as (
  select * from (values {{ csv_values(
      [
        {"domain":"example.com", "label":"internal"},
        {"domain":"gmail.com",   "label":"consumer"},
      ],
      ["domain","label"]
  ) }}) as t(domain, label)
)
```

### `fct_user_sales.ff.sql` (final aggregation)

Joins `stg_orders` with `dim_users` and aggregates.

---

## Tests (examples)

Declared in `project.yml`:

* `not_null(dim_users.user_id)`
* `row_count_between(fct_user_sales, min_rows=1)`

Run with:

```bash
fft test examples/macros_demo --env dev_duckdb --select tag:example:macros_demo
```

---

## Troubleshooting

* **`jinja2.exceptions.UndefinedError: 'var'/'env'/'engine' is undefined`**
  Ensure your core’s Jinja environment registers these globals before loading templates:

  ```python
  env.globals.update(var=..., env=..., engine=...)
  ```
* **Engine differences (types & functions):**
  Always branch in macros (`engine(...)`) when types or functions differ.
* **`adapter_columns(...)` returns none:**
  The `star_except` macro will fallback to `*`. For strict behavior, replace with static column lists per engine.

---

## Extending this demo

* Add more helpers to `helpers.py` (e.g., `render_json(obj)`, `join_csv(list)`).
* Create reusable macro libraries under `models/macros/` (date handling, SCD helpers, etc.).
* Use `var(...)` to parameterize behavior per environment or profile.

---

Happy macro-ing!

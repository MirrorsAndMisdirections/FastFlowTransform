# FastFlowTransform Modeling Reference

> Authoritative reference for FastFlowTransform’s modeling layer: SQL/Python models, configuration macros, templating helpers, and testing hooks.
> Supported engines: DuckDB, Postgres, BigQuery (pandas & BigFrames), Databricks/Spark, Snowflake/Snowpark.
> **Execution & Cache quick notes**
> - Parallelism is level-wise; use `fft run --jobs N`.
> - Use `--cache={off|ro|rw|wo}` to control skipping behavior.
> - Fingerprints include rendered SQL / Python function source, selected `FF_*` env vars, `sources.yml` and upstream fingerprints.
> - Change any of these → downstream nodes rebuild.
> - `--rebuild <glob>` forces rebuilding selected models (ignores cache).


For an operational walkthrough (CLI usage, troubleshooting, pipelines) see the [Technical Overview](./Technical_Overview.md). This document focuses purely on how you author and test models.

---

## Docs Navigation
1. [Getting Started](./index.md)
2. [User Guide](./Technical_Overview.md#part-i-operational-guide)
3. **Modeling Reference** — you are here (`Config_and_Macros.md`)
4. [Developer Guide](./Technical_Overview.md#part-ii-architecture-internals)

---

## Table of Contents

- [Docs Navigation](#docs-navigation)
- [1. Model Fundamentals](#1-model-fundamentals)
  - [1.1 SQL models (`*.ff.sql`)](#11-sql-models-ffsql)
  - [1.2 Python models (`*.ff.py`)](#12-python-models-ffpy)
  - [1.3 Seeds, sources, and dependencies](#13-seeds-sources-and-dependencies)
- [2. `config()` options](#2-config-options)
- [3. Variables with `var()`](#3-variables-with-var)
- [4. Template context & helpers](#4-template-context-helpers)
- [5. Macros & reusable Jinja code](#5-macros-reusable-jinja-code)
- [6. Materialization semantics](#6-materialization-semantics)
- [7. Testing & quality gates](#7-testing-quality-gates)
- [8. Quick cheat sheet](#8-quick-cheat-sheet)

---

## 1. Model Fundamentals

FastFlowTransform discovers models under `<project>/models/` with two primary flavours:

### 1.1 SQL models (`*.ff.sql`)

- File stem defines the logical DAG node (`users.ff.sql` → `users.ff`).
- Jinja template rendered with FastFlowTransform context (helpers like `ref`, `source`, `var`, `config`, `this`).
- Output relation defaults to the stem without `.ff` (configurable via `config(alias=...)` if supported in future releases).

```sql
-- models/users.ff.sql
{{ config(materialized='table', tags=['staging']) }}
create or replace table users as
select id, email
from {{ source('crm', 'users') }};
```

### 1.2 Python models (`*.ff.py`)

Use the `@model` decorator from `fastflowtransform.core` to register a callable. The decorator accepts:

- `name` (optional) → overrides the logical name (defaults to stem).
- `deps` → list of dependency nodes (file stems or logical names).
- `requires` → column contract per dependency (validated via `validation.validate_required_columns`).
- `materialized` (optional) → `'table' | 'view' | 'ephemeral'`; mirrors `config(materialized=...)` for SQL.
- `tags` (optional) → convenience for attaching selection labels without writing `meta={"tags": ...}`.

Dependencies determine the call signature:

- Single dependency → function receives a single `pandas.DataFrame`.
- Multiple dependencies → function receives `dict[str, pandas.DataFrame]` keyed by physical relation name (e.g. `"users"`).

```python
# models/users_enriched.ff.py
from fastflowtransform.core import model
import pandas as pd

@model(
    name="users_enriched",
    deps=["users.ff"],
    requires={"users": {"id", "email"}},
    materialized="view",
)
def enrich(df: pd.DataFrame) -> pd.DataFrame:
    out = df.copy()
    out["is_gmail"] = out["email"].str.endswith("@gmail.com")
    return out
```

#### Engine-scoped registration

When the same project supports multiple execution backends, use `engine_model` to register a Python model only for specific engines. The decorator wraps `@model` but bails out early if the active engine (from `FF_ENGINE` or the selected profile) is not allowed.

```python
from fastflowtransform import engine_model
import pandas as pd

@engine_model(
    only=("duckdb", "postgres"),
    name="api_users_requests",
    deps=["users.ff"],
    tags=["example:api_demo", "scope:engine"],
)
def fetch(_: pd.DataFrame) -> pd.DataFrame:
    ...
```

Allowed values are case-insensitive strings or tuples. If the engine does not match, the function is left undecorated and no node is created, preventing duplicate registrations across engine-specific folders.

### 1.3 Seeds, sources, and dependencies

- Declare external tables in `sources.yml`; they become available via `source('group','table')`.
- Provide reproducible inputs with CSV/Parquet seeds in `<project>/seeds/`.
- FastFlowTransform auto-detects dependencies:
  - SQL models → parse `ref()` / `source()` calls.
  - Python models → use the decorator’s `deps`.
  - Additional runtime dependencies can be expressed via `relation_for(<node>)`.

> **Warning:** SQL dependency detection is static. Only literal calls such as `ref('users.ff')` are registered. When you need to gate a dependency behind a variable, materialise the options in a mapping (`{'foo': ref('foo'), 'bar': ref('bar')}`) and pick from that map at runtime; a bare `ref(variable)` will not show up in the DAG.

- Persistence (e.g. Spark/Databricks): configure default targets under `project.yml → models.storage` (and optionally `seeds.storage`). Example:

  ```yaml
  models:
    storage:
      api_users_http:
        path: ".local/spark/api_users_http"
        format: delta
        options:
          mergeSchema: true

  seeds:
    storage:
      users:
        path: ".local/spark/seeds/users"
  ```

  Entries end up in `node.meta["storage"]` (keys: `path`, `format`, `options`) and are respected by the matching executor.

```yaml
# sources.yml
version: 2

sources:
  - name: crm
    tables:
      - name: users
        identifier: seed_users
  - name: erp
    tables:
      - name: orders
        identifier: seed_orders
```

Each source can declare defaults such as `schema`, `database`, or `catalog`. Tables may
override those defaults, add per-engine overrides, or point at files:

```yaml
  - name: raw
    schema: staging
    tables:
      - name: seed_users
        identifier: seed_users
        overrides:
          postgres:
            schema: raw
          databricks_spark:
            format: delta
            location: "/mnt/delta/raw/seed_users"
```

---

## 2. `config()` options

Call `config()` at the top of SQL models. Python models get the same options via the `@model(..., materialized=..., tags=...)` decorator kwargs.

```sql
{{ config(
     materialized='view',
     tags=['mart', 'daily']
) }}
```

Supported keys:

| Key            | Type            | Description                                                                  |
|----------------|-----------------|------------------------------------------------------------------------------|
| `materialized` | `"table" \| "view" \| "ephemeral"` | Controls how FastFlowTransform persists the model. See [Materialization semantics](#6-materialization-semantics). |
| `tags`         | `list[str]`     | Arbitrary labels surfaced in docs / selection tooling.                       |
| `engines`      | `list[str]` or `str` | Restrict registration to the listed engines (case-insensitive). Requires the active engine to be known (profile selection or `FF_ENGINE`). |
| (future)       | –               | Additional metadata is stored under `node.meta[...]` if added later.         |

**Tips**

- Place `config()` before any SQL text.
- Use tags to power custom filters in docs or to drive test selection.
- Combine `engines=[...]` with per-engine subfolders to keep one physical file per backend without name clashes. When no engine is active, FastFlowTransform raises a clear error to avoid silent skips.
- Ephemeral models inline into downstream SQL; pick `view` for shareable logic without materializing a table.

---

## 3. Variables with `var()`

Project-level variables live under `project.yml → vars:` and can be overridden from the CLI:

```yaml
# project.yml
vars:
  snapshot_day: "2000-01-01"
  limit: 100
```

```bash
fft run . --vars snapshot_day='2025-10-01' limit=50
```

Usage in templates:

```sql
select *
from {{ source('crm','users') }}
where signup_date <= '{{ var("snapshot_day", "1970-01-01") }}'
limit {{ var("limit", 1000) }}
```

Resolution order: CLI overrides → project vars → default argument.

---

## 4. Template context & helpers

Every model (SQL & Python) gets a rich Jinja context. Key helpers:

| Helper             | Purpose                                                                                  |
|--------------------|------------------------------------------------------------------------------------------|
| `this`             | Object exposing `name`, `relation`, `materialized`, `schema`, `database`.                |
| `ref("model")`     | Resolves another model’s physical relation (or inlines ephemeral SQL).                   |
| `source("group","table")` | Resolves entries defined in `sources.yml`.                                             |
| `relation_for(node)` (Python utility) | Maps logical node names to physical relations (helpful inside UDFs/tests). |
| `var("key", default)` | Retrieves project/CLI variables (see above).                                           |

Example:

```sql
{{ config(materialized='view') }}
select
    u.id,
    u.email,
    {{ var("country_column", "'US'") }} as country_code
from {{ ref('users.ff') }} as u
-- rendered relation for logging/debugging
-- {{ this.relation }}
```

---

## 5. Macros & reusable Jinja code

Organise shared SQL snippets in `models/macros/` (all `.sql` files are auto-loaded):

```
models/
  macros/
    string_utils.sql
  marts/
    users.ff.sql
```

```jinja
{# models/macros/string_utils.sql #}
{% macro safe_lower(col) -%}
lower(trim({{ col }}))
{%- endmacro %}
```

Use the macro anywhere within the project:

```sql
select {{ safe_lower("email") }} as email_lower
from {{ ref('users.ff') }};
```

**Best practices**

- Keep macros idempotent and side-effect free.
- Group related macros per file (e.g., string utilities, date helpers).
- Document macros with inline comments; FastFlowTransform’s generated docs list each macro with its path.

---

## 6. Materialization semantics

### SQL models

| Materialization | Behaviour |
|-----------------|-----------|
| `table`         | `CREATE OR REPLACE TABLE … AS <SELECT …>` |
| `view`          | `CREATE OR REPLACE VIEW … AS <SELECT …>` |
| `ephemeral`     | No object is created; downstream `ref()` expands to a subquery. |

**Postgres-specific:** FastFlowTransform rewrites the “create or replace” pattern into `DROP TABLE IF EXISTS …; CREATE TABLE … AS …` for compatibility.

### Python models

- Default → materialized as `table`.
- `materialized='view'` produces an engine-specific temporary table first, then creates/overwrites a view that selects from it.
- Ephemeral Python models are not supported.

---

## 7. Testing & quality gates

### 7.1 Column contracts (`requires`)

Use the decorator’s `requires` argument (Python models) to ensure upstream inputs carry expected columns. Under the hood FastFlowTransform calls `validation.validate_required_columns`, raising `RequiredColumnsError` with a descriptive diff.

```python
@model(
    deps=["orders.ff", "users_enriched"],
    requires={
        "orders": {"order_id", "user_id", "amount"},
        "users_enriched": {"id", "email", "is_gmail"}
    }
)
def join_orders(inputs: dict[str, pd.DataFrame]) -> pd.DataFrame:
    ...
```

### 7.2 Data quality tests (`project.yml`)

Declare checks under `project.yml → tests:`. Each entry maps directly to a function in `fastflowtransform.testing` (`not_null`, `unique`, `row_count_between`, `greater_equal`, `non_negative_sum`, `freshness`). Run them via `fft test …`.

```yaml
tests:
  - type: not_null
    table: users
    column: email
    tags: [batch]
```

### 7.3 Model unit tests (`fft utest`)

Keep transformation logic honest with small, engine-agnostic specs:

- Place YAML files under `<project>/tests/unit/`.
- Express inputs via inline rows or CSV paths.
- Declare expected output rows plus comparison options (`order_by`, `any_order`, `ignore_columns`, `approx`).

```yaml
# tests/unit/users_enriched.yml
model: users_enriched
defaults:
  inputs:
    users:
      rows:
        - {id: 1, email: "a@example.com"}
        - {id: 2, email: "b@gmail.com"}
  expect:
    relation: users_enriched
    order_by: [id]

cases:
  - name: flags_gmail
    expect:
      rows:
        - {id: 1, email: "a@example.com", is_gmail: false}
        - {id: 2, email: "b@gmail.com",   is_gmail: true}
```

Run with:

```bash
fft utest . --env dev
fft utest . --model users_enriched --case flags_gmail
```

See the [Model Unit Tests guide](./Unit_Tests.md) for an exhaustive walkthrough (engine overrides, CI examples, troubleshooting).

---

## 8. Quick cheat sheet

| Task | Snippet / Pointer |
|------|-------------------|
| Set materialization | `{{ config(materialized='view') }}` |
| Add tags | `{{ config(tags=['mart','daily']) }}` |
| Read project variable | `{{ var('run_date', '1970-01-01') }}` |
| Current relation name | `{{ this.relation }}` |
| Reference another model | `{{ ref('users.ff') }}` |
| Reference source | `{{ source('crm','users') }}` |
| Macro definition | `models/macros/*.sql` |
| Guarantee columns (Python) | `@model(..., requires={'users': {'id','email'}})` |
| Data-quality test | `project.yml → tests` + `fft test …` |
| Unit test | `tests/unit/*.yml` + `fft utest …` |

---

Return to the [Docs Hub](./index.md) or switch to the [User/Developer Guide](./Technical_Overview.md).

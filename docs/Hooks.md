# Hooks

Hooks let you plug custom behavior into a FastFlowTransform run without changing your models.
They’re mainly used for:

* Auditing (run & model audit tables)
* Notifications/logging
* Lightweight data quality checks
* Custom side-effects around model execution

This page explains:

* **Lifecycle events** you can hook into
* How to **configure hooks in `project.yml`**
* How to write **Python hooks with `@fft_hook`**
* How to write **SQL hooks (inline & file-based)**
* What **context** each hook receives

---

## 1. Lifecycle events

There are two scopes:

* **Run-level hooks** – fire once per `fft run`
* **Model-level hooks** – fire for each model (either globally via `select` or directly on the model)

### Run-level events

| Event          | When it fires                                        | Config key in `project.yml` | Typical use                     |
| -------------- | ---------------------------------------------------- | --------------------------- | ------------------------------- |
| `on_run_start` | Right after the project loads, before any model runs | `hooks.on_run_start`        | Create audit tables, banners    |
| `on_run_end`   | After all models run & budgets are evaluated         | `hooks.on_run_end`          | Final audit row, summary, alert |

> **Note:** `on_run_end` is invoked even if some models fail.
> The hook receives a `run.status` of `"success"` or `"error"`.

### Model-level events

These are defined in `project.yml` under `hooks:` and applied to *models matching a selector*.

| Event          | Meaning                                           | Config key in `project.yml` |
| -------------- | ------------------------------------------------- | --------------------------- |
| `before_model` | Right before a model starts (per matching model)  | `hooks.before_model`        |
| `after_model`  | Right after a model finishes (per matching model) | `hooks.after_model`         |

Under the hood, these are attached to the model’s meta and executed as “pre/post hooks” around the model.

---

## 2. Configuring hooks in `project.yml`

Hooks live under the top-level `hooks:` key in `project.yml`.

### 2.1 Run-level hooks

Each hook entry is a **HookSpec** with at least:

* `name`: logical name of the hook
* `kind`: `"sql"` or `"python"`
* optional `engines`: list of engine names to restrict execution (e.g. `["duckdb", "bigquery"]`)
* optional `sql`: inline SQL body (for SQL hooks)
* optional `params`: extra free-form values passed to Python hook context

Example:

```yaml
hooks:
  on_run_start:
    - name: create_audit_tables
      kind: sql
      sql: |
        create table if not exists _ff_run_audit (
          run_id       text,
          started_at   timestamp,
          finished_at  timestamp,
          status       text,
          env          text,
          engine       text
        );

    - name: audit_run_start
      kind: sql
      sql: |
        insert into _ff_run_audit (run_id, started_at, status, env, engine)
        values (
          {{ run.run_id      | sql_literal }},
          current_timestamp,
          'running',
          {{ run.env_name    | sql_literal }},
          {{ run.engine_name | sql_literal }}
        );

    - name: python_banner
      kind: python

  on_run_end:
    - name: audit_run_end
      kind: sql      # SQL body lives in hooks/audit_run_end.sql
    - name: python_summary
      kind: python
```

Notes:

* For **Python hooks**, `name` must match the registration name in the decorator (see below).
* For **SQL hooks**, the SQL can be inline (`sql:`) or come from a `.sql` file in `hooks/` (see section 4).

---

### 2.2 Model-level hooks with `select`

Model hooks can be attached **by selector**, not by hard-coding them into each model.
This is what `before_model` and `after_model` are for:

```yaml
hooks:
  before_model:
    - name: model_start_audit
      kind: sql
      select: "tag:example:hooks_demo"
      sql: |
        insert into _ff_model_audit (
          run_id,
          model_name,
          event,
          status,
          started_at
        )
        values (
          {{ run.run_id   | sql_literal }},
          {{ model.name   | sql_literal }},
          'start',
          'running',
          current_timestamp
        );

  after_model:
    - name: model_end_audit
      kind: sql
      select: "tag:example:hooks_demo"
      sql: |
        update _ff_model_audit
        set finished_at    = current_timestamp,
            status         = 'success',
            rows_affected  = null,
            elapsed_ms     = null
        where run_id      = {{ run.run_id  | sql_literal }}
          and model_name  = {{ model.name  | sql_literal }}
          and event       = 'start';

    - name: model_end_log_python
      kind: python
      select: "tag:scope:mart"
```

* `select` uses the same selector language as `fft run` (`tag:…`, `model:…`, etc.).
* The hook is executed for each model that matches the selector.

---

## 3. Python hooks

Python hooks live in the project’s `hooks/` directory (any subfolder), and are registered via the `@fft_hook` decorator.

### 3.1 Basic structure

Example `hooks/notify.py`:

```python
from __future__ import annotations
from typing import Any
from fastflowtransform.hooks.registry import fft_hook

def _fmt_env(env: dict[str, Any]) -> str:
    parts = []
    for key in ("FF_ENGINE", "FFT_ACTIVE_ENV"):
        if key in env:
            parts.append(f"{key}={env[key]}")
    return ", ".join(parts) if parts else "<no env>"

@fft_hook(name="python_banner", when="on_run_start")
def on_run_start(ctx: dict[str, Any]) -> None:
    run = ctx.get("run", {})
    env = ctx.get("env", {})
    print(
        f"[hooks_demo] on_run_start: run_id={run.get('run_id')} "
        f"(env_name={run.get('env_name')}, engine={run.get('engine_name')}; {_fmt_env(env)})"
    )

@fft_hook(name="python_summary", when="on_run_end")
def on_run_end(ctx: dict[str, Any]) -> None:
    run   = ctx.get("run", {})
    stats = ctx.get("stats", {}) or {}
    print(
        "[hooks_demo] on_run_end: run_id=%s status=%s (built=%s, skipped=%s, failed=%s)"
        % (
            run.get("run_id"),
            run.get("status"),
            stats.get("models_built"),
            stats.get("models_skipped"),
            stats.get("models_failed"),
        )
    )

@fft_hook(name="model_end_log_python", when="after_model")
def on_model_end(ctx: dict[str, Any]) -> None:
    run   = ctx.get("run", {})
    model = ctx.get("model", {})
    print(
        "[hooks_demo] on_model_end: run_id=%s model=%s"
        % (run.get("run_id"), model.get("name"))
    )
```

> All Python files under `hooks/**.py` are loaded when the run starts; their `@fft_hook` decorators populate the registry.

### 3.2 The `@fft_hook` decorator

```python
@fft_hook(name="python_banner", when="on_run_start")
def on_run_start(ctx: dict[str, Any]) -> None:
    ...
```

* **`name`**
  Logical name; must match `project.yml`’s `name` field for the hook.
  If omitted, defaults to the function name.

* **`when`**
  Lifecycle event this hook is for, e.g.:

  * `"on_run_start"`
  * `"on_run_end"`
  * `"before_model"`
  * `"after_model"`

> Only these values are accepted; anything else will raise at registration time.

### 3.3 Python hook context

Python hooks always receive a **single dictionary argument**, the *hook context*.

Shape (simplified):

```python
ctx: dict[str, Any] = {
    "when": "on_run_start" | "on_run_end" | "before_model" | "after_model",
    "run": {
        "run_id": str,          # unique run identifier
        "env_name": str,        # profile/env name (e.g. 'dev_duckdb')
        "engine_name": str,     # engine (e.g. 'duckdb')
        "started_at": str,      # ISO timestamp
        "status": str | None,   # on_run_end: 'success' | 'error'
        "row_count": int | None,
        "error": str | None,
    },
    "model": {
        "name": str,
        "path": pathlib.Path,
        "tags": list[str],
        "meta": dict[str, Any],
        # (future extensions: status/rows/elapsed/error for model events)
    } | None,
    "env": dict[str, str],      # env vars relevant to FFT (FF_* etc.)
    # Only for on_run_end:
    "stats": {
        "models_built": int,
        "models_failed": int,
        "models_skipped": int,
    } | None,
    # Plus any extra keys from HookSpec.params:
    # e.g. ctx["slack_channel"], ctx["threshold"], ...
}
```

So for simple hooks you can do:

```python
run = ctx["run"]
if ctx["when"] == "on_run_end" and run["status"] == "error":
    ...
```

---

## 4. SQL hooks

SQL hooks are just Jinja-templated SQL statements that are executed via your target engine.

You can define them:

1. **Inline** in `project.yml`; or
2. In a **`.sql` file under `hooks/`**, referenced by name.

### 4.1 Inline SQL

Inline SQL was shown in the examples above:

```yaml
- name: audit_run_start
  kind: sql
  sql: |
    insert into _ff_run_audit (run_id, started_at, status, env, engine)
    values ({{ run.run_id }}, current_timestamp, 'running', {{ run.env_name }}, {{ run.engine_name }});
```

### 4.2 File-based SQL (`hooks/**/*.sql`)

If `kind: sql` has **no `sql:` body**, FFT will look for a `.sql` file:

* Root: `<project_dir>/hooks`
* Pattern: `hooks/**/<name>.sql`
* `name` is the `HookSpec.name` from `project.yml`

Example:

```yaml
hooks:
  on_run_end:
    - name: audit_run_end
      kind: sql      # SQL body lives in hooks/audit_run_end.sql
```

File layout:

```text
hooks/
  audit_run_end.sql
  model_start_audit.sql
  audit/
    complex_audit_for_marts.sql   # name: complex_audit_for_marts
```

`hooks/audit_run_end.sql`:

```sql
-- examples/hooks_demo/hooks/audit_run_end.sql
-- Update the run-level audit row when the run finishes.

update _ff_run_audit
set
    finished_at = current_timestamp,
    status      = 'success',
    row_count   = NULL,
    error       = NULL
where run_id = {{ run.run_id | sql_literal }};
```

If no matching file is found, the run fails with a clear error.

---

## 5. Jinja context for SQL hooks

SQL hook templates are rendered with:

* `run`: run context (similar to Python hooks, but with some fields already converted to SQL literals)
* `model`: model context (for model-level hooks), or `None` for pure run hooks
* `node`: alias of `model`

### 5.1 `run` context

In SQL hooks:

* All `run.*` fields are **plain values** in the Jinja context.
* When inlining them into SQL, always pass them through `| sql_literal` to get a safe SQL literal.

Example:

```sql
insert into _ff_run_audit (run_id, started_at, status, env, engine)
values ({{ run.run_id }}, current_timestamp, 'running', {{ run.env_name }}, {{ run.engine_name }});
```

### 5.2 `model` context

For model-level hooks:

```jinja2
{{ model.name }}   -- logical model name ('events_clean.ff')
{{ model.path }}   -- full filesystem path
{{ model.tags }}   -- list of tags
{{ model.meta }}   -- model meta dict from config(...)
```

You can use `model` or `node` – they’re the same object.

### 5.3 `sql_literal` filter

To safely inline values into SQL hook templates, use the `sql_literal` filter:

* `None` or Jinja `Undefined` → `NULL`
* `bool` → `TRUE` / `FALSE`
* `int` / `float` → `123`, `1.23`
* `str` → single-quoted with internal quotes escaped
* Other types → JSON-dumped and then single-quoted

Examples:

```jinja2
where run_id = {{ run.run_id | sql_literal }};

set status = {{ model.status | sql_literal }};
set rows_affected = {{ model.rows_affected | sql_literal }};
```

This helps avoid syntax errors and SQL injection in generated hook SQL.

---

## 6. Error handling & logging

* If a hook (SQL or Python) raises an exception, the run fails with a message like:

  > `Failed to execute on_run_end hook #1 for run: ...`

* Hook execution is logged with the `[hooks]` prefix, for example:

  ```text
  [FFT] [hooks] when=on_run_start node=<run>: executing 3 hook(s): sql:create_audit_tables, sql:audit_run_start, python:python_banner
  [FFT] [hooks] when=on_run_start node=<run> hook#1 kind=sql name='create_audit_tables' – rendering SQL
  [FFT] [hooks] when=on_run_start node=<run> hook#1 name='create_audit_tables' executing SQL:
  ...
  [FFT] [hooks] when=on_run_end node=<run> hook#2 kind=python name='python_summary' – invoking python hook
  ```

This makes it easy to see **which hooks were registered** and **exactly what SQL** they ran.

---

## 7. Best practices

* **Keep hooks idempotent**
  Especially SQL hooks: include `run_id` and `model_name` in audit tables so reruns don’t break things.

* **Scope hooks with `select`**
  Use tags (`tag:scope:mart`, `tag:example:hooks_demo`) so hooks don’t run on every model.

* **Be defensive in Python hooks**
  Treat `ctx` as a dict that may or may not have everything you expect (use `.get()`).

* **Avoid heavy work in hooks**
  Hooks run inside the main pipeline. Use them for auditing/logging/notifications, not for big ETL jobs.

* **Use `sql_literal` in SQL hooks**
  Whenever you inline values, go through `| sql_literal` instead of crafting quotes by hand.

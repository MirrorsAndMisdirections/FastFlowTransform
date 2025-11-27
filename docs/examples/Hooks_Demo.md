# Hooks Demo

This example project shows how to use **run-level** and **model-level** hooks in FastFlowTransform to build a simple **audit + logging** system around a run.

You’ll see how to:

* Configure hooks in `project.yml`
* Implement **Python hooks** with `@fft_hook`
* Implement **SQL hooks** (inline and file-based)
* Write models that are targeted via **selectors** (tags)
* Store run/model metadata in simple **audit tables**

---

## 1. What this example does

When you run the project:

1. **Run-level hooks** create two audit tables and write a **run-start** row.
2. For each selected model:

   * A **before-model** SQL hook writes a `start` row to `_ff_model_audit`.
   * The model runs (`events_clean.ff`, then `mart_events_daily.ff`).
   * An **after-model** SQL hook updates that row with status, timestamps, etc.
   * A **Python model hook** logs some info for mart models.
3. **Run-end hooks**:

   * A SQL hook updates the run row in `_ff_run_audit` to `success` or `error`.
   * A Python hook prints a human-readable summary of the run.

At the end you can query:

* `_ff_run_audit` – high-level runs overview
* `_ff_model_audit` – per-model lifecycle events

---

## 2. Project layout

Rough structure:

```text
hooks_demo/
  project.yml
  models/
    events_clean.ff.sql
    mart_events_daily.ff.sql
  hooks/
    notify.py              # Python hooks (using @fft_hook)
    audit_run_end.sql      # File-based SQL hook (on_run_end)
```

### Models

Both models are standard SQL models with tags:

* `models/events_clean.ff.sql`

  * Staging/cleaning model
  * Tags include: `example:hooks_demo`, `scope:staging`, `engine:duckdb`, …

* `models/mart_events_daily.ff.sql`

  * Simple daily mart over events
  * Tags include: `example:hooks_demo`, `scope:mart`, `engine:duckdb`, …

The important part is the **tags**, because the model hooks use selectors like `tag:example:hooks_demo` and `tag:scope:mart`.

---

## 3. Hooks in `project.yml`

Open `project.yml` and look at the `hooks:` section. It roughly looks like this:

```yaml
hooks:
  on_run_start:
    - name: create_audit_tables
      kind: sql
      sql: |-
        -- create _ff_run_audit and _ff_model_audit if they don't exist
        ...

    - name: audit_run_start
      kind: sql
      sql: |-
        insert into _ff_run_audit (run_id, started_at, status, env, engine)
        values (
          {{ run.run_id }},
          current_timestamp,
          'running',
          {{ run.env_name }},
          {{ run.engine_name }}
        );

    - name: python_banner
      kind: python

  on_run_end:
    - name: audit_run_end
      kind: sql          # body in hooks/audit_run_end.sql
    - name: python_summary
      kind: python

  before_model:
    - name: model_start_audit
      kind: sql
      select: "tag:example:hooks_demo"
      sql: |-
        insert into _ff_model_audit (...)
        values (... {{ run.run_id }}, {{ model.name }}, 'start', 'running', current_timestamp);

  after_model:
    - name: model_end_audit
      kind: sql
      select: "tag:example:hooks_demo"
      sql: |-
        update _ff_model_audit
        set finished_at = current_timestamp,
            status      = 'success',
            rows_affected = NULL,
            elapsed_ms    = NULL
        where run_id     = {{ run.run_id }}
          and model_name = {{ model.name }}
          and event      = 'start';

    - name: model_end_log_python
      kind: python
      select: "tag:scope:mart"
```

Key ideas:

* **Run-level hooks** (`on_run_start`, `on_run_end`) apply to the whole run.
* **Model-level hooks** (`before_model`, `after_model`) are scoped with `select`.
* `kind: sql` hooks are SQL templates.
* `kind: python` hooks are Python functions registered via `@fft_hook` in `hooks/*.py`.

---

## 4. Python hooks (`hooks/notify.py`)

Python hooks are implemented using the `@fft_hook` decorator.

In `hooks/notify.py` you’ll see something like:

```python
from __future__ import annotations
from typing import Any
from fastflowtransform.hooks.registry import fft_hook

def _fmt(env: dict[str, Any]) -> str:
    parts = []
    for key in ("FFT_ACTIVE_ENV", "FF_ENGINE", "FF_ENGINE_VARIANT"):
        if key in env:
            parts.append(f"{key}={env[key]}")
    return ", ".join(parts) if parts else "<no FFT_* env>"

@fft_hook(name="python_banner", when="on_run_start")
def on_run_start(context: dict[str, Any]) -> None:
    run = context.get("run", {})
    env = context.get("env", {})
    info = _fmt(env if isinstance(env, dict) else {})
    print(
        f"[hooks_demo] on_run_start: run_id={run.get('run_id')} "
        f"(env_name={run.get('env_name')}, engine={run.get('engine_name')}; {info})"
    )

@fft_hook(name="python_summary", when="on_run_end")
def on_run_end(context: dict[str, Any]) -> None:
    run   = context.get("run", {})
    stats = context.get("stats", {}) or {}
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
def on_model_end(context: dict[str, Any]) -> None:
    run   = context.get("run", {})
    model = context.get("model", {})
    print(
        "[hooks_demo] on_model_end: run_id=%s model=%s status=%s"
        % (run.get("run_id"), model.get("name"), model.get("status"))
    )
```

Important bits:

* `@fft_hook(name=..., when=...)` must match the `name` and event in `project.yml`.
* Each hook receives a single `context: dict[str, Any]` argument with:

  * `context["when"]` – lifecycle event string
  * `context["run"]` – run-level info (run_id, env_name, engine_name, status, …)
  * `context["model"]` – model info for model-level events (`before_model` / `after_model`)
  * `context["env"]` – environment variables snapshot (selected `FF_*` vars)
  * `context["stats"]` – only for `on_run_end`, contains counts like `models_built`

For this demo, the Python hooks just print human-readable lines with `[hooks_demo]` prefixes so you can see them in the CLI output.

---

## 5. SQL hooks (`hooks/*.sql` + inline)

### 5.1 Audit tables

The **run-level** audit table:

```sql
create table if not exists _ff_run_audit (
  run_id       text,
  started_at   timestamp,
  finished_at  timestamp,
  status       text,        -- 'running' | 'success' | 'error'
  env          text,
  engine       text,
  row_count    bigint,      -- optional aggregate info
  error        text         -- error message if the run fails
);
```

The **model-level** audit table:

```sql
create table if not exists _ff_model_audit (
  run_id        text,
  model_name    text,
  event         text,       -- 'start' | 'end'
  status        text,       -- 'running' | 'success' | 'error'
  started_at    timestamp,
  finished_at   timestamp,
  rows_affected bigint,
  elapsed_ms    bigint,
  error         text
);
```

These are created by the `create_audit_tables` SQL hook on `on_run_start`.

### 5.2 File-based `on_run_end` SQL hook

The `on_run_end` SQL hook `audit_run_end` is defined without inline SQL in `project.yml`, so its body lives in `hooks/audit_run_end.sql`:

```yaml
on_run_end:
  - name: audit_run_end
    kind: sql
```

`hooks/audit_run_end.sql`:

```sql
-- Update the run-level audit row when the run finishes.

update _ff_run_audit
set
    finished_at = current_timestamp,
    status      = 'success',  -- or use {{ run.status | sql_literal }} if you want dynamic
    row_count   = NULL,
    error       = NULL
where run_id = {{ run.run_id | sql_literal }};
```

This demonstrates the file-based hook resolution:

* `kind: sql` with **no** `sql:` body → look for `hooks/**/<name>.sql`.
* `name: audit_run_end` → `hooks/audit_run_end.sql`.

### 5.3 Inline model-level SQL hooks

The model-level hooks are inline SQL templates in `project.yml`.
They use `{{ run.run_id }}`, `{{ model.name }}`, and the `sql_literal` filter when needed.

---

## 6. Data quality tests for the demo

`project.yml` also includes some simple tests to show that the audit tables are populated (you can adjust these as needed):

```yaml
tests:
  # Event-level model tests (example)
  - type: not_null
    table: events_clean
    column: event_id
    tags: [example_hooks_demo]

  - type: row_count_between
    table: mart_events_daily
    min_rows: 1
    max_rows: 1000
    tags: [example_hooks_demo]

  # Audit tables (sanity checks)
  - type: row_count_between
    table: _ff_run_audit
    min_rows: 1
    max_rows: 100
    tags: [example_hooks_demo, audit, run]

  - type: row_count_between
    table: _ff_model_audit
    min_rows: 2
    max_rows: 200
    tags: [example_hooks_demo, audit, model]
```

If the audits are not written as expected, these tests will fail and point you at the relevant table.

---

## 7. Running the demo

Assuming:

* Your working directory is `examples/hooks_demo` (or similar)
* You have a profile called `dev_duckdb` configured for DuckDB

Run:

```bash
fft run . \
  --env dev_duckdb \
  --select tag:example:hooks_demo \
  --select tag:engine:duckdb
```

You should see log lines like:

```text
[FFT] Profile: dev_duckdb | Engine: duckdb
[FFT] [hooks] when=on_run_start node=<run>: executing 3 hook(s): ...
[hooks_demo] on_run_start: run_id=... (env_name=dev_duckdb, engine=duckdb; ...)
[FFT] ▶ L01 [DUCK] events_clean.ff (hooks_demo.events_clean)
[FFT] ✓ L01 ...
[FFT] ▶ L02 [DUCK] mart_events_daily.ff (hooks_demo.mart_events_daily)
[FFT] ✓ L02 ...
[FFT] [hooks] when=on_run_end node=<run>: executing 2 hook(s): ...
[hooks_demo] on_run_end: run_id=... status=success (built=2, skipped=0, failed=0)
```

The `[hooks]` lines come from the internal hook runner; the `[hooks_demo]` lines come from your Python hooks in `hooks/notify.py`.

---

## 8. Inspecting the results

After running the demo, connect to your DuckDB database (or whatever engine you used) and inspect the audit tables.

Examples (DuckDB):

```sql
select * from _ff_run_audit order by started_at desc;

select * from _ff_model_audit
order by run_id, model_name, started_at;
```

You should see:

* One `_ff_run_audit` row for the run you just executed.
* At least two `_ff_model_audit` rows (one per model, possibly two per model if you log both start and end).

---

## 9. How to extend this demo

Ideas:

* Add a **Slack/Teams/email** notification in a Python `on_run_end` hook when `run.status == "error"`.
* Record **row counts** and **elapsed time** from your executor into `_ff_model_audit` and expose them in hooks.
* Add more **selective hooks**:

  * e.g. `select: "tag:scope:mart"` for mart-only audits or notifications.
* Create multiple **file-based SQL hooks** under `hooks/` and wire them via `kind: sql` + `name:` in `project.yml`.

This demo is mainly a **template for hook patterns**: once you understand how these pieces fit together, you can copy the same approach to real projects (with your own audit tables, logging conventions, and alerts).

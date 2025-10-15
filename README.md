# FlowForge (PoC 0.1)

[![CI](https://github.com/<org>/<repo>/actions/workflows/ci.yml/badge.svg)](https://github.com/<org>/<repo>/actions/workflows/ci.yml)
[![PyPI version](https://img.shields.io/pypi/v/flowforge.svg)](https://pypi.org/project/flowforge/)

> ⚠️ **Project status:** early proof-of-concept. Stable enough for demos and smaller workflows. Public APIs may still change.

---

## Table of Contents

- [Overview](#overview)
- [Key Features](#key-features)
- [Requirements](#requirements)
- [Installation](#installation)
- [Quickstart](#quickstart)
- [Project Layout](#project-layout)
- [CLI Commands](#cli-commands)
- [Configuration](#configuration)
- [Data Quality Checks](#data-quality-checks)
- [Examples](#examples)
- [Troubleshooting](#troubleshooting)
- [Roadmap Snapshot](#roadmap-snapshot)
- [Contributing](#contributing)
- [License](#license)

---

## Overview

FlowForge combines SQL and Python models in a lightweight DAG engine. A project is simply a directory with models, optional seeds, and configuration. The CLI renders SQL, runs Python models, materialises results, generates HTML documentation, and executes data-quality checks against multiple execution backends.

---

## Key Features

- **Polyglot modelling:** build transformation nodes in SQL (`*.ff.sql`) or Python (`*.ff.py`) and wire them together with `ref()`/`source()` and `deps=[...]`.
- **Multiple executors:** DuckDB (local default), Postgres, BigQuery (classic + BigFrames), Databricks Spark, and Snowflake Snowpark are supported via pluggable executors.
- **Deterministic DAG:** dependencies are resolved statically; `flowforge dag` renders either Mermaid source or a ready-to-view HTML mini site.
- **Data quality built in:** configure checks such as `not_null`, `unique`, `row_count_between`, `greater_equal`, `non_negative_sum`, and `freshness` in `project.yml`.
- **Environment-aware configuration:** `profiles.yml` plus environment variables (`FF_*`) drive executor settings; CLI flags can override at runtime.
- **Seeds & docs:** `flowforge seed` loads CSV/Parquet seeds, and `flowforge dag --html` produces browsable documentation for every model.

---

## Requirements

- Python **3.12+**
- Optional client libraries per executor (e.g. `google-cloud-bigquery`, `snowflake-snowpark-python`, `pyspark`, appropriate database drivers). Install only what you need for your chosen backend.

---

## Installation

```bash
python -m pip install --upgrade pip
pip install -e .
# Optional: install pre-commit hooks
pip install pre-commit
pre-commit install
```

You can also bootstrap everything with the provided Makefile:

```bash
make install      # upgrades pip + installs FlowForge in editable mode
```

---

## Quickstart

Run the end-to-end DuckDB demo (seed → run → docs → tests) in under a minute:

```bash
make demo
```

The target project lives in `examples/simple_duckdb`. After the demo finishes you'll find the rendered DAG at `examples/simple_duckdb/docs/index.html`. Open it via:

```bash
open examples/simple_duckdb/docs/index.html    # macOS
xdg-open examples/simple_duckdb/docs/index.html  # Linux
```

If you prefer manual control:

```bash
flowforge seed examples/simple_duckdb --env dev
flowforge run  examples/simple_duckdb --env dev
flowforge dag  examples/simple_duckdb --env dev --html
flowforge test examples/simple_duckdb --env dev --select batch
```

---

## Project Layout

```
my_project/
├─ models/
│  ├─ users.ff.sql
│  └─ users_enriched.ff.py
├─ seeds/              # optional CSV/Parquet seeds (+ schema.yml)
├─ profiles.yml        # executor configuration per environment
├─ project.yml         # optional data-quality config
└─ docs/               # generated output lives here
```

**SQL model example (`models/users.ff.sql`):**

```sql
create or replace table users as
select id, email
from {{ source('crm', 'users') }};
```

**Python model example (`models/users_enriched.ff.py`):**

```python
from flowforge import model
import pandas as pd

@model(name="users_enriched", deps=["users"])
def enrich(df: pd.DataFrame) -> pd.DataFrame:
    out = df.copy()
    out["is_gmail"] = out["email"].str.endswith("@gmail.com")
    return out
```

- **Dependencies** in SQL use `ref("other_model")` or `source("source_name","table")`.
- **Python models** receive either a DataFrame (single dependency) or a mapping of relation name → DataFrame (multiple dependencies).

---

## CLI Commands

| Command | Description | Typical Usage |
|---------|-------------|----------------|
| `flowforge run <project> [--env ENV]` | Renders SQL, executes Python models, materialises outputs. | `flowforge run examples/simple_duckdb --env dev` |
| `flowforge dag <project> [--env ENV] [--html]` | Generates a Mermaid DAG (`docs/dag.mmd`) or full HTML docs (`docs/index.html`). | `flowforge dag . --env dev --html` |
| `flowforge test <project> [--env ENV] [--select TAG]` | Runs configured data-quality checks. Returns exit code `2` if any check fails. | `flowforge test . --env dev --select batch` |
| `flowforge seed <project> [--env ENV]` | Loads CSV/Parquet seeds into the executor (DuckDB/Postgres/etc.). | `flowforge seed . --env dev` |
| `flowforge --version` | Prints the installed FlowForge version. | `flowforge --version` |

The CLI honours `FF_*` environment variables. Set `FLOWFORGE_SQL_DEBUG=1` to log dependency columns while executing Python models.

---

## Configuration

### `profiles.yml`

Defines executor settings per environment:

```yaml
dev:
  engine: duckdb
  duckdb:
    path: ":memory:"

stg:
  engine: postgres
  postgres:
    dsn: postgresql+psycopg://postgres:postgres@localhost:5432/ffdb
    db_schema: public

prod:
  engine: bigquery
  bigquery:
    dataset: my_company_analytics
    location: EU
    use_bigframes: false   # optional
```

### Environment variables (`FF_*`)

Override profile defaults without editing `profiles.yml`:

```bash
export FF_ENGINE=postgres
export FF_PG_DSN=postgresql+psycopg://postgres:postgres@localhost:5432/ffdb
export FF_PG_SCHEMA=analytics
```

### Priority

`profiles.yml` < `.env` / environment variables (`FF_*`) < CLI flag (`--engine`).

---

## Data Quality Checks

Declare checks in `project.yml` under the `tests:` key. Each entry maps directly to functions in `flowforge.testing`:

```yaml
tests:
  - type: not_null
    table: users
    column: id
    tags: [batch]

  - type: unique
    table: users
    column: email
    tags: [batch]

  - type: row_count_between
    table: mart_orders_enriched
    min: 1
    max: 1_000_000
    tags: [batch]

  - type: freshness
    table: mart_events
    column: event_timestamp
    max_delay_minutes: 15
    tags: [streaming]
```

- Tag checks (e.g. `batch`, `streaming`) to drive selective execution via `--select`.
- Failures raise `TestFailure` and surface the offending SQL statement.
- The testing shim automatically adapts to the active executor (DuckDB, Postgres, BigQuery, Spark, Snowflake).

---

Here’s a drop-in section you can copy into your `README.md` to document the **unit tests for models (utests)**.

---

## 🧪 Model Unit Tests (`flowforge utest`)

`flowforge utest` lets you write **tiny, engine-agnostic unit tests** for each model.
You provide **inputs** (small tables as rows/CSV) and **expected output** rows, and FlowForge executes **only the target model** in an isolated sandbox (DuckDB or Postgres) and compares the result.

### Why?

* Fast feedback on model logic (SQL or Python).
* Reproducible, small fixtures (no large seed DB required).
* Works across engines to spot dialect differences.

---

### Folder layout

FlowForge discovers YAML specs at `<project>/tests/unit/*.yml` (relative to the project path passed to the CLI, i.e., where `models/` lives):

```
your-project/
├── models/
│   ├── users.ff.sql
│   ├── users_enriched.ff.py
│   └── marts_daily.ff.sql
└── tests/
    └── unit/
        ├── users_enriched.yml
        └── marts_daily.yml
```

---

### YAML DSL (with `defaults`)

Each YAML describes tests for **one model** by logical node name (as registered in the DAG).

#### Example: Python model `users_enriched`

```yaml
# tests/unit/users_enriched.yml
model: users_enriched

# Defaults are deep-merged into each case (cases can override).
defaults:
  inputs:
    users:                 # physical relation name (relation_for('users.ff') => "users")
      rows:
        - {id: 1, email: "a@example.com"}
        - {id: 2, email: "b@gmail.com"}
  expect:
    relation: users_enriched
    order_by: [id]

cases:
  - name: basic_gmail_flag
    expect:
      rows:
        - {id: 1, email: "a@example.com", is_gmail: false}
        - {id: 2, email: "b@gmail.com",   is_gmail: true}

  - name: override_inputs
    inputs:
      users:
        rows:
          - {id: 3, email: "c@hotmail.com"}
          - {id: 4, email: "d@gmail.com"}
    expect:
      rows:
        - {id: 3, email: "c@hotmail.com", is_gmail: false}
        - {id: 4, email: "d@gmail.com",   is_gmail: true}
```

#### Example: SQL model `marts_daily.ff.sql` (produces `mart_users`)

```yaml
# tests/unit/marts_daily.yml
model: marts_daily.ff     # node name (file stem including .ff)

defaults:
  inputs:
    users_enriched:       # physical relation (ref('users_enriched') -> "users_enriched")
      rows:
        - {id: 1, email: "a@example.com", is_gmail: false}
        - {id: 2, email: "b@gmail.com",   is_gmail: true}
  expect:
    relation: mart_users  # IMPORTANT: matches the CREATE TABLE name in your SQL
    order_by: [id]

cases:
  - name: passthrough_columns
    expect:
      rows:
        - {id: 1, email: "a@example.com", is_gmail: false}
        - {id: 2, email: "b@gmail.com",   is_gmail: true}

  - name: override_upstream
    inputs:
      users_enriched:
        rows:
          - {id: 10, email: "x@gmail.com", is_gmail: true}
    expect:
      rows:
        - {id: 10, email: "x@gmail.com", is_gmail: true}
```

#### Multi-dependency model (outline)

If a model depends on multiple inputs (e.g., `orders.ff` and `users_enriched`), provide **both** physical relations:

```yaml
model: mart_orders_enriched
defaults:
  inputs:
    users_enriched:
      rows:
        - {id: 1, email: "x@gmail.com", is_gmail: true}
    orders:
      rows:
        - {order_id: 10, user_id: 1, amount: 19.9}
        - {order_id: 11, user_id: 1, amount: -1.0}
cases:
  - name: join_and_flag
    expect:
      any_order: true
      rows:
        - {order_id: 10, user_id: 1, email: "x@gmail.com", is_gmail: true, amount: 19.9, valid_amt: true}
        - {order_id: 11, user_id: 1, email: "x@gmail.com", is_gmail: true, amount: -1.0, valid_amt: false}
```

---

### Input formats

* **Inline rows**

  ```yaml
  inputs:
    users:
      rows:
        - {id: 1, email: "a@example.com"}
  ```

* **CSV**

  ```yaml
  inputs:
    users:
      csv: tests/fixtures/users_small.csv
  ```

> Keys under `inputs` are **physical** relation names (the actual table/view names models read from).
> Tip: `relation_for('users.ff') == "users"`.

---

### Expected output & comparison options

Under `expect` (in defaults or per case):

* `relation`: target relation to compare (default: `relation_for(model)`).
* `rows`: expected rows (list of objects).
* Ordering:

  * `order_by: [col, ...]`
  * `any_order: true` (sorts deterministically by common columns before comparing)
* Columns:

  * `ignore_columns: [col, ...]`
  * `subset: true` (expected rows must be a subset of actual)
* Numeric tolerances:

  * `approx: { col: 1e-9, other_col: 0.01 }`
    (write numbers without quotes; `"1e-9"` as a string is also accepted by the runner, which casts to float)

---

### Running utests

Run from the project root (the folder containing `models/`):

```bash
# discover and run all YAML specs under ./tests/unit
flowforge utest .

# with a specific env profile
flowforge utest . --env dev

# only a specific model / case
flowforge utest . --model users_enriched
flowforge utest . --model mart_orders_enriched --case join_and_flag

# run just one YAML file
flowforge utest . --path tests/unit/users_enriched.yml
```

#### Engine override

You can override the execution backend for all specs:

```bash
# Postgres example (ensure DSN/Schema are set)
export FF_PG_DSN="postgresql+psycopg://postgres:postgres@localhost:5432/ffdb"
export FF_PG_SCHEMA="public"
flowforge utest . --engine postgres
```

Engine precedence (highest → lowest):

1. CLI `--engine`
2. YAML `engine:` (optional, per file)
3. `profiles.yml`
4. environment overrides

> If a model uses engine-specific SQL functions, pin `engine: duckdb` in that YAML file.

---

## Logging & Verbosity

FlowForge provides consistent, CLI-wide logging controls and a dedicated SQL debug channel.

### Flags

* `-q` / `--quiet`
  Show only errors (`ERROR`).

* *(default)*
  Concise output (`WARNING`).

* `-v` / `--verbose`
  Show progress/info (`INFO`). Useful to see which models run.

* `-vv`
  Full debug (`DEBUG`), including SQL-related debug messages.

> Internally, `-vv` also enables the SQL debug channel (same effect as the env var below).

### SQL debug (inputs, columns, DQ helpers)

Some executors log helpful details about inputs and columns per Python model.
These appear when either:

* you pass `-vv`, **or**
* you set the environment variable `FLOWFORGE_SQL_DEBUG=1`.

```bash
# full debug (recommended)
flowforge run . -vv

# equivalent using the env var (kept for compatibility)
FLOWFORGE_SQL_DEBUG=1 flowforge run .
```

### Examples

```bash
# Quiet (errors only)
flowforge run . -q

# Normal (concise)
flowforge run .

# Verbose progress (which model runs, engine info)
flowforge run . -v

# Debug everything (progress + SQL debug)
flowforge run . -vv
```

### Notes

* Debug output from data-quality helpers (`flowforge.testing`) and Python-model inputs is routed through the `flowforge.sql` logger; that’s why `-vv` or `FLOWFORGE_SQL_DEBUG=1` is required to see it.
* You don’t need to change existing projects: the env var continues to work exactly as before.

---

### Exit codes & reporting

* **0** → all cases passed
* **2** → at least one failure

Output shows each case with ✅/❌ and a compact CSV-style diff on mismatch.
Set `FLOWFORGE_SQL_DEBUG=1` for dependency/column debug logs.

---

### Common pitfalls & troubleshooting

* **“Dependency table not found: 'X'”**:
  The case didn’t supply all required **physical** inputs for the model’s dependencies.
  Check the model’s deps and add missing relations to `inputs:` (e.g., both `orders` and `users_enriched`).

* **Approx mismatch / type issues**:
  Use numeric `approx` values (e.g., `1e-9`, **not** `"1e-9"`). The runner also auto-casts strings to numeric for approx columns.

* **Postgres schema**:
  Set `FF_PG_SCHEMA` (or configure in profiles) so reads/writes land in the intended schema.

* **BigQuery**:
  Local utests are optimized for DuckDB/Postgres. BigQuery support typically requires real credentials & dataset; consider keeping utests local and add separate integration tests for BQ.

---

### CI example (GitHub Actions)

```yaml
name: utests
on: [push, pull_request]
jobs:
  duckdb:
    runs-on: ubuntu-latest
    steps:
      - uses: actions/checkout@v4
      - uses: actions/setup-python@v5
        with: { python-version: "3.11" }
      - run: pip install -e .  # installs flowforge CLI
      - run: flowforge utest . --env dev
```

(For Postgres, add a service container and run `flowforge utest . --engine postgres` with `FF_PG_DSN/FF_PG_SCHEMA`.)

---

### Design notes

* The runner executes **only the target model**. All dependencies must be provided as input relations (keeps tests “unit-level”).
* `defaults` are **deep-merged**: dicts merge; lists and scalars overwrite (ideal for large shared inputs + per-case tweaks).
* Results are compared as DataFrames with options for order, subset, ignored columns, and numeric tolerances.

---

## Examples

### DuckDB (default)

```bash
make -C examples/simple_duckdb seed run dag test
```

### Postgres (Docker)

```bash
make -C examples/postgres up seed run dag test
make -C examples/postgres down   # tear down the container
```

### BigQuery / Snowflake / Databricks

1. Configure credentials (ADC for BigQuery, Snowflake connection dictionary, Databricks Spark master/app name).
2. Update `profiles.yml` or set `FF_*` variables.
3. Run the regular CLI commands (`run`, `dag`, `test`, `seed`) – the executor is selected via the profile.

---

## Troubleshooting

- **DuckDB seeds not showing up:** ensure the same database file/path (`FF_DUCKDB_PATH`) is used for `seed`, `run`, `dag`, and `test`.
- **Postgres connection refused:** verify `FF_PG_DSN`, container status (`docker ps`), and open port `5432`.
- **BigQuery permissions:** set `GOOGLE_APPLICATION_CREDENTIALS` and confirm dataset/region matches your profile.
- **HTML docs missing:** run `flowforge dag <project> --html` and open the generated `docs/index.html`.
- **Tests failing unexpectedly:** inspect the rendered SQL in the error message, narrow runs via `--select`, and refresh seeds if necessary.

---

## Roadmap Snapshot

| Phase | Focus | Key OSS Deliverables | SaaS/Enterprise Direction |
|-------|-------|----------------------|----------------------------|
| 0.1 (current) | Proof of Concept | SQL/Python DSL, DuckDB/Postgres execution, HTML DAG, basic DQ checks | – |
| 0.5 (planned) | Developer Adoption | Stronger streaming support, richer test suite, auto docs, lightweight web UI, plugin system | Hosted UI & monitoring (alpha) |
| 1.0 | Production Ready | Scheduler, backfills, multi-environment workflows, observability, community packages | SaaS beta with alerts, logs, multi-env management |
| 2.0 | Enterprise | Governance guardrails in OSS | Budgets, RBAC/SSO, advanced observability, ML integrations |
| 3.x | Ecosystem | Community marketplace, data contracts | Cross-cloud orchestration, compliance tooling, hybrid execution |

See `Contributing.md` for current priorities and ways to help.

---

## Contributing

Issues and pull requests are welcome! Please read [`Contributing.md`](./Contributing.md) for guidelines, development setup, and testing instructions. Sharing minimal reproduction steps plus `flowforge --version` output greatly speeds up reviews.

---

## License

FlowForge is licensed under the [Apache License 2.0](./License).


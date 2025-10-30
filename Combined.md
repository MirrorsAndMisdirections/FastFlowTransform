# Combined Documentation



<!-- >>> FILE: index.md >>> -->

# FastFlowTransform Documentation Hub

Welcome! This page is your starting point for FastFlowTransform docs. Pick the track that matches what you want to do and follow the links to the detailed guides.

---

## Docs Navigation
- **Getting Started** — you are here (`docs/index.md`)
- [User Guide](./Technical_Overview.md#part-i-operational-guide)
- [Modeling Reference](./Config_and_Macros.md)
- [Parallelism & Cache](./Cache_and_Parallelism.md)
- [API calls in Python models](./Api_Models.md)
- [Incremental Models](./Incremental.md)
- [YAML Tests (Schema-bound)](./YAML_Tests.md)
- [Data Quality Tests Reference](./Data_Quality_Tests.md)
- [Profiles & Environments](./Profiles.md)
- [Sources Declaration](./Sources.md)
- [Project Configuration](./Project_Config.md)
- [State Selection (changed & results)](./State_Selection.md)
- [Cross-Table Reconciliations](./Technical_Overview.md#cross-table-reconciliations)
- [Auto-Docs & Lineage](./Technical_Overview.md#auto-docs-lineage)
- [Developer Guide](./Technical_Overview.md#part-ii-architecture-internals)

## Table of Contents

- [Docs Navigation](#docs-navigation)
- [Choose Your Path](#choose-your-path)
- [Reference Map](#reference-map)
- [Need Help?](#need-help)

---

## Choose Your Path

### 1. Build & Operate Projects (Data Practitioners)

- **Get set up quickly:** follow the dedicated [Quickstart](Quickstart.md) guide for installation, seeding, and a first run.
- **Need local runtimes?** The [API demo local engine setup](examples/Local_Engine_Setup.md) walks through DuckDB, Postgres, and Databricks Spark.
- **Understand the project layout & CLI workflow:** see *Project Layout*, *Makefile Targets*, and *CLI Flows* in the [Technical Overview](Technical_Overview.md#project-layout).
- **Configure runtimes & profiles:** review executor profiles, environment overrides, and logging options in the [Technical Overview](Technical_Overview.md#profiles-environment-overrides).
- **Model data quality & troubleshoot runs:** the [Technical Overview](Technical_Overview.md#model-unit-tests-fft-utest) covers unit tests, troubleshooting tips, and exit codes.
- **Explore runnable demos:** browse the `examples/` directory in the repo; each subproject comes with its own README.

### 2. Extend FastFlowTransform (Developers & Contributors)

- **Dive into architecture & core modules:** start with [Architecture Overview](Technical_Overview.md#architecture-overview) and [Core Modules](Technical_Overview.md#core-modules) for registry, DAG, executors, validation, and more.
- **Add tests & seeds:** see [Sample Models](Technical_Overview.md#sample-models), [Seeds & Example Data](Technical_Overview.md#seeds-example-data), and the unit test guide in [Model Unit Tests](Technical_Overview.md#model-unit-tests-fft-utest).
- **Contribute code:** follow the workflow described in [`./Contributing.md`](./Contributing.md) and consult the module-level docs for internal APIs.
- **Plan ahead:** check the roadmap snapshot in the [Technical Overview](Technical_Overview.md#roadmap-snapshot) to understand upcoming work.

---

## Reference Map

- **Modeling reference** — Jinja configuration, macros, helper functions: [`Config_and_Macros.md`](Config_and_Macros.md)
- **CLI entry point & commands** — `src/fastflowtransform/cli.py`
- **Registry & node loading** — `src/fastflowtransform/core.py`
- **Unit test runner** — `src/fastflowtransform/utest.py`
- **Rendered DAG templates** — `src/fastflowtransform/docs/templates/`

---

## Need Help?

- Open an issue or PR — see [`./Contributing.md`](./Contributing.md) for guidelines.
- Join the discussion (planning doc / roadmap highlights) — see the roadmap section in the [Technical Overview](Technical_Overview.md#roadmap-snapshot).
- If you spot gaps in the docs, file an issue with the context and links to the relevant section.



<!-- >>> FILE: Quickstart.md >>> -->

# Quickstart

This guide walks you through creating a minimal FastFlowTransform project from scratch and running it end-to-end.

## 1. Install & bootstrap

```bash
python -m venv .venv
. .venv/bin/activate
pip install -e ./fastflowtransform
fft --help
```

## 2. Create project layout

```bash
mkdir -p demo/{models,seeds}
cat <<'YAML' > demo/sources.yml
version: 2

sources:
  - name: raw
    schema: staging
    tables:
      - name: users
        identifier: seed_users
YAML

cat <<'CSV' > demo/seeds/seed_users.csv
id,email
1,a@example.com
2,b@example.com
CSV

cat <<'SQL' > demo/models/users.ff.sql
{{ config(materialized='table') }}
select id, email
from {{ source('raw', 'users') }}
SQL
```

## 3. Seed static inputs

```bash
fft seed demo --profile dev
```

This materializes the CSV into the configured engine (DuckDB by default) using `seed_users` as the physical table.

## 4. Run the pipeline

```bash
fft run demo --cache off
```

You should see log lines similar to `✓ L01 [DUCK] users.ff`. The resulting table lives in the target schema (`staging` in this example).

## 5. Inspect artifacts

- `.fastflowtransform/target/manifest.json` → model graph + sources
- `.fastflowtransform/target/run_results.json` → run outcomes and durations

## 6. Add more models (optional)

- Reference other models with `{{ ref('model_name') }}`
- Configure tags or materializations via `{{ config(...) }}` at the top of each SQL file

## 7. Next steps

- Add `project.yml` for reusable `vars:` and metadata
- Explore `fft docs` to generate HTML documentation
- Use engine profiles under `profiles.yml` to target Postgres, BigQuery, or Databricks (path-based sources supported via `format` + `location` overrides)

Refer to `docs/Config_and_Macros.md` for advanced configuration options.



<!-- >>> FILE: Technical_Overview.md >>> -->

# 🧭 FastFlowTransform – Technical Developer Documentation (v0.4)

> Status: latest updates from your context dump. This document consolidates project structure, architecture, core APIs, error handling, CLI, examples, and roadmap into a print/git-friendly Markdown.
>
> Looking for an overview? Start at the [`docs/index.md`](./index.md) hub, then dive back here when you need details.
>
> Project: **FastFlowTransform** — SQL & Python Data Modeling (Batch + Streaming), DAG, CLI, Auto-Docs, DQ Tests.

---

## Docs Navigation
1. [Getting Started](./index.md)
2. **User Guide** — see [Part I – Operational Guide](#part-i-operational-guide) (this document)
3. [Modeling Reference](./Config_and_Macros.md)
4. **Developer Guide** — see [Part II – Architecture & Internals](#part-ii-architecture-internals) (this document)

---

## Table of Contents

- [Docs Navigation](#docs-navigation)
- [Part I – Operational Guide](#part-i-operational-guide)
  - [Project Layout](#project-layout)
  - [Sample Models](#sample-models)
  - [Seeds & Example Data](#seeds-example-data)
  - [Makefile Targets](#makefile-targets)
  - [CLI Flows](#cli-flows)
  - [Logging & Verbosity](#logging-verbosity)
  - [Model Unit Tests (`fft utest`)](#model-unit-tests-fft-utest)
  - [Troubleshooting](#troubleshooting)
  - [Error Codes](#error-codes)
  - [Profiles & Environment Overrides](#profiles-environment-overrides)
  - [Parallel Scheduler (v0.3)](#parallel-scheduler-v03)
  - [Cache Policy (v0.3)](#cache-policy-v03)
  - [Fingerprint Formula (v0.3)](#fingerprint-formula-v03)
  - [Meta Table Schema (v0.3)](#meta-table-schema-v03)
  - [Jinja DSL Quick Reference](#jinja-dsl-quick-reference)
  - [Roadmap Snapshot](#roadmap-snapshot)
  - [Cross-Table Reconciliations](#cross-table-reconciliations)
  - [Auto-Docs & Lineage](#auto-docs-lineage)
- [Part II – Architecture & Internals](#part-ii-architecture-internals)
  - [Architecture Overview](#architecture-overview)
  - [Core Modules](#core-modules)
    - [`core.py`](#corepy)
    - [`dag.py`](#dagpy)
    - [`errors.py`](#errorspy)
    - [Executors](#executors)
    - [`validation.py`](#validationpy)
    - [`testing.py`](#testingpy)
    - [`docs.py` & Templates](#docspy-templates)
    - [`seeding.py`](#seedingpy)
  - [CLI Implementation](#cli-implementation)
  - [Settings Infrastructure](#settings-infrastructure)
  - [Streaming Components](#streaming-components)
  - [Mini End-to-End Example (Python API)](#mini-end-to-end-example-python-api)

---

## Part I – Operational Guide

### Project Layout

```text
fastflowtransform/
├── pyproject.toml
├── src/
│   └── fastflowtransform/
│       ├── __init__.py
│       ├── cli.py
│       ├── core.py
│       ├── dag.py
│       ├── docs.py
│       ├── errors.py
│       ├── settings.py
│       ├── seeding.py
│       ├── testing.py
│       ├── validation.py
│       ├── decorators.py                 # optional, if not kept in core.py
│       ├── docs/
│       │   └── templates/
│       │       ├── index.html.j2
│       │       └── model.html.j2
│       ├── executors/
│       │   ├── __init__.py
│       │   ├── base.py
│       │   ├── duckdb_exec.py
│       │   ├── postgres_exec.py
│       │   ├── bigquery_exec.py          # pandas + BigQuery client
│       │   ├── bigquery_bf_exec.py       # BigQuery DataFrames (bigframes)
│       │   ├── databricks_spark_exec.py  # PySpark (without pandas)
│       │   └── snowflake_snowpark_exec.py# Snowpark (without pandas)
│       └── streaming/
│           ├── __init__.py
│           ├── file_tail.py
│           └── sessionizer.py
│
├── examples/
│   ├── simple_duckdb/
│   │   ├── models/
│   │   │   ├── users.ff.sql
│   │   │   ├── users_enriched.ff.py
│   │   │   ├── orders.ff.sql
│   │   │   ├── mart_orders_enriched.ff.py
│   │   │   └── mart_users.ff.sql
│   │   ├── seeds/
│   │   │   ├── seed_users.csv
│   │   │   └── seed_orders.csv
│   │   ├── sources.yml
│   │   ├── project.yml
│   │   ├── Makefile
│   │   └── .local/demo.duckdb  (after make seed/run)
│   └── postgres/                # similar structure if needed
│
├── tests/
│   ├── conftest.py
│   ├── duckdb/ …                # end-to-end + unit
│   ├── postgres/ …
│   └── streaming/ …
└── README.md
```

### Sample Models

The demo project `examples/simple_duckdb` showcases the typical mix of SQL and Python models plus downstream marts. Use it as a template for your own projects.

- Batch models live under `models/` (`*.ff.sql`, `*.ff.py`).
- External tables are declared in `sources.yml`; reusable tests in `project.yml`.
- Seeds in `seeds/` keep demos deterministic.

> ℹ️ **Need full code samples and decorator details?**
> See [Model Fundamentals](./Config_and_Macros.md#1-model-fundamentals) in the Modeling Reference.

### Seeds & Example Data

`seeds/seed_users.csv`

```csv
id,email
1,a@example.com
2,b@gmail.com
3,c@gmail.com
```

`seeds/seed_orders.csv`

```csv
order_id,user_id,amount
100,1,19.9
101,2,0
```

### Makefile Targets

```makefile
DB ?= .local/demo.duckdb
PROJECT ?= examples/simple_duckdb

seed:
	fft seed $(PROJECT) --env dev

run:
	FF_ENGINE=duckdb FF_DUCKDB_PATH="$(DB)" fft run "$(PROJECT)" --env dev

dag:
	fft dag "$(PROJECT)" --env dev --html

test:
	fft test "$(PROJECT)" --env dev --select batch
```

Targets wrap the CLI commands showcased below. Feel free to copy the pattern into your own projects.

### CLI Flows

- CLI flags and internals are documented under [CLI Implementation](#cli-implementation).
- Automation examples appear in the [Makefile Targets](#makefile-targets).


#### HTTP/API in Python models
See [API calls in Python models](./Api_Models.md) for `get_json`/`get_df`, pagination, cache/offline flags.


#### DAG & Documentation

- Narrow the graph with `fft dag ... --select <pattern>` (for example `state:modified` or `tag:finance`). Combined with `--html` this produces a focused mini site.
- Control schema introspection via `--with-schema/--no-schema`. Use `--no-schema` when the executor should avoid fetching column metadata (for example, BigQuery without sufficient permissions).
- `fft docgen` renders the DAG, model pages, and an optional JSON manifest in one command. Append `--open-source` to open `index.html` in your default browser after rendering.

#### Sync Database Comments

`fft sync-db-comments <project> --env <env>` pushes model and column descriptions from project YAML or Markdown into database comments. The command currently supports Postgres and Snowflake Snowpark:

- Start with `--dry-run` to review the generated `COMMENT` statements.
- Postgres honors `profiles.yml -> postgres.db_schema` (and any `FF_PG_SCHEMA` override).
- Snowflake reuses the session or connection exposed by the executor.

If no descriptions are found, the command exits without making changes.

### Logging & Verbosity

FastFlowTransform exposes uniform logging controls across all CLI commands plus a dedicated SQL debug channel.

#### Flags

- `-q` / `--quiet` → only errors (`ERROR`)
- *(default)* → concise warnings (`WARNING`)
- `-v` / `--verbose` → progress/info (`INFO`)
- `-vv` → full debug (`DEBUG`), including SQL debug output

`-vv` flips on the SQL debug channel automatically (same as setting `FFT_SQL_DEBUG=1`

#### SQL debug channel

Enable it to inspect Python-model inputs, dependency columns, and helper SQL emitted by data-quality checks:

```bash
# full debug (recommended)
fft run . -vv

# equivalent using the env var (legacy behaviour retained)
FFT_SQL_DEBUG=1 fft run .
```

#### Usage patterns

```bash
fft run . -q     # quiet (errors only)
fft run .        # default (concise)
fft run . -v     # verbose progress (model names, executor info)
fft run . -vv    # full debug + SQL channel
```

#### Parallel logging UX

- Per node: start/end lines with duration, truncated name, and engine abbrev (DUCK/PG/BQ/…).
- Output is line-stable via a thread-safe log queue; per-level summaries at the end.
- On errors, the familiar “error block” is shown per node.

**Notes**

- SQL debug output routes through the `fastflowtransform.sql` logger; use `-vv` or the env var to see it.
- Existing projects do not need changes: the env var continues to work even without `-vv`.

### Model Unit Tests (`fft utest`)

`fft utest` executes a single model in isolation, loading only the inputs you provide and comparing the result to an expected dataset. It works for SQL and Python models and runs against DuckDB or Postgres by default.

#### Unit tests & cache

`fft utest --cache {off|ro|rw}` (default: `off`)

- `off`: deterministic, never skips.
- `ro`: skip on cache hit; on miss, build but **do not write** cache.
- `rw`: skip on hit; on miss, build **and write** fingerprint.

Notes:
- UTests key the cache with `profile="utest"`.
- Fingerprints include case inputs (CSV content hash / inline rows), so changing inputs invalidates the cache.
- `--reuse-meta` is currently a reserved flag: it is exposed in the CLI, acts as a no-op today, and will enable future meta-table optimizations.


#### Why?

- Fast feedback on transformation logic without full DAG runs
- Small, reproducible fixtures (rows inline or external CSV)
- Engine-agnostic: swap DuckDB/Postgres to spot dialect differences

#### Folder layout

Specs live under `<project>/tests/unit/*.yml` relative to the project root (the directory passed to the CLI that contains `models/`):

```
your-project/
├── models/
│   ├── users.ff.sql
│   ├── users_enriched.ff.py
│   └── mart_users.ff.sql
└── tests/
    └── unit/
        ├── users_enriched.yml
        └── mart_users.yml
```

#### YAML DSL (with `defaults`)

Each file targets one logical node (the DAG name). Defaults are deep-merged into every case so you can share inputs/expectations and override per scenario.

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

SQL models use the file stem (including `.ff`) as `model`. Provide expected relation names that match the materialized table/view:

```yaml
# tests/unit/mart_users.yml
model: mart_users.ff

defaults:
  inputs:
    users_enriched:
      rows:
        - {id: 1, email: "a@example.com", is_gmail: false}
        - {id: 2, email: "b@gmail.com",   is_gmail: true}
  expect:
    relation: mart_users
    order_by: [id]

cases:
  - name: passthrough_columns
    expect:
      rows:
        - {id: 1, email: "a@example.com", is_gmail: false}
        - {id: 2, email: "b@gmail.com",   is_gmail: true}
```

For multi-dependency models, include every physical relation name (what `relation_for(dep)` returns):

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

#### Input formats

- `rows`: inline dictionaries per row
- `csv`: reference a CSV file (relative paths allowed)

Keys under `inputs` are physical relations; use `relation_for('users.ff')` if unsure.

#### Expected output & comparison

- `relation`: actual table/view name produced by the model (defaults to `relation_for(model)`)
- Ordering: `order_by: [...]` or `any_order: true`
- Columns: `ignore_columns: [...]`, `subset: true`
- Numeric tolerance: `approx: true` or `approx: { col: 1e-9, other_col: 0.01 }`
  (numbers can be plain `1e-9` or quoted; they are cast to float)

#### Running utests

```bash
fft utest .                      # discover all specs
fft utest . --env dev            # use a specific profile
fft utest . --model users_enriched
fft utest . --model mart_orders_enriched --case join_and_flag
fft utest . --path tests/unit/users_enriched.yml
```

Override the executor for all specs (ensure credentials/DSNs are set):

```bash
export FF_PG_DSN="postgresql+psycopg://postgres:postgres@localhost:5432/ffdb"
export FF_PG_SCHEMA="public"
fft utest . --engine postgres
```

Executor precedence (highest → lowest): CLI `--engine`, YAML `engine:` (optional), `profiles.yml`, environment overrides.

#### Design notes

- Only the target model runs; supply all upstream relations the model expects.
- `defaults` deep-merge: dicts merge, lists/scalars overwrite.
- Results compare as DataFrames with configurable order, subset, ignored columns, and numeric tolerances.
- Exit codes: `0` for success, `2` when at least one case fails (compact CSV-style diff is printed).

**CI example (GitHub Actions)**

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
      - run: pip install -e .
      - run: fft utest . --env dev
```

(For Postgres, add a service container and run `fft utest . --engine postgres` with `FF_PG_DSN` / `FF_PG_SCHEMA`.)

### Troubleshooting

- **DuckDB seeds not visible** → ensure `FF_DUCKDB_PATH` (or profile path) is identical for `seed`, `run`, `dag`, and `test`.
- **Postgres connection refused** → confirm `FF_PG_DSN`, container status (`docker ps`), and that port `5432` is open.
- **BigQuery permissions** → set `GOOGLE_APPLICATION_CREDENTIALS` and match dataset/location to your profile.
- **HTML docs missing** → run `fft dag <project> --html` and open `<project>/docs/index.html`.
- **Unexpected test failures** → inspect rendered SQL in CLI output, refine selection via `--select`, refresh seeds if needed.
- **Dependency table not found** in utests → provide all physical upstream relations in the YAML spec.

### Error Codes

| Type                      | Class/Source              | Exit | Notes                                                   |
|---------------------------|---------------------------|------|---------------------------------------------------------|
| Missing dependency        | `DependencyNotFoundError` | 1    | Per-node list; tips for `ref()` / names                |
| Cycle in DAG              | `ModelCycleError`         | 1    | "Cycle detected among nodes: ..."                      |
| Model execution (KeyError)| `cli.py` → formatted block| 1    | Inspect columns, use `relation_for(dep)` as keys       |
| Data quality failures     | `cli test` → summary      | 2    | "Totals ... passed/failed"; each failure on its own line |
| Unknown/unexpected        | generic                   | 99   | Optional trace via `FFT_TRACE=1` |

Error types map to the classes documented in [Core Modules](#core-modules) and [CLI Implementation](#cli-implementation).

### Profiles & Environment Overrides

**`profiles.yml` example:**

```yaml
default:
  engine: duckdb
  duckdb: { path: ":memory:" }

stg:
  engine: postgres
  postgres:
    dsn: postgresql+psycopg://postgres:postgres@localhost:5432/ffdb
    db_schema: public

bq:
  engine: bigquery
  bigquery:
    project: my-gcp-proj
    dataset: demo
    location: EU
    use_bigframes: false
```

**ENV overrides (examples):**

`FF_ENGINE`, `FF_DUCKDB_PATH`, `FF_PG_DSN`, `FF_PG_SCHEMA`, `FF_BQ_DATASET`, `FF_BQ_LOCATION`, `FF_BQ_USE_BIGFRAMES=1`

**Priority (lowest → highest):** `profiles.yml` < environment variables (`FF_*`) < CLI flags (e.g. `--engine`).

For the Pydantic models and resolution flow, see [Settings Infrastructure](#settings-infrastructure).

### Parallel Scheduler (v0.3)

FastFlowTransform executes the DAG in **levels**. Each level contains nodes without mutual dependencies.

- `--jobs N` limits the **maximum concurrency per level**.
- `--keep-going` keeps tasks within the current level running even if one fails; subsequent levels are not started.

**CLI**
```bash
fft run . --env dev --jobs 4            # parallel (level-wise)
fft run . --env dev --jobs 4 --keep-going

fft run . --select model_b --jobs 4     # Run only model_b and whatever it depends on
fft run . --rebuild-only model_b        # Rebuild only model_b, even if cache hits
```

**Internals**
- `dag.levels(nodes)` builds level lists using indegrees.
- `run_executor.schedule(levels, jobs, fail_policy)` spawns a thread pool per level and aggregates timings.

### Cache Policy (v0.3)

**Modes**
```
off  – always build
rw   – default; skip if fingerprint matches and relation exists; write cache after build
ro   – skip on match; on miss build but do not write cache
wo   – always build and write cache
```
`--rebuild <glob>` ignores cache for matching nodes.

**Skip condition**
1) Fingerprint matches the stored value (file-backed cache)  
2) Physical relation exists on the target engine

**Examples**
```bash
fft run . --env dev --cache=rw
fft run . --env dev --cache=ro
fft run . --env dev --cache=rw --rebuild marts_daily.ff
```

### Fingerprint Formula (v0.3)

**SQL nodes**:  
`fingerprint_sql(node, rendered_sql, env_ctx, dep_fps)`

**Python nodes**:  
`fingerprint_py(node, func_src, env_ctx, dep_fps)`

**`env_ctx` content**
- `engine` (e.g. duckdb, postgres, bigquery)
- `profile_name` (CLI `--env`)
- selected environment keys/values: all `FF_*`
- normalized excerpt of `sources.yml` (sorted dump)

**Properties**
- Same inputs ⇒ same hash.
- Minimal change in SQL/function ⇒ different hash.
- Any dependency fingerprint change bubbles downstream via `dep_fps`.

### Meta Table Schema (v0.3)

FastFlowTransform writes a per-node audit row after successful builds:

```
_ff_meta (
  node_name   TEXT / STRING      -- logical name, e.g. "users.ff"
  relation    TEXT / STRING      -- physical name, e.g. "users"
  fingerprint TEXT / STRING
  engine      TEXT / STRING
  built_at    TIMESTAMP
)
```

**Backends**
- DuckDB: table `_ff_meta` in `main`.
- Postgres: table `_ff_meta` in the active schema.
- BigQuery: table `<dataset>._ff_meta`.

**Notes**
- Meta is currently used for auditing and tooling; skip logic relies on fingerprint cache + relation existence checks.

#### Executor meta hook

After a successful materialization the executor calls:
  on_node_built(node, relation, fingerprint)

This performs an upsert into `_ff_meta` with `(node_name, relation, fingerprint, built_at, engine)`.

Skipped nodes do **not** touch the meta table.


### Jinja DSL Quick Reference

`ref()`, `source()`, `var()`, `config()`, `this` – see details in the [Modeling Reference](./Config_and_Macros.md).

### Roadmap Snapshot

| Version | Content                                           |
|---------|---------------------------------------------------|
| 0.2     | `config(materialized=...)`, Jinja macros, variables |
| 0.3     | Parallel execution, cache                         |
| 0.4     | Incremental models                                |
| 0.5     | Streaming connectors (Kafka, S3)                  |
| 1.0     | Stable API, plugin SDK                            |

> See also: feature pyramid & roadmap phases (OSS/SaaS) in the separate document.

---

### Cross-Table Reconciliations

FastFlowTransform can compare aggregates and key coverage **across two tables** and surface drift with clear, numeric messages. These checks run via the standard `fft test` entrypoint and integrate into the DQ summary output.

**CLI**
```bash
# only run reconciliation checks
fft test . --env dev --select reconcile
```

**YAML DSL**

All checks live under `project.yml → tests:` and should carry the tag `reconcile` for easy selection.

1) **Equality / Approx Equality**
```yaml
- type: reconcile_equal
  name: orders_total_equals_mart
  tags: [reconcile]
  left:  { table: orders,               expr: "sum(amount)" }
  right: { table: mart_orders_enriched, expr: "sum(amount)", where: "valid_amt" }
  # optional tolerances:
  abs_tolerance: 0.01          # |L - R| <= 0.01
  rel_tolerance_pct: 0.1       # |L - R| / max(|R|, eps) <= 0.1% (0.1)
```

2) **Ratio within bounds**
```yaml
- type: reconcile_ratio_within
  name: orders_vs_mart_ratio
  tags: [reconcile]
  left:  { table: orders,               expr: "sum(amount)" }
  right: { table: mart_orders_enriched, expr: "sum(amount)" }
  min_ratio: 0.999
  max_ratio: 1.001
```

3) **Absolute difference within limit**
```yaml
- type: reconcile_diff_within
  name: count_stability
  tags: [reconcile]
  left:  { table: events_raw, expr: "count(*)", where: "event_type='purchase'" }
  right: { table: fct_sales,  expr: "sum(txn_count)" }
  max_abs_diff: 10
```

4) **Coverage (anti-join = 0)**
```yaml
- type: reconcile_coverage
  name: all_orders_covered
  tags: [reconcile]
  source: { table: orders,               key: "order_id" }
  target: { table: mart_orders_enriched, key: "order_id" }
  # optional filters
  source_where: "order_date >= current_date - interval '7 days'"
  target_where: "valid_amt"
```

**Parameter semantics**
- `expr`: SQL snippet placed into `SELECT {expr} FROM {table}` (keep it engine-neutral: `sum(...)`, `count(*)`, simple filters).
- `where`: optional SQL appended as `WHERE {where}`.
- `abs_tolerance`: absolute tolerance on the difference.
- `rel_tolerance_pct`: relative tolerance in **percent**; denominator is `max(|right|, 1e-12)`.
- `min_ratio` / `max_ratio`: inclusive bounds for `left/right`.
- Coverage uses an anti-join (`source` minus `target` on the given key). The check passes if missing = 0.

**Summary output**
Each reconciliation contributes a line in the summary with a compact scope, e.g.:
```
✅ reconcile_equal       orders ⇔ mart_orders_enriched        (4ms)
✅ reconcile_coverage    orders ⇒ mart_orders_enriched        (3ms)
```

**Engine notes**
- DuckDB and Postgres are supported out-of-the-box. BigQuery works with simple aggregates/filters (expressions should avoid dialect-specific functions).
- For relative tolerances, the implementation guards against zero denominators with a small epsilon (`1e-12`).


### Auto-Docs & Lineage

FastFlowTransform can generate a lightweight documentation site (DAG + model detail pages) from your project:

```bash
# Classic
fft dag . --env dev --html

# Convenience wrapper (loads schema + descriptions + lineage, can emit JSON)
fft docgen . --env dev --out site/docs --emit-json site/docs/docs_manifest.json
```

Add `--open-source` if you want the default browser to open the rendered `index.html` immediately.

**Descriptions** can be provided in YAML (project.yml) and/or Markdown files. Markdown has higher priority.

YAML in `project.yml`:

```yaml
docs:
  models:
    users.ff:
      description: "Raw users table imported from CRM."
      columns:
        id: "Primary key."
        email: "User email address."
    users_enriched:
      description: "Adds gmail flag."
      columns:
        is_gmail: "True if email ends with @gmail.com"
```

Markdown (overrides YAML if present):

```
<project>/docs/models/<model>.md
<project>/docs/columns/<relation>/<column>.md
```

Optional front matter is ignored for now (title/tags may be used later).

**Column lineage (heuristic, best effort).**

- SQL models: expressions like `col` / `alias AS out` / `upper(u.email) AS email_upper)` are parsed;
  `u` must come from a `FROM ... AS u` that resolves to a relation. Functions mark lineage as *transformed*.
- Python (pandas) models: simple patterns like `rename`, `out["x"] = df["y"]`, `assign(x=...)` are recognized.
- You can override hints in YAML:

```yaml
docs:
  models:
    mart_orders_enriched:
      lineage:
        email_upper:
          from: [{ table: users, column: email }]
          transformed: true
```

**JSON manifest** (optional via `--emit-json`) includes models, relations, descriptions, columns (with nullable/dtype),
and lineage per column. This is useful for custom doc portals or CI checks.

Notes:
- Schema introspection currently supports DuckDB and Postgres. For other engines, the Columns card may be empty.
- Lineage is optional; when uncertain, entries fall back to “unknown” and never fail doc generation.



## Part II – Architecture & Internals

### Architecture Overview

```
CLI (Typer)
│
├── Registry (core.py)
│   ├── Discover models (*.ff.sql / *.ff.py)
│   ├── Load Python models (decorator)
│   ├── Parse/validate dependencies
│   └── Jinja environment + sources.yml
│
├── DAG (dag.py)
│   ├── topo_sort (Kahn, deterministic)
│   └── mermaid() (styled + stable IDs)
│
├── Executors (executors/*)
│   ├── BaseExecutor (SQL rendering, dependency loading, materialization, requires guard)
│   ├── DuckExecutor (DuckDB)
│   ├── PostgresExecutor (SQLAlchemy, shims)
│   ├── BigQueryExecutor (pandas)
│   ├── BigQueryBFExecutor (BigQuery DataFrames / bigframes)
│   ├── DatabricksSparkExecutor (PySpark, without pandas)
│   └── SnowflakeSnowparkExecutor (Snowpark, without pandas)
│
├── Testing (testing.py)
│   ├── generic _exec / _scalar
│   └── Checks: not_null, unique, row_count_between, greater_equal, non_negative_sum, freshness
│
├── Seeding (seeding.py)
│   └── Load seeds (CSV/Parquet/SQL) → engine agnostic
│
├── Docs (docs.py + templates/)
│   ├── Mermaid + overview table (index.html)
│   └── Model detail pages (model.html)
│
├── Settings/Profiles (settings.py)
│   └── Pydantic v2 discriminated union + ENV overrides
│
└── Streaming (streaming/*)
    ├── FileTailSource
    └── StreamSessionizer
```

---

### Core Modules

#### `core.py`

Key data structures and the project loading process.

```python
@dataclass
class Node:
    name: str                # logical name (stem or @model(name=...))
    kind: str                # "sql" | "python"
    path: Path
    deps: List[str] = field(default_factory=list)

class Registry:
    def load_project(self, project_dir: Path) -> None: ...
    def _register_node(self, node: Node) -> None: ...
    def _load_py_module(self, path: Path) -> types.ModuleType: ...
    def _scan_sql_deps(self, path: Path) -> List[str]: ...
```

**Helpers & decorator:**

```python
def relation_for(node_name: str) -> str: ...
def ref(name: str) -> str: ...
def source(source_name: str, table_name: str) -> str: ...

def model(name=None, deps=None, requires=None) -> Callable[[Callable[..., Any]], Callable[..., Any]]: ...
```

**Python models (example):**

```python
@model(name="users_enriched", deps=["users.ff"], requires={"users": {"id","email"}})
def enrich(df: pd.DataFrame) -> pd.DataFrame: ...
```

---

#### `dag.py`

Deterministic topological sort plus Mermaid export.

```python
def topo_sort(nodes: Dict[str, Node]) -> List[str]: ...
def mermaid(nodes: Dict[str, Node]) -> str: ...
```

---

#### `errors.py`

Primary error types with helpful messages.

```python
class FastFlowTransformError(Exception): ...
class ModuleLoadError(FastFlowTransformError): ...
class DependencyNotFoundError(FastFlowTransformError): ...
class ModelCycleError(FastFlowTransformError): ...
class TestFailureError(FastFlowTransformError): ...
```

---

#### Executors

Shared logic (`BaseExecutor`) plus engine implementations.

```python
class BaseExecutor(ABC):
    def render_sql(self, node: Node, env: Environment, ref_resolver=None, source_resolver=None) -> str: ...
    def run_python(self, node: Node) -> None: ...
    @abstractmethod
    def _read_relation(self, relation: str, node: Node, deps: Iterable[str]) -> pd.DataFrame: ...
    @abstractmethod
    def _materialize_relation(self, relation: str, df: pd.DataFrame, node: Node) -> None: ...
```

**DuckDB (`duckdb_exec.py`)**

- `run_sql(node, env)` renders Jinja (`ref/source`) and executes the SQL.
- `_read_relation` loads a table as `DataFrame`; surfaces actionable errors when a dependency is missing.
- `_materialize_relation` writes the `DataFrame` as a table (`create or replace table ...`).

**Postgres (`postgres_exec.py`)**

- `_SAConnShim` (compatible with `testing._exec`).
- `run_sql` renders SQL and rewrites `CREATE OR REPLACE TABLE` to `DROP + CREATE AS`.
- `_read_relation` uses pandas, handles schemas, and provides clear guidance.
- `_materialize_relation` writes via `to_sql(if_exists="replace")`.

**BigQuery / BigQuery DataFrames / Spark / Snowpark**

- Identical signatures; IO uses the respective native dataframes (no pandas for Spark/Snowpark).

---

#### `validation.py`

Required-column checks for Python models (single and multi dependency).

```python
class RequiredColumnsError(ValueError): ...
def validate_required_columns(node_name: str, inputs: Any, requires: dict[str, set[str]]): ...
```

---

#### `testing.py`

Minimal data quality framework (engine agnostic via `_exec`).

**Checks:** `not_null`, `unique`, `greater_equal`, `non_negative_sum`, `row_count_between`, `freshness`

```python
class TestFailure(Exception): ...
def _exec(con: Any, sql: Any): ...
def _scalar(con: Any, sql: Any): ...
```

---

#### `docs.py` & Templates

- `render_site(out_dir, nodes)` produces `index.html` plus `model.html` per model.
- Templates (`docs/templates/`) include dark mode, filters, copy buttons, legend.
- Uses `dag.mermaid(nodes)` for the graph.

---

#### `seeding.py`

Engine-agnostic seed loading (CSV/Parquet/SQL).

```python
def seed_project(project_dir: Path, executor, schema: Optional[str] = None) -> int: ...
```

---

### CLI Implementation

Operational usage lives in [CLI Flows](#cli-flows). This section drills into the Typer command definitions in `cli.py`.

**Commands:**

- `fft run <project> [--env dev] [--engine ...]`
- `fft dag <project> [--env dev] [--html] [--select ...] [--with-schema/--no-schema]`
- `fft docgen <project> [--env dev] [--out dir] [--emit-json path] [--open-source]`
- `fft test <project> [--env dev] [--select batch|streaming|tag:...]`
- `fft seed <project> [--env dev]`
- `fft sync-db-comments <project> [--env dev] [--dry-run]`
- `fft utest <project> [--env dev] [--cache off|ro|rw] [--reuse-meta]`
- `fft --version`

**Key components:**

```python
def _load_project_and_env(project_arg) -> tuple[Path, Environment]: ...
def _resolve_profile(env_name, engine, proj) -> tuple[EnvSettings, Profile]: ...
def _get_test_con(executor: Any) -> Any: ...
```

**Test summary (exit 2 on failures):**

```
Data Quality Summary
────────────────────
✅ not_null           users.email                              (3ms)
❌ unique             users.id                                 (2ms)
   ↳ users.id has 1 duplicate

Totals
──────
✓ passed: 1
✗ failed: 1
```

---

### Settings Infrastructure

`settings.py` uses a **Pydantic v2 discriminated union** (`engine` as discriminator) plus ENV overrides.

Profile types:
- `DuckDBProfile(engine="duckdb", duckdb: {path})`
- `PostgresProfile(engine="postgres", postgres: {dsn, db_schema})`
- `BigQueryProfile(engine="bigquery", bigquery: {project?, dataset, location?, use_bigframes?})`
- `DatabricksSparkProfile(engine="databricks_spark", ...)`
- `SnowflakeSnowparkProfile(engine="snowflake_snowpark", ...)`

Resolver idea:

```python
def resolve_profile(project_dir: Path, env_name: str, env: EnvSettings) -> Profile: ...
```

---

### Streaming Components

**`streaming/sessionizer.py`**

- Normalizes events (JSONL / batch DF) and writes `fct_sessions_streaming`.
- `process_batch(df)` aggregates sessions (start/end, pageviews, revenue).

**Smoke test (DuckDB):**

```python
def test_stream_sessionizer_produces_sessions(): ...
```

---

### Mini End-to-End Example (Python API)

```python
from pathlib import Path
from jinja2 import Environment, FileSystemLoader
from fastflowtransform.core import REGISTRY
from fastflowtransform.dag import topo_sort
from fastflowtransform.executors.duckdb_exec import DuckExecutor

proj = Path("examples/simple_duckdb").resolve()
REGISTRY.load_project(proj)
env = REGISTRY.env  # Jinja env from the registry load

order = topo_sort(REGISTRY.nodes)
ex = DuckExecutor(db_path=str(proj / ".local" / "demo.duckdb"))

for name in order:
    node = REGISTRY.nodes[name]
    if node.kind == "sql":
        ex.run_sql(node, env)
    else:
        ex.run_python(node)

print("✓ Done")
```

---

Need a different angle? Head back to the [Docs Hub](./index.md) or deep-dive into the [Modeling Reference](./Config_and_Macros.md).



<!-- >>> FILE: Api_Models.md >>> -->

# API Calls in Python Models

> **Status:** Experimental but stable for demos and smaller workflows.  
> **Goal:** Query HTTP APIs from Python models, return responses as DataFrames, cache and instrument them cleanly, and support reproducible offline runs.

* [Motivation](#motivation)
* [Quickstart](#quickstart)
* [Programming API](#programming-api)
  * [`get_json`](#get_json)
  * [`get_df`](#get_df)
  * [Pagination](#pagination)
  * [Context & Telemetry](#context-telemetry)
* [CLI Flags & Environment Variables](#cli-flags-environment-variables)
* [Example Model](#example-model)
* [Artifacts](#artifacts)
* [Tests & Offline Demos](#tests-offline-demos)
* [Best Practices](#best-practices)
* [Troubleshooting](#troubleshooting)
* [Security & Compliance](#security-compliance)
* [FAQ](#faq)

---

## Motivation

Many pipelines need small, reliable API fetchers: configuration tables, miniature dimensions, feature flags, SaaS exports. This feature provides:

- Simple HTTP calls inside Python models
- File-backed cache (reproducible builds, works offline)
- Per-node telemetry (requests, hits, bytes, hashes)
- CLI switches `--offline` and `--http-cache` for reproducible runs

---

## Quickstart

1. **Optionally enable flags** (recommended):

   ```bash
   # No network - cache hits only
   fft run . --env dev --offline
   # Cache mode
   fft run . --env dev --http-cache rw   # rw|ro|off
   ```

2. **Write a Python model**:

   ```python
   # models/users_from_api.ff.py
   import pandas as pd
   from fastflowtransform.core import model
   from fastflowtransform.api.http import get_df

   @model(name="users_from_api", deps=["users.ff"])
   def fetch(_: pd.DataFrame) -> pd.DataFrame:
       df = get_df(
           url="https://api.example.com/users",
           params={"page": 1},
           record_path=["data"],        # JSON -> list -> DataFrame
       )
       return df
   ```

3. **Run it**:

   ```bash
   fft run . --env dev --select users_from_api
   ```

---

## Programming API

> Module: `fastflowtransform.api.http`

### `get_json`

```python
from fastflowtransform.api.http import get_json

data = get_json(
    url="https://api.example.com/objects",
    params={"page": 1},        # optional
    headers={"Authorization": "Bearer ..."},  # optional
    timeout=20,                # optional (seconds)
)
# -> Python dict / list
```

**Behavior**

- Reads from the local cache (when present and valid).
- Writes to the cache (`rw` mode), including the response body.
- Respects offline mode (no network traffic).

### `get_df`

```python
from fastflowtransform.api.http import get_df

df = get_df(
    url="https://api.example.com/users",
    params={"page": 1},
    record_path=["data"],     # path to the JSON list
    normalize=True,           # optional: flatten nested objects
    paginator=None,           # optional: pagination strategy (see below)
    output="pandas",          # pandas|spark (default=pandas)
)
# -> pandas.DataFrame
```

**Conversion**

- Default: `record_path` points to the array payload (for example `["data"]`).
- `normalize=True` delegates to `json_normalize` for deeper structures.
- `output='spark'` (plus an optional `session=SparkSession`) converts the normalized result into a `pyspark.sql.DataFrame`. Additional backends will reuse the same parameter.

### Pagination

For paged APIs you can describe the next request declaratively:

```python
def paginator(url: str, params: dict | None, json_obj: dict):
    next_url = json_obj.get("next")          # e.g. absolute URL
    if next_url:
        return {"next_request": {"url": next_url}}
    return None

df = get_df(
    "https://api.example.com/users?page=1",
    paginator=paginator,
    record_path=["data"],
)
```

The paginator may return the following fields:

- `{"next_request": {"url": "...", "params": {...}, "headers": {...}}}`
  (any missing field keeps its previous value)

### Context & Telemetry

During a model run the executor collects telemetry per node and writes it into `run_results.json`:

- `requests` (count)
- `cache_hits`
- `bytes` (sum of response bodies)
- `used_offline` (bool)
- `keys` (cache keys)
- `entries` (optional compact array with URL, status, content hash)

You will find these metrics under the `http` block of each node (see [Artifacts](#artifacts)).

---

## CLI Flags & Environment Variables

**CLI**

- `--offline`  
  Sets `FF_HTTP_OFFLINE=1`; network requests are blocked, **cache hits only**.
- `--http-cache {off|ro|rw}`  
  Sets `FF_HTTP_CACHE_MODE`:

  - `off`: neither read nor write.
  - `ro`: read-only (hits), **no** writes.
  - `rw`: read and write (default).

**Environment (optional to set directly)**

| Variable                 | Default                         | Effect                              |
| ------------------------ | ------------------------------- | ----------------------------------- |
| `FF_HTTP_OFFLINE`        | `0`                             | `1/true/on` -> offline mode         |
| `FF_HTTP_CACHE_MODE`     | `rw`                            | `off` / `ro` / `rw`                 |
| `FF_HTTP_CACHE_DIR`      | `.fastflowtransform/http_cache` | Cache directory                     |
| `FF_HTTP_TTL`            | `0`                             | Seconds; 0 = never expires          |
| `FF_HTTP_TIMEOUT`        | `20`                            | Request timeout (seconds)           |
| `FF_HTTP_MAX_RETRIES`    | `3`                             | Basic retry count                   |
| `FF_HTTP_RATE_LIMIT_RPS` | `0`                             | Requests per second (0 = unlimited) |

---

## Example Model

```python
# models/dim_countries_from_api.ff.py
import pandas as pd
from fastflowtransform.core import model
from fastflowtransform.api.http import get_df

@model(name="dim_countries_from_api", deps=["users.ff"])
def countries(_: pd.DataFrame) -> pd.DataFrame:
    def pager(u, p, js):
        nxt = js.get("paging", {}).get("next")
        return {"next_request": {"url": nxt}} if nxt else None

    df = get_df(
        url="https://api.example.com/countries?page=1",
        paginator=pager,
        record_path=["data"],
        normalize=True,
    )
    # lightweight post-processing
    if "code" in df.columns:
        df["code"] = df["code"].str.upper()
    return df
```

Run:

```bash
fft run . --env dev --select dim_countries_from_api --http-cache ro
```

---

## Artifacts

`<project>/.fastflowtransform/target/run_results.json` (excerpt):

```json
{
  "results": [
    {
      "name": "dim_countries_from_api",
      "status": "success",
      "duration_ms": 153,
      "http": {
        "requests": 2,
        "cache_hits": 2,
        "bytes": 1842,
        "used_offline": true,
        "keys": ["GET:https://api.example.com/countries?page=1|{}|{}", "..."],
        "entries": [
          {"url": "https://api.example.com/countries?page=1", "status": 200, "content_hash": "sha256:..."},
          {"url": "https://api.example.com/countries?page=2", "status": 200, "content_hash": "sha256:..."}
        ]
      }
    }
  ]
}
```

> Note: When a node is **skipped** (fingerprint cache hit), no new `http` block is emitted - the model did not run.

---

## Tests & Offline Demos

- Place unit tests under `tests/api/...` and seed the cache directly (no real HTTP calls).
- Suggested scenarios:

  - **Offline hit:** set `FF_HTTP_OFFLINE=1`, seed the cache, `get_json/get_df` must succeed.
  - **Cache mode `off`:** even with cache entries, **no** reads; expect a failure in offline mode.
  - **`ro`:** allow read hits; **no** cache writes after a real or mocked request.
  - **Pagination:** stitch several pages from offline fixtures; telemetry should count requests/hits.

---

## Best Practices

- **Stable URLs and parameter order** produce identical cache keys and reproducible builds.
- **Keep `record_path` shallow**; use `normalize=True` only when necessary (performance).
- **Never cache secrets:** provide tokens via headers; the response body and metadata are cached.
- **Use `--offline` in CI** for deterministic tests with a pre-seeded cache.
- **Set TTL intentionally** when APIs change frequently.
- **Scope engine-specific variants** with `engine_model(only=...)` so each execution backend registers only the models it can run (pair with SQL `config(engines=[...])` when duplicating logical names).

---

## Troubleshooting

- **“offline + cache miss”**  
  Seed the cache (see tests) or disable offline mode.
- **“Schema mismatch”**  
  Harmonize columns after `get_df` (types, missing keys).
- **“Too many requests”**  
  Configure `FF_HTTP_RATE_LIMIT_RPS`; make pagination more efficient (larger `page_size`).
- **“No http block”**  
  Was the node **skipped** (fingerprint cache)? Or did the model avoid HTTP calls altogether?

---

## Security & Compliance

- **Do not commit secrets** - use environment variables or a secret manager.
- **PII/GDPR:** verify whether the API returns personal data; minimise retention.
- **Cache directory:** keep it in `.gitignore`; encrypt or isolate it if necessary.

---

## FAQ

**Q:** Can I call other libraries (for example `requests`, `httpx`) directly?  
**A:** Yes, but you lose telemetry and caching. The recommended entrypoint is `fastflowtransform.api.http`.

**Q:** How do I add custom headers (for example OAuth)?  
**A:** Pass `headers={...}`. Store sensitive values in env vars and inject them into your models.

**Q:** Does this work for POST requests?  
**A:** Release R1 focuses on GET. Please open an issue for POST/PUT support; the design can be extended.

---

**See also:**

- Technical guide: *Developer Guide – Architecture & Internals*
- Unit tests: `tests/api/test_http_*.py`
- Runtime & cache: *Parallelism & Cache (v0.3)*



<!-- >>> FILE: Config_and_Macros.md >>> -->

# FastFlowTransform Modeling Reference (v0.1)

> Authoritative reference for FastFlowTransform’s modeling layer: SQL/Python models, configuration macros, templating helpers, and testing hooks.
> Works with FastFlowTransform v0.1 (T1–T11). Supported engines: DuckDB, Postgres, BigQuery (pandas & BigFrames), Databricks/Spark, Snowflake/Snowpark.
> **Execution & Cache (v0.3) quick notes**
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
    requires={"users": {"id", "email"}}
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

Call `config()` at the top of SQL models (and optionally within Python models via decorator kwargs in future versions).

```sql
{{ config(
     materialized='view',
     tags=['mart', 'daily']
) }}
```

Supported keys (v0.1):

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
- Ephemeral Python models are not supported in v0.1.

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

See the [Technical Overview](./Technical_Overview.md#model-unit-tests-fft-utest) for an exhaustive walkthrough (engine overrides, CI examples, troubleshooting).

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



<!-- >>> FILE: Cache_and_Parallelism.md >>> -->

### 🆕 `docs/Cache_and_Parallelism.md`

````markdown
# Parallelism & Cache (FastFlowTransform v0.3)

FastFlowTransform 0.3 introduces a level-wise parallel scheduler and a build cache driven by stable fingerprints. This document explains **how parallel execution works**, **when nodes are skipped**, the exact **fingerprint formula**, and the **meta table** written after successful builds.

---

## Table of Contents
- [Parallel Scheduler](#parallel-scheduler)
- [Cache Policy](#cache-policy)
- [Fingerprint Formula](#fingerprint-formula)
- [Meta Table Schema](#meta-table-schema)
- [CLI Recipes](#cli-recipes)
- [Troubleshooting & FAQ](#troubleshooting--faq)
- [Example: simple_duckdb](#example-simple_duckdb)
- [Appendix: Environment Inputs](#appendix-environment-inputs)

---

## Parallel Scheduler

FastFlowTransform splits the DAG into **levels** (all nodes that can run together without violating dependencies). Within a level, up to `--jobs` nodes execute in **parallel**.

- Dependencies are **never** violated.
- `--keep-going`: tasks already started in a level finish; **subsequent levels won’t start** if any task in the current level fails.
- Logs are serialized through an internal queue to keep lines readable and per-node timing visible.

**Quick start**
```bash
# Run with 4 workers per level
fft run . --env dev --jobs 4

# Keep tasks in the same level running even if one fails
fft run . --env dev --jobs 4 --keep-going
````

---

## Cache Policy

The cache decides whether a node can be **skipped** when nothing relevant changed. Modes:

```
--cache=off  # always build
--cache=rw   # default; skip on match; write cache after build
--cache=ro   # skip on match; on miss build but don't write cache
--cache=wo   # always build and write cache
--rebuild <glob>  # ignore cache for matching nodes
--no-cache       # alias for --cache=off
```

### Skip condition

A node is skipped iff:

1. The current **fingerprint** matches the on-disk cache value, **and**
2. The **physical relation exists** on the target engine.

If the relation was dropped externally, FastFlowTransform will **rebuild** even if the fingerprint matches.

---

## Fingerprint Formula

Fingerprints are stable hashes that change on any relevant input:

* **SQL models**: `fingerprint_sql(node, rendered_sql, env_ctx, dep_fps)`

  * Uses **rendered** SQL (after Jinja), not the raw template.
* **Python models**: `fingerprint_py(node, func_src, env_ctx, dep_fps)`

  * Uses `inspect.getsource(func)` with a **file-content fallback** if needed.

`env_ctx` includes:

* `engine` (e.g., `duckdb`, `postgres`, `bigquery`)
* `profile_name` (CLI `--env`)
* Selected environment entries: **all `FF_*` keys** (key + value)
* A **normalized** portion of `sources.yml` (sorted keys/dump)

`dep_fps` are upstream fingerprints; **any upstream change** invalidates downstream fingerprints.

**Properties**

* Same inputs ⇒ same hash.
* Minimal change in SQL/function ⇒ different hash.
* Dependency changes propagate downstream.

---

## Meta Table Schema

After a successful build, FastFlowTransform writes a per-node audit row:

```
_ff_meta (
  node_name   TEXT/STRING,   -- logical name, e.g. "users.ff"
  relation    TEXT/STRING,   -- physical table/view, e.g. "users"
  fingerprint TEXT/STRING,
  engine      TEXT/STRING,
  built_at    TIMESTAMP
)
```

Backends:

* **DuckDB:** table `_ff_meta` in `main`.
* **Postgres:** table `_ff_meta` in the active schema.
* **BigQuery:** table `<dataset>._ff_meta`.

> Note: Skip logic uses the file-backed fingerprint cache and a direct relation existence check; the meta table is for auditing and tooling.

---

## CLI Recipes

```bash
# First run — builds everything, writes cache and meta
fft run . --env dev --cache=rw

# No-op run — should skip all nodes (if nothing changed)
fft run . --env dev --cache=rw

# Force rebuild of a single model (ignores cache for it)
fft run . --env dev --cache=rw --rebuild marts_daily.ff

# Read-only cache (skip on match, build on miss, no writes)
fft run . --env dev --cache=ro

# Always build and write cache
fft run . --env dev --cache=wo

# Disable cache entirely
fft run . --env dev --no-cache
```

With parallelism:

```bash
fft run . --env dev --jobs 4
fft run . --env dev --jobs 4 --keep-going
```

---

## Troubleshooting & FAQ

**“Why did it skip?”**
A skip requires a fingerprint match and an existing relation. Fingerprints include:

* rendered SQL / Python function source,
* `sources.yml` (normalized),
* engine/profile,
* **all `FF_*` environment variables**,
* upstream fingerprints.

Any change in the above triggers a rebuild downstream.

**“Relation missing but cache says skip?”**
We also check relation existence. If the table/view was dropped externally, FastFlowTransform will **rebuild**.

**“My logs interleave under parallelism.”**
Logs are serialized via a queue; use `-v` / `-vv` for richer but still stable output. Each node prints start/end and duration; levels summarize.

**“Utest cache?”**
`fft utest --cache {off|ro|rw}` defaults to `off` for deterministic runs. With `rw`, expensive unit cases can be accelerated. Unit tests do not rely on the meta table by default.

---

## Example: simple_duckdb

The demo contains two independent staging nodes (`users.ff.sql`, `orders.ff.sql`). They run in **parallel** within the same level.

Makefile targets:

```makefile
run_parallel:
	FF_ENGINE=duckdb FF_DUCKDB_PATH="$(DB)" fft run "$(PROJECT)" --env dev --jobs 4

cache_rw_first:
	FF_ENGINE=duckdb FF_DUCKDB_PATH="$(DB)" fft run "$(PROJECT)" --env dev --cache=rw

cache_rw_second:
	FF_ENGINE=duckdb FF_DUCKDB_PATH="$(DB)" fft run "$(PROJECT)" --env dev --cache=rw

cache_invalidate_env:
	FF_ENGINE=duckdb FF_DUCKDB_PATH="$(DB)" FF_DEMO_TOGGLE=1 fft run "$(PROJECT)" --env dev --cache=rw
```

---

## Appendix: Environment Inputs

Only environment variables with the `FF_` prefix affect fingerprints (keys and values). If you change one (e.g., `FF_RUN_DATE`, `FF_REGION`), fingerprints change and downstream nodes rebuild.

```bash
# Will invalidate fingerprints and rebuild affected nodes
FF_RUN_DATE=2025-01-01 fft run . --env dev --cache=rw
```

````

---

### 🔗 `docs/index.md` – Link zum neuen Kapitel

```diff
--- a/docs/index.md
+++ b/docs/index.md
@@ -10,6 +10,7 @@
 - [User Guide – Operational](./Technical_Overview.md#part-i--operational-guide)
 - [Modeling Reference](./Config_and_Macros.md)
 - [Parallelism & Cache (v0.3)](./Cache_and_Parallelism.md)
 - [Developer Guide – Architecture & Internals](./Technical_Overview.md#part-ii--architecture--internals)
````



<!-- >>> FILE: Incremental.md >>> -->

# Incremental Models (R1)

This guide explains how to configure incremental models, use `is_incremental()` in SQL, engine compatibility, and schema change policies.

## Quick Start

A minimal incremental model:

```sql
-- examples/r1_demo/models/fct_events_inc.ff.sql
{{ config(
  materialized='incremental',
  unique_key=['event_id'],
  on_schema_change='append_new_columns'  -- or 'sync_all_columns'
) }}
with src as (
  select * from {{ source('app', 'events') }}
  {% if is_incremental() %}
    where ingested_at > (select coalesce(max(ingested_at), timestamp '1970-01-01') from {{ this.name }})
  {% endif %}
)
select
  event_id,
  user_id,
  event_type,
  ingested_at,
  -- evolving column: will appear later
  meta_json
from src;
````

### `is_incremental()`

* Available in SQL templates during rendering.
* Returns `true` when the model exists and the current `materialized='incremental'` run chooses an incremental path (insert/merge) instead of full rebuild.
* Typical usage: filter the source to “new” rows only.

### Engine Matrix (MVP)

| Engine             | Incremental Insert | Merge/Upsert | Schema Change Policy |
| ------------------ | ------------------ | ------------ | -------------------- |
| DuckDB             | ✅ insert           | 🚧 fallback* | ✅ append new cols    |
| Postgres           | ✅ insert           | 🚧 fallback* | ✅ append new cols    |
| BigQuery (classic) | ✅ insert           | 🚧 fallback* | 🚧 best-effort       |
| BigQuery BigFrames | ✅ insert           | 🚧 fallback* | 🚧 best-effort       |
| Databricks Spark   | ✅ insert           | 🚧 fallback* | 🚧 best-effort       |
| Snowflake Snowpark | ✅ insert           | 🚧 fallback* | 🚧 best-effort       |

* Fallback strategy merges by delete-on-keys + insert (best effort) if native merge isn’t wired.

### Schema Change Policies

* `append_new_columns` (default): new columns appear in target if they show up in the select.
* `sync_all_columns` (planned): attempt to keep type/nullable alignment. Currently not enforced; prefer append in R1.

### End-to-End

```bash
# Seeds → initial incremental build → run again with filter
fft seed examples/r1_demo --env dev
fft run  examples/r1_demo --env dev --select fct_events_inc.ff
# simulate new data (re-seed or append), then:
fft run  examples/r1_demo --env dev --select fct_events_inc.ff
```

**Artifacts:** see `.fastflowtransform/target/{manifest.json, run_results.json, catalog.json}`.



<!-- >>> FILE: Profiles.md >>> -->

# Profiles Configuration

FastFlowTransform uses `profiles.yml` to describe how each environment connects to the execution engine (DuckDB, Postgres, BigQuery, Databricks Spark, Snowflake Snowpark, …). This document covers file layout, supported features, environment overrides, and loading precedence.

## File Location

`profiles.yml` lives at the project root (same level as `models/`, `project.yml`). The CLI loads it whenever you run `fft` commands (seed/run/test/dag/utest/docgen …).

```
project/
├── models/
├── project.yml
└── profiles.yml
```

## Basic Structure

The file is parsed as YAML after optional Jinja rendering. Top-level keys represent profile “names” (e.g. `dev`, `prod`, `dev_postgres`). Each profile must include an `engine` plus engine-specific configuration.

```yaml
dev:
  engine: duckdb
  duckdb:
    path: "{{ env('FF_DUCKDB_PATH', '.local/dev.duckdb') }}"

stg:
  engine: postgres
  postgres:
    dsn: "{{ env('FF_PG_DSN') }}"
    db_schema: "{{ env('FF_PG_SCHEMA', 'public') }}"

prod:
  engine: bigquery
  bigquery:
    project: "{{ env('FF_BQ_PROJECT') }}"
    dataset: "{{ env('FF_BQ_DATASET') }}"
    location: EU

default:
  engine: duckdb
  duckdb:
    path: ":memory:"
```

### Engines and Sections

Supported engines and their expected sections:

| Engine               | Section            | Key Fields                                        |
|----------------------|--------------------|---------------------------------------------------|
| `duckdb`             | `duckdb`           | `path` (file path or `:memory:`)                  |
| `postgres`           | `postgres`         | `dsn`, `db_schema`                                |
| `bigquery`           | `bigquery`         | `project` (optional), `dataset`, `location`       |
| `databricks_spark`   | `databricks_spark` | `master`, `app_name`, optional `extra_conf`, `warehouse_dir`, `use_hive_metastore`, `database`, `table_format`, `table_options` |
| `snowflake_snowpark` | `snowflake_snowpark`| `account`, `user`, `password`, `warehouse`, `database`, `db_schema`, optional `role` |

Each profile can define its own `vars:` block (values exposed via `var('key')` inside templates).

## Environment Variables

`profiles.yml` supports Jinja expressions. The helper `env('FF_VAR', 'fallback')` reads process environment variables and substitutes the default if unset. Examples:

```yaml
dev_postgres:
  engine: postgres
  postgres:
    dsn: "{{ env('FF_PG_DSN') }}"
    db_schema: "{{ env('FF_PG_SCHEMA', 'analytics') }}"
```

These expressions are rendered *before* YAML parsing. If the environment variable is missing and no default is provided, the expression resolves to an empty string and validation will fail with a clear error message.

## Loading Order & Precedence

When running `fft` commands, `_load_dotenv_layered()` loads `.env` files in ascending precedence:

1. `<repo>/.env`
2. `<project>/.env`
3. `<project>/.env.local`
4. `<project>/.env.<env_name>`
5. `<project>/.env.<env_name>.local`

Earlier values fill defaults; later files override earlier ones *only for keys that are not already defined*. **Values set in the shell (e.g. via `FF_ENGINE=duckdb fft run …`) have highest priority**—they remain untouched, even if `.env` files define the same key.

After `.env` loading, `profiles.yml` is rendered with Jinja (using the current `os.environ`) and parsed by Pydantic. Validation ensures required fields are present for each engine and produces human-readable errors for missing DSNs, schemas, etc.

## Selecting Profiles

- **Via `--env` flag**: `fft run . --env dev_postgres`
- **Via `FFT_ACTIVE_ENV`**: set in shell or `.env` to choose the active profile name.
- **Legacy `FF_ENGINE`** (overrides `engine` field post-parse): useful for quick experiments but explicit `profiles.yml` entries are preferred.

Example Makefile snippet that switches profiles without exposing secrets:

```make
ENGINE ?= duckdb

ifeq ($(ENGINE),duckdb)
  PROFILE_ENV = dev_duckdb
endif
ifeq ($(ENGINE),postgres)
  PROFILE_ENV = dev_postgres
endif

seed:
	FFT_ACTIVE_ENV=$(PROFILE_ENV) uv run fft seed . --env $(PROFILE_ENV)
```

## Using `.env` for Secrets

Keep sensitive credentials out of VCS by storing them in `.env` files referenced above:

```
examples/api_demo/
├── .env.dev_duckdb        # FF_DUCKDB_PATH=...
├── .env.dev_postgres      # FF_PG_DSN=..., FF_PG_SCHEMA=...
├── .env.dev_databricks    # FF_SPARK_MASTER=..., FF_SPARK_APP_NAME=...
└── profiles.yml
```

These files stay out of git (via `.gitignore`), while `profiles.yml` contains only non-sensitive wiring.

## Summary of Features

- Multiple profiles in a single YAML file.
- Jinja templating with `env()` helper for dynamic values.
- `.env` layered loading with shell overrides taking precedence.
- Validation for engine-specific parameters (clear error messages).
- Profile-specific `vars` exposed to Jinja `var()` function in models.
- Works seamlessly across CLI commands: seed, run, dag, test, docgen, utest.

Keep `profiles.yml` declarative, `.env` files secret, and use CLI or Makefiles to select the active profile per run. This pattern scales from local DuckDB demos to production Postgres/BigQuery/Snowflake deployments.



<!-- >>> FILE: Sources.md >>> -->

# Sources Configuration

`sources.yml` declares external tables (seeds, raw inputs, lakehouse paths) that models can reference via `{{ source('group', 'table') }}`. This document covers the schema, engine overrides, file paths, and best practices.

## File Location

Place `sources.yml` at your project root (same level as `models/`). Example:

```
project/
├── models/
├── sources.yml
└── seeds/
```

## YAML Schema (Version 2)

FastFlowTransform expects a dbt-style structure:

```yaml
version: 2
sources:
  - name: raw
    schema: staging                # default schema for this source group
    overrides:
      postgres:
        schema: raw_main           # engine-specific default override

    tables:
      - name: seed_users
        identifier: seed_users     # optional physical name
        overrides:
          duckdb:
            schema: main
          databricks_spark:
            format: delta
            location: "/mnt/delta/raw/seed_users"
```

### Fields

| Level    | Field       | Description |
|----------|-------------|-------------|
| source   | `name`      | Logical group identifier referenced by `source('name', ...)`. |
|          | `schema`    | Default target schema/database for the group. |
|          | `database`/`catalog` | Optional qualifiers per engine (BigQuery, Snowflake). |
|          | `overrides` | Map of engine → config snippet (schema overrides, formats, locations). |
| table    | `name`      | Logical table name (second argument in `source()`). |
|          | `identifier`| Physical name; defaults to `name` if omitted. |
|          | `location`  | File/path location (used with `format`). |
|          | `format`    | Ingestion format for engines supporting path-based sources (`delta`, `parquet`, …). |
|          | `options`   | Dict of format options (Spark/Databricks). |
|          | `overrides` | Additional engine-specific settings merged with source-level overrides. |

Engine-specific overrides follow this merge order:

1. Source defaults (`schema`, `database`, …)
2. Source-level `overrides[engine]`
3. Table-level `overrides[engine]`

### Engine Behavior

- **DuckDB / Postgres / BigQuery / Snowflake**: expect `identifier` (plus `schema`/`database` where relevant). Path-based sources raise errors.
- **Databricks Spark**: supports `format` + `location`. The executor registers a temp view with optional `options` (e.g. `compression`).

### Path-Based Sources Example

```yaml
  - name: raw_events
    tables:
      - name: landing
        overrides:
          databricks_spark:
            format: json
            location: "abfss://landing@storage.dfs.core.windows.net/events/*.json"
            options:
              multiline: true
```

## Referencing Sources in Models

```sql
select id, email
from {{ source('raw', 'seed_users') }}
```

After rendering, the executor resolves the fully-qualified relation or path depending on the active engine.

## Seed Integration

When combined with `seeds/schema.yml`, you can map CSV/Parquet seeds into schemas per engine:

```yaml
targets:
  raw/users:
    schema: raw
    schema_by_engine:
      duckdb: main
      postgres: staging
```

## Validation & Errors

- Missing `identifier` *and* `location` produce `KeyError` during rendering.
- Unknown source/table names raise `KeyError` with suggestions.
- Unsupported path-based sources on an engine (`location` provided but no `format`) raise descriptive `NotImplementedError`.

Keep `sources.yml` declarative, use engine overrides for schema differences, and lean on `.env` files where credentials or URIs vary per environment.



<!-- >>> FILE: Project_Config.md >>> -->

# Project Configuration (`project.yml`)

`project.yml` defines global metadata, documentation, variables, and data-quality tests for a FastFlowTransform project. This reference walks through the supported sections and common patterns.

## File Location

`project.yml` lives at the root of your project.

```
project/
├── models/
├── project.yml
└── profiles.yml
```

## Top-Level Keys

```yaml
name: my_project
version: "0.1"
models_dir: models          # optional, defaults to "models"

docs:
  dag_dir: site/dag         # output for fft dag --html
  models:
    users:
      description: "Raw users table"
      columns:
        id: "Primary key"
        email: "Email address"

vars:
  snapshot_day: "2024-01-01"
  default_limit: 100

tests:
  - type: not_null
    table: users
    column: id
    tags: [batch]
```

### Metadata

| Key         | Description |
|-------------|-------------|
| `name`      | Project identifier (used in docs/metadata). |
| `version`   | Arbitrary version string. |
| `models_dir`| Relative directory containing models (`*.ff.sql` / `*.ff.py`). |

### Documentation (`docs`)

- `dag_dir`: where `fft dag --html` writes the static site.
- `models`: per-model descriptions and column docs surfaced in the generated DAG/docs.

### Variables (`vars`)

Key/value pairs accessible via `{{ var('key', default) }}` in Jinja templates. CLI overrides (`--vars key=value`) take precedence.

### Tests (`tests`)

Project-wide data quality checks run by `fft test`. Each test is a dict with:

- `type`: `not_null`, `unique`, `accepted_values`, `row_count_between`, `greater_equal`, `non_negative_sum`, `freshness`, or reconciliation checks (`reconcile_equal`, `reconcile_diff_within`, `reconcile_ratio_within`, `reconcile_coverage`).
- `table`: target table or relation.
- `column`: required for column-based tests.
- Optional: `tags`, `severity` (`error`/`warn`), additional parameters (e.g. `values`, `min`, `max`).

Example:

```yaml
tests:
  - type: accepted_values
    table: mart_users
    column: status
    values: [active, invited]
    severity: warn
  - type: reconcile_equal
    name: revenue_vs_bookings
    left:  { table: fct_revenue,   expr: "sum(amount)" }
    right: { table: fct_bookings, expr: "sum(expected_amount)" }
    abs_tolerance: 5.0
```

## Interaction with `.env` and Profiles

`project.yml` does not read environment variables directly. However:

- `vars:` can reference `var('key')` defaults overridden by CLI or `.env`.
- Tests often depend on `profiles.yml` and `sources.yml` for the actual connection details.
- Makefiles may set `FFT_ACTIVE_ENV` or other `FF_*` variables influencing runs, but `project.yml` remains static.

## Best Practices

- Keep `project.yml` committed to version control (no secrets).
- Use `docs/` to provide richer Markdown descriptions; reference them via `columns` or `description` fields if desired.
- Organize tests by tag (`tags: [batch]`, `tags: [reconcile]`) to support selective execution: `fft test . --select tag:reconcile`.

Refer to `docs/Data_Quality_Tests.md` for detailed test semantics and `docs/Profiles.md` for profile/env loading behavior.



<!-- >>> FILE: State_Selection.md >>> -->

# State Selection — R1

Build only changed nodes or select by last run results.

## Changed Nodes

- `state:modified` — models that have changed since last cached fingerprint.
- `state:modified+` — the above plus all downstream dependents.

```bash
# First run populates cache
fft run examples/r1_demo --env dev --cache rw
# Touch files / change SQL → next run:
fft run examples/r1_demo --env dev --cache rw --select state:modified
fft run examples/r1_demo --env dev --cache rw --select state:modified+
````

## Result-based Selection

Use the last `run_results.json`:

* `result:ok`   — successful models (no warnings)
* `result:warn` — successful but with warnings
* `result:fail` — alias of `result:error`
* `result:error`— failed models

```bash
fft run examples/r1_demo --env dev --select result:error
```

### Artifacts

```
examples/r1_demo/.fastflowtransform/target/
├── manifest.json
├── run_results.json
└── catalog.json
```



<!-- >>> FILE: YAML_Tests.md >>> -->

# YAML Tests (Schema-bound)

Schema-bound tests live in `models/*.yml` or `models/**/schema.yml` and complement (or replace) `project.yml`-based tests.

## Example

```yaml
# examples/r1_demo/models/users_enriched.yml
version: 2
models:
  - name: users_enriched
    description: "Adds gmail flag"
    columns:
      - name: id
        tests:
          - not_null: { severity: error }
          - unique
      - name: email
        tests:
          - not_null
          - accepted_values:
              values: ["a@example.com","b@example.com","c@gmail.com"]
              severity: warn
````

### Severities

* `error` → contributes to failures (exit code 2).
* `warn` → surfaced in summary as ❕, does not affect exit code.

### Run

```bash
fft test examples/r1_demo --env dev
# Select only tests tagged 'reconcile' (if present)
fft test examples/r1_demo --env dev --select tag:reconcile
```

### Output (excerpt)

```
Data Quality Summary
────────────────────
✅ not_null           users.id                               (3ms)
❌ unique             users.id                               (2ms)
   ↳ [unique] users.id: found 1 duplicate
❕ accepted_values     users_enriched.email                   (1ms)

Totals
──────
✓ passed: 2
✗ failed: 1
! warnings: 1
```



<!-- >>> FILE: Data_Quality_Tests.md >>> -->

# Data Quality Test Reference

FastFlowTransform exposes a set of built-in data quality checks that you can configure in `project.yml → tests:` and execute with `fft test`. This document lists every supported test, required parameters, and example configurations.

## Usage Overview

```yaml
# project.yml
tests:
  - type: not_null
    table: users
    column: id
    severity: error          # default (omit for error)
    tags: [batch]

  - type: unique
    table: users
    column: email
    tags: [batch]

  - type: accepted_values
    table: users
    column: status
    values: [active, invited]
    severity: warn           # warn keeps run green on failure

  - type: row_count_between
    table: users_enriched
    min: 1
    max: 100000

  - type: reconcile_equal
    name: revenue_vs_bookings  # optional label in summaries
    tags: [reconcile]
    left:  { table: fct_revenue,   expr: "sum(amount)" }
    right: { table: fct_bookings, expr: "sum(expected_amount)" }
    abs_tolerance: 5.0
```

Every entry is a single dictionary describing one check. The common keys are:

| Key        | Description |
|------------|-------------|
| `type`     | Test kind (see tables below). |
| `table`    | Target table for table-level checks or display hint for reconciliations. |
| `column`   | Required for column-scoped checks (`not_null`, `unique`, …). |
| `severity` | `error` (default) or `warn`. |
| `tags`     | Optional list of selectors for `fft test --select tag:...`. |
| `name`     | Optional identifier surfaced in summaries (useful for reconciliations). |

Run all configured checks:

```bash
fft test . --env dev
```

Use `--select tag:<name>` to restrict by tags (legacy `--select batch` reads the same tags list). Tests always execute regardless of cache settings.

Each entry produces a summary line. Failures stop the command unless `severity: warn` is set.

## Table-Level Checks

These checks operate on a single table (optionally filtered with `where:`). Unless noted, they require a `column` argument.

### `not_null`
- **Purpose:** Assert that a column never contains NULLs.
- **Parameters:**
  - `column` *(str, required)*
  - `where` *(str, optional)* — SQL predicate applied before the NULL check.
- **Failure:** Reports the number of NULL rows and shows the underlying SQL.

### `unique`
- **Purpose:** Detect duplicates within a column.
- **Parameters:**
  - `column` *(str, required)*
  - `where` *(str, optional)*
- **Failure:** Indicates how many duplicate groups were found (HAVING count > 1) and shows a sample query.

### `accepted_values`
- **Purpose:** Ensure every non-NULL value is inside an allowed set.
- **Parameters:**
  - `column` *(str, required)*
  - `values` *(list, required)* — permitted literals (strings are quoted automatically).
  - `where` *(str, optional)*
- **Failure:** Shows the number of out-of-set values plus up to five sample values.

### `greater_equal`
- **Purpose:** Require all values to be greater than or equal to a threshold.
- **Parameters:**
  - `column` *(str, required)*
  - `threshold` *(number, default `0`)*
- **Failure:** Lists how many rows fell below the threshold.

### `non_negative_sum`
- **Purpose:** Validate that the sum of a numeric column is not negative.
- **Parameters:**
  - `column` *(str, required)*
- **Failure:** Reports the signed sum when it is negative.

### `row_count_between`
- **Purpose:** Guard minimum (and optional maximum) row counts for a table.
- **Parameters:**
  - `min` *(int, default `1`)*
  - `max` *(int, optional)* — omit for open-ended upper bounds.
- **Failure:** Indicates the observed row count when it falls outside `[min, max]`.

### `freshness`
- **Purpose:** Warn when the latest timestamp is older than an allowed delay.
- **Parameters:**
  - `column` *(str, required)* — timestamp column.
  - `max_delay_minutes` *(int, required)* — permitted staleness.
- **Failure:** Reports the computed lag in minutes. Uses ANSI-style `DATE_PART` (works on DuckDB/Postgres; extend for other engines as needed).

## Cross-Table Reconciliations

Reconciliation checks compare aggregates or keys across two relations. Their configuration accepts dictionaries describing the left/right side expressions or keys.

### `reconcile_equal`
- **Purpose:** Compare two scalar expressions with optional tolerances.
- **Parameters:**
  - `left`, `right` *(dict, required)* with keys:
    - `table` *(str, required)*
    - `expr` *(str, required)* — SQL select expression (e.g. `sum(amount)`).
    - `where` *(str, optional)*
  - `abs_tolerance` *(float, optional)* — maximum absolute difference.
  - `rel_tolerance_pct` *(float, optional)* — maximum relative difference in percent.
- **Failure:** Displays both values, absolute and relative differences.

### `reconcile_ratio_within`
- **Purpose:** Constrain the ratio `left/right` within bounds.
- **Parameters:**
  - `left`, `right` *(dict, required as above)*
  - `min_ratio`, `max_ratio` *(float, required)*
- **Failure:** Shows the computed ratio and expected interval.

### `reconcile_diff_within`
- **Purpose:** Limit the absolute difference between two aggregates.
- **Parameters:**
  - `left`, `right` *(dict, required)*
  - `max_abs_diff` *(float, required)*
- **Failure:** Reports the absolute difference when it exceeds `max_abs_diff`.

### `reconcile_coverage`
- **Purpose:** Ensure every key present in a source table appears in a target table (anti-join zero).
- **Parameters:**
  - `source` *(dict, required)* — `table` and `key` column.
  - `target` *(dict, required)* — `table` and `key` column.
  - `source_where` *(str, optional)* — filter applied to the source.
  - `target_where` *(str, optional)* — filter applied to the target.
- **Failure:** Reports the number of missing keys.

## Severity & Selectors

- `severity: error` (default) makes failures stop the test run with exit code 1.
- `severity: warn` records the result but keeps the run successful.
- `selectors:` lets you group checks under named tokens (e.g. `batch`, `streaming`). Use `fft test --select tag:batch` to execute a subset.

## CLI Summary Output

Each executed check produces a line in the summary:

```
✓ not_null          users.email                     (3ms)
✖ accepted_values   events.status    values=['new', 'active']   (warn)
```

Failures include the generated SQL (where available) to simplify debugging. Use `fft test --verbose` for more detail, or `FFT_SQL_DEBUG=1` to log the underlying queries.

## Further Reading

- [`docs/YAML_Tests.md`](YAML_Tests.md) – schema for YAML-defined tests and advanced scenarios.
- [`fft test --help`] — command-line switches, selectors, and cache options.



<!-- >>> FILE: examples/Environment_Matrix.md >>> -->

# Environment Matrix (DuckDB-only) — Example

This tiny project demonstrates **per-environment configuration** (dev / stg / prod) while keeping everything on **DuckDB**.
Each environment uses its **own DuckDB file**, so you can switch environments without changing code.

It also includes a **seed step** (CSV → table) and two minimal models:

* `env_vars.ff` (Python) — echoes which env is active and which DuckDB file is used
* `hello.ff` (SQL view) — shows how `{{ this.* }}` resolves from the active profile
* `users.ff` (SQL table) — reads from the seeded CSV table to prove seeding works

---

## What this shows

* Layered environment files: `.env.dev`, `.env.stg`, `.env.prod` (+ optional `*.local` overrides)
* `profiles.yml` that reads from `env('…')` so connection details live in env files
* All environments use **DuckDB**, but **different DB files** (e.g. `.local/dev.duckdb`, `.local/stg.duckdb`, …)
* Seeding CSV → `seed_users` table, then a simple model consuming it

---

## Project layout

```
examples/env_matrix/
├─ models/
│  ├─ env_vars.ff.py          # Python model: shows env + DuckDB file info
│  └─ users.ff.sql            # SQL table: reads from seeded 'seed_users'
├─ seeds/
│  └─ users.csv               # sample data for seeding (-> seed_users)
├─ profiles.yml               # all envs = DuckDB, different paths
├─ .env                       # shared defaults (optional)
├─ .env.dev                   # dev environment vars
├─ .env.stg                   # stg environment vars
├─ .env.prod                  # prod environment vars
├─ .env.dev.local             # private overrides (gitignored; optional)
├─ .env.stg.local             # private overrides (gitignored; optional)
├─ .env.prod.local            # private overrides (gitignored; optional)
└─ Makefile                   # convenience targets (run, seed, dag)
```

---

## Environment files

Each env file sets a different DuckDB path:

* `.env.dev`

  ```
  FFT_ACTIVE_ENV=dev
  FF_ENGINE=duckdb
  FF_DUCKDB_PATH=.local/env_matrix.dev.duckdb
  ```

* `.env.stg`

  ```
  FFT_ACTIVE_ENV=stg
  FF_ENGINE=duckdb
  FF_DUCKDB_PATH=.local/env_matrix.stg.duckdb
  ```

* `.env.prod`

  ```
  FFT_ACTIVE_ENV=prod
  FF_ENGINE=duckdb
  FF_DUCKDB_PATH=.local/env_matrix.prod.duckdb
  ```

> You can place secrets or machine-local tweaks in `.env.<env>.local` (ignored by git).
> Optional toggles (if you want verbose SQL logs):
> `FFT_SQL_DEBUG=1`, `FFT_LOG_JSON=1`

---

## `profiles.yml` (DuckDB for all envs)

```yaml
default:
  dev:
    engine: "{{ env('FF_ENGINE', 'duckdb') }}"
    duckdb:
      path: "{{ env('FF_DUCKDB_PATH', ':memory:') }}"

  stg:
    engine: "{{ env('FF_ENGINE', 'duckdb') }}"
    duckdb:
      path: "{{ env('FF_DUCKDB_PATH', ':memory:') }}"

  prod:
    engine: "{{ env('FF_ENGINE', 'duckdb') }}"
    duckdb:
      path: "{{ env('FF_DUCKDB_PATH', ':memory:') }}"
```

---

## Models

### `models/env_vars.ff.py` (Python)

Returns one row with:

* `active_env_hint` (from `.env.*`),
* `ff_engine` (should be `duckdb` here),
* `duckdb_path`, `duckdb_exists`, `duckdb_size_bytes`.

### `models/hello.ff.sql` (SQL view)

Uses `{{ this.materialized }}`, `{{ this.schema }}`, `{{ this.database }}` so you can see what the active profile provides. (The simple `SELECT` is compatible with DuckDB; if you added casts like `::text`, they’re fine in DuckDB too.)

### `models/users.ff.sql` (SQL table)

Reads from the seeded table `seed_users`:

```sql
{{ config(materialized='table', tags=['demo', 'seed']) }}

select
  id,
  email
from "seed_users";
```

> If you see an error “table seed_users does not exist”, you **haven’t run `fft seed`** for that environment yet.

---

## Seeds

`seeds/users.csv` is loaded by `fft seed` into a table named `seed_users`.
(That’s the default naming convention: `users.csv` → `seed_users`.)

---

## Running it

From the repo root:

### Using `uv` directly

**Dev**

```bash
uv run fft seed examples/env_matrix --env dev
uv run fft run  examples/env_matrix --env dev
uv run fft dag  examples/env_matrix --env dev --html
```

**Staging**

```bash
uv run fft seed examples/env_matrix --env stg
uv run fft run  examples/env_matrix --env stg
```

**Prod**

```bash
uv run fft seed examples/env_matrix --env prod
uv run fft run  examples/env_matrix --env prod
```

### Using the Makefile (inside `examples/env_matrix/`)

```bash
make run-dev     # runs the DAG on dev
make run-stg
make run-prod

make seed-dev    # seed only (dev)
make seed-stg
make seed-prod

make dag-dev     # generate HTML DAG for dev
make clean       # remove .local/, docs/, site/, .fastflowtransform/
```

> Tip: re-run `fft seed` whenever you switch environments or change `seeds/*.csv`.

---

## Inspecting results

* The **HTML DAG** (after `make dag-dev`) will be at:

  ```
  examples/env_matrix/site/dag/index.html
  ```
* The **artifacts** are under:

  ```
  examples/env_matrix/.fastflowtransform/target/{manifest.json, run_results.json, catalog.json}
  ```
* Query the DuckDB files directly with `duckdb` CLI or `python` + `duckdb` module if you want to peek inside.

---

## Troubleshooting

* **`seed_users` not found**
  Run `fft seed` for the same environment:
  `uv run fft seed examples/env_matrix --env dev`

* **No logs showing**
  Use `-v`/`-vv` and/or `--sql-debug` on the CLI, or set:

  ```
  FFT_SQL_DEBUG=1
  FFT_LOG_JSON=1  # optional JSON logs
  ```

* **Wrong environment picked**
  Double-check the `--env` flag in your CLI call and ensure the `.env.<env>` file exists.

---

## Clean up

```bash
make clean              # from examples/env_matrix/
# or manually:
rm -rf examples/env_matrix/.local examples/env_matrix/site examples/env_matrix/docs
rm -rf examples/env_matrix/.fastflowtransform
```



<!-- >>> FILE: examples/API_Demo.md >>> -->

# API Demo Project

The `examples/api_demo` scenario demonstrates how FastFlowTransform blends local data, external APIs, and multiple execution engines. It highlights:

- **Hybrid data model**: joins a local seed (`crm.users`) with live user data from JSONPlaceholder.
- **Multiple environments**: switch between DuckDB, Postgres, and Databricks Spark using `profiles.yml` + `.env.*`.
- **HTTP integration**: compare the built-in FastFlowTransform HTTP client (`api_users_http`) with a plain `requests` implementation (`api_users_requests`).
- **Offline caching & telemetry**: inspect HTTP snapshots via `run_results.json`.
- **Engine-aware registration**: scope Python models via `engine_model` and SQL models via `config(engines=[...])` so only the active engine’s nodes load.

## Data Model

1. **Seed staging** – `models/common/users.ff.sql`
   ```sql
   {{ config(
       materialized='table',
       tags=[
           'example:api_demo',
           'scope:common',
           'kind:seed-consumer',
           'engine:duckdb',
           'engine:postgres',
           'engine:databricks_spark'
       ]
   ) }}
   select id, email
   from {{ source('crm', 'users') }};
   ```
   Consumes `sources.yml → crm.users` (seeded from `seeds/seed_users.csv`).

2. **API enrichment** – two Python implementations under `models/engines/duckdb/`:
   - `api_users_http.ff.py` uses the built-in HTTP wrapper (`fastflowtransform.api.http.get_df`) with cache/offline support.
   - `api_users_requests.ff.py` uses raw `requests` for maximum flexibility.
   - Wrap engine-specific callables with `engine_model(only="duckdb", ...)` to skip registration when another engine is selected.

3. **Mart join** – `models/common/mart_users_join.ff.sql`
   ```sql
   {{ config(engines=['duckdb','postgres','databricks_spark']) }}
   {% set api_users_model = var('api_users_model', 'api_users_http') %}
   {% set api_users_refs = {
       'api_users_http': ref('api_users_http'),
       'api_users_requests': ref('api_users_requests')
   } %}
   {% set api_users_relation = api_users_refs.get(api_users_model, api_users_refs['api_users_http']) %}
   with a as (
     select u.id as user_id, u.email from {{ ref('users.ff') }} u
   ),
   b as (
     select * from {{ api_users_relation }}
   )
   select ...
   ```
   Ties everything together and exposes the `var('api_users_model')` hook to choose the HTTP implementation while still keeping literal `ref('…')` calls in the template (required for DAG detection). `config(engines=[...])` keeps the SQL node registered only for the engines you list, preventing duplicate names across engine-specific folders.

   > **Warning:** The DAG builder only detects dependencies from literal `ref('model_name')` strings. A pure `ref(api_users_model)` (without the mapping shown above) compiles, but the graph would miss the edge to `api_users_http`/`api_users_requests`.

## Profiles & Secrets

`profiles.yml` defines per-engine profiles that reference environment variables:

```yaml
dev_duckdb:
  engine: duckdb
  duckdb:
    path: "{{ env('FF_DUCKDB_PATH', '.local/api_demo.duckdb') }}"

dev_postgres:
  engine: postgres
  postgres:
    dsn: "{{ env('FF_PG_DSN') }}"
    db_schema: "{{ env('FF_PG_SCHEMA', 'public') }}"
```

`.env.dev_*` files supply the actual values. `_load_dotenv_layered()` loads them in priority order: repo `.env` → project `.env` → `.env.<env>` → shell overrides (highest priority). Secrets stay out of version control.


## Makefile Workflow

`Makefile` chooses the profile via `ENGINE` (`duckdb`/`postgres`/`databricks_spark`) and wraps the main commands:

```make
ENGINE ?= duckdb

ifeq ($(ENGINE),duckdb)
  PROFILE_ENV = dev_duckdb
endif
...

seed:
	uv run fft seed "$(PROJECT)" --env $(PROFILE_ENV)
run:
	env FFT_ACTIVE_ENV=$(PROFILE_ENV) ... uv run fft run ...
```

Common targets:

| Target                   | Description |
|--------------------------|-------------|
| `make ENGINE=duckdb seed`| Materialize seeds into DuckDB. |
| `make ENGINE=postgres run`| Execute the full pipeline against Postgres. |
| `make dag`               | Render documentation (`site/dag/`). |
| `make api-run`           | Run only API models (uses HTTP cache). |
| `make api-offline`       | Force offline mode (`FF_HTTP_OFFLINE=1`). |
| `make api-show-http`     | Display HTTP snapshot metrics via `jq`. |

HTTP tuning parameters (`FF_HTTP_ALLOWED_DOMAINS`, cache dir, timeouts) live in `.env` and are appended via `HTTP_ENV` when running commands.

## End-to-End Demo

1. **Select engine**: `make ENGINE=duckdb` (default). Set `ENGINE=postgres` or `ENGINE=databricks_spark` to switch.
2. **Seed data**: `make seed`
3. **Run pipeline**: `make run`
4. **Explore docs**: `make dag` → open `examples/api_demo/site/dag/index.html`
5. **Inspect HTTP usage**: `make api-show-http`

This example demonstrates multi-engine configuration, environment-driven secrets, and API enrichment within FastFlowTransform.



<!-- >>> FILE: examples/Local_Engine_Setup.md >>> -->

## Local Engine Setup

### DuckDB

- Copy `.env.dev_duckdb` and adjust `FF_DUCKDB_PATH` if you want a different location (default: `.local/api_demo.duckdb`).
- Create the target directory once: `mkdir -p examples/api_demo/.local`.
- Run `make ENGINE=duckdb seed run` to build the seeds and models inside the DuckDB file.

### Postgres

- Start a local database, e.g. via Docker:  
  `docker run --name fft-postgres -e POSTGRES_PASSWORD=postgres -p 5432:5432 -d postgres:15`.
- Set `FF_PG_DSN` in `.env.dev_postgres` (for example `postgresql+psycopg://postgres:postgres@localhost:5432/fft`) and optionally override `FF_PG_SCHEMA` (defaults to `api_demo`).  
  The executor ensures the schema exists via `CREATE SCHEMA IF NOT EXISTS` on first connect.
- Execute `make ENGINE=postgres seed run` to materialize seeds and models in Postgres.

### Databricks Spark (local)

- Install Java (JDK ≥ 17) and declare `JAVA_HOME`, for example:  
  `brew install openjdk@17`  
  `echo 'JAVA_HOME=/opt/homebrew/opt/openjdk@17' >> examples/api_demo/.env.dev_databricks`.
- Optionally tweak `FF_SPARK_MASTER` / `FF_SPARK_APP_NAME` in `.env.dev_databricks` (default: `local[*]`).
- To persist tables across separate `seed`/`run` sessions, enable the bundled Hive metastore defaults:  
  `FF_DBR_ENABLE_HIVE=1`, `FF_DBR_WAREHOUSE_DIR=examples/api_demo/spark-warehouse`, `FF_DBR_DATABASE=api_demo`.
- Switch the physical format by setting `FF_DBR_TABLE_FORMAT` (e.g. `delta`, requires the Delta Lake runtime); extra writer options can be supplied via `profiles.yml → databricks_spark.table_options`.
- Ensure your shell loads `.env.dev_databricks` (via `make`, `direnv`, or manual export) and run `make ENGINE=databricks_spark seed run`.



<!-- >>> FILE: Contributing.md >>> -->

--8<-- "Contributing.md"



<!-- >>> FILE: License.md >>> -->

# License

--8<-- "License"

# 🧭 FlowForge – Technical Developer Documentation (v0.1)

> Status: latest updates from your context dump. This document consolidates project structure, architecture, core APIs, error handling, CLI, examples, and roadmap into a print/git-friendly Markdown.
>
> Looking for an overview? Start at the [`docs/index.md`](./index.md) hub, then dive back here when you need details.
>
> Project: **FlowForge** — SQL & Python Data Modeling (Batch + Streaming), DAG, CLI, Auto-Docs, DQ Tests.

---

## Docs Navigation
1. [Getting Started](./index.md)
2. **User Guide** — see [Part I – Operational Guide](#part-i--operational-guide) (this document)
3. [Modeling Reference](./Config_and_Macros.md)
4. **Developer Guide** — see [Part II – Architecture & Internals](#part-ii--architecture--internals) (this document)

---

## Table of Contents

- [Docs Navigation](#docs-navigation)
- [Part I – Operational Guide](#part-i--operational-guide)
  - [Project Layout](#project-layout)
  - [Sample Models](#sample-models)
  - [Seeds & Example Data](#seeds--example-data)
  - [Makefile Targets](#makefile-targets)
  - [CLI Flows](#cli-flows)
  - [Logging & Verbosity](#logging--verbosity)
  - [Model Unit Tests (`flowforge utest`)](#model-unit-tests-flowforge-utest)
  - [Troubleshooting](#troubleshooting)
  - [Error Codes](#error-codes)
  - [Profiles & Environment Overrides](#profiles--environment-overrides)
  - [Parallel Scheduler (v0.3)](#parallel-scheduler-v03)
  - [Cache Policy (v0.3)](#cache-policy-v03)
  - [Fingerprint Formula (v0.3)](#fingerprint-formula-v03)
  - [Meta Table Schema (v0.3)](#meta-table-schema-v03)
  - [Jinja DSL Quick Reference](#jinja-dsl-quick-reference)
  - [Roadmap Snapshot](#roadmap-snapshot)
- [Part II – Architecture & Internals](#part-ii--architecture--internals)
  - [Architecture Overview](#architecture-overview)
  - [Core Modules](#core-modules)
    - [`core.py`](#corepy)
    - [`dag.py`](#dagpy)
    - [`errors.py`](#errorspy)
    - [Executors](#executors)
    - [`validation.py`](#validationpy)
    - [`testing.py`](#testingpy)
    - [`docs.py` & Templates](#docspy--templates)
    - [`seeding.py`](#seedingpy)
  - [CLI Implementation](#cli-implementation)
  - [Settings Infrastructure](#settings-infrastructure)
  - [Streaming Components](#streaming-components)
  - [Mini End-to-End Example (Python API)](#mini-end-to-end-example-python-api)

---

## Part I – Operational Guide

### Project Layout

```text
flowforge/
├── pyproject.toml
├── src/
│   └── flowforge/
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
	flowforge seed $(PROJECT) --env dev

run:
	FF_ENGINE=duckdb FF_DUCKDB_PATH="$(DB)" flowforge run "$(PROJECT)" --env dev

dag:
	flowforge dag "$(PROJECT)" --env dev --html

test:
	flowforge test "$(PROJECT)" --env dev --select batch
```

Targets wrap the CLI commands showcased below. Feel free to copy the pattern into your own projects.

### CLI Flows

> 📚 **Mehr lesen … Quickstart & Rezepte**
> Die Schritt-für-Schritt-Befehle findest du im [`README.md`](../README.md#quickstart). Dort bleibt der vollständige Ablauf gepflegt; dieser Abschnitt fokussiert auf weiterführende Hinweise.

- CLI-Flags und interne Abläufe sind im Abschnitt [CLI Implementation](#cli-implementation) dokumentiert.
- Beispiele für Automatisierung findest du in den [Makefile Targets](#makefile-targets).

### Logging & Verbosity

FlowForge exposes uniform logging controls across all CLI commands plus a dedicated SQL debug channel.

#### Flags

- `-q` / `--quiet` → only errors (`ERROR`)
- *(default)* → concise warnings (`WARNING`)
- `-v` / `--verbose` → progress/info (`INFO`)
- `-vv` → full debug (`DEBUG`), including SQL debug output

`-vv` flips on the SQL debug channel automatically (same as setting `FLOWFORGE_SQL_DEBUG=1`).

#### SQL debug channel

Enable it to inspect Python-model inputs, dependency columns, and helper SQL emitted by data-quality checks:

```bash
# full debug (recommended)
flowforge run . -vv

# equivalent using the env var (legacy behaviour retained)
FLOWFORGE_SQL_DEBUG=1 flowforge run .
```

#### Usage patterns

```bash
flowforge run . -q     # quiet (errors only)
flowforge run .        # default (concise)
flowforge run . -v     # verbose progress (model names, executor info)
flowforge run . -vv    # full debug + SQL channel
```

**Notes**

- SQL debug output routes through the `flowforge.sql` logger; use `-vv` or the env var to see it.
- Existing projects do not need changes: the env var continues to work even without `-vv`.

### Model Unit Tests (`flowforge utest`)

`flowforge utest` executes a single model in isolation, loading only the inputs you provide and comparing the result to an expected dataset. It works for SQL and Python models and runs against DuckDB or Postgres by default.

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
flowforge utest .                      # discover all specs
flowforge utest . --env dev            # use a specific profile
flowforge utest . --model users_enriched
flowforge utest . --model mart_orders_enriched --case join_and_flag
flowforge utest . --path tests/unit/users_enriched.yml
```

Override the executor for all specs (ensure credentials/DSNs are set):

```bash
export FF_PG_DSN="postgresql+psycopg://postgres:postgres@localhost:5432/ffdb"
export FF_PG_SCHEMA="public"
flowforge utest . --engine postgres
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
      - run: flowforge utest . --env dev
```

(For Postgres, add a service container and run `flowforge utest . --engine postgres` with `FF_PG_DSN` / `FF_PG_SCHEMA`.)

### Troubleshooting

- **DuckDB seeds not visible** → ensure `FF_DUCKDB_PATH` (or profile path) is identical for `seed`, `run`, `dag`, and `test`.
- **Postgres connection refused** → confirm `FF_PG_DSN`, container status (`docker ps`), and that port `5432` is open.
- **BigQuery permissions** → set `GOOGLE_APPLICATION_CREDENTIALS` and match dataset/location to your profile.
- **HTML docs missing** → run `flowforge dag <project> --html` and open `<project>/docs/index.html`.
- **Unexpected test failures** → inspect rendered SQL in CLI output, refine selection via `--select`, refresh seeds if needed.
- **Dependency table not found** in utests → provide all physical upstream relations in the YAML spec.

### Error Codes

| Type                      | Class/Source              | Exit | Notes                                                   |
|---------------------------|---------------------------|------|---------------------------------------------------------|
| Missing dependency        | `DependencyNotFoundError` | 1    | Per-node list; tips for `ref()` / names                |
| Cycle in DAG              | `ModelCycleError`         | 1    | "Cycle detected among nodes: ..."                      |
| Model execution (KeyError)| `cli.py` → formatted block| 1    | Inspect columns, use `relation_for(dep)` as keys       |
| Data quality failures     | `cli test` → summary      | 2    | "Totals ... passed/failed"; each failure on its own line |
| Unknown/unexpected        | generic                   | 99   | Optional trace via `FLOWFORGE_TRACE=1`                 |

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

FlowForge executes the DAG in **levels**. Each level contains nodes without mutual dependencies.

- `--jobs N` limits the **maximum concurrency per level**.
- `--keep-going` keeps tasks within the current level running even if one fails; subsequent levels are not started.

**CLI**
```bash
flowforge run . --env dev --jobs 4            # parallel (level-wise)
flowforge run . --env dev --jobs 4 --keep-going
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
flowforge run . --env dev --cache=rw
flowforge run . --env dev --cache=ro
flowforge run . --env dev --cache=rw --rebuild marts_daily.ff
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

FlowForge writes a per-node audit row after successful builds:

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
class FlowForgeError(Exception): ...
class ModuleLoadError(FlowForgeError): ...
class DependencyNotFoundError(FlowForgeError): ...
class ModelCycleError(FlowForgeError): ...
class TestFailureError(FlowForgeError): ...
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

- `flowforge run <project> [--env dev] [--engine ...]`
- `flowforge dag <project> [--env dev] [--html]`
- `flowforge test <project> [--env dev] [--select batch|streaming]`
- `flowforge seed <project> [--env dev]`
- `flowforge --version`

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
from flowforge.core import REGISTRY
from flowforge.dag import topo_sort
from flowforge.executors.duckdb_exec import DuckExecutor

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

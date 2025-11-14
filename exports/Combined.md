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
- [CLI Guide](./CLI_Guide.md)
- [Logging & Verbosity](./Logging.md)
- [API calls in Python models](./Api_Models.md)
- [Incremental Models](./Incremental.md)
- [YAML Tests (Schema-bound)](./YAML_Tests.md)
- [Model Unit Tests](./Unit_Tests.md)
- [Data Quality Tests Reference](./Data_Quality_Tests.md)
- [Auto-Docs & Lineage](./Auto_Docs.md)
- [Troubleshooting & Error Codes](./Troubleshooting.md)
- [Profiles & Environments](./Profiles.md)
- [Sources Declaration](./Sources.md)
- [Project Configuration](./Project_Config.md)
- [State Selection (changed & results)](./State_Selection.md)
- [Basic Demo](./examples/Basic_Demo.md)
- [Materializations Demo](./examples/Materializations_Demo.md)
- [Data Quality Tests Demo](./examples/DQ_Demo.md)
- [Macros Demo](./examples/Macros_Demo.md)
- [Cache Demo](./examples/Cache_Demo.md)
- [Environment Matrix Demo](./examples/Environment_Matrix.md)
- [Incremental & Delta Demo](examples/Incremental_Demo.md)
- [Local Engine Setup](./examples/Local_Engine_Setup.md)
- [API Demo](./examples/API_Demo.md)
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
- **Understand the project layout & CLI workflow:** start with *Project Layout* in the [Technical Overview](Technical_Overview.md#project-layout) and pair it with the [CLI Guide](CLI_Guide.md) for command patterns.
- **Configure runtimes & profiles:** review executor profiles and environment overrides in the dedicated [Profiles guide](Profiles.md) plus [Logging & Verbosity](Logging.md) for observability flags.
- **Model data quality & troubleshoot runs:** combine the [Model Unit Tests guide](Unit_Tests.md) with [Troubleshooting & Error Codes](Troubleshooting.md) to keep runs deterministic and easy to debug.
- **Explore runnable demos:** start with the [Basic Demo Overview](examples/Basic_Demo.md) or browse the `examples/` directory; each subproject ships with its own README.

### 2. Extend FastFlowTransform (Developers & Contributors)

- **Dive into architecture & core modules:** start with [Architecture Overview](Technical_Overview.md#architecture-overview) and [Core Modules](Technical_Overview.md#core-modules) for registry, DAG, executors, validation, and more.
- **Add tests & seeds:** reuse the curated demos under `docs/examples/` for seeds/Makefiles and follow the [Model Unit Tests guide](Unit_Tests.md) for deterministic fixtures.
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

## 0. Create a skeleton (optional)

Start with a minimal project structure:

```bash
fft init demo_project --engine duckdb
```

The command is non-interactive, refuses to overwrite existing directories, and leaves inline comments that point back to the relevant docs (`Project_Config.md`, `Profiles.md`, etc.). Populate the generated files before running the steps below.

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
  - [Example Projects and Seeds](#example-projects-and-seeds)
  - [CLI Flows](#cli-flows)
  - [Logging & Verbosity](#logging-verbosity)
  - [Model Unit Tests (`fft utest`)](#model-unit-tests-fft-utest)
  - [Troubleshooting](#troubleshooting)
  - [Profiles & Environment Overrides](#profiles-environment-overrides)
  - [Parallel Execution and Cache](#parallel-execution-and-cache)
  - [Roadmap Snapshot](#roadmap-snapshot)
  - [Cross-Table Reconciliations](#cross-table-reconciliations)
  - [Auto-Docs and Lineage](#auto-docs-and-lineage)
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

### Example Projects and Seeds

Need runnable references? Start with the curated demos under `docs/examples/`:

- [Basic Demo](./examples/Basic_Demo.md) shows the minimum viable project (seeds, staging, marts) plus Makefile targets you can copy.
- [API Demo](./examples/API_Demo.md) focuses on HTTP-powered Python models.
- [Environment Matrix](./examples/Environment_Matrix.md) demonstrates multiple profiles talking to different engines.

Each demo includes deterministic seeds (`seeds/*.csv`), schema YAML, and Makefile shortcuts, so the detailed CSV listings and commands here would be redundant. Follow the demo docs (or the [Quickstart](./Quickstart.md)) for the full walkthrough.

### CLI Flows

Looking for command recipes, selection filters, or sync workflows? See the dedicated [CLI Guide](./CLI_Guide.md) for a task-by-task breakdown (seed/run/dag/docgen/test/utest/sync-db-comments) plus links to API-model helpers.

### Logging & Verbosity

Need the exact behaviour of `-q/-v/-vv`, SQL debug output, or the parallel log queue? Head over to [Logging.md](./Logging.md) for the full matrix plus usage snippets.

### Model Unit Tests (`fft utest`)

The full how-to (cache modes, YAML DSL, CI snippets) moved to [Unit_Tests.md](./Unit_Tests.md). Keep this Section in mind whenever you need fast feedback on SQL/Python models without executing the entire DAG.

### Troubleshooting

Common fixes (engines, docs generation, tests) plus the exit-code matrix live in [Troubleshooting.md](./Troubleshooting.md). Skim that doc whenever you hit connectivity issues or need to decode return codes.

### Profiles & Environment Overrides
Need to understand profile precedence, `.env` layering, or the Pydantic models that back settings? Jump to the [Profiles guide](./Profiles.md) which covers file layout, environment helpers, validation, and selection precedence in depth.

### Parallel Execution and Cache

Level-wise parallelism, cache modes, fingerprint formula, and the `_ff_meta` audit table are documented in [Cache_and_Parallelism.md](./Cache_and_Parallelism.md). Use that reference for CLI examples (`--jobs`, `--cache`, `--rebuild`), skip conditions, and troubleshooting tips related to concurrency.

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

Reconciliation tests (`reconcile_equal`, `reconcile_ratio_within`, `reconcile_diff_within`, `reconcile_coverage`) are fully documented in the [Data Quality Test Reference](./Data_Quality_Tests.md#cross-table-reconciliations). Use that guide for YAML schemas, tolerance parameters, and engine notes before wiring the checks into `fft test`.

### Auto-Docs and Lineage

Rendering the DAG site, feeding project descriptions/lineage, and exporting JSON manifests are covered in [Auto_Docs.md](./Auto_Docs.md). Head there for command flags, markdown/YAML resolution, and lineage overrides.

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



<!-- >>> FILE: Cache_and_Parallelism.md >>> -->

# Parallelism & Cache

**TL;DR:** FastFlowTransform executes models in parallel DAG levels and uses deterministic
fingerprints to skip unchanged nodes — while a separate HTTP cache accelerates API models.

FastFlowTransform introduces a level-wise parallel scheduler and a build cache driven by stable fingerprints. This document explains **how parallel execution works**, **when nodes are skipped**, the exact **fingerprint formula**, and the **meta table** written after successful builds.

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
```

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

### HTTP Response Cache

In addition to the build cache, FastFlowTransform provides an **HTTP response cache** for API models using
`fastflowtransform.api.http.get_df(...)`.

- **Purpose:** Avoid redundant API calls and support offline mode.
- **Location:** Controlled by `FF_HTTP_CACHE_DIR` (e.g. `.local/http-cache`).
- **Controls (environment):**
  - `FF_HTTP_ALLOWED_DOMAINS`: comma-separated list of hosts allowed to cache.
  - `FF_HTTP_MAX_RPS`, `FF_HTTP_MAX_RETRIES`, `FF_HTTP_TIMEOUT`: rate limiting & retry policy.
  - `FF_HTTP_OFFLINE=1`: run in offline mode — serve only from cache, no network calls.
- **CLI visibility:** Each run writes HTTP stats (`requests`, `cache_hits`, `bytes`, `used_offline`)
  to `.fastflowtransform/target/run_results.json`.
- **Makefile helpers:** see `make api-show-http` in the API demo to inspect HTTP cache usage.

> This cache is independent from the build cache; it stores API responses, not SQL or fingerprints.

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

> **Note:** The active engine and profile name are part of the fingerprint.
> Switching from `duckdb` to `postgres` automatically invalidates the cache, so cross-engine runs
> never reuse outdated fingerprints.

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

# Incremental models

Incremental models let you **reuse existing data** and only process **new or changed rows** instead of rebuilding a table from scratch on every run. This is essential for larger datasets or frequently running pipelines.

This page explains the **concepts and configuration** of incremental models in FastFlowTransform (FFT) independently of any specific example project.

---

## Why incremental models?

By default, a model is built with a **full refresh**:

* Read all sources
* Recompute all transformations
* Overwrite the target table

For small tables this is fine. For anything medium-sized or larger, this quickly becomes:

* slow,
* expensive (especially on cloud warehouses / Spark),
* and unnecessary if only a small portion of rows changed.

Incremental models solve this by:

1. Reusing existing target data.
2. Processing only **new / changed** rows.
3. Applying an **incremental strategy** (append or merge).

---

## High-level architecture

Incremental behaviour is coordinated between three layers:

1. **Model configuration**

   You declare that a model is incremental and provide hints:

   * Does it append or upsert?
   * What is the **unique key**?
   * Which column(s) indicate freshness (e.g. `updated_at`)?

   This lives in the model’s `config(...)` (SQL) or `meta` (Python) and is validated against a strict schema.

2. **Planner / Core**

   FFT looks at:

   * the model’s incremental config (`incremental={...}`),
   * whether the physical table already exists,
   * CLI flags like `--full-refresh`,

   and decides whether to:

   * run a **full rebuild**, or
   * run an **incremental update** using engine hooks.

3. **Engine executors** (DuckDB, Postgres, Databricks/Spark, …)

   Each engine implements a small incremental API:

   * `exists_relation(relation)`
   * `create_table_as(relation, select_sql)`       – initial full build
   * `full_refresh_table(relation, select_sql)`    – forced rebuild
   * `incremental_insert(relation, select_sql)`    – append-only
   * `incremental_merge(relation, select_sql, unique_key)` – upsert / merge
   * `alter_table_sync_schema(relation, select_sql, mode=...)` – optional schema evolution

   The planner calls these methods – you just configure the model.

---

## Enabling incremental mode

You enable incremental mode **per model** via the model config.

### SQL models

Inside the Jinja `config` block you use a structured `incremental` dictionary:

```sql
{{ config(
    materialized='incremental',
    tags=['example:incremental', 'engine:duckdb'],
    incremental={
        "enabled": true,
        "strategy": "merge",          # or "append", "insert", "full_refresh"
        "unique_key": ["event_id"],
        "updated_at_column": "updated_at"
    }
) }}

select
  event_id,
  updated_at,
  value
from some_source
````

Key points:

* `materialized='incremental'` tells FFT to use the incremental pipeline.
* `incremental.enabled: true` declares that this model supports incremental processing.
* `unique_key` declares one or more columns that uniquely identify a row in the target.
* `strategy` is a hint for how deltas should be applied (append vs merge etc.).
* `updated_at_column` (or `delta_columns`/`updated_at_columns`) tells FFT which column is used for “new vs old” comparisons (usually a timestamp or monotonically increasing surrogate).

There is **no extra `meta={...}` wrapper** anymore – the fields of `config(...)` are validated directly.

### Python engine models

For `@engine_model` functions you pass the same information via the `meta` parameter – but again with **top-level incremental config**, not inside another `meta` key:

```python
from fastflowtransform import engine_model

@engine_model(
    only="duckdb",
    name="fct_events_py_incremental",
    deps=["events_base.ff"],
    tags=["incremental", "engine:duckdb"],
    meta={
        "materialized": "incremental",
        "incremental": {
            "enabled": True,
            "strategy": "merge",
            "unique_key": ["event_id"],
            "updated_at_column": "updated_at",
        },
    },
)
def build(df):
    # Return a frame with event_id, updated_at, value, ...
    return df
```

The **frame you return** (pandas, Spark, etc.) is treated as the *delta dataset* for incremental processing – FFT does not care how you compute it, only about the columns and the meta.

---

## Incremental strategies

The core supports at least two conceptual strategies:

### 1. Append / insert-only (`strategy: "append"` / `"insert"`)

Use this when:

* data is immutable once written, and
* new rows have strictly increasing `updated_at` / timestamp or surrogate key.

Behaviour:

* For the **first run**, FFT calls `create_table_as(relation, SELECT ...)`.
* For **subsequent runs**:

  * Only rows considered “new” are included in the SELECT (using your configured watermark columns).
  * The executor calls `incremental_insert(relation, SELECT ...)` which typically becomes:

    ```sql
    INSERT INTO target_table
    SELECT ...
    ```

Good for:

* log/event style tables
* audit trails
* many ingestion pipelines

### 2. Merge / upsert (`strategy: "merge"`)

Use this when:

* rows may change later,
* you want the target table to always reflect the **latest version** per `unique_key`.

Behaviour:

* For the **first run**, same as full refresh: `create_table_as`.
* For **later runs**:

  * The SELECT (or delta query, see below) produces a *delta* frame with new/updated rows.
  * Executor tries `incremental_merge(relation, select_sql, unique_key)`.

Engine-specific behaviour:

* **Databricks / Spark (Delta)**
  The executor attempts a native Delta MERGE:

  ```sql
  MERGE INTO target AS t
  USING (SELECT ...) AS s
  ON t.key1 = s.key1 AND ...
  WHEN MATCHED THEN UPDATE SET *
  WHEN NOT MATCHED THEN INSERT *
  ```

  If MERGE is not supported (non-Delta table), it falls back to a safe full rebuild.

* **Other engines (DuckDB, Postgres, …)**
  The executor can implement merge using:

  * `INSERT ... ON CONFLICT ... DO UPDATE` (Postgres),
  * a **full-refresh emulation**: build a new version by combining old rows and delta rows and overwrite.

In all cases, the `unique_key` list is used to match rows between existing table and delta frame.

---

## Watermark / delta SQL and default behaviour

To decide **which rows are “new enough”** for an incremental run, FFT uses the configuration you provide (for example `updated_at_column` or `delta_columns`) plus the existing table.

A typical default pattern is:

```sql
where updated_at > (
  select coalesce(max(updated_at), timestamp '1970-01-01 00:00:00')
  from {{ this }}
)
```

The exact SQL will vary by engine, but the core idea is:

* Read the current maximum of your watermark column in the target.
* Select only rows strictly newer than that.

### Overriding the delta logic

If the default “`updated_at > max(updated_at)`” is not enough, you have a few options:

1. **Additional delta columns**

   Use `delta_columns` / `updated_at_columns` in `incremental={...}` to indicate multiple fields that drive change detection (especially for Python incremental).

2. **Inline delta SQL (`delta_sql`)**

   Provide a custom **delta SELECT** that FFT should use on incremental runs:

   ```sql
   {{ config(
       materialized='incremental',
       incremental={
         "enabled": true,
         "strategy": "merge",
         "unique_key": ["event_id"],
         "updated_at_column": "updated_at",
         "delta_sql": "
           with base as (
             select event_id, updated_at, value
             from {{ ref('events_base.ff') }}
           )
           select *
           from base
           where updated_at > (
             select coalesce(max(updated_at), timestamp '1970-01-01 00:00:00')
             from {{ this }}
           )
         "
       }
   ) }}
   ```

3. **External delta config (`delta_config`)**

   Keep the base query in the model, but put the delta SQL into a separate YAML file and reference it via `delta_config: "config/incremental/my_model.delta.yml"`.

In all cases, FFT still delegates the **merge/insert mechanics** to the executor; you only control what qualifies as “delta”.

---

## Full refresh vs incremental

You can always force a full rebuild:

```bash
fft run . --env dev --full-refresh
```

The logic is:

* If `--full-refresh` is set → **ignore incremental** and call `full_refresh_table`.

* Otherwise, if the model has `incremental.enabled` and the target exists:

  * attempt incremental path (`incremental_insert` / `incremental_merge`),

* Otherwise:

  * do initial full build via `create_table_as`.

---

## Schema evolution for incremental models

Real tables evolve. To avoid incremental runs failing when the output schema changes, executors can implement:

```python
alter_table_sync_schema(relation: str, select_sql: str, mode: str = "append_new_columns")
```

Typical behaviour (Spark example):

1. Run the SELECT with `LIMIT 0` to infer the **output schema**.
2. Compare it to the existing table schema.
3. For any **new columns**:

   * issue `ALTER TABLE ... ADD COLUMNS (...)`,
   * map complex types to reasonable SQL types (often defaulting to `STRING` in Spark for safety).

Modes:

* `"append_new_columns"` – only new columns are added; existing columns are left untouched.
* `"sync_all_columns"` – more aggressive sync, may also adjust types (implementation-specific).

For DuckDB/Postgres, the simplest implementation may be a no-op initially; more advanced engines (or future versions) can support automatic `ALTER TABLE` statements.

---

## Storage overrides and Delta Lake integration

Incremental models work with both:

1. **Managed / catalog tables**, and
2. **Storage overrides** via `project.yml` / model config, e.g.:

   ```yaml
   models:
     storage:
       fct_events:
         path: ".local/spark/fct_events"
         format: delta
   ```

The storage layer (`fastflowtransform.storage`) provides helpers like:

* `get_model_storage(name)` – resolve per-model `path`/`format`/`options`
* `spark_write_to_path(spark, identifier, df, storage=..., default_format=...)`

For Spark/Delta:

* Incremental models can be backed by **Delta files** at a fixed path.

* The executor writes the DataFrame to a temporary directory, then atomically renames it into place and wires up:

  ```sql
  CREATE TABLE `db`.`tbl`
  USING DELTA
  LOCATION '/path/to/model'
  ```

* Incremental MERGE (`incremental_merge`) then runs against this Delta table.

This keeps:

* a stable location on disk / in the lake,
* and a proper table in the metastore/catalog.

When the Databricks/Spark executor's `table_format` (or `FF_DBR_TABLE_FORMAT`) resolves to `delta`,
FastFlowTransform automatically pulls in `delta-spark` and configures both
`spark.sql.extensions=io.delta.sql.DeltaSparkSessionExtension` and
`spark.sql.catalog.spark_catalog=org.apache.spark.sql.delta.catalog.DeltaCatalog` (unless you
already provided custom values). Install `delta-spark >= 4.0` and you can seed/run Delta-backed
models without manually adding Spark CLI flags.

---

## Interaction with metadata and DAG selection

After each successful build, executors call:

```python
on_node_built(node, relation, fingerprint)
```

which uses the meta helpers:

* `ensure_meta_table(executor)`
* `upsert_meta(executor, node_name, relation, fingerprint, engine_name)`

The `_ff_meta` table records, for each model and engine:

* the relation name,
* the last fingerprint/hash,
* timestamps, etc.

While this metadata is **not strictly required** for incremental mechanics, it is used for advanced features such as:

* **state-based selection** (`--select state:modified`, etc.),
* change-aware DAG runs.

Incremental models work together with these features: you can, for example, run only models whose source files changed and let the incremental planner update them efficiently.

---

## Best practices & recommendations

* **Always define a `unique_key`** for merge strategies.
  Without a stable key, upserts can behave unpredictably.

* **Use timestamps or monotonically increasing columns** for delta selection.
  Avoid non-deterministic expressions (e.g. `now()` in your model SQL) in incremental filters.

* **Start simple**:

  * Begin with `strategy: "append"` and a single `updated_at_column`.
  * Move to `strategy: "merge"` only when you truly need updates.

* **Test both fresh and incremental runs**:

  * First run with an empty database (initial full build).
  * Then run again with new rows and verify the target grew as expected.
  * Add automated tests that run the same model twice and assert row counts / contents.

* **Use `--full-refresh` when semantics change**:
  If you change the business logic of a model in a way that invalidates old rows, do a full rebuild at least once.



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

## Supported Test Types

The following values are currently supported for `type`:

- `not_null`
- `unique`
- `accepted_values`
- `greater_equal`
- `non_negative_sum`
- `row_count_between`
- `freshness`
- `reconcile_equal`
- `reconcile_ratio_within`
- `reconcile_diff_within`
- `reconcile_coverage`

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

  - type: greater_equal
    table: orders
    column: amount
    threshold: 0

  - type: non_negative_sum
    table: orders
    column: amount

  - type: row_count_between
    table: users_enriched
    min_rows: 1
    max_rows: 100000

  - type: freshness
    table: events
    column: event_ts
    max_delay_minutes: 30

  - type: reconcile_equal
    name: revenue_vs_bookings  # optional label in summaries
    tags: [reconcile]
    left:  { table: fct_revenue,   expr: "sum(amount)" }
    right: { table: fct_bookings, expr: "sum(expected_amount)" }
    abs_tolerance: 5.0
````

Every entry is a single dictionary describing one check. The common keys are:

| Key        | Description                                                              |
| ---------- | ------------------------------------------------------------------------ |
| `type`     | Test kind (see list above).                                              |
| `table`    | Target table for table-level checks or display hint for reconciliations. |
| `column`   | Required for column-scoped checks (`not_null`, `unique`, …).             |
| `severity` | `error` (default) or `warn`.                                             |
| `tags`     | Optional list of selectors for `fft test --select tag:...`.              |
| `name`     | Optional identifier surfaced in summaries (useful for reconciliations).  |

Run all configured checks:

```bash
fft test . --env dev
```

Use `--select tag:<name>` to restrict by tags (e.g. `fft test --select tag:batch`). Tests always execute regardless of cache settings.

Each entry produces a summary line. Failures stop the command unless `severity: warn` is set.

## Table-Level Checks

These checks operate on a single table (optionally filtered with `where:`). Unless noted, they require a `column` argument.

### `not_null`

* **Purpose:** Assert that a column never contains NULLs.
* **Parameters:**

  * `column` *(str, required)*
  * `where` *(str, optional)* — SQL predicate applied before the NULL check.
* **Failure:** Reports the number of NULL rows and shows the underlying SQL.

---

### `unique`

* **Purpose:** Detect duplicates within a column.
* **Parameters:**

  * `column` *(str, required)*
  * `where` *(str, optional)*
* **Failure:** Indicates how many duplicate groups were found (HAVING `count(*) > 1`) and shows a sample query.

---

### `accepted_values`

* **Purpose:** Ensure every non-NULL value is inside an allowed set.
* **Parameters:**

  * `column` *(str, required)*
  * `values` *(list, required)* — permitted literals (strings are quoted automatically).
  * `where` *(str, optional)* — additional filter condition.
* **Behaviour note:** If `values` is omitted or an empty list, the check is treated as a no-op and always passes. The summary still shows the configured test.
* **Failure:** Shows the number of out-of-set values plus up to five sample values.

---

### `greater_equal`

* **Purpose:** Require all values to be greater than or equal to a threshold.
* **Parameters:**

  * `column` *(str, required)*
  * `threshold` *(number, default `0`)*
* **Failure:** Lists how many rows fell below the threshold.

---

### `non_negative_sum`

* **Purpose:** Validate that the sum of a numeric column is not negative.
* **Parameters:**

  * `column` *(str, required)*
* **Failure:** Reports the signed sum when it is negative.

---

### `row_count_between`

* **Purpose:** Guard minimum (and optional maximum) row counts for a table.
* **Parameters:**

  * `min_rows` *(int, default `1`)* — minimum expected number of rows.
  * `max_rows` *(int, optional)* — omit for open-ended upper bounds.
* **Failure:** Indicates the observed row count when it falls outside `[min_rows, max_rows]`.

---

### `freshness`

* **Purpose:** Warn when the latest timestamp is older than an allowed delay.
* **Parameters:**

  * `column` *(str, required)* — timestamp column.
  * `max_delay_minutes` *(int, required)* — permitted staleness in whole minutes.
* **Failure:** Reports the computed lag in minutes. Uses:

  ```sql
  select date_part('epoch', now() - max(column)) / 60.0 as delay_min
  from <table>
  ```

  This is straightforward for DuckDB/Postgres; other engines may need adaptations.

## Cross-Table Reconciliations

Reconciliation checks compare aggregates or keys across two relations. Their configuration accepts dictionaries describing the left/right side expressions or keys. The top-level `table`/`column` fields are used only for display and grouping; the actual queries are defined via the nested dictionaries.

### `reconcile_equal`

* **Purpose:** Compare two scalar expressions with optional tolerances.
* **Parameters:**

  * `left`, `right` *(dict, required)* with keys:

    * `table` *(str, required)*
    * `expr` *(str, required)* — SQL select expression (e.g. `sum(amount)`).
    * `where` *(str, optional)*
  * `abs_tolerance` *(float, optional)* — maximum absolute difference.
  * `rel_tolerance_pct` *(float, optional)* — maximum relative difference in percent.
* **Failure:** Displays both values, absolute and relative differences. If no tolerance is provided, strict equality is enforced (diff must be exactly `0.0`).

---

### `reconcile_ratio_within`

* **Purpose:** Constrain the ratio `left/right` within bounds.
* **Parameters:**

  * `left`, `right` *(dict, required as above)*
  * `min_ratio`, `max_ratio` *(float, required)*
* **Failure:** Shows the computed ratio and expected interval.

---

### `reconcile_diff_within`

* **Purpose:** Limit the absolute difference between two aggregates.
* **Parameters:**

  * `left`, `right` *(dict, required)*
  * `max_abs_diff` *(float, required)*
* **Failure:** Reports the absolute difference when it exceeds `max_abs_diff`.

---

### `reconcile_coverage`

* **Purpose:** Ensure every key present in a source table appears in a target table (anti-join zero).
* **Parameters:**

  * `source` *(dict, required)* — must contain:

    * `table` *(str)* — source table.
    * `key` *(str)* — key column in the source.
  * `target` *(dict, required)* — must contain:

    * `table` *(str)* — target table.
    * `key` *(str)* — key column in the target.
  * `source_where` *(str, optional)* — filter applied to the source.
  * `target_where` *(str, optional)* — filter applied to the target.
* **Failure:** Reports the number of missing keys.

## Severity & Tags

* `severity: error` (default) makes failures stop the test run with exit code 1.
* `severity: warn` records the result but keeps the run successful.
* `tags:` lets you group checks under named tokens (e.g. `batch`, `streaming`). Use `fft test --select tag:batch` to execute a subset.

## CLI Summary Output

Each executed check produces a line in the summary:

```text
✓ not_null          users.email                     (3ms)
✖ accepted_values   events.status    values=['new', 'active']   (warn)
```

Failures include the generated SQL (where available) to simplify debugging. Use `fft test --verbose` for more detail, or `FFT_SQL_DEBUG=1` to log the underlying queries.

## Further Reading

* `docs/YAML_Tests.md` – schema for YAML-defined tests and advanced scenarios.
* `fft test --help` — command-line switches, selectors, and cache options.



<!-- >>> FILE: CLI_Guide.md >>> -->

# CLI Guide

FastFlowTransform’s CLI is the entry point for seeding data, running DAGs, generating docs, syncing metadata, and executing quality tests. This guide summarizes the day-to-day commands and how they fit together. See `src/fastflowtransform/cli.py` for Typer definitions.

## Core Commands

| Command | Purpose |
|---------|---------|
| `fft seed <project> [--env dev]` | Materialize CSV/Parquet seeds into the configured engine. |
| `fft run <project> [--env dev]` | Execute the DAG (obeys cache + parallel flags). |
| `fft dag <project> --html` | Render the DAG graph/site for quick inspection. |
| `fft docgen <project> --out site/docs` | Generate the full documentation bundle (graph + model pages + optional JSON). |
| `fft test <project> [--env dev]` | Run schema/data-quality tests defined in `project.yml` or schema YAML files. |
| `fft utest <project>` | Execute unit tests defined under `tests/unit/*.yml`. |
| `fft sync-db-comments <project>` | Push model/column descriptions into Postgres or Snowflake comments. |

Use `--select` to scope `run`, `dag`, or `test` commands (e.g. `state:modified`, `tag:finance`, `result:error`). Environment overrides rely on the selected profile in `profiles.yml` or the `FF_*` variables.

## HTTP/API Helpers

Python models can make HTTP calls via `fastflowtransform.api.http`. When you need examples, head over to `docs/Api_Models.md` for `get_json`, `get_df`, pagination helpers, caching, and offline modes.

## DAG & Documentation

- Narrow the graph with `fft dag ... --select <pattern>` (for example `state:modified` or `tag:finance`). Combined with `--html` this produces a focused mini-site under `<project>/docs/index.html`.
- Control schema introspection via `--with-schema/--no-schema`. Use `--no-schema` when the executor should avoid fetching column metadata (for example, BigQuery without sufficient permissions).
- `fft docgen` renders the DAG, model pages, and an optional JSON manifest in one command. Append `--open-source` to open `index.html` in your default browser after rendering.

## Sync Database Comments

`fft sync-db-comments <project> --env <env>` pushes model and column descriptions from project YAML or Markdown into database comments. The command currently supports Postgres and Snowflake Snowpark:

- Start with `--dry-run` to review the generated `COMMENT` statements.
- Postgres honors `profiles.yml -> postgres.db_schema` (and any `FF_PG_SCHEMA` override).
- Snowflake reuses the session or connection exposed by the executor.

If no descriptions are found, the command exits without making changes.



<!-- >>> FILE: Auto_Docs.md >>> -->

# Auto-Docs & Lineage

FastFlowTransform can generate a lightweight documentation site (DAG + model detail pages) plus an optional JSON manifest for external tooling.

## Commands

```bash
# Classic
fft dag . --env dev --html

# Convenience wrapper (loads schema + descriptions + lineage, can emit JSON)
fft docgen . --env dev --out site/docs --emit-json site/docs/docs_manifest.json
```

Add `--open-source` if you want the default browser to open the rendered `index.html` immediately.

## Descriptions

Descriptions can be provided in YAML (`project.yml`) and/or Markdown files. Markdown has higher priority.

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

Markdown overrides YAML when present:

```
<project>/docs/models/<model>.md
<project>/docs/columns/<relation>/<column>.md
```

Optional front matter is ignored for now (title/tags may be used later).

## Column Lineage

- SQL models: expressions like `col`, `alias AS out`, `upper(u.email) AS email_upper)` are parsed; `u` must come from a `FROM ... AS u` clause that resolves to a relation. Functions mark lineage as *transformed*.
- Python (pandas) models: simple patterns like `rename`, `out["x"] = df["y"]`, `assign(x=...)` are recognized.
- Override hints in YAML when the heuristic is insufficient:

```yaml
docs:
  models:
    mart_orders_enriched:
      lineage:
        email_upper:
          from: [{ table: users, column: email }]
          transformed: true
```

## JSON Manifest

The optional manifest (via `--emit-json`) includes models, relations, descriptions, columns (with nullable/dtype), and lineage per column—useful for custom doc portals or CI checks.

## Notes

- Schema introspection currently supports DuckDB and Postgres. For other engines, the Columns card may be empty.
- Lineage is optional; when uncertain, entries fall back to “unknown” and never fail doc generation.



<!-- >>> FILE: Logging.md >>> -->

# Logging & Verbosity

FastFlowTransform exposes uniform logging controls across all CLI commands plus a dedicated SQL debug channel for tracing rendered SQL, dependency loading, and auxiliary queries.

## CLI Flags

- `-q` / `--quiet` → only errors (`ERROR`)
- *(default)* → concise warnings (`WARNING`)
- `-v` / `--verbose` → progress/info (`INFO`)
- `-vv` → full debug (`DEBUG`) including SQL debug output

`-vv` automatically flips on the SQL debug channel (same effect as `FFT_SQL_DEBUG=1`).

## SQL Debug Channel

Enable it to inspect Python-model inputs, dependency columns, and helper SQL emitted by data-quality checks:

```bash
# full debug (recommended)
fft run . -vv

# equivalent using the env var (legacy behaviour retained)
FFT_SQL_DEBUG=1 fft run .
```

## Usage Patterns

```bash
fft run . -q     # quiet (errors only)
fft run .        # default (concise)
fft run . -v     # verbose progress (model names, executor info)
fft run . -vv    # full debug + SQL channel
```

## Parallel Logging UX

- Each node emits start/end lines with duration, truncated name, and engine abbreviation (DUCK/PG/BQ/…).
- Output remains line-stable via a thread-safe log queue; per-level summaries trail each run.
- Failures still surface the familiar “error block” per node for quick diagnosis.

**Notes**

- SQL debug output routes through the `fastflowtransform.sql` logger; use `-vv` or `FFT_SQL_DEBUG=1` to reveal it.
- Existing projects do not need changes: the environment variable keeps working even without `-vv`.



<!-- >>> FILE: Unit_Tests.md >>> -->

# Model Unit Tests (`fft utest`)

`fft utest` executes a single model in isolation, loading only the inputs you provide and comparing the result to an expected dataset. It works for SQL and Python models and runs against DuckDB or Postgres by default.

## Cache Modes

`fft utest --cache {off|ro|rw}` (default: `off`)

- `off`: deterministic, never skips.
- `ro`: skip on cache hit; on miss, build but **do not write** cache.
- `rw`: skip on hit; on miss, build **and write** fingerprint.

Notes:

- UTests key the cache with `profile="utest"`.
- Fingerprints include case inputs (CSV content hash / inline rows), so changing inputs invalidates the cache.
- `--reuse-meta` is currently a reserved flag: exposed in the CLI, acts as a no-op today, and will enable future meta-table optimizations.

## Why Use UTests?

- Fast feedback on transformation logic without full DAG runs.
- Small, reproducible fixtures (rows inline or external CSV).
- Engine-agnostic: swap DuckDB/Postgres to spot dialect differences.

## Folder Layout

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

## YAML DSL (with `defaults`)

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

## Input Formats

- `rows`: inline dictionaries per row.
- `csv`: reference a CSV file (relative paths allowed).

Keys under `inputs` are physical relations; use `relation_for('users.ff')` if unsure.

## Expected Output & Comparison

- `relation`: actual table/view name produced by the model (defaults to `relation_for(model)`).
- Ordering: `order_by: [...]` or `any_order: true`.
- Columns: `ignore_columns: [...]`, `subset: true`.
- Numeric tolerance: `approx: true` or `approx: { col: 1e-9, other_col: 0.01 }`
  (numbers can be plain `1e-9` or quoted; they are cast to float).

## Running UTests

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

## Design Notes

- Only the target model runs; supply all upstream relations the model expects.
- `defaults` deep-merge: dicts merge, lists/scalars overwrite.
- Results compare as DataFrames with configurable order, subsets, ignored columns, and numeric tolerances.
- Exit codes: `0` for success, `2` when at least one case fails (compact CSV-style diff is printed).

## CI Example

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

For Postgres, add a service container and run `fft utest . --engine postgres` with `FF_PG_DSN` / `FF_PG_SCHEMA`.



<!-- >>> FILE: Troubleshooting.md >>> -->

# Troubleshooting & Error Codes

Use this checklist when FastFlowTransform commands misbehave. Each item points to the quickest fix plus the relevant CLI options.

## Quick Fixes

- **DuckDB seeds not visible** → ensure `FF_DUCKDB_PATH` (or the profile path) is identical for `seed`, `run`, `dag`, and `test`. If you configure `FF_DUCKDB_SCHEMA` / `FF_DUCKDB_CATALOG`, keep them consistent across commands so unqualified references resolve to the right namespace.
- **Postgres connection refused** → confirm `FF_PG_DSN`, container status (`docker ps`), and that port `5432` is open.
- **BigQuery permissions** → set `GOOGLE_APPLICATION_CREDENTIALS` and match dataset/location to your profile.
- **HTML docs missing** → run `fft dag <project> --html` and open `<project>/docs/index.html`.
- **Unexpected test failures** → inspect rendered SQL in CLI output, refine selection via `--select`, refresh seeds if needed.
- **Dependency table not found in utests** → provide all physical upstream relations in the YAML spec.

## Error Codes

| Type                      | Class/Source              | Exit | Notes                                                   |
|---------------------------|---------------------------|------|---------------------------------------------------------|
| Missing dependency        | `DependencyNotFoundError` | 1    | Per-node list; tips for `ref()` / names                |
| Cycle in DAG              | `ModelCycleError`         | 1    | “Cycle detected among nodes: …”                        |
| Model execution (KeyError)| `cli.py` → formatted block| 1    | Inspect columns, use `relation_for(dep)` as keys       |
| Data quality failures     | `cli test` → summary      | 2    | Totals section prints passed/failed counts             |
| Unknown/unexpected        | generic                   | 99   | Optional trace via `FFT_TRACE=1`                       |

Error types map to the classes documented in `docs/Technical_Overview.md#core-modules` and the CLI source.



<!-- >>> FILE: examples/Basic_Demo.md >>> -->

# Basic Demo Project

The `examples/basic_demo` project shows the smallest end-to-end FastFlowTransform pipeline. It combines one seed, a staging model, and a final mart while staying portable across DuckDB, Postgres, and Databricks Spark.

## Why it exists
- **Start small** – demonstrate the minimum folder structure (`seeds/`, `models/`, `profiles.yml`) needed to run `fft`.
- **Engine parity** – prove that a single project can target multiple engines by swapping profiles.
- **Understand outputs** – show where documentation and manifests land after a run.

Use it as a sandbox before adding your own sources, macros, or Python models.

## Project layout

| Path | Purpose |
|------|---------|
| `seeds/seed_users.csv` | Sample CRM-style user data. `fft seed` materializes it as `crm.users`. |
| `models/staging/users_clean.ff.sql` | Normalizes emails, casts types, and tags the model for all engines. |
| `models/marts/mart_users_by_domain.ff.sql` | Aggregates users per email domain and records the first/last signup dates. |
| `models/engines/*/mart_latest_signup.ff.py` | Engine-specific Python models (pandas for DuckDB/Postgres, PySpark for Databricks) selecting the most recent signup per domain from the staging view. |
| `profiles.yml` | Declares `dev_duckdb`, `dev_postgres`, and `dev_databricks` profiles driven by environment variables. |
| `.env.dev_*` | Template environment files you can `source` per engine. |
| `Makefile` | One command (`make demo ENGINE=…`) to seed, run, document, test, and preview results. |

## Running the demo

1. `cd examples/basic_demo`
2. Choose an engine and export its environment variables:
   ```bash
   set -a; source .env.dev_duckdb; set +a
   # swap to .env.dev_postgres or .env.dev_databricks for other engines
   ```
3. Execute the full flow:
   ```bash
   make demo ENGINE=duckdb
   ```
   The Makefile runs `fft seed`, `fft run`, `fft dag`, `fft test`, and `fft show basic_demo.mart_users_by_domain`. To preview the Python mart, run `make show ENGINE=duckdb SHOW_MODEL=mart_latest_signup` (or swap `ENGINE` as needed).
4. Inspect artifacts:
   - `.fastflowtransform/target/manifest.json` and `run_results.json`
   - `site/dag/index.html` for the rendered model graph
   - CLI output from `fft show` displaying the aggregated mart

The demo also enables baseline data quality checks in `project.yml`. Running `fft test` (or `make test`) verifies that primary keys remain unique/not-null across `seed_users`, `users_clean`, `mart_users_by_domain`, and the Python mart, while ensuring aggregate metrics such as `user_count` never drop below zero and each domain appears only once in `mart_latest_signup`.

## Next steps

- Add more CSVs under `seeds/` and declare them in `sources.yml`.
- Create additional staging models so marts can reuse normalized data.
- Introduce Python models or macros mirroring how the API demo scales up.
- Update `.env.dev_*` with real credentials once you connect to shared databases.



<!-- >>> FILE: examples/Materializations_Demo.md >>> -->

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



<!-- >>> FILE: examples/DQ_Demo.md >>> -->

# Data Quality Demo Project

The **Data Quality Demo** shows how to use **all built-in FFT data quality tests** on a small, understandable model:

* Column checks:

  * `not_null`
  * `unique`
  * `accepted_values`
  * `greater_equal`
  * `non_negative_sum`
  * `row_count_between`
  * `freshness`
* Cross-table reconciliations:

  * `reconcile_equal`
  * `reconcile_ratio_within`
  * `reconcile_diff_within`
  * `reconcile_coverage`

It uses a simple **customers / orders / mart** setup so you can see exactly what each test does and how it fails when something goes wrong.

---

## What this example demonstrates

1. **Basic column checks** on staging tables
   Ensure IDs are present and unique, amounts are non-negative, and status values are valid.

2. **Freshness** on a timestamp column
   Check that the most recent order in your mart is not “too old”, using `last_order_ts`.

3. **Row count sanity checks**
   Guard against empty tables and unexpectedly large row counts.

4. **Cross-table reconciliations** between staging and mart
   Verify that sums and counts match between `orders` and the aggregated `mart_orders_agg`, and that every customer has a corresponding mart row.

5. **Tagged tests and selective execution**
   All tests are tagged (e.g. `example:dq_demo`, `reconcile`) so you can run exactly the subset you care about.

---

## Project layout (example)

```text
examples/dq_demo/
  .env
  .env.dev_duckdb
  .env.dev_postgres
  .env.dev_databricks
  Makefile                  # optional, convenience wrapper around fft commands
  profiles.yml
  project.yml
  sources.yml

  seeds/
    customers.csv
    orders.csv

  models/
    staging/
      customers.ff.sql
      orders.ff.sql
    marts/
      mart_orders_agg.ff.sql
```

### Seeds

* `seeds/customers.csv`
  Simple customer dimension (e.g. `customer_id`, `name`, `status`).

* `seeds/orders.csv`
  Order fact data (e.g. `order_id`, `customer_id`, `amount`, `order_ts` as a string).

### Models

**1. Staging: `customers.ff.sql`**

* Materialized as a table.
* Casts IDs and other fields into proper types.
* Used as the “clean” customer dimension for downstream checks.

**2. Staging: `orders.ff.sql`**

* Materialized as a table.
* Casts fields to proper types so DQ tests work reliably:

  ```sql
  {{ config(
      materialized='table',
      tags=[
          'example:dq_demo',
          'scope:staging',
          'engine:duckdb',
          'engine:postgres',
          'engine:databricks_spark'
      ],
  ) }}

  select
    cast(order_id    as int)        as order_id,
    cast(customer_id as int)        as customer_id,
    cast(amount      as double)     as amount,
    cast(order_ts    as timestamp)  as order_ts
  from {{ source('crm', 'orders') }};
  ```

  This is important for:

  * numeric checks (`greater_equal`, `non_negative_sum`)
  * timestamp-based `freshness` checks

**3. Mart: `mart_orders_agg.ff.sql`**

Aggregates orders per customer and prepares data for reconciliation + freshness:

```sql
{{ config(
    materialized='table',
    tags=[
        'example:dq_demo',
        'scope:mart',
        'engine:duckdb',
        'engine:postgres',
        'engine:databricks_spark'
    ],
) }}

-- Aggregate orders per customer for DQ & reconciliation tests
with base as (
  select
    o.order_id,
    o.customer_id,
    -- Ensure numeric and timestamp types for downstream DQ checks
    cast(o.amount   as double)    as amount,
    cast(o.order_ts as timestamp) as order_ts,
    c.name   as customer_name,
    c.status as customer_status
  from {{ ref('orders.ff') }} o
  join {{ ref('customers.ff') }} c
    on o.customer_id = c.customer_id
)
select
  customer_id,
  customer_name,
  customer_status as status,
  count(*)        as order_count,
  sum(amount)     as total_amount,
  min(order_ts)   as first_order_ts,
  max(order_ts)   as last_order_ts
from base
group by customer_id, customer_name, customer_status;
```

The important columns for DQ tests are:

* `status` → used for `accepted_values`
* `order_count` and `total_amount` → used for numeric and reconciliation tests
* `last_order_ts` → used for `freshness`

---

## Data quality configuration (`project.yml`)

All tests live under `project.yml → tests:`.
This example uses the tag `example:dq_demo` for easy selection.

### Column-level checks

```yaml
tests:
  # 1) IDs must be present and unique
  - type: not_null
    table: customers
    column: customer_id
    tags: [example:dq_demo, batch]

  - type: unique
    table: customers
    column: customer_id
    tags: [example:dq_demo, batch]

  # 2) Order amounts must be >= 0
  - type: greater_equal
    table: orders
    column: amount
    threshold: 0
    tags: [example:dq_demo, batch]

  # 3) Total sum of amounts must not be negative
  - type: non_negative_sum
    table: orders
    column: amount
    tags: [example:dq_demo, batch]

  # 4) Customer status values must be within a known set
  - type: accepted_values
    table: mart_orders_agg
    column: status
    values: ["active", "churned", "prospect"]
    severity: warn         # show as warning, not hard failure
    tags: [example:dq_demo, batch]

  # 5) Row count sanity check on mart
  - type: row_count_between
    table: mart_orders_agg
    min_rows: 1
    max_rows: 100000
    tags: [example:dq_demo, batch]

  # 6) Freshness: last order in the mart must not be "too old"
  - type: freshness
    table: mart_orders_agg
    column: last_order_ts
    max_delay_minutes: 100000000
    tags: [example:dq_demo, batch]
```

### Cross-table reconciliations

```yaml
  # 7) Reconcile total revenue between orders and mart
  - type: reconcile_equal
    name: total_amount_orders_vs_mart
    tags: [example:dq_demo, reconcile]
    left:
      table: orders
      expr: "sum(amount)"
    right:
      table: mart_orders_agg
      expr: "sum(total_amount)"
    abs_tolerance: 0.01

  # 8) Ratio of sums should be ~1 (within tight bounds)
  - type: reconcile_ratio_within
    name: total_amount_ratio
    tags: [example:dq_demo, reconcile]
    left:
      table: orders
      expr: "sum(amount)"
    right:
      table: mart_orders_agg
      expr: "sum(total_amount)"
    min_ratio: 0.999
    max_ratio: 1.001

  # 9) Row count diff between orders and mart should be bounded
  - type: reconcile_diff_within
    name: order_count_diff
    tags: [example:dq_demo, reconcile]
    left:
      table: orders
      expr: "count(*)"
    right:
      table: mart_orders_agg
      expr: "sum(order_count)"
    max_abs_diff: 0

  # 10) Coverage: every customer should appear in the mart
  - type: reconcile_coverage
    name: customers_covered_in_mart
    tags: [example:dq_demo, reconcile]
    source:
      table: customers
      key: "customer_id"
    target:
      table: mart_orders_agg
      key: "customer_id"
```

This set of tests touches **all available test types** and ties directly back to the simple data model.

---

## Running the demo

Assuming you are in the repo root and using DuckDB as a starting point:

### 1. Seed the data

```bash
fft seed examples/dq_demo --env dev_duckdb
```

This reads `seeds/customers.csv` and `seeds/orders.csv` and materializes them as tables referenced by `sources.yml`.

### 2. Run the models

```bash
fft run examples/dq_demo --env dev_duckdb
```

This builds:

* `customers` (staging)
* `orders` (staging)
* `mart_orders_agg` (mart)

### 3. Run all DQ tests

```bash
fft test examples/dq_demo --env dev_duckdb --select tag:example:dq_demo
```

You should see a summary like:

```text
Data Quality Summary
────────────────────
✅ not_null           customers.customer_id
✅ unique             customers.customer_id
✅ greater_equal      orders.amount
✅ non_negative_sum   orders.amount
❕ accepted_values    mart_orders_agg.status
✅ row_count_between  mart_orders_agg
✅ freshness          mart_orders_agg.last_order_ts
✅ reconcile_equal    total_amount_orders_vs_mart
✅ reconcile_ratio_within total_amount_ratio
✅ reconcile_diff_within  order_count_diff
✅ reconcile_coverage customers_covered_in_mart

Totals
──────
✓ passed: 10
! warnings: 1
```

(Exact output will differ, but you’ll see pass/failed/warned checks listed.)

### 4. Run only reconciliation tests

```bash
fft test examples/dq_demo --env dev_duckdb --select tag:reconcile
```

This executes just the cross-table checks, which is handy when you’re iterating on a mart.

---

## Things to experiment with

To understand the tests better, intentionally break the data and re-run `fft test`:

* Set one `customers.customer_id` to `NULL` → watch `not_null` fail.
* Duplicate a `customer_id` → watch `unique` fail.
* Put a negative `amount` in `orders.csv` → `greater_equal` and `non_negative_sum` fail.
* Add a new `status` value (e.g. `"paused"`) → `accepted_values` warns.
* Drop a customer from `mart_orders_agg` manually (or filter it out in SQL) → `reconcile_coverage` fails.
* Change an amount in the mart only → reconciliation tests fail.

This makes it very clear what each test guards against.

---

## Summary

The Data Quality Demo is designed to be:

* **Small and readable** – customers, orders, and a single mart.
* **Complete** – exercises every built-in FFT DQ test type.
* **Practical** – real-world patterns like:

  * typing in staging models,
  * testing freshness on a mart timestamp,
  * reconciling sums and row counts across tables.

Once you’re comfortable with this example, you can copy the patterns into your real project: start with staging-level checks, then layer in reconciliations and freshness on your most important marts.



<!-- >>> FILE: examples/Macros_Demo.md >>> -->

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



<!-- >>> FILE: examples/Cache_Demo.md >>> -->

# 🧠 Cache & Parallelism Demo

This example demonstrates FastFlowTransform’s **build cache**, **fingerprint logic**, **parallel scheduler**, and **HTTP response caching**.
It’s a compact playground to visualize **when nodes are skipped**, **what triggers rebuilds**, and **how caching accelerates iterative runs**.

---

## 🗂 Directory Structure

```text
cache_demo/
  .env.dev_duckdb
  Makefile
  profiles.yml
  project.yml
  sources.yml
  models/
    seeds_consumers/
      stg_users.ff.sql
      stg_orders.ff.sql
    marts/
      mart_user_orders.ff.sql
    python/
      py_constants.ff.py
    http/
      http_users.ff.py
  seeds/
    seed_users.csv
    seed_orders.csv
  README.md
```

---

## ⚙️ Overview

This demo showcases several FastFlowTransform features:

| Feature                    | Demonstrated by                                 |
| -------------------------- | ----------------------------------------------- |
| Level-wise parallelism     | Multiple models running concurrently (`--jobs`) |
| Deterministic fingerprints | Build cache skipping unchanged nodes            |
| Upstream invalidation      | Seed → staging → mart rebuilds                  |
| Environment invalidation   | Any `FF_*` change triggers rebuild              |
| Python model caching       | Fingerprints derived from function source       |
| HTTP response caching      | Persistent API result cache with offline mode   |

---

## ⚡ Quickstart

```bash
cd examples/cache_demo
make cache_first       # builds all nodes, writes cache
make cache_second      # no-op run (everything skipped)
make change_sql        # touch a model -> rebuilds dependent mart
make change_seed       # change seed -> rebuilds staging + mart
make change_env        # set FF_* env -> invalidates cache globally
make change_py         # edit py_constants.ff.py -> rebuilds that model
make run_parallel      # runs entire DAG with 4 workers per level
```

Inspect results:

* `.fastflowtransform/target/run_results.json` – fingerprints, results, timings, HTTP stats
* `site/dag/index.html` – DAG visualization
* `.local/http-cache/` – persisted API responses

---

## 🧩 Model Summary

| Model                     | Kind   | Purpose                     | Notes                                |
| ------------------------- | ------ | --------------------------- | ------------------------------------ |
| `stg_users.ff.sql`        | SQL    | Load & normalize users seed | Rebuilds if seed changes             |
| `stg_orders.ff.sql`       | SQL    | Load orders seed            | Builds as a view                     |
| `mart_user_orders.ff.sql` | SQL    | Join staging tables         | Rebuilds if any staging changes      |
| `py_constants.ff.py`      | Python | Simple constant DataFrame   | Fingerprint based on function source |
| `http_users.ff.py`        | Python | HTTP fetch with cache       | Uses `get_df()` and offline cache    |

---

## 🌐 HTTP Response Cache

The `http_users.ff.py` model demonstrates the built-in HTTP cache:

* **First run:** downloads `https://jsonplaceholder.typicode.com/users`
* **Subsequent runs:** reuse cached responses from `.local/http-cache`
* **Offline mode:** works with `FF_HTTP_OFFLINE=1`

```bash
make http_first        # warms HTTP cache
make http_offline      # reuses cached response, no network access
make http_cache_clear  # deletes cache directory
```

You can inspect HTTP usage in the `run_results.json` file:

```bash
jq -r '.results[] | select(.http!=null)
  | "\(.name): requests=\(.http.requests) cache_hits=\(.http.cache_hits) offline=\(.http.used_offline)"' \
  .fastflowtransform/target/run_results.json
```

---

## ⚙️ Cache Logic Recap

FastFlowTransform caches model fingerprints and skips nodes when:

1. **Fingerprints match** (SQL text, Python source, vars, engine, env, deps).
2. The **physical relation exists** in the database.

Changing *any* of the following invalidates the cache:

* SQL/Jinja content
* Python model code
* `sources.yml`
* `FF_*` environment variables
* Seed file contents
* Engine or profile name

You can control cache behavior via CLI:

```bash
--cache=off   # always build
--cache=rw    # default; skip on match; write cache
--cache=ro    # read-only; skip on hit, build on miss
--cache=wo    # always build, always write
```

---

## 🧮 Parallel Scheduler

FastFlowTransform executes models **level-wise**:

* Each level contains nodes whose dependencies are fully satisfied.
* Up to `--jobs` nodes per level run concurrently.
* Logs are serialized for clean output.

Example:

```bash
fft run . --env dev_duckdb --jobs 4
```

---

## 🧪 Example Experiments

| Scenario                  | Command                                | Expected behavior               |
| ------------------------- | -------------------------------------- | ------------------------------- |
| First full run            | `make cache_first`                     | All models build, cache written |
| No-op run                 | `make cache_second`                    | All skipped (no rebuilds)       |
| Modify SQL                | `make change_sql`                      | Downstream mart rebuilds        |
| Add seed row              | `make change_seed`                     | Staging + mart rebuild          |
| Change env                | `make change_env`                      | All nodes rebuild               |
| Edit Python constant      | `make change_py`                       | Only that Python model rebuilds |
| Warm & offline HTTP cache | `make http_first && make http_offline` | HTTP cache reused, no network   |

---

## 🧩 DAG Example

After the first run, generate the DAG visualization:

```bash
make dag
open site/dag/index.html
```

You’ll see:

```
seed_users   → stg_users.ff
seed_orders  → stg_orders.ff
(stg_users + stg_orders) → mart_user_orders.ff
py_constants
http_users
```

* `py_constants` runs independently (parallel)
* `mart_user_orders.ff` depends on both staging nodes

---

## 🧰 Tips

* **Inspect fingerprints:** stored in `.fastflowtransform/target/manifest.json`
* **Audit table:** `_ff_meta` table in the engine stores build metadata
* **Clear cache:** delete `.fastflowtransform/` or use `make clean`
* **Parallel debugging:** use `--keep-going` to continue unaffected levels

---

## ✅ Takeaways

* FFT’s build cache uses stable fingerprints to skip unchanged nodes.
* Fingerprints propagate downstream, ensuring correctness.
* The HTTP cache supports deterministic, offline API pipelines.
* Parallel execution accelerates runs without breaking dependencies.

Together, these features make iterative development **fast, reliable, and reproducible**.



<!-- >>> FILE: examples/Incremental_Demo.md >>> -->

````markdown
# Incremental & Delta Demo

This example project shows how to use **incremental models** and **Delta-style merges** in FastFlowTransform across DuckDB, Postgres and Databricks Spark.

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
  .env.dev_databricks
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
     * Databricks Spark: `MERGE INTO` for Delta where available, with a fallback full-refresh strategy

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

## Delta variant (Databricks / Spark)

In addition to the “regular” incremental models, the demo also includes a **Delta Lake variant**
that shows how to:

- route a model to **Delta tables** via `project.yml`  
- reuse the same incremental pattern, but with a **Delta-backed** table on Databricks/Spark  
- keep Parquet and Delta models side-by-side in the same project

This is optional and only relevant for the `databricks_spark` engine.

---

### Storage configuration for the Delta model

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

Packen würde ich den Hinweis direkt an die Stelle, wo du schon beschreibst, wie man die Demo auf Databricks startet – also deine aktuelle Sektion:

````markdown
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

---

## How to link this page into your docs

### 1. MkDocs (`mkdocs.yml`)

If you use MkDocs, place this file under e.g.:

```text
docs/examples/incremental_demo.md
```

and add it to your `mkdocs.yml` nav:

```yaml
nav:
  - Overview: index.md
  - Examples:
      - API demo: examples/api_demo.md
      - Incremental & Delta demo: examples/incremental_demo.md
```

### 2. Sphinx (`index.rst` + Markdown)

If you use Sphinx with MyST or Markdown support, put the file under:

```text
docs/examples/incremental_demo.md
```

and reference it from your main `index.rst`:

```rst
Welcome to FastFlowTransform's documentation!
=============================================

.. toctree::
   :maxdepth: 2

   overview
   examples/api_demo
   examples/incremental_demo
```

(Adjust paths to match your actual layout.)

### 3. Top-level `index.md` (Markdown-only docs)

If your docs use a pure Markdown index, just add a link:

```markdown
## Examples

- [API demo](examples/api_demo.md)
- [Incremental & Delta demo](examples/incremental_demo.md)
```

This way, the incremental demo appears alongside your existing API demo and other examples in your global documentation navigation.

```
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
  Optionally set `FF_DUCKDB_SCHEMA` (default schema for models/seeds) and `FF_DUCKDB_CATALOG` (catalog alias) if you need to isolate namespaces.
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

--8<-- "License.md"

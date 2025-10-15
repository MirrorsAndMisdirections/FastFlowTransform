# 🧭 FlowForge – Technische Entwicklerdokumentation (v0.1)

> Stand: letzte Änderungen aus deinem Kontextdump. Dieses Dokument bündelt Projektstruktur, Architektur, Kern-APIs, Fehlermeldungen, CLI, Beispiele und Roadmap in einem druck-/git-freundlichen Markdown.
>
> Projekt: **FlowForge** — SQL & Python Data Modeling (Batch + Streaming), DAG, CLI, Auto-Docs, DQ-Tests.

---

## Inhaltsverzeichnis

- [1) Projektstruktur](#1-projektstruktur)
- [2) Architekturüberblick](#2-architekturüberblick)
- [3) Kernmodule](#3-kernmodule)
  - [3.1 `core.py`](#31-corepy)
  - [3.2 `dag.py`](#32-dagpy)
  - [3.3 `errors.py`](#33-errorspy)
  - [3.4 Executors](#34-executors)
  - [3.5 `validation.py`](#35-validationpy)
  - [3.6 `testing.py`](#36-testingpy)
  - [3.7 `docs.py` + Templates](#37-docspy--templates)
  - [3.8 `seeding.py`](#38-seedingpy)
- [4) CLI (`cli.py`)](#4-cli-clipy)
- [5) Settings/Profiles (`settings.py`)](#5-settingsprofiles-settingspy)
- [6) Streaming](#6-streaming)
- [7) Beispielmodelle](#7-beispielmodelle)
- [8) Seeds](#8-seeds)
- [9) Makefile](#9-makefile)
- [10) CLI-Beispiele](#10-cli-beispiele)
- [11) Fehlerbilder & Exit-Codes](#11-fehlerbilder--exit-codes)
- [12) Profile & ENV (Kurz)](#12-profile--env-kurz)
- [13) Roadmap](#13-roadmap)
- [14) Mini End-to-End Beispiel (Python API)](#14-mini-end-to-end-beispiel-python-api)

---

## 1) Projektstruktur

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
│       ├── decorators.py                 # optional, falls nicht in core.py
│       ├── docs/
│       │   └── templates/
│       │       ├── index.html.j2
│       │       └── model.html.j2
│       ├── executors/
│       │   ├── __init__.py
│       │   ├── base.py
│       │   ├── duckdb_exec.py
│       │   ├── postgres_exec.py
│       │   ├── bigquery_exec.py          # pandas + BigQuery Client
│       │   ├── bigquery_bf_exec.py       # BigQuery DataFrames (bigframes)
│       │   ├── databricks_spark_exec.py  # PySpark (ohne pandas)
│       │   └── snowflake_snowpark_exec.py# Snowpark (ohne pandas)
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
│   │   │   └── marts_daily.ff.sql
│   │   ├── seeds/
│   │   │   ├── seed_users.csv
│   │   │   └── seed_orders.csv
│   │   ├── sources.yml
│   │   ├── project.yml
│   │   ├── Makefile
│   │   └── .local/demo.duckdb  (nach make seed/run)
│   └── postgres/                # analog, falls benötigt
│
├── tests/
│   ├── conftest.py
│   ├── duckdb/ …                # End-to-End + Unit
│   ├── postgres/ …
│   └── streaming/ …
└── README.md
```

---

## 2) Architekturüberblick

```
CLI (Typer)
│
├── Registry (core.py)
│   ├── Modelle entdecken (*.ff.sql / *.ff.py)
│   ├── Python-Modelle laden (Decorator)
│   ├── Abhängigkeiten parsen/validieren
│   └── Jinja Environment + sources.yml
│
├── DAG (dag.py)
│   ├── topo_sort (Kahn, deterministisch)
│   └── mermaid() (styled + IDs sicher)
│
├── Executors (executors/*)
│   ├── BaseExecutor (SQL-Render, Dep-Loading, Materialisierung, Requires-Check)
│   ├── DuckExecutor (DuckDB)
│   ├── PostgresExecutor (SQLAlchemy, Shims)
│   ├── BigQueryExecutor (pandas)
│   ├── BigQueryBFExecutor (BigQuery DataFrames / bigframes)
│   ├── DatabricksSparkExecutor (PySpark, ohne pandas)
│   └── SnowflakeSnowparkExecutor (Snowpark, ohne pandas)
│
├── Testing (testing.py)
│   ├── generisches _exec / _scalar
│   └── Checks: not_null, unique, row_count_between, greater_equal, non_negative_sum, freshness
│
├── Seeding (seeding.py)
│   └── Seeds laden (CSV/Parquet/SQL) → Engine-agnostisch
│
├── Docs (docs.py + templates/)
│   ├── Mermaid + Übersichtstabelle (index.html)
│   └── Model-Detailseiten (model.html)
│
├── Settings/Profiles (settings.py)
│   └── Pydantic v2 Discriminated Union + ENV Overrides
│
└── Streaming (streaming/*)
    ├── FileTailSource
    └── StreamSessionizer
```

---

## 3) Kernmodule

### 3.1 `core.py`

Wichtigste Datenstrukturen & Projekt-Ladeprozess.

```python
@dataclass
class Node:
    name: str                # logischer Name (Dateistamm oder @model(name=...))
    kind: str                # "sql" | "python"
    path: Path
    deps: List[str] = field(default_factory=list)

class Registry:
    def load_project(self, project_dir: Path) -> None: ...
    def _register_node(self, node: Node) -> None: ...
    def _load_py_module(self, path: Path) -> types.ModuleType: ...
    def _scan_sql_deps(self, path: Path) -> List[str]: ...
```

**Helpers & Decorator:**

```python
def relation_for(node_name: str) -> str: ...
def ref(name: str) -> str: ...
def source(source_name: str, table_name: str) -> str: ...

def model(name=None, deps=None, requires=None) -> Callable[[Callable[..., Any]], Callable[..., Any]]: ...
```

**Python-Modelle (Beispiel):**

```python
@model(name="users_enriched", deps=["users.ff"], requires={"users": {"id","email"}})
def enrich(df: pd.DataFrame) -> pd.DataFrame: ...
```

---

### 3.2 `dag.py`

Deterministische Toposort + Mermaid-Export.

```python
def topo_sort(nodes: Dict[str, Node]) -> List[str]: ...
def mermaid(nodes: Dict[str, Node]) -> str: ...
```

---

### 3.3 `errors.py`

Hauptfehlertypen mit hilfreichen Meldungen.

```python
class FlowForgeError(Exception): ...
class ModuleLoadError(FlowForgeError): ...
class DependencyNotFoundError(FlowForgeError): ...
class ModelCycleError(FlowForgeError): ...
class TestFailureError(FlowForgeError): ...
```

---

### 3.4 Executors

Gemeinsame Logik (`BaseExecutor`) + Engines.

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

- `run_sql(node, env)` rendert Jinja (`ref/source`), führt SQL aus.
- `_read_relation` liest Tabelle als `DataFrame`; hilfreiche Fehlermeldungen bei fehlender Dep.
- `_materialize_relation` schreibt `DataFrame` als Tabelle (`create or replace table …`).

**Postgres (`postgres_exec.py`)**

- `_SAConnShim` (kompatibel zu `testing._exec`).
- `run_sql` rendert SQL und übersetzt `CREATE OR REPLACE TABLE` → `DROP + CREATE AS`.
- `_read_relation` liest via pandas, mit Schema-Handling und klaren Hinweisen.
- `_materialize_relation` schreibt via `to_sql(if_exists="replace")`.

**BigQuery / BigQuery DataFrames / Spark / Snowpark**

- identische Signaturen; IO sind jeweilige Native-DFs (ohne pandas bei Spark/Snowpark).

---

### 3.5 `validation.py`

Required-Columns-Checks für Python-Modelle (Single- & Multi-Dep).

```python
class RequiredColumnsError(ValueError): ...
def validate_required_columns(node_name: str, inputs: Any, requires: dict[str, set[str]]): ...
```

---

### 3.6 `testing.py`

Minimal DQ-Framework (engine-agnostisch via `_exec`).

**Checks:** `not_null`, `unique`, `greater_equal`, `non_negative_sum`, `row_count_between`, `freshness`

```python
class TestFailure(Exception): ...
def _exec(con: Any, sql: Any): ...
def _scalar(con: Any, sql: Any): ...
```

---

### 3.7 `docs.py` + Templates

- `render_site(out_dir, nodes)` erzeugt `index.html` + `model.html` je Modell.
- Templates (`docs/templates/`) enthalten Dark-Mode, Filter, Copy-Buttons, Legend.
- Nutzt `dag.mermaid(nodes)` für den Graphen.

---

### 3.8 `seeding.py`

Engine-agnostisches Laden von Seeds (CSV/Parquet/SQL).

```python
def seed_project(project_dir: Path, executor, schema: Optional[str] = None) -> int: ...
```

---

## 4) CLI (`cli.py`)

**Commands:**

- `flowforge run <project> [--env dev] [--engine ...]`
- `flowforge dag <project> [--env dev] [--html]`
- `flowforge test <project> [--env dev] [--select batch|streaming]`
- `flowforge seed <project> [--env dev]`
- `flowforge --version`

**Kernelemente:**

```python
def _load_project_and_env(project_arg) -> tuple[Path, Environment]: ...
def _resolve_profile(env_name, engine, proj) -> tuple[EnvSettings, Profile]: ...
def _get_test_con(executor: Any) -> Any: ...
```

**Test-Summary (Exit 2 bei Fehlern):**

```
Data Quality Summary
────────────────────
✅ not_null           users.email                              (3ms)
❌ unique             users.id                                 (2ms)
   ↳ users.id has 1 duplicates

Totals
──────
✓ passed: 1
✗ failed: 1
```

---

## 5) Settings/Profiles (`settings.py`)

**Pydantic v2 Discriminated Union** (`engine` als Discriminator) + ENV-Overrides.

Profile-Typen:
- `DuckDBProfile(engine="duckdb", duckdb: {path})`
- `PostgresProfile(engine="postgres", postgres: {dsn, db_schema})`
- `BigQueryProfile(engine="bigquery", bigquery: {project?, dataset, location?, use_bigframes?})`
- `DatabricksSparkProfile(engine="databricks_spark", ...)`
- `SnowflakeSnowparkProfile(engine="snowflake_snowpark", ...)`

Resolver-Idee:

```python
def resolve_profile(project_dir: Path, env_name: str, env: EnvSettings) -> Profile: ...
```

---

## 6) Streaming

**`streaming/sessionizer.py`**

- Normalisiert Events (JSONL / Batch-DF) und schreibt `fct_sessions_streaming`.
- `process_batch(df)` aggregiert Sessions (Start/Ende, Pageviews, Revenue).

**Smoke-Test (DuckDB):**

```python
def test_stream_sessionizer_produces_sessions(): ...
```

---

## 7) Beispielmodelle

```sql
-- models/users.ff.sql
create or replace table users as
select id, email
from {{ source('crm','users') }};
```

```python
# models/users_enriched.ff.py
@model(name="users_enriched", deps=["users.ff"], requires={"users": {"id","email"}})
def enrich(df: pd.DataFrame) -> pd.DataFrame: ...
```

```sql
-- models/orders.ff.sql
create or replace table orders as
select order_id, user_id, amount
from {{ source('erp','orders') }};
```

```python
# models/mart_orders_enriched.ff.py
@model(
    name="mart_orders_enriched",
    deps=["orders.ff", "users_enriched"],
    requires={
      "orders": {"order_id","user_id","amount"},
      "users_enriched": {"id","email","is_gmail"}
    }
)
def orders_enrich(dfs: dict[str, pd.DataFrame]) -> pd.DataFrame: ...
```

```sql
-- models/marts_daily.ff.sql
create or replace table mart_users as
select id, email, is_gmail
from {{ ref('users_enriched') }};
```

**`sources.yml`**

```yaml
crm:
  users:
    identifier: seed_users
erp:
  orders:
    identifier: seed_orders
```

**`project.yml` (DQ-Tests)**

```yaml
tests:
  - type: not_null
    table: users
    column: email
    tags: [batch]

  - type: unique
    table: users
    column: id
    tags: [batch]

  - type: row_count_between
    table: orders
    min: 1
    tags: [batch]
```

---

## 8) Seeds

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

---

## 9) Makefile

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

---

## 10) CLI-Beispiele

```bash
# seeden
flowforge seed examples/simple_duckdb --env dev

# ausführen
flowforge run examples/simple_duckdb --env dev

# DAG (Mermaid-Datei)
flowforge dag examples/simple_duckdb --env dev

# DAG (HTML)
flowforge dag examples/simple_duckdb --env dev --html

# Datenqualität
flowforge test examples/simple_duckdb --env dev --select batch
```

---

## 11) Fehlerbilder & Exit-Codes

| Typ                         | Klasse/Quelle             | Exit | Hinweis                                           |
|----------------------------|---------------------------|------|---------------------------------------------------|
| Fehlende Dependency         | `DependencyNotFoundError` | 1    | Liste pro Knoten; Tipps zu `ref()` / Namen        |
| Zyklus im DAG               | `ModelCycleError`         | 1    | „Cycle detected among nodes: …“                   |
| Modellausführung (KeyError) | `cli.py` → hübscher Block | 1    | Spalten prüfen, `relation_for(dep)` als Keys      |
| DQ-Fehlschläge              | `cli test` → Summary      | 2    | „Totals … passed/failed“, je Fehler eigene Zeile  |
| Unbekannt/Unerwartet        | generisch                 | 99   | Trace via `FLOWFORGE_TRACE=1` optional            |

---

## 12) Profile & ENV (Kurz)

**`profiles.yml` Beispiel:**

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

**ENV-Overrides (Beispiele):**

`FF_ENGINE`, `FF_DUCKDB_PATH`, `FF_PG_DSN`, `FF_PG_SCHEMA`, `FF_BQ_DATASET`, `FF_BQ_LOCATION`, `FF_BQ_USE_BIGFRAMES=1`

---

## 13) Roadmap

| Version | Inhalt                                            |
|---------|---------------------------------------------------|
| 0.2     | `config(materialized=…)`, Jinja-Macros, Variablen |
| 0.3     | Parallel Execution, Cache                         |
| 0.4     | Inkrementelle Modelle                             |
| 0.5     | Streaming-Connectoren (Kafka, S3)                 |
| 1.0     | Stabile API, Plugin-SDK                           |

> Siehe ergänzend: Feature-Pyramide & Roadmap-Phasen (OSS/SaaS) im separaten Dokument.

---

## 14) Mini End-to-End Beispiel (Python API)

```python
from pathlib import Path
from jinja2 import Environment, FileSystemLoader
from flowforge.core import REGISTRY
from flowforge.dag import topo_sort
from flowforge.executors.duckdb_exec import DuckExecutor

proj = Path("examples/simple_duckdb").resolve()
REGISTRY.load_project(proj)
env = REGISTRY.env  # jinja env vom Registry-Load

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

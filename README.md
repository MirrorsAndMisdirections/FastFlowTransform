# FlowForge (PoC 0.3)

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
- [Documentation](#documentation)
- [Contributing](#contributing)
- [License](#license)

---

## Overview

FlowForge combines SQL and Python models in a lightweight DAG engine. A project is simply a directory with models, optional seeds, and configuration. The CLI renders SQL, runs Python models, materialises results, generates HTML documentation, and executes data-quality checks against multiple execution backends.

> ℹ️ **Project layout & CLI overview**
> Curious about the full folder structure, Makefile targets, or example models? See the *Project Layout* and related sections in the [User Guide](docs/Technical_Overview.md#project-layout).

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

> 📚 **Mehr lesen … CLI-Details**
> Für Flag-Referenzen, Automatisierung und Hintergründe siehe [`docs/Technical_Overview.md`](docs/Technical_Overview.md#cli-flows).

Run the end-to-end DuckDB demo (seed → run → docs → tests) in under a minute:

```bash
make demo
```

The target project lives in `examples/simple_duckdb`. After the demo finishes you'll find the rendered DAG at `examples/simple_duckdb/site/dag/index.html`. Open it via:

```bash
open examples/simple_duckdb/site/dag/index.html    # macOS
xdg-open examples/simple_duckdb/site/dag/index.html  # Linux
```

If you prefer manual control:

```bash
flowforge seed examples/simple_duckdb --env dev
flowforge run  examples/simple_duckdb --env dev
flowforge dag  examples/simple_duckdb --env dev --html
flowforge test examples/simple_duckdb --env dev --select batch
```

---

> For a deep dive into the v0.3 features, see **[Parallelism & Cache](docs/Cache_and_Parallelism.md)**.

## Parallelism & Cache (v0.3)

FlowForge 0.3 adds a level-wise parallel scheduler and an opt-in build cache.

### Parallel execution
- DAG is split into **levels** (all nodes with the same maximum distance from sources).
- Within a level, up to `--jobs` nodes run **in parallel**. Dependencies are never violated.
- `--keep-going`: tasks already started in a level run to completion, but **subsequent levels won’t start** if any task in the current level fails.

**Examples**
```bash
# run with 4 workers per level
flowforge run examples/simple_duckdb --env dev --jobs 4

# keep tasks in the current level running even if one fails
flowforge run examples/simple_duckdb --env dev --jobs 4 --keep-going
```

### Cache modes
The cache decides whether a node can be **skipped** when nothing relevant changed.

```
--cache=off  # always build
--cache=rw   # default: skip on match; write cache after build
--cache=ro   # skip on match; build on miss, but don't write cache
--cache=wo   # always build and write cache
--rebuild <glob>  # ignore cache for selected nodes
--no-cache       # alias for --cache=off
```

**When is a node skipped?**
FlowForge computes a **fingerprint** from:
- SQL/Python source (rendered SQL or function source)
- environment context (engine, profile name, selected `FF_*` env vars, normalized `sources.yml`)
- **dependency fingerprints** (change upstream ⇒ downstream fingerprint changes)
The node is skipped if the fingerprint matches the on-disk cache **and** the physical relation exists.

**Examples**
```bash
# first run (build + cache write)
flowforge run . --env dev --cache=rw

# second run (no-op if nothing changed)
flowforge run . --env dev --cache=rw

# force rebuild of a specific model
flowforge run . --env dev --cache=rw --rebuild marts_daily.ff

# diagnose a surprising skip: change an FF_* env var to invalidate fingerprints
FF_DEMO_TOGGLE=1 flowforge run . --env dev --cache=rw
```

**Troubleshooting**
- *“Why did it skip?”* → Compare your last changes: SQL/Python code, `sources.yml`, `FF_*` env vars, profile/engine. Any change alters the fingerprint.
- *“Relation missing but cache says skip”* → FlowForge also checks relation existence; if it was dropped externally, it will **rebuild**.
- *“Parallel tasks interleave logs”* → Logs are serialized via an internal queue to keep lines readable; use `-v`/`-vv` for more detail.

---

## Selective runs

Use patterns to run only a subgraph.

- `--select <pattern>`: builds only targets that match **and their dependencies**.
- `--exclude <pattern>`: excludes matching targets from the build (deps remain if still required).

Examples:
  flowforge run . --select marts_daily.ff
  flowforge run . --exclude 'mart_*'

---

## Rebuild controls

- `--rebuild`              → rebuild **all selected** nodes (ignore cache).
- `--rebuild-only NAME …`  → rebuild only the specified nodes (ignore cache).

These flags compose with `--select/--exclude`.

Examples:
  # Rebuild everything that matches --select
  flowforge run . --select marts_daily.ff --rebuild

  # Rebuild only a specific node
  flowforge run . --rebuild-only marts_daily.ff

---

## Documentation

- **Documentation hub:** choose your path (operators vs contributors) — see [`docs/index.md`](docs/index.md).
- **User & operator guide:** project layout, CLI usage, troubleshooting tips — see [`docs/Technical_Overview.md`](docs/Technical_Overview.md).
- **Modeling reference:** configuration, Jinja helpers, macros — see [`docs/Config_and_Macros.md`](docs/Config_and_Macros.md).
- **Examples:** runnable demo projects live under `examples/`; each README covers engine-specific setup.

---

## Contributing

Issues and pull requests are welcome! Please read [`Contributing.md`](./Contributing.md) for guidelines, development setup, and testing instructions. Sharing minimal reproduction steps plus `flowforge --version` output greatly speeds up reviews.

---

## License

FlowForge is licensed under the [Apache License 2.0](./License).

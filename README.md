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

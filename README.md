# FastFlowTransform

[![CI](https://github.com/MirrorsAndMisdirections/FastFlowTransform/actions/workflows/ci.yml/badge.svg)](https://github.com/MirrorsAndMisdirections/FastFlowTransform/actions/workflows/ci.yml)
[![PyPI version](https://img.shields.io/pypi/v/fastflowtransform.svg)](https://pypi.org/project/fastflowtransform/)

FastFlowTransform (FFT) is a SQL + Python data modeling engine with a deterministic DAG, level-wise parallelism, optional caching, incremental builds, auto-docs, and built-in data-quality tests. Projects are plain directories containing models, seeds, and YAML config; the `fft` CLI handles compilation, execution, docs, and validation across multiple execution engines.

## Highlights
- SQL or Python models (`*.ff.sql` / `*.ff.py`) wired with `ref()` / `source()` / `deps=[...]`.
- Executors for DuckDB, Postgres, BigQuery (pandas + BigFrames), Databricks/Spark, and Snowflake Snowpark.
- Level-wise parallel scheduler with cache fingerprints, rebuild flags, and state/result selectors.
- Incremental and materialized models with engine-specific merge/append hooks.
- Tests everywhere: schema/YAML checks, reconciliation rules, and fast model unit tests (`fft utest`).
- Docs on demand: `fft dag --html` and `fft docgen` generate a browsable site plus JSON artifacts; optional `sync-db-comments` to push descriptions to Postgres/Snowflake.
- HTTP helpers for Python models (`fastflowtransform.api.http`) and Jinja macros/config for templating.

## Requirements
- Python 3.12+
- Engine extras installed only as needed (e.g. BigQuery, Snowflake, Spark/Delta, Postgres drivers). The core DuckDB path works out of the box.

## Install & Quickstart
- Pick the engine extras you need (combine as `pkg[a,b]`):
  - DuckDB/core: `pip install fastflowtransform`
  - Postgres: `pip install fastflowtransform[postgres]`
  - BigQuery (pandas): `pip install fastflowtransform[bigquery]`
  - BigQuery (BigFrames): `pip install fastflowtransform[bigquery_bf]`
  - Databricks/Spark + Delta: `pip install fastflowtransform[spark]`
  - Snowflake Snowpark: `pip install fastflowtransform[snowflake]`
  - Everything: `pip install fastflowtransform[full]`
- Installation and first run: see `docs/Quickstart.md` (venv + editable install, DuckDB demo, and init walkthrough).
- CLI usage and flags: see `docs/CLI_Guide.md`.
- Makefile shortcut: `make demo` runs the simple DuckDB example end-to-end and opens the DAG (`examples/simple_duckdb`).

## Docs & examples
- Docs hub: `docs/index.md` or https://fastflowtransform.com.
- Operational guide & architecture: `docs/Technical_Overview.md`.
- Modeling reference & macros: `docs/Config_and_Macros.md`.
- Parallelism, cache, and state selection: `docs/Cache_and_Parallelism.md`, `docs/State_Selection.md`.
- Incremental models: `docs/Incremental.md`.
- Data-quality + YAML tests: `docs/Data_Quality_Tests.md`, `docs/YAML_Tests.md`, `docs/Unit_Tests.md`.
- CLI details and troubleshooting: `docs/CLI_Guide.md`, `docs/Troubleshooting.md`.
- Runnable demos live under `examples/` (basic, materializations, incremental, DQ, macros, cache, env matrix, API, events).

## Contributing
Issues and PRs are welcome. See `Contributing.md` for development setup, testing (`make demo`, `uv run pytest`, `fft utest`), and code-style guidelines.

## License
Apache 2.0 — see `License.md`.

# FastFlowTransform Documentation Hub

FastFlowTransform (FFT) is a SQL + Python data modeling engine with a deterministic DAG, parallel executor, optional caching, incremental builds, auto-generated docs, snapshots, and built-in data-quality tests. The `fft` CLI orchestrates compilation, execution, docs, validation, and history tracking across DuckDB, Postgres, BigQuery (pandas + BigFrames), Databricks/Spark, and Snowflake Snowpark.

Use this page as the front door into the docs: start with the orientation section, then jump to the guide that matches the task you have at hand.

---

## Table of Contents

- [Quick Orientation](#quick-orientation)
- [Build & Run Projects](#build-run-projects)
- [Modeling & Configuration](#modeling-configuration)
- [Execution & State Management](#execution-state-management)
- [Testing & Data Quality](#testing-data-quality)
- [Docs, Debugging & Operations](#docs-debugging-operations)
- [Examples & Tutorials](#examples-tutorials)
- [Reference & Contribution](#reference-contribution)
- [Need Help?](#need-help)

---

## Quick Orientation

- **New to FFT?** Read the [Quickstart](Quickstart.md) for installation (venv + editable install), seeding, and the first `fft run`.
- **Want the bigger picture?** The [Technical Overview](Technical_Overview.md) explains the project layout, DAG, scheduler, registry, executors, and the roadmap snapshot.
- **Learning the CLI surface area?** Browse the [CLI Guide](CLI_Guide.md) for command groups such as `fft run`, `fft snapshot run`, `fft dag`, `fft docgen`, `fft test`, and `fft utest`.

---

## Build & Run Projects

- **Project layout & CLI workflow:** Pair the “Project Layout” chapter of the [Technical Overview](Technical_Overview.md#project-layout) with the [CLI Guide](CLI_Guide.md) to understand how `fft run`, `fft test`, and `fft dag` fit together.
- **Profiles & environments:** [Profiles & Environments](Profiles.md) covers executor profiles, environment overrides, credential handling, and engine-specific flags.
- **Runtimes & observability flags:** [Logging & Verbosity](Logging.md) explains log levels, JSON logs, progress indicators, and metrics toggles during `fft run`.
- **Local runtimes & engines:** [Local Engine Setup](examples/Local_Engine_Setup.md) walks through DuckDB, Postgres, Spark/Delta, BigQuery, and Snowflake Snowpark bootstrapping for the demos.
- **CI-friendly workflows:** [CI Checks & Change-Aware Runs](CI_Check.md) introduces `fft ci-check` and `fft run --changed-since` for structural validation and diff-aware pipelines.

---

## Modeling & Configuration

- **SQL + Python authoring model:** [API & Models](Api_Models.md) documents the Python node decorators, HTTP helper (`fastflowtransform.api.http`), and how `ref()` / `source()` bindings work in both SQL and Python models.
- **Templates, macros, and config keys:** [Configuration & Macros](Config_and_Macros.md) lists the `config(...)` options, reusable macros, helper functions, and naming rules for `.ff.sql` / `.ff.py`.
- **Project-level metadata:** [Project Configuration](Project_Config.md) describes `project.yml`, default materializations, tags, exposures, docs strings, and the `models/` hierarchy.
- **Sources & seeds:** [Sources](Sources.md) shows how to register upstream tables/files, snapshots of raw data, and how state tracking interacts with sources.

---

## Execution & State Management

- **Parallelism, caching & rebuilds:** [Cache & Parallelism](Cache_and_Parallelism.md) dives into the level-wise scheduler, fingerprint cache, and `--rebuild` / `--no-cache` behaviors.
- **Incremental models:** [Incremental Processing](Incremental.md) explains merge vs append strategies, cleanup rules, and engine-specific hooks.
- **Snapshots / history tables:** [Snapshots](Snapshots.md) documents the `materialized='snapshot'` config, timestamp vs check strategies, and the dedicated `fft snapshot run . --env <profile>` entrypoint.
- **Selective runs:** [State Selection](State_Selection.md) covers `--selector`, `--select`, `--exclude`, `--changed`, and `--results` filters across DAGs.

---

## Testing & Data Quality

- **Schema-bound YAML tests:** [YAML Tests](YAML_Tests.md) details how to define and run column-level constraints declared in `.yml`.
- **Reusable data-quality suites:** [Data Quality Tests](Data_Quality_Tests.md) catalogs reconciliation, freshness, and anomaly rules that can attach to models or sources.
- **Source freshness guard-rails:** [Source Freshness](Source_Freshness.md) covers `fft source freshness`, metadata in `sources.yml`, and interpreting warn/error thresholds in the docs UI.
- **Fast model unit tests:** [Unit Tests](Unit_Tests.md) shows how to author `.sql` / `.py` assertions, seed fixtures, and run them via `fft utest`.

---

## Docs, Debugging & Operations

- **Auto-generated docs & lineage:** [Auto Docs](Auto_Docs.md) explains `fft dag --html`, `fft docgen`, JSON exports, and optional `sync-db-comments` for Postgres/Snowflake.
- **Visibility & logging:** [Logging & Verbosity](Logging.md) lists CLI flags for structured logs, progress bars, and verbose executor info.
- **Troubleshooting:** [Troubleshooting & Error Codes](Troubleshooting.md) enumerates the most common failures, retry strategies, and diagnostic commands.

---

## Examples & Tutorials

- **Core walkthroughs:** [Basic Demo](examples/Basic_Demo.md) and [Materializations Demo](examples/Materializations_Demo.md) cover the standard table/view/incremental builds and DAG navigation.
- **Testing-focused:** [Data Quality Tests Demo](examples/DQ_Demo.md) and [Macros Demo](examples/Macros_Demo.md) showcase advanced assertions and templating.
- **Performance & state:** [Cache Demo](examples/Cache_Demo.md), [Environment Matrix Demo](examples/Environment_Matrix.md), and [Incremental Demo](examples/Incremental_Demo.md) highlight rebuilds and selective runs.
- **API & integrations:** [API Demo](examples/API_Demo.md) illustrates Python HTTP models; [Local Engine Setup](examples/Local_Engine_Setup.md) provides engine-specific Makefiles.
- **History tracking:** [Snapshot Demo](examples/Snapshot_Demo.md) demonstrates the snapshot materialization end-to-end with timestamp/check strategies.

All demos live in the top-level `examples/` directory and ship with Makefiles plus runnable seeds.

---

## Reference & Contribution

- **API reference:** Browse the generated [API Reference](reference/index.md) (MkDocStrings) for public functions, classes, and executors under `src/fastflowtransform`.
- **Architecture internals:** The [Technical Overview](Technical_Overview.md#part-ii-architecture-internals) dives into registries, DAG building, validation, and engine abstractions.
- **Contributing:** Follow [Contributing.md](Contributing.md) for dev environment setup (`uv`, `pyproject.toml`), coding standards, tests, and PR expectations.
- **License:** Apache 2.0 — see [License.md](License.md).

---

## Need Help?

- Open an issue or PR with context — start with [Contributing.md](Contributing.md) if you want to propose changes.
- Surface documentation gaps, bugs, or missing examples via GitHub issues in [MirrorsAndMisdirections/FastFlowTransform](https://github.com/MirrorsAndMisdirections/FastFlowTransform).
- For roadmap highlights or planning threads, check the final section of the [Technical Overview](Technical_Overview.md#roadmap-snapshot).

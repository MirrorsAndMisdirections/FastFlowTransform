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
- [Basic Demo Overview](./examples/Basic_Demo.md)
- [API Demo](./examples/API_Demo.md)
- [Environment Matrix Demo](./examples/Environment_Matrix.md)
- [Incremental & Delta demo](examples/Incremental_Demo.md)
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
- **Explore runnable demos:** start with the [Basic Demo Overview](examples/Basic_Demo.md) or browse the `examples/` directory; each subproject ships with its own README.

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

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
- [Data Quality Tests Demo](./examples/DQ_Demo.md)
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

# FlowForge Documentation Hub

Welcome! This page is your starting point for FlowForge docs. Pick the track that matches what you want to do and follow the links to the detailed guides.

---

## Docs Navigation
1. **Getting Started** — you are here (`docs/index.md`)
2. [User Guide](./Technical_Overview.md#part-i--operational-guide)
3. [Modeling Reference](./Config_and_Macros.md)
4. [Developer Guide](./Technical_Overview.md#part-ii--architecture--internals)

## Table of Contents

- [Docs Navigation](#docs-navigation)
- [Choose Your Path](#choose-your-path)
- [Reference Map](#reference-map)
- [Need Help?](#need-help)

---

## Choose Your Path

### 1. Build & Operate Projects (Data Practitioners)

- **Get set up quickly:** run the DuckDB demo or install locally via the [Quickstart](../README.md#quickstart).
- **Understand the project layout & CLI workflow:** see *Project Layout*, *Makefile Targets*, and *CLI Flows* in the [Technical Overview](Technical_Overview.md#project-layout).
- **Configure runtimes & profiles:** review executor profiles, environment overrides, and logging options in the [Technical Overview](Technical_Overview.md#profiles--environment-overrides).
- **Model data quality & troubleshoot runs:** the [Technical Overview](Technical_Overview.md#model-unit-tests-flowforge-utest) covers unit tests, troubleshooting tips, and exit codes.
- **Explore runnable demos:** browse the `examples/` directory in the repo; each subproject comes with its own README.

### 2. Extend FlowForge (Developers & Contributors)

- **Dive into architecture & core modules:** start with [Architecture Overview](Technical_Overview.md#architecture-overview) and [Core Modules](Technical_Overview.md#core-modules) for registry, DAG, executors, validation, and more.
- **Add tests & seeds:** see [Sample Models](Technical_Overview.md#sample-models), [Seeds & Example Data](Technical_Overview.md#seeds--example-data), and the unit test guide in [Model Unit Tests](Technical_Overview.md#model-unit-tests-flowforge-utest).
- **Contribute code:** follow the workflow described in [`../Contributing.md`](../Contributing.md) and consult the module-level docs for internal APIs.
- **Plan ahead:** check the roadmap snapshot in the [Technical Overview](Technical_Overview.md#roadmap-snapshot) to understand upcoming work.

---

## Reference Map

- **Modeling reference** — Jinja configuration, macros, helper functions: [`Config_and_Macros.md`](Config_and_Macros.md)
- **CLI entry point & commands** — `src/flowforge/cli.py`
- **Registry & node loading** — `src/flowforge/core.py`
- **Unit test runner** — `src/flowforge/utest.py`
- **Rendered DAG templates** — `src/flowforge/docs/templates/`

---

## Need Help?

- Open an issue or PR — see [`../Contributing.md`](../Contributing.md) for guidelines.
- Join the discussion (planning doc / roadmap highlights) — see the roadmap section in the [Technical Overview](Technical_Overview.md#roadmap-snapshot).
- If you spot gaps in the docs, file an issue with the context and links to the relevant section.

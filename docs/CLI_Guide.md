# CLI Guide

FastFlowTransform’s CLI is the entry point for seeding data, running DAGs, generating docs, syncing metadata, and executing quality tests. This guide summarizes the day-to-day commands and how they fit together. See `src/fastflowtransform/cli.py` for Typer definitions.

## Core Commands

| Command | Purpose |
|---------|---------|
| `fft seed <project> [--env dev]` | Materialize CSV/Parquet seeds into the configured engine. |
| `fft run <project> [--env dev]` | Execute the DAG (obeys cache + parallel flags). |
| `fft dag <project> --html` | Render the DAG graph/site for quick inspection. |
| `fft docgen <project> [--out site/docs] [--emit-json path] [--open-source]` | Generate the full documentation bundle (graph + model pages + optional JSON). Default output is `<project>/site/docs`. |
| `fft test <project> [--env dev]` | Run schema/data-quality tests defined in `project.yml` or schema YAML files. |
| `fft source-freshness <project> [--env dev] [--select …]` | Evaluate freshness rules for sources and emit a summary (e.g. in the DQ demo). |
| `fft utest <project>` | Execute unit tests defined under `tests/unit/*.yml`. |
| `fft sync-db-comments <project>` | Push model/column descriptions into Postgres or Snowflake comments. |

Use `--select` to scope `run`, `dag`, `test`, or `source-freshness` commands (e.g. `state:modified`, `tag:finance`, `result:error`). Environment overrides rely on the selected profile in `profiles.yml` or the `FF_*` variables.

## Source Freshness Checks

`fft source-freshness <project> --env <env>` evaluates **freshness rules for sources** (typically configured alongside your `sources.yml` / project metadata).

Typical usage:

```bash
fft source-freshness examples/dq_demo --env dev_duckdb
fft source-freshness . --env dev --select tag:critical_source
````

Key points:

* Uses the active profile (`--env`) to connect to the warehouse.
* Honors `--select`/`--exclude` so you can restrict checks to specific tags or source groups.
* Prints a per-source summary (status, max loaded timestamp, configured thresholds) plus an overall status code suitable for CI.
* Integrates naturally with the DQ demo: the same environment and seeds are reused, but checks focus on **source recency** rather than row-level tests in marts.

## HTTP/API Helpers

Python models can make HTTP calls via `fastflowtransform.api.http`. When you need examples, head over to `docs/Api_Models.md` for `get_json`, `get_df`, pagination helpers, caching, and offline modes.

## DAG & Documentation

* Narrow the graph with `fft dag ... --select <pattern>` (for example `state:modified` or `tag:finance`). Combined with `--html` this produces a focused mini-site under `<project>/docs/index.html`.
* Control schema introspection via `--with-schema/--no-schema`. Use `--no-schema` when the executor should avoid fetching column metadata (for example, BigQuery without sufficient permissions).
* `fft docgen` renders the DAG, model pages, and an optional JSON manifest in one command. Append `--open-source` to open `index.html` in your default browser after rendering.

## Sync Database Comments

`fft sync-db-comments <project> --env <env>` pushes model and column descriptions from project YAML or Markdown into database comments. The command currently supports Postgres and Snowflake Snowpark:

* Start with `--dry-run` to review the generated `COMMENT` statements.
* Postgres honors `profiles.yml -> postgres.db_schema` (and any `FF_PG_SCHEMA` override).
* Snowflake reuses the session or connection exposed by the executor.

If no descriptions are found, the command exits without making changes.

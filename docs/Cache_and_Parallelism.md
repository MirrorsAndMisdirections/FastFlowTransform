### 🆕 `docs/Cache_and_Parallelism.md`

````markdown
# Parallelism & Cache (FlowForge v0.3)

FlowForge 0.3 introduces a level-wise parallel scheduler and a build cache driven by stable fingerprints. This document explains **how parallel execution works**, **when nodes are skipped**, the exact **fingerprint formula**, and the **meta table** written after successful builds.

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

FlowForge splits the DAG into **levels** (all nodes that can run together without violating dependencies). Within a level, up to `--jobs` nodes execute in **parallel**.

- Dependencies are **never** violated.
- `--keep-going`: tasks already started in a level finish; **subsequent levels won’t start** if any task in the current level fails.
- Logs are serialized through an internal queue to keep lines readable and per-node timing visible.

**Quick start**
```bash
# Run with 4 workers per level
flowforge run . --env dev --jobs 4

# Keep tasks in the same level running even if one fails
flowforge run . --env dev --jobs 4 --keep-going
````

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

If the relation was dropped externally, FlowForge will **rebuild** even if the fingerprint matches.

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

---

## Meta Table Schema

After a successful build, FlowForge writes a per-node audit row:

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
flowforge run . --env dev --cache=rw

# No-op run — should skip all nodes (if nothing changed)
flowforge run . --env dev --cache=rw

# Force rebuild of a single model (ignores cache for it)
flowforge run . --env dev --cache=rw --rebuild marts_daily.ff

# Read-only cache (skip on match, build on miss, no writes)
flowforge run . --env dev --cache=ro

# Always build and write cache
flowforge run . --env dev --cache=wo

# Disable cache entirely
flowforge run . --env dev --no-cache
```

With parallelism:

```bash
flowforge run . --env dev --jobs 4
flowforge run . --env dev --jobs 4 --keep-going
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
We also check relation existence. If the table/view was dropped externally, FlowForge will **rebuild**.

**“My logs interleave under parallelism.”**
Logs are serialized via a queue; use `-v` / `-vv` for richer but still stable output. Each node prints start/end and duration; levels summarize.

**“Utest cache?”**
`flowforge utest --cache {off|ro|rw}` defaults to `off` for deterministic runs. With `rw`, expensive unit cases can be accelerated. Unit tests do not rely on the meta table by default.

---

## Example: simple_duckdb

The demo contains two independent staging nodes (`users.ff.sql`, `orders.ff.sql`). They run in **parallel** within the same level.

Makefile targets:

```makefile
run_parallel:
	FF_ENGINE=duckdb FF_DUCKDB_PATH="$(DB)" flowforge run "$(PROJECT)" --env dev --jobs 4

cache_rw_first:
	FF_ENGINE=duckdb FF_DUCKDB_PATH="$(DB)" flowforge run "$(PROJECT)" --env dev --cache=rw

cache_rw_second:
	FF_ENGINE=duckdb FF_DUCKDB_PATH="$(DB)" flowforge run "$(PROJECT)" --env dev --cache=rw

cache_invalidate_env:
	FF_ENGINE=duckdb FF_DUCKDB_PATH="$(DB)" FF_DEMO_TOGGLE=1 flowforge run "$(PROJECT)" --env dev --cache=rw
```

---

## Appendix: Environment Inputs

Only environment variables with the `FF_` prefix affect fingerprints (keys and values). If you change one (e.g., `FF_RUN_DATE`, `FF_REGION`), fingerprints change and downstream nodes rebuild.

```bash
# Will invalidate fingerprints and rebuild affected nodes
FF_RUN_DATE=2025-01-01 flowforge run . --env dev --cache=rw
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
+- [Parallelism & Cache (v0.3)](./Cache_and_Parallelism.md)
 - [Developer Guide – Architecture & Internals](./Technical_Overview.md#part-ii--architecture--internals)
````

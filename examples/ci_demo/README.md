# CI Demo Project

The **CI Demo** is a minimal FastFlowTransform project designed to showcase:

- `fft ci-check` for **DB-free PR checks**
- `fft run --changed-since <git-ref>` for **change-aware runs**

It uses three tiny models:

- `base_events.ff.sql` → synthetic one-row events table
- `fct_events.ff.sql`  → aggregates events
- `py_enrich.ff.py`    → trivial Python post-processing

No seeds, no DQ tests – just the essentials for CI.

---

## 1. Quick start

From the repo root:

```bash
cd examples/ci_demo
````

### 1.1 Structural CI check (no DB required)

```bash
fft ci-check . --env dev_duckdb --engine duckdb
```

What this does:

* Loads `project.yml` and models
* Parses SQL and Python models
* Builds the dependency graph
* Detects:

  * missing refs
  * cycles
  * basic config/schema issues

It **does not** connect to a database.

Exit codes are CI-friendly:

* `0` = all good
* `1` = parse/graph/config problems (suitable for PR blocking)
* `2` (optional) = only warnings, depending on your CI policy

---

## 2. Change-aware runs (`--changed-since`)

To run **only models affected by code changes** (since `origin/main`):

```bash
fft run . \
  --env dev_duckdb \
  --engine duckdb \
  --changed-since origin/main
```

Semantics:

* FFT inspects git diffs to find changed model files (SQL/Python).
* Computes the affected subgraph (changed + upstream/downstream).
* Intersects that with your `--select` / `--exclude` patterns, if provided.

Examples:

```bash
# Only run affected models that also match tag:example:ci_demo
fft run . \
  --env dev_duckdb \
  --engine duckdb \
  --changed-since origin/main \
  --select tag:example:ci_demo

# Combine with cache:
fft run . \
  --env dev_duckdb \
  --engine duckdb \
  --changed-since origin/main \
  --cache RW
```

---

## 3. CI integration sketch

### GitHub Actions (example)

```yaml
name: fft-ci

on:
  pull_request:
    paths:
      - "examples/ci_demo/**"
      - "src/fastflowtransform/**"

jobs:
  fft-ci:
    runs-on: ubuntu-latest

    steps:
      - uses: actions/checkout@v4
        with:
          fetch-depth: 0  # required for --changed-since

      - uses: actions/setup-python@v5
        with:
          python-version: "3.12"

      - name: Install FFT
        run: |
          pip install -e .

      - name: Structural CI check (no DB)
        working-directory: examples/ci_demo
        run: |
          fft ci-check . --env dev_duckdb --engine duckdb

      - name: Change-aware run
        working-directory: examples/ci_demo
        run: |
          fft run . --env dev_duckdb --engine duckdb --changed-since origin/main

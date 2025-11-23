# CI Checks & Change-Aware Runs

This page documents the new **CI integration** primitives in FastFlowTransform:

* `fft ci-check` – **DB-free CI checks** (parsing, DAG, basic lint).
* `fft run --changed-since` – **change-aware runs** for “only what matters” PR jobs.

These are designed to plug straight into GitHub Actions / GitLab / any CI system.

---

## 1. `fft ci-check`: DB-free CI validation

`fft ci-check` is a **lightweight, database-free** command that validates your project is structurally sound and ready to run.

### What it does

Given a project directory, `fft ci-check`:

1. Loads `project.yml`, `sources.yml`, and `models/`.
2. Parses all SQL & Python models (`*.ff.sql`, `*.ff.py`).
3. Builds the DAG and checks for:

   * **Missing dependencies** (`ref('...')` pointing to non-existent models).
   * **Cycles** in the model graph.
4. Runs basic **lint/quality** checks (extensible via `ci.core`).
5. Prints a concise summary and exits with a CI-friendly exit code.

No database connection is created – it never calls `ctx.make_executor()`.

### Basic usage

```bash
# From the project root
fft ci-check .

# Explicit env/engine (for engine-specific model filtering)
fft ci-check . --env dev_duckdb --engine duckdb
```

Typical CI snippet (GitHub Actions):

```yaml
- name: FFT ci-check
  run: |
    uv run fft ci-check . --env dev_duckdb --engine duckdb
```

### Exit codes

* `0` – all checks passed, **no errors**.
* `1` – at least one **error**-level issue (e.g. missing model, cycle).
* `>1` – reserved for future use (e.g. internal failures).

Warnings (style nits, non-fatal issues) do **not** change the exit code, but are printed in the summary.

### Output format

The CLI prints a human-readable summary, e.g.:

```text
CI Summary
──────────
✓ models parsed:           12
✓ graph built:             ok
✖ issues (2)
  • [MISSING_DEP] Error:   orders.ff → depends on missing model 'dim_users'
  • [CYCLE] Error:         Cycle detected among nodes: a.ff, b.ff

Totals
──────
✓ ok:      0
! warn:    0
✖ error:   2
```

The underlying objects are:

* `CiIssue` – single issue:

  * `code`: short ID (`MISSING_DEP`, `CYCLE`, `STYLE`, …)
  * `level`: `"error"` or `"warn"`
  * `message`: human-readable description
  * `obj_name`: model name, where applicable
  * `file`, `line`, `column`: optional source location
* `CiSummary` – overall result:

  * `issues: list[CiIssue]`
  * `selected_nodes: list[str]` – models considered in this run
  * `all_nodes: list[str]` – all models in the project

> These live under `fastflowtransform/ci/core.py` if you want to extend them.

---

## 2. SARIF output for code scanning

`fft ci-check` can optionally emit a **SARIF** file that GitHub / Azure DevOps / other tools can ingest as a code-scanning result.

### Writing SARIF

Use `fastflowtransform.ci.sarif.write_sarif` in a small wrapper script:

```python
from pathlib import Path

from fastflowtransform.ci.core import run_ci_checks
from fastflowtransform.ci.sarif import write_sarif

def main() -> None:
    # project_dir: repo root or project root
    summary = run_ci_checks(project_dir=".", env_name="dev_duckdb", engine_name="duckdb")

    out = Path("artifacts/fft-ci.sarif.json")
    write_sarif(
        summary,
        out,
        tool_name="FastFlowTransform CI",
        tool_version=None,  # or your package version
    )

if __name__ == "__main__":
    main()
```

Then in CI you can upload `artifacts/fft-ci.sarif.json` to your code-scanning provider.

### SARIF mapping

Each `CiIssue` becomes a SARIF `result`:

* `code` → `ruleId`
* `level`:

  * `"error"` → SARIF `"error"`
  * anything else → SARIF `"warning"`
* `message` → `message.text`
* `file`, `line`, `column` → `locations[0].physicalLocation.*`

Minimal example of a single result:

```json
{
  "ruleId": "MISSING_DEP",
  "level": "error",
  "message": { "text": "Model has missing dependency 'dim_users'" },
  "locations": [
    {
      "physicalLocation": {
        "artifactLocation": { "uri": "models/orders.ff.sql" },
        "region": {
          "startLine": 12,
          "startColumn": 5
        }
      }
    }
  ]
}
```

---

## 3. Change-aware runs with `--changed-since`

You can now ask `fft run` to only process models affected by **Git changes**, via:

```bash
fft run . --env dev_duckdb --changed-since origin/main
```

### How it works

1. `get_changed_models(project_dir, git_ref)`
   Looks at `git diff --name-only <git_ref>..HEAD` and filters paths
   to known model files (`*.ff.sql`, `*.ff.py`) under your project.

2. `compute_affected_models(changed, REGISTRY.nodes)`
   Computes the closure of **upstream and downstream** nodes from those
   changed models in the DAG (so that dependencies and dependents are included).

3. `_apply_changed_since_filter(...)` in `cli/run.py` merges this with
   your existing `--select` / `--exclude` selection.

### Selection semantics

Let:

* `wanted` = models selected by existing `--select` / `--exclude` logic.
* `affected` = models impacted by `--changed-since`.

Then:

* **No `--changed-since`:**

  ```text
  final selection = wanted
  ```

* **`--changed-since` but NO `--select` / `--exclude`:**

  ```text
  final selection = affected
  ```

  > The set of changed+affected models becomes the universe; your original `wanted` is ignored.

* **`--changed-since` AND `--select` and/or `--exclude`:**

  ```text
  final selection = wanted ∩ affected
  ```

  > This lets you combine tag/name selectors with git awareness, e.g. “only DQ models that were affected”.

If `affected` ends up empty (e.g. no relevant files changed), `fft run` exits early with a friendly “Nothing to run” message and `exit 0`.

### Examples

**1. CI: only run changed models (plus deps) on PRs**

```bash
# In CI, from the project root
fft run . \
  --env dev_duckdb \
  --engine duckdb \
  --changed-since origin/main
```

This will:

* Inspect the diff vs. `origin/main`.
* Determine all affected models (changed + upstream/downstream).
* Run only those.

**2. Combine with tags**

```bash
# Only run affected models tagged 'finance'
fft run . \
  --env dev_duckdb \
  --engine duckdb \
  --select tag:finance \
  --changed-since origin/main
```

Here, the final selection is:

```text
final = {models with tag:finance} ∩ {affected_by_git_changes}
```

**3. Combine with state-based selectors**

`--changed-since` plays nicely with `state:modified` selectors:

```bash
fft run . \
  --env dev_duckdb \
  --select "state:modified,tag:dq_demo" \
  --changed-since origin/main
```

This narrows the run to:

* Models modified according to fingerprint cache (`state:modified`),
* Tagged with `tag:dq_demo`,
* **and** affected by Git changes since `origin/main`.

---

## 4. Practical CI patterns

### A. Pure structural check (no DB)

Good for **fast feedback** on every PR:

```yaml
- name: FFT structural check
  run: |
    uv run fft ci-check . --env dev_duckdb --engine duckdb
```

### B. Change-aware run + tests on a dev DB

```yaml
- name: Seed demo data
  run: |
    uv run fft seed . --env dev_duckdb

- name: Run affected models only
  run: |
    uv run fft run . --env dev_duckdb --engine duckdb --changed-since origin/main

- name: Run DQ tests on affected marts
  run: |
    uv run fft test . --env dev_duckdb --select tag:dq_demo
```

### C. SARIF publishing (GitHub Actions example)

```yaml
- name: FFT ci-check + SARIF
  run: |
    uv run python scripts/fft_ci_sarif.py

- name: Upload SARIF to GitHub
  uses: github/codeql-action/upload-sarif@v2
  with:
    sarif_file: artifacts/fft-ci.sarif.json
```

Where `scripts/fft_ci_sarif.py` is the small wrapper shown earlier.

---

## 5. Summary

The new CI features are meant to be:

* **Safe & fast** – `fft ci-check` runs without a DB.
* **Integration-friendly** – clear exit codes, SARIF for code scanning.
* **Efficient** – `fft run --changed-since` targets only what changed (plus dependencies).

From here you can:

* Add `fft ci-check` to your **PR pipelines**.
* Use `--changed-since` in **run jobs** to avoid reprocessing the world on every commit.
* Extend `fastflowtransform.ci.core` with project-specific checks if needed.

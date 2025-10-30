# Project Configuration (`project.yml`)

`project.yml` defines global metadata, documentation, variables, and data-quality tests for a FastFlowTransform project. This reference walks through the supported sections and common patterns.

## File Location

`project.yml` lives at the root of your project.

```
project/
├── models/
├── project.yml
└── profiles.yml
```

## Top-Level Keys

```yaml
name: my_project
version: "0.1"
models_dir: models          # optional, defaults to "models"

docs:
  dag_dir: site/dag         # output for fft dag --html
  models:
    users:
      description: "Raw users table"
      columns:
        id: "Primary key"
        email: "Email address"

vars:
  snapshot_day: "2024-01-01"
  default_limit: 100

tests:
  - type: not_null
    table: users
    column: id
    tags: [batch]
```

### Metadata

| Key         | Description |
|-------------|-------------|
| `name`      | Project identifier (used in docs/metadata). |
| `version`   | Arbitrary version string. |
| `models_dir`| Relative directory containing models (`*.ff.sql` / `*.ff.py`). |

### Documentation (`docs`)

- `dag_dir`: where `fft dag --html` writes the static site.
- `models`: per-model descriptions and column docs surfaced in the generated DAG/docs.

### Variables (`vars`)

Key/value pairs accessible via `{{ var('key', default) }}` in Jinja templates. CLI overrides (`--vars key=value`) take precedence.

### Tests (`tests`)

Project-wide data quality checks run by `fft test`. Each test is a dict with:

- `type`: `not_null`, `unique`, `accepted_values`, `row_count_between`, `greater_equal`, `non_negative_sum`, `freshness`, or reconciliation checks (`reconcile_equal`, `reconcile_diff_within`, `reconcile_ratio_within`, `reconcile_coverage`).
- `table`: target table or relation.
- `column`: required for column-based tests.
- Optional: `tags`, `severity` (`error`/`warn`), additional parameters (e.g. `values`, `min`, `max`).

Example:

```yaml
tests:
  - type: accepted_values
    table: mart_users
    column: status
    values: [active, invited]
    severity: warn
  - type: reconcile_equal
    name: revenue_vs_bookings
    left:  { table: fct_revenue,   expr: "sum(amount)" }
    right: { table: fct_bookings, expr: "sum(expected_amount)" }
    abs_tolerance: 5.0
```

## Interaction with `.env` and Profiles

`project.yml` does not read environment variables directly. However:

- `vars:` can reference `var('key')` defaults overridden by CLI or `.env`.
- Tests often depend on `profiles.yml` and `sources.yml` for the actual connection details.
- Makefiles may set `FFT_ACTIVE_ENV` or other `FF_*` variables influencing runs, but `project.yml` remains static.

## Best Practices

- Keep `project.yml` committed to version control (no secrets).
- Use `docs/` to provide richer Markdown descriptions; reference them via `columns` or `description` fields if desired.
- Organize tests by tag (`tags: [batch]`, `tags: [reconcile]`) to support selective execution: `fft test . --select tag:reconcile`.

Refer to `docs/Data_Quality_Tests.md` for detailed test semantics and `docs/Profiles.md` for profile/env loading behavior.

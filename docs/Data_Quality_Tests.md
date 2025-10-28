# Data Quality Test Reference

FastFlowTransform exposes a set of built-in data quality checks that you can configure in `project.yml → tests:` and execute with `fft test`. This document lists every supported test, required parameters, and example configurations.

## Usage Overview

```yaml
# project.yml
tests:
  - type: not_null
    table: users
    column: id
    severity: error          # default (omit for error)
    tags: [batch]

  - type: unique
    table: users
    column: email
    tags: [batch]

  - type: accepted_values
    table: users
    column: status
    values: [active, invited]
    severity: warn           # warn keeps run green on failure

  - type: row_count_between
    table: users_enriched
    min: 1
    max: 100000

  - type: reconcile_equal
    name: revenue_vs_bookings  # optional label in summaries
    tags: [reconcile]
    left:  { table: fct_revenue,   expr: "sum(amount)" }
    right: { table: fct_bookings, expr: "sum(expected_amount)" }
    abs_tolerance: 5.0
```

Every entry is a single dictionary describing one check. The common keys are:

| Key        | Description |
|------------|-------------|
| `type`     | Test kind (see tables below). |
| `table`    | Target table for table-level checks or display hint for reconciliations. |
| `column`   | Required for column-scoped checks (`not_null`, `unique`, …). |
| `severity` | `error` (default) or `warn`. |
| `tags`     | Optional list of selectors for `fft test --select tag:...`. |
| `name`     | Optional identifier surfaced in summaries (useful for reconciliations). |

Run all configured checks:

```bash
fft test . --env dev
```

Use `--select tag:<name>` to restrict by tags (legacy `--select batch` reads the same tags list). Tests always execute regardless of cache settings.

Each entry produces a summary line. Failures stop the command unless `severity: warn` is set.

## Table-Level Checks

These checks operate on a single table (optionally filtered with `where:`). Unless noted, they require a `column` argument.

### `not_null`
- **Purpose:** Assert that a column never contains NULLs.
- **Parameters:**
  - `column` *(str, required)*
  - `where` *(str, optional)* — SQL predicate applied before the NULL check.
- **Failure:** Reports the number of NULL rows and shows the underlying SQL.

### `unique`
- **Purpose:** Detect duplicates within a column.
- **Parameters:**
  - `column` *(str, required)*
  - `where` *(str, optional)*
- **Failure:** Indicates how many duplicate groups were found (HAVING count > 1) and shows a sample query.

### `accepted_values`
- **Purpose:** Ensure every non-NULL value is inside an allowed set.
- **Parameters:**
  - `column` *(str, required)*
  - `values` *(list, required)* — permitted literals (strings are quoted automatically).
  - `where` *(str, optional)*
- **Failure:** Shows the number of out-of-set values plus up to five sample values.

### `greater_equal`
- **Purpose:** Require all values to be greater than or equal to a threshold.
- **Parameters:**
  - `column` *(str, required)*
  - `threshold` *(number, default `0`)*
- **Failure:** Lists how many rows fell below the threshold.

### `non_negative_sum`
- **Purpose:** Validate that the sum of a numeric column is not negative.
- **Parameters:**
  - `column` *(str, required)*
- **Failure:** Reports the signed sum when it is negative.

### `row_count_between`
- **Purpose:** Guard minimum (and optional maximum) row counts for a table.
- **Parameters:**
  - `min` *(int, default `1`)*
  - `max` *(int, optional)* — omit for open-ended upper bounds.
- **Failure:** Indicates the observed row count when it falls outside `[min, max]`.

### `freshness`
- **Purpose:** Warn when the latest timestamp is older than an allowed delay.
- **Parameters:**
  - `column` *(str, required)* — timestamp column.
  - `max_delay_minutes` *(int, required)* — permitted staleness.
- **Failure:** Reports the computed lag in minutes. Uses ANSI-style `DATE_PART` (works on DuckDB/Postgres; extend for other engines as needed).

## Cross-Table Reconciliations

Reconciliation checks compare aggregates or keys across two relations. Their configuration accepts dictionaries describing the left/right side expressions or keys.

### `reconcile_equal`
- **Purpose:** Compare two scalar expressions with optional tolerances.
- **Parameters:**
  - `left`, `right` *(dict, required)* with keys:
    - `table` *(str, required)*
    - `expr` *(str, required)* — SQL select expression (e.g. `sum(amount)`).
    - `where` *(str, optional)*
  - `abs_tolerance` *(float, optional)* — maximum absolute difference.
  - `rel_tolerance_pct` *(float, optional)* — maximum relative difference in percent.
- **Failure:** Displays both values, absolute and relative differences.

### `reconcile_ratio_within`
- **Purpose:** Constrain the ratio `left/right` within bounds.
- **Parameters:**
  - `left`, `right` *(dict, required as above)*
  - `min_ratio`, `max_ratio` *(float, required)*
- **Failure:** Shows the computed ratio and expected interval.

### `reconcile_diff_within`
- **Purpose:** Limit the absolute difference between two aggregates.
- **Parameters:**
  - `left`, `right` *(dict, required)*
  - `max_abs_diff` *(float, required)*
- **Failure:** Reports the absolute difference when it exceeds `max_abs_diff`.

### `reconcile_coverage`
- **Purpose:** Ensure every key present in a source table appears in a target table (anti-join zero).
- **Parameters:**
  - `source` *(dict, required)* — `table` and `key` column.
  - `target` *(dict, required)* — `table` and `key` column.
  - `source_where` *(str, optional)* — filter applied to the source.
  - `target_where` *(str, optional)* — filter applied to the target.
- **Failure:** Reports the number of missing keys.

## Severity & Selectors

- `severity: error` (default) makes failures stop the test run with exit code 1.
- `severity: warn` records the result but keeps the run successful.
- `selectors:` lets you group checks under named tokens (e.g. `batch`, `streaming`). Use `fft test --select tag:batch` to execute a subset.

## CLI Summary Output

Each executed check produces a line in the summary:

```
✓ not_null          users.email                     (3ms)
✖ accepted_values   events.status    values=['new', 'active']   (warn)
```

Failures include the generated SQL (where available) to simplify debugging. Use `fft test --verbose` for more detail, or `FFT_SQL_DEBUG=1` to log the underlying queries.

## Further Reading

- [`docs/YAML_Tests.md`](YAML_Tests.md) – schema for YAML-defined tests and advanced scenarios.
- [`fastflowtransform/testing.py`](../src/fastflowtransform/testing.py) – implementation details and helper functions.
- [`fft test --help`] — command-line switches, selectors, and cache options.

# Data Quality Test Reference

FastFlowTransform exposes a set of built-in data quality checks that you can configure in `project.yml → tests:` and execute with `fft test`. This document lists every supported test, required parameters, and example configurations.

## Supported Test Types

The following values are currently supported for `type`:

- `not_null`
- `unique`
- `accepted_values`
- `greater_equal`
- `non_negative_sum`
- `row_count_between`
- `freshness`
- `reconcile_equal`
- `reconcile_ratio_within`
- `reconcile_diff_within`
- `reconcile_coverage`

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

  - type: greater_equal
    table: orders
    column: amount
    threshold: 0

  - type: non_negative_sum
    table: orders
    column: amount

  - type: row_count_between
    table: users_enriched
    min_rows: 1
    max_rows: 100000

  - type: freshness
    table: events
    column: event_ts
    max_delay_minutes: 30

  - type: reconcile_equal
    name: revenue_vs_bookings  # optional label in summaries
    tags: [reconcile]
    left:  { table: fct_revenue,   expr: "sum(amount)" }
    right: { table: fct_bookings, expr: "sum(expected_amount)" }
    abs_tolerance: 5.0
````

Every entry is a single dictionary describing one check. The common keys are:

| Key        | Description                                                              |
| ---------- | ------------------------------------------------------------------------ |
| `type`     | Test kind (see list above).                                              |
| `table`    | Target table for table-level checks or display hint for reconciliations. |
| `column`   | Required for column-scoped checks (`not_null`, `unique`, …).             |
| `severity` | `error` (default) or `warn`.                                             |
| `tags`     | Optional list of selectors for `fft test --select tag:...`.              |
| `name`     | Optional identifier surfaced in summaries (useful for reconciliations).  |

Run all configured checks:

```bash
fft test . --env dev
```

Use `--select tag:<name>` to restrict by tags (e.g. `fft test --select tag:batch`). Tests always execute regardless of cache settings.

Each entry produces a summary line. Failures stop the command unless `severity: warn` is set.

## Table-Level Checks

These checks operate on a single table (optionally filtered with `where:`). Unless noted, they require a `column` argument.

### `not_null`

* **Purpose:** Assert that a column never contains NULLs.
* **Parameters:**

  * `column` *(str, required)*
  * `where` *(str, optional)* — SQL predicate applied before the NULL check.
* **Failure:** Reports the number of NULL rows and shows the underlying SQL.

---

### `unique`

* **Purpose:** Detect duplicates within a column.
* **Parameters:**

  * `column` *(str, required)*
  * `where` *(str, optional)*
* **Failure:** Indicates how many duplicate groups were found (HAVING `count(*) > 1`) and shows a sample query.

---

### `accepted_values`

* **Purpose:** Ensure every non-NULL value is inside an allowed set.
* **Parameters:**

  * `column` *(str, required)*
  * `values` *(list, required)* — permitted literals (strings are quoted automatically).
  * `where` *(str, optional)* — additional filter condition.
* **Behaviour note:** If `values` is omitted or an empty list, the check is treated as a no-op and always passes. The summary still shows the configured test.
* **Failure:** Shows the number of out-of-set values plus up to five sample values.

---

### `greater_equal`

* **Purpose:** Require all values to be greater than or equal to a threshold.
* **Parameters:**

  * `column` *(str, required)*
  * `threshold` *(number, default `0`)*
* **Failure:** Lists how many rows fell below the threshold.

---

### `non_negative_sum`

* **Purpose:** Validate that the sum of a numeric column is not negative.
* **Parameters:**

  * `column` *(str, required)*
* **Failure:** Reports the signed sum when it is negative.

---

### `row_count_between`

* **Purpose:** Guard minimum (and optional maximum) row counts for a table.
* **Parameters:**

  * `min_rows` *(int, default `1`)* — minimum expected number of rows.
  * `max_rows` *(int, optional)* — omit for open-ended upper bounds.
* **Failure:** Indicates the observed row count when it falls outside `[min_rows, max_rows]`.

---

### `freshness`

* **Purpose:** Warn when the latest timestamp is older than an allowed delay.
* **Parameters:**

  * `column` *(str, required)* — timestamp column.
  * `max_delay_minutes` *(int, required)* — permitted staleness in whole minutes.
* **Failure:** Reports the computed lag in minutes. Uses:

  ```sql
  select date_part('epoch', now() - max(column)) / 60.0 as delay_min
  from <table>
  ```

  This is straightforward for DuckDB/Postgres; other engines may need adaptations.

## Cross-Table Reconciliations

Reconciliation checks compare aggregates or keys across two relations. Their configuration accepts dictionaries describing the left/right side expressions or keys. The top-level `table`/`column` fields are used only for display and grouping; the actual queries are defined via the nested dictionaries.

### `reconcile_equal`

* **Purpose:** Compare two scalar expressions with optional tolerances.
* **Parameters:**

  * `left`, `right` *(dict, required)* with keys:

    * `table` *(str, required)*
    * `expr` *(str, required)* — SQL select expression (e.g. `sum(amount)`).
    * `where` *(str, optional)*
  * `abs_tolerance` *(float, optional)* — maximum absolute difference.
  * `rel_tolerance_pct` *(float, optional)* — maximum relative difference in percent.
* **Failure:** Displays both values, absolute and relative differences. If no tolerance is provided, strict equality is enforced (diff must be exactly `0.0`).

---

### `reconcile_ratio_within`

* **Purpose:** Constrain the ratio `left/right` within bounds.
* **Parameters:**

  * `left`, `right` *(dict, required as above)*
  * `min_ratio`, `max_ratio` *(float, required)*
* **Failure:** Shows the computed ratio and expected interval.

---

### `reconcile_diff_within`

* **Purpose:** Limit the absolute difference between two aggregates.
* **Parameters:**

  * `left`, `right` *(dict, required)*
  * `max_abs_diff` *(float, required)*
* **Failure:** Reports the absolute difference when it exceeds `max_abs_diff`.

---

### `reconcile_coverage`

* **Purpose:** Ensure every key present in a source table appears in a target table (anti-join zero).
* **Parameters:**

  * `source` *(dict, required)* — must contain:

    * `table` *(str)* — source table.
    * `key` *(str)* — key column in the source.
  * `target` *(dict, required)* — must contain:

    * `table` *(str)* — target table.
    * `key` *(str)* — key column in the target.
  * `source_where` *(str, optional)* — filter applied to the source.
  * `target_where` *(str, optional)* — filter applied to the target.
* **Failure:** Reports the number of missing keys.

## Severity & Tags

* `severity: error` (default) makes failures stop the test run with exit code 1.
* `severity: warn` records the result but keeps the run successful.
* `tags:` lets you group checks under named tokens (e.g. `batch`, `streaming`). Use `fft test --select tag:batch` to execute a subset.

## CLI Summary Output

Each executed check produces a line in the summary:

```text
✓ not_null          users.email                     (3ms)
✖ accepted_values   events.status    values=['new', 'active']   (warn)
```

Failures include the generated SQL (where available) to simplify debugging. Use `fft test --verbose` for more detail, or `FFT_SQL_DEBUG=1` to log the underlying queries.

## Further Reading

* `docs/YAML_Tests.md` – schema for YAML-defined tests and advanced scenarios.
* `fft test --help` — command-line switches, selectors, and cache options.

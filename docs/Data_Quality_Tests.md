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

In addition, you can register **custom tests** (Python or SQL) with any logical
name (e.g. `min_positive_share`, `no_future_orders`) and use that name in
`project.yml → tests:`. See [Custom DQ Tests (Python & SQL)](#custom-dq-tests-python-sql).

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

## Custom DQ Tests (Python & SQL)

FastFlowTransform lets you plug in your own test logic and still reuse:

* `project.yml → tests:` configuration,
* the same `fft test` command,
* the standard summary output and exit codes.

Custom tests come in two flavours:

1. **Python-based tests** registered via `@dq_test(...)` in `tests/**/*.ff.py`
2. **SQL-based tests** defined in `tests/**/*.ff.sql` with a small `{{ config(...) }}` header.

Both kinds participate in the same Pydantic validation pipeline as built-in
tests: built-ins use the strict `ProjectTestConfig` union, while custom tests
are validated against a generated parameter model derived from their `config(...)`.

### Python-based custom tests

Create a file like `tests/dq/min_positive_share.ff.py`:

```python
from __future__ import annotations

from typing import Any

from fastflowtransform.decorators import dq_test
from fastflowtransform.testing import base as testing


@dq_test("min_positive_share")
def min_positive_share(
    con: Any,
    table: str,
    column: str | None,
    params: dict[str, Any],
) -> tuple[bool, str | None, str | None]:
    """
    Custom DQ test: require that at least `min_share` of rows have column > 0.

    Parameters (from project.yml → tests → params):
      - min_share: float in [0,1], e.g. 0.75
      - where: optional filter (string) to restrict the population
    """
    if column is None:
        example = f"select count(*) from {table} where <column> > 0"
        return False, "min_positive_share requires a 'column' parameter", example

    # For project.yml tests the user payload lives under params["params"]
    cfg: dict[str, Any] = params.get("params") or params
    min_share: float = cfg["min_share"]
    where: str | None = cfg.get("where")

    where_clause = f" where {where}" if where else ""

    total_sql = f"select count(*) from {table}{where_clause}"
    if where:
        pos_sql = f"select count(*) from {table}{where_clause} and {column} > 0"
    else:
        pos_sql = f"select count(*) from {table} where {column} > 0"

    total = testing._scalar(con, total_sql)
    positives = testing._scalar(con, pos_sql)

    example_sql = f"{pos_sql};  -- positives\n{total_sql}; -- total"

    if not total:
        return False, f"min_positive_share: table {table} is empty", example_sql

    share = float(positives or 0) / float(total)
    if share < min_share:
        msg = (
            f"min_positive_share failed: positive share {share:.4f} "
            f"< required {min_share:.4f} "
            f"({positives} of {total} rows have {column} > 0)"
        )
        return False, msg, example_sql

    return True, None, example_sql
```

The decorator `@dq_test("min_positive_share")` registers the function under that
logical name. You can then reference it from `project.yml`:

```yaml
tests:
  - type: min_positive_share
    table: orders
    column: amount
    params:
      min_share: 0.75
      where: "amount <> 0"
    tags: [batch]
```

Notes:

* The function **must** return `(ok: bool, message: str | None, example_sql: str | None)`.
* The signature is the same as built-in runners: `(con, table, column, params)`.
* For project-level tests, the full YAML dict is passed as `params`; by
  convention, custom tests read their own options from `params["params"]`.

### SQL-based custom tests

Create a file like `tests/dq/no_future_orders.ff.sql`:

```sql
{{ config(
    type="no_future_orders",
    params=["where"]
) }}

-- Custom DQ test: fail if any row has a timestamp in the future.
--
-- Conventions:
--   - {{ table }}  : table name (e.g. "orders")
--   - {{ column }} : timestamp column (e.g. "order_ts")
--   - {{ where }}  : optional filter, passed via params["where"]

select count(*) as failures
from {{ table }}
where {{ column }} > current_timestamp
  {%- if where %} and ({{ where }}){%- endif %}
```

The `config(...)` header declares:

* `type`: logical name used in `project.yml → tests:`.
* `params`: list of allowed parameter keys for this test.

FFT turns this into a small Pydantic model and validates your YAML config
against it. Any unknown key under `params:` results in a clear error at
**config-load time**, before executing SQL.

Hook it up in `project.yml`:

```yaml
tests:
  - type: no_future_orders
    table: orders
    column: order_ts
    params:
      where: "amount <> 0"
    tags: [batch]
```

At runtime FFT:

1. discovers `tests/**/*.ff.sql`,
2. registers each file as a test of the given `type`,
3. validates `params:` using the `params=[...]` schema,
4. renders and executes the SQL as a “violation count” query
   (`0` = pass, `> 0` = fail).

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

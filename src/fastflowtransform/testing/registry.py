# fastflowtransform/testing/registry.py
from __future__ import annotations

from typing import Any, Protocol

from fastflowtransform.testing import base as testing


class Runner(Protocol):
    """Callable signature for a generic test runner.

    Returns:
        ok (bool): Whether the test passed.
        message (str | None): Optional human-friendly message (usually set on failure).
        example_sql (str | None): Optional example SQL (shown in summary on failure).
    """

    def __call__(
        self, con: Any, table: str, column: str | None, params: dict[str, Any]
    ) -> tuple[bool, str | None, str | None]: ...


# ---------------------------------------------------------------------------
# Helper
# ---------------------------------------------------------------------------


def _example_where(where: str | None) -> str:
    """Return a ' where (...)' suffix if where is provided, otherwise empty string."""
    return f" where ({where})" if where else ""


# ---------------------------------------------------------------------------
# Basic column-level tests
# ---------------------------------------------------------------------------


def run_not_null(
    con: Any, table: str, column: str | None, params: dict[str, Any]
) -> tuple[bool, str | None, str | None]:
    where = params.get("where")
    example = f"select count(*) from {table} where {column} is null" + (
        f" and ({where})" if where else ""
    )
    if column is None:
        # Column is required for not_null
        return False, "missing required parameter: column", example
    col = column
    try:
        testing.not_null(con, table, col, where=where)
        return True, None, example
    except testing.TestFailure as e:
        return False, str(e), example


def run_unique(
    con: Any, table: str, column: str | None, params: dict[str, Any]
) -> tuple[bool, str | None, str | None]:
    where = params.get("where")
    example = (
        f"select {column} as key, count(*) c from {table}"
        + (f" where ({where})" if where else "")
        + " group by 1 having count(*) > 1 limit 5"
    )
    if column is None:
        return False, "missing required parameter: column", example
    col = column
    try:
        testing.unique(con, table, col, where=where)
        return True, None, example
    except testing.TestFailure as e:
        return False, str(e), example


def run_accepted_values(
    con: Any, table: str, column: str | None, params: dict[str, Any]
) -> tuple[bool, str | None, str | None]:
    """Runner for testing.accepted_values."""
    values = params.get("values") or []
    where = params.get("where")

    if column is None:
        example = "-- accepted_values: column parameter is required"
        return False, "missing required parameter: column", example

    if not values:
        # No values configured -> we treat this as a no-op check.
        example = f"-- accepted_values: no values provided; check is skipped for {table}.{column}"
        return True, None, example

    in_list = testing.sql_list(values)
    example = (
        f"select distinct {column} from {table} "
        + f"where {column} is not null and {column} not in ({in_list})"
        + (f" and ({where})" if where else "")
        + " limit 5"
    )

    col = column
    try:
        testing.accepted_values(con, table, col, values=values, where=where)
        return True, None, example
    except testing.TestFailure as e:
        return False, str(e), example


def run_greater_equal(
    con: Any, table: str, column: str | None, params: dict[str, Any]
) -> tuple[bool, str | None, str | None]:
    """Runner for testing.greater_equal (column >= threshold)."""
    threshold = float(params.get("threshold", 0.0))
    if column is None:
        example = f"select count(*) from {table} where <column> < {threshold}"
        return False, "missing required parameter: column", example

    example = f"select count(*) from {table} where {column} < {threshold}"
    col = column
    try:
        testing.greater_equal(con, table, col, threshold=threshold)
        return True, None, example
    except testing.TestFailure as e:
        return False, str(e), example


def run_non_negative_sum(
    con: Any, table: str, column: str | None, params: dict[str, Any]
) -> tuple[bool, str | None, str | None]:
    """Runner for testing.non_negative_sum."""
    if column is None:
        example = f"select coalesce(sum(<column>), 0) from {table}"
        return False, "missing required parameter: column", example

    example = f"select coalesce(sum({column}), 0) from {table}"
    col = column
    try:
        testing.non_negative_sum(con, table, col)
        return True, None, example
    except testing.TestFailure as e:
        return False, str(e), example


# ---------------------------------------------------------------------------
# Table-level tests
# ---------------------------------------------------------------------------


def run_row_count_between(
    con: Any, table: str, column: str | None, params: dict[str, Any]
) -> tuple[bool, str | None, str | None]:
    """Runner for testing.row_count_between."""
    min_rows = int(params.get("min_rows", 1))
    max_rows_param = params.get("max_rows")
    max_rows = int(max_rows_param) if max_rows_param is not None else None

    example = f"select count(*) from {table}"
    try:
        testing.row_count_between(con, table, min_rows=min_rows, max_rows=max_rows)
        return True, None, example
    except testing.TestFailure as e:
        return False, str(e), example


def run_freshness(
    con: Any, table: str, column: str | None, params: dict[str, Any]
) -> tuple[bool, str | None, str | None]:
    """Runner for testing.freshness (max timestamp delay in minutes)."""
    if column is None:
        example = (
            f"select date_part('epoch', now() - max(<ts_column>)) / 60.0 as delay_min from {table}"
        )
        return False, "missing required parameter: column (ts_col)", example

    max_delay_raw = params.get("max_delay_minutes")
    example = f"select date_part('epoch', now() - max({column})) / 60.0 as delay_min from {table}"

    if max_delay_raw is None:
        return False, "missing required parameter: max_delay_minutes", example

    try:
        max_delay_int = int(max_delay_raw)
    except (TypeError, ValueError):
        return (
            False,
            f"invalid max_delay_minutes (expected integer minutes, got {max_delay_raw!r})",
            example,
        )

    col = column
    try:
        testing.freshness(con, table, col, max_delay_minutes=max_delay_int)
        return True, None, example
    except testing.TestFailure as e:
        return False, str(e), example


# ---------------------------------------------------------------------------
# Helpers for reconcile tests
# ---------------------------------------------------------------------------


def _example_scalar_side(side: dict[str, Any]) -> str:
    """Render an example SELECT for a reconcile side."""
    tbl = side.get("table", "<table>")
    expr = side.get("expr", "<expr>")
    where = side.get("where")
    return f"select {expr} from {tbl}" + (f" where {where}" if where else "")


def _example_coverage_sql(
    source: dict[str, Any],
    target: dict[str, Any],
    source_where: str | None,
    target_where: str | None,
) -> str:
    """Render an example SQL for reconcile_coverage."""
    s_tbl, s_key = source.get("table", "<source_table>"), source.get("key", "<source_key>")
    t_tbl, t_key = target.get("table", "<target_table>"), target.get("key", "<target_key>")
    s_w = f" where {source_where}" if source_where else ""
    t_w = f" where {target_where}" if target_where else ""
    return f"""
with src as (select {s_key} as k from {s_tbl}{s_w}),
     tgt as (select {t_key} as k from {t_tbl}{t_w})
select count(*) from src s
left join tgt t on s.k = t.k
where t.k is null
""".strip()


# ---------------------------------------------------------------------------
# Reconcile tests
# ---------------------------------------------------------------------------


def run_reconcile_equal(
    con: Any, table: str, column: str | None, params: dict[str, Any]
) -> tuple[bool, str | None, str | None]:
    """Runner for testing.reconcile_equal (left == right within tolerances)."""
    left = params.get("left")
    right = params.get("right")
    abs_tol = params.get("abs_tolerance")
    rel_tol = params.get("rel_tolerance_pct")

    if not isinstance(left, dict) or not isinstance(right, dict):
        example = "-- reconcile_equal requires 'left' and 'right' dict parameters"
        return False, "missing or invalid 'left'/'right' parameters", example

    example = _example_scalar_side(left) + ";\n" + _example_scalar_side(right)

    try:
        testing.reconcile_equal(
            con,
            left=left,
            right=right,
            abs_tolerance=abs_tol,
            rel_tolerance_pct=rel_tol,
        )
        return True, None, example
    except testing.TestFailure as e:
        return False, str(e), example


def run_reconcile_ratio_within(
    con: Any, table: str, column: str | None, params: dict[str, Any]
) -> tuple[bool, str | None, str | None]:
    """Runner for testing.reconcile_ratio_within (min_ratio <= L/R <= max_ratio)."""
    left = params.get("left")
    right = params.get("right")
    min_ratio = params.get("min_ratio")
    max_ratio = params.get("max_ratio")

    if not isinstance(left, dict) or not isinstance(right, dict):
        example = "-- reconcile_ratio_within requires 'left' and 'right' dict parameters"
        return False, "missing or invalid 'left'/'right' parameters", example

    if min_ratio is None or max_ratio is None:
        example = _example_scalar_side(left) + ";\n" + _example_scalar_side(right)
        return False, "missing required parameters: min_ratio / max_ratio", example

    example = _example_scalar_side(left) + ";\n" + _example_scalar_side(right)

    try:
        testing.reconcile_ratio_within(
            con,
            left=left,
            right=right,
            min_ratio=float(min_ratio),
            max_ratio=float(max_ratio),
        )
        return True, None, example
    except testing.TestFailure as e:
        return False, str(e), example


def run_reconcile_diff_within(
    con: Any, table: str, column: str | None, params: dict[str, Any]
) -> tuple[bool, str | None, str | None]:
    """Runner for testing.reconcile_diff_within (|L - R| <= max_abs_diff)."""
    left = params.get("left")
    right = params.get("right")
    max_abs_diff = params.get("max_abs_diff")

    if not isinstance(left, dict) or not isinstance(right, dict):
        example = "-- reconcile_diff_within requires 'left' and 'right' dict parameters"
        return False, "missing or invalid 'left'/'right' parameters", example

    if max_abs_diff is None:
        example = _example_scalar_side(left) + ";\n" + _example_scalar_side(right)
        return False, "missing required parameter: max_abs_diff", example

    example = _example_scalar_side(left) + ";\n" + _example_scalar_side(right)

    try:
        testing.reconcile_diff_within(
            con,
            left=left,
            right=right,
            max_abs_diff=float(max_abs_diff),
        )
        return True, None, example
    except testing.TestFailure as e:
        return False, str(e), example


def run_reconcile_coverage(
    con: Any, table: str, column: str | None, params: dict[str, Any]
) -> tuple[bool, str | None, str | None]:
    """Runner for testing.reconcile_coverage (anti-join count == 0)."""
    source = params.get("source")
    target = params.get("target")
    source_where = params.get("source_where")
    target_where = params.get("target_where")

    if not isinstance(source, dict) or not isinstance(target, dict):
        example = "-- reconcile_coverage requires 'source' and 'target' dict parameters"
        return False, "missing or invalid 'source'/'target' parameters", example

    example = _example_coverage_sql(source, target, source_where, target_where)

    try:
        testing.reconcile_coverage(
            con,
            source=source,
            target=target,
            source_where=source_where,
            target_where=target_where,
        )
        return True, None, example
    except testing.TestFailure as e:
        return False, str(e), example


# ---------------------------------------------------------------------------
# Registry
# ---------------------------------------------------------------------------

# Public registry (extensible).
TESTS: dict[str, Runner] = {
    "not_null": run_not_null,
    "unique": run_unique,
    "accepted_values": run_accepted_values,
    "greater_equal": run_greater_equal,
    "non_negative_sum": run_non_negative_sum,
    "row_count_between": run_row_count_between,
    "freshness": run_freshness,
    # Reconcile tests
    "reconcile_equal": run_reconcile_equal,
    "reconcile_ratio_within": run_reconcile_ratio_within,
    "reconcile_diff_within": run_reconcile_diff_within,
    "reconcile_coverage": run_reconcile_coverage,
}


# ---------------------------------------------------------------------------
# Public registration API
# ---------------------------------------------------------------------------


def register_test(name: str, runner: Runner, *, overwrite: bool = False) -> None:
    """
    Register (or override) a data-quality test runner.

    Usage:

        from fastflowtransform.testing import register_test

        def my_runner(con, table, column, params):
            ...
            return True, None, None

        register_test("my_custom_test", my_runner)

    Args:
        name: Name of the test as used in project.yml / schema.yml (`type:` field).
        runner: Callable implementing the Runner protocol.
        overwrite: If False (default), attempting to override an existing name
                   raises ValueError. Set True to replace built-ins or earlier
                   registrations.

    Raises:
        ValueError: If name is empty or already registered (and overwrite=False).
        TypeError: If runner is not callable.
    """
    if not isinstance(name, (str, bytes)) or not str(name).strip():
        raise ValueError("Test name must be a non-empty string")

    if not callable(runner):
        raise TypeError("runner must be callable")

    key = str(name).strip()
    if key in TESTS and not overwrite:
        raise ValueError(
            f"Test '{key}' is already registered. "
            "Pass overwrite=True to replace the existing runner."
        )

    TESTS[key] = runner

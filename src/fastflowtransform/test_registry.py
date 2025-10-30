from __future__ import annotations

from typing import Any, Protocol

from fastflowtransform import testing


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


def _sql_list(values: list[Any]) -> str:
    """Render a simple SQL literal list, portable enough for DuckDB/Postgres/BigQuery."""

    def lit(v: Any) -> str:
        if v is None:
            return "NULL"
        if isinstance(v, (int, float)):
            return str(v)
        s = str(v).replace("'", "''")
        return f"'{s}'"

    return ", ".join(lit(v) for v in (values or []))


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
    values = params.get("values") or []
    where = params.get("where")
    in_list = _sql_list(values)
    example = (
        f"select distinct {column} from {table} "
        + f"where {column} is not null and {column} not in ({in_list})"
        + (f" and ({where})" if where else "")
        + " limit 5"
    )
    if column is None:
        return False, "missing required parameter: column", example
    col = column
    try:
        testing.accepted_values(con, table, col, values=values, where=where)
        return True, None, example
    except testing.TestFailure as e:
        return False, str(e), example


# Public registry (extensible).
TESTS: dict[str, Runner] = {
    "not_null": run_not_null,
    "unique": run_unique,
    "accepted_values": run_accepted_values,  # NEW
    # "relationships": run_relationships,    # (later)
}

# src/fastflowtransform/testing.py
from __future__ import annotations

from collections.abc import Iterable, Sequence
from typing import Any, cast

from sqlalchemy import text
from sqlalchemy.engine import Connection as _SAConn
from sqlalchemy.sql.elements import ClauseElement

from fastflowtransform.logging import dprint

# ===== Execution helpers (consistent for DuckDB / Postgres / BigQuery) ==


def _exec(con: Any, sql: Any) -> Any:
    """
    Execute SQL robustly and consistently.
    Accepts:
      - str
      - (str, params: dict)
      - SQLAlchemy ClauseElement (if available)
      - Sequence[ of the above types ]  -> executed sequentially (return result of the last)
    Delegates to `con.execute(sql)` when available (e.g. DuckDB or our executor shims).
    Fallback: use Connection.begin() + SQLAlchemy text().
    """
    # 1) Direct delegation to existing con.execute (e.g. DuckDB, our PG/BQ shims)
    if hasattr(con, "execute"):
        dprint("con.execute <-", _pretty_sql(sql))
        try:
            if isinstance(con, _SAConn) or "sqlalchemy" in type(con).__module__:
                sql_tuple_len = 2
                if isinstance(sql, str):
                    return con.execute(text(sql))
                if (
                    isinstance(sql, tuple)
                    and len(sql) == sql_tuple_len
                    and isinstance(sql[0], str)
                    and isinstance(sql[1], dict)
                ):
                    return con.execute(text(sql[0]), sql[1])
            return cast(Any, con).execute(sql)
        except Exception:
            # The check name is unknown at this point → the caller adds that context
            raise

    # 2) Fallback: generic SQLAlchemy handling

    statement_tuple_len = 2

    def _exec_one(c: Any, stmt: Any) -> Any:
        if (
            isinstance(stmt, tuple)
            and len(stmt) == statement_tuple_len
            and isinstance(stmt[0], str)
            and isinstance(stmt[1], dict)
        ):
            dprint("run (sql, params):", stmt[0], stmt[1])
            return c.execute(text(stmt[0]), stmt[1])
        if isinstance(stmt, ClauseElement):
            dprint("run ClauseElement")
            return c.execute(stmt)
        if isinstance(stmt, str):
            dprint("run sql:", stmt)
            return c.execute(text(stmt))
        # Sequences (recursive)
        if isinstance(stmt, Iterable) and not isinstance(stmt, (bytes, bytearray, str)):
            res = None
            for s in stmt:
                res = _exec_one(c, s)
            return res
        raise TypeError(f"Unsupported statement type: {type(stmt)} → {stmt!r}")

    if hasattr(con, "begin"):
        with con.begin() as c:
            return _exec_one(c, sql)
    # Last resort: best effort
    return _exec_one(con, sql)


def _scalar(con: Any, sql: Any) -> Any:
    """Execute SQL and return the first column of the first row (or None)."""
    try:
        res = _exec(con, sql)
    except Exception as e:
        # Caller adds the check name in _fail()
        raise e
    row = getattr(res, "fetchone", lambda: None)()
    return None if row is None else row[0]


def _fail(check: str, table: str, column: str | None, sql: str, detail: str) -> None:
    raise TestFailure(
        f"[{check}] {table}{('.' + column) if column else ''}: {detail}\nSQL:\n{sql.strip()}"
    )


def _pretty_sql(sql: Any) -> str:
    """Compact, human-readable rendering for debugging."""
    sql_tuple_len = 2

    if isinstance(sql, str):
        return sql.strip()
    if isinstance(sql, tuple) and len(sql) == sql_tuple_len and isinstance(sql[0], str):
        return f"{sql[0].strip()}  -- params={sql[1]}"
    if isinstance(sql, ClauseElement):
        return "<SQLAlchemy ClauseElement>"
    if isinstance(sql, Sequence) and not isinstance(sql, (bytes, bytearray, str)):
        parts = []
        for s in sql:
            parts.append(_pretty_sql(s))
        return "[\n  " + ",\n  ".join(parts) + "\n]"
    return repr(sql)


def _sql_list(values: list) -> str:
    def lit(v):
        if v is None:
            return "NULL"
        if isinstance(v, (int, float)):
            return str(v)
        s = str(v).replace("'", "''")
        return f"'{s}'"

    return ", ".join(lit(v) for v in values)


def accepted_values(
    con: Any, table: str, column: str, *, values: list, where: str | None = None
) -> bool:
    """
    Checks that all non-NULL values of table.column are in the set 'values'.
    """
    in_list = _sql_list(values or [])
    sql = f"select count(*) from {table} where {column} is not null and {column} not in ({in_list})"
    if where:
        sql += f" and ({where})"
    n = _scalar(con, sql)
    if int(n or 0) > 0:
        # Beispielwerte zeigen
        sample_sql = (
            f"select distinct {column} "
            f"from {table} "
            f"where {column} is not null and {column} not in ({in_list})"
        )
        if where:
            sample_sql += f" and ({where})"
        sample_sql += " limit 5"
        rows = [r[0] for r in _exec(con, sample_sql).fetchall()]
        raise TestFailure(f"{table}.{column} has {n} value(s) outside accepted set; e.g. {rows}")
    return True


# ===== Tests ==============================================================


class TestFailure(Exception):
    """Raised when a data-quality check fails."""

    # Prevent pytest from collecting this as a test when imported into a test module.
    __test__ = False
    pass


def _wrap_db_error(
    check: str, table: str, column: str | None, sql: str, err: Exception
) -> TestFailure:
    msg = [f"[{check}] Error in {table}{('.' + column) if column else ''}"]
    msg.append(f"DB-Error: {type(err).__name__}: {err}")
    # Common Postgres/SQLAlchemy hints
    txt = str(err).lower()
    if "undefinedcolumn" in txt and "having" in sql.lower():
        msg.append("Note: Postgres does not permit alias usage in HAVING statement.")
    if "f405" in txt or "textual sql expression" in txt:
        msg.append("Note: SQLAlchemy 2.0 requires text('...') for raw SQL stings.")
    msg.append("SQL:\n" + sql.strip())
    return TestFailure("\n".join(msg))


def not_null(con: Any, table: str, column: str, where: str | None = None) -> None:
    """Fails if any non-filtered row has NULL in `column`."""
    sql = f"select count(*) from {table} where {column} is null"
    if where:
        sql += f" and ({where})"
    try:
        c = _scalar(con, sql)
    except Exception as e:
        raise _wrap_db_error("not_null", table, column, sql, e) from e
    c = _scalar(con, sql)
    dprint("not_null:", sql, "=>", c)
    if c and c != 0:
        _fail("not_null", table, column, sql, f"has {c} NULL-values")


def unique(con: Any, table: str, column: str, where: str | None = None) -> None:
    """Fails if any duplicate appears in `column` within the (optionally) filtered set."""
    sql = (
        "select count(*) from (select {col} as v, "
        "count(*) as c from {tbl}{w} group by 1 having count(*) > 1) as q"
    )
    w = f" where ({where})" if where else ""
    sql = sql.format(col=column, tbl=table, w=w)
    try:
        c = _scalar(con, sql)
    except Exception as e:
        raise _wrap_db_error("unique", table, column, sql, e) from e
    dprint("unique:", sql, "=>", c)
    if c and c != 0:
        _fail("unique", table, column, sql, f"contains {c} duplicates")


def greater_equal(con: Any, table: str, column: str, threshold: float = 0.0) -> None:
    sql = f"select count(*) from {table} where {column} < {threshold}"
    c = _scalar(con, sql)
    dprint("greater_equal:", sql, "=>", c)
    if c and c != 0:
        raise TestFailure(f"{table}.{column} has {c} values < {threshold}")


def non_negative_sum(con: Any, table: str, column: str) -> None:
    sql = f"select coalesce(sum({column}),0) from {table}"
    s = _scalar(con, sql)
    dprint("non_negative_sum:", sql, "=>", s)
    if s is not None and s < 0:
        raise TestFailure(f"sum({table}.{column}) is negative: {s}")


def row_count_between(con: Any, table: str, min_rows: int = 1, max_rows: int | None = None) -> None:
    sql = f"select count(*) from {table}"
    c = _scalar(con, sql)
    dprint("row_count_between:", sql, "=>", c)
    if c is None or c < min_rows:
        raise TestFailure(f"{table} has too few rows: {c} < {min_rows}")
    if max_rows is not None and c > max_rows:
        raise TestFailure(f"{table} has too many rows: {c} > {max_rows}")


def freshness(con: Any, table: str, ts_col: str, max_delay_minutes: int) -> None:
    # Straightforward for DuckDB/PG; BQ would need a TIMESTAMPDIFF variant.
    sql = f"select date_part('epoch', now() - max({ts_col})) / 60.0 as delay_min from {table}"
    # Note: DuckDB has different date_diff signatures; for DuckDB-only might use:
    # sql_duckdb =
    # f\"\"\"select date_diff('minute', max({ts_col}), now()) as delay_min from {table}\"\"\"
    delay = _scalar(con, sql)
    dprint("freshness:", sql, "=>", delay)
    if delay is None or delay > max_delay_minutes:
        raise TestFailure(
            f"freshness of {table}.{ts_col} too old: {delay} min > {max_delay_minutes} min"
        )


# ===== Cross-table reconciliations (FF-310) ======================================


def _scalar_where(con: Any, table: str, expr: str, where: str | None = None) -> Any:
    """Return the first scalar from `SELECT {expr} FROM {table} [WHERE ...]`."""
    sql = f"select {expr} from {table}" + (f" where {where}" if where else "")
    dprint("reconcile:", sql)
    return _scalar(con, sql)


def reconcile_equal(
    con: Any,
    left: dict,
    right: dict,
    abs_tolerance: float | None = None,
    rel_tolerance_pct: float | None = None,
) -> None:
    """Assert left == right within absolute and/or relative tolerances.

    Both sides are dictionaries: {"table": str, "expr": str, "where": Optional[str]}.
    If both tolerances are omitted, exact equality is enforced.
    """
    L = _scalar_where(con, left["table"], left["expr"], left.get("where"))
    R = _scalar_where(con, right["table"], right["expr"], right.get("where"))
    if L is None or R is None:
        raise TestFailure(f"One side is NULL (left={L}, right={R})")
    diff = abs(float(L) - float(R))

    # Absolute tolerance check
    if abs_tolerance is not None and diff <= float(abs_tolerance):
        return

    # Relative tolerance check (percentage)
    if rel_tolerance_pct is not None:
        denom = max(abs(float(R)), 1e-12)
        rel = diff / denom
        if (rel * 100.0) <= float(rel_tolerance_pct):
            return

    # If neither tolerance was provided, enforce strict equality via diff==0.
    if abs_tolerance is None and rel_tolerance_pct is None and diff == 0.0:
        return
    raise TestFailure(
        f"Reconcile equal failed: left={L}, right={R}, diff={diff}, "
        f"rel%={(diff / max(abs(float(R)), 1e-12)) * 100:.6f}"
    )


def reconcile_ratio_within(
    con: Any, left: dict, right: dict, min_ratio: float, max_ratio: float
) -> None:
    """Assert min_ratio <= (left/right) <= max_ratio."""
    L = _scalar_where(con, left["table"], left["expr"], left.get("where"))
    R = _scalar_where(con, right["table"], right["expr"], right.get("where"))
    if L is None or R is None:
        raise TestFailure(f"One side is NULL (left={L}, right={R})")
    eps = 1e-12
    denom = float(R) if abs(float(R)) > eps else eps
    ratio = float(L) / denom
    if not (float(min_ratio) <= ratio <= float(max_ratio)):
        raise TestFailure(
            f"Ratio {ratio:.6f} out of bounds [{min_ratio}, {max_ratio}] (L={L}, R={R})"
        )


def reconcile_diff_within(con: Any, left: dict, right: dict, max_abs_diff: float) -> None:
    """Assert |left - right| <= max_abs_diff."""
    L = _scalar_where(con, left["table"], left["expr"], left.get("where"))
    R = _scalar_where(con, right["table"], right["expr"], right.get("where"))
    if L is None or R is None:
        raise TestFailure(f"One side is NULL (left={L}, right={R})")
    diff = abs(float(L) - float(R))
    if diff > float(max_abs_diff):
        raise TestFailure(f"Abs diff {diff} > max_abs_diff {max_abs_diff} (L={L}, R={R})")


def reconcile_coverage(
    con: Any,
    source: dict,
    target: dict,
    source_where: str | None = None,
    target_where: str | None = None,
) -> None:
    """Assert that every key from `source` exists in `target` (anti-join count == 0)."""
    s_tbl, s_key = source["table"], source["key"]
    t_tbl, t_key = target["table"], target["key"]
    s_w = f" where {source_where}" if source_where else ""
    t_w = f" where {target_where}" if target_where else ""
    sql = f"""
      with src as (select {s_key} as k from {s_tbl}{s_w}),
           tgt as (select {t_key} as k from {t_tbl}{t_w})
      select count(*) from src s
      left join tgt t on s.k = t.k
      where t.k is null
    """
    missing = _scalar(con, sql)
    dprint("reconcile_coverage:", sql, "=>", missing)
    if missing and missing != 0:
        raise TestFailure(f"Coverage failed: {missing} source keys missing in target")

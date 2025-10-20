# src/fastflowtransform/testing.py
from __future__ import annotations

import logging
import os
from collections.abc import Iterable, Sequence
from typing import Any, cast

from sqlalchemy import text
from sqlalchemy.engine import Connection as _SAConn
from sqlalchemy.sql.elements import ClauseElement

# ===== Debug toggle =====================================================


def _dprint(*a):
    if logging.getLogger("fastflowtransform.sql").isEnabledFor(logging.DEBUG) or (
        os.getenv("FFT_SQL_DEBUG") == "1"
    ):
        print("[DQ]", *a)


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
        _dprint("con.execute <-", _pretty_sql(sql))
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
            _dprint("run (sql, params):", stmt[0], stmt[1])
            return c.execute(text(stmt[0]), stmt[1])
        if isinstance(stmt, ClauseElement):
            _dprint("run ClauseElement")
            return c.execute(stmt)
        if isinstance(stmt, str):
            _dprint("run sql:", stmt)
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


# ===== Tests ==============================================================


class TestFailure(Exception):
    """Error class for data-quality checks."""

    pass


def _wrap_db_error(
    check: str, table: str, column: str | None, sql: str, err: Exception
) -> TestFailure:
    msg = [f"[{check}] Fehler in {table}{('.' + column) if column else ''}"]
    msg.append(f"DB-Fehler: {type(err).__name__}: {err}")
    # Common Postgres/SQLAlchemy hints
    txt = str(err).lower()
    if "undefinedcolumn" in txt and "having" in sql.lower():
        msg.append(
            "Hinweis: Postgres erlaubt Alias-Nutzung im HAVING nicht. "
            "Lösung: Alias außerhalb verwenden (Subquery) oder die Bedingung "
            "in WHERE der äußeren Abfrage prüfen."
        )
    if "f405" in txt or "textual sql expression" in txt:
        msg.append("Hinweis: SQLAlchemy 2.0 verlangt text('...') für rohe SQL-Strings.")
    msg.append("SQL:\n" + sql.strip())
    return TestFailure("\n".join(msg))


def not_null(con: Any, table: str, column: str) -> None:
    sql = f"select count(*) from {table} where {column} is null"
    try:
        c = _scalar(con, sql)
    except Exception as e:
        raise _wrap_db_error("not_null", table, column, sql, e) from e
    c = _scalar(con, sql)
    _dprint("not_null:", sql, "=>", c)
    if c and c != 0:
        _fail("not_null", table, column, sql, f"hat {c} NULL-Werte")


def unique(con: Any, table: str, column: str) -> None:
    sql = (
        "select count(*) from ("
        f"  select {column}, count(*) as c"
        f"  from {table}"
        "  group by 1"
        ") t where c > 1"
    )
    try:
        c = _scalar(con, sql)
    except Exception as e:
        raise _wrap_db_error("unique", table, column, sql, e) from e
    _dprint("unique:", sql, "=>", c)
    if c and c != 0:
        _fail("unique", table, column, sql, f"enthält {c} Duplikate")


def greater_equal(con: Any, table: str, column: str, threshold: float = 0.0) -> None:
    sql = f"select count(*) from {table} where {column} < {threshold}"
    c = _scalar(con, sql)
    _dprint("greater_equal:", sql, "=>", c)
    if c and c != 0:
        raise TestFailure(f"{table}.{column} has {c} values < {threshold}")


def non_negative_sum(con: Any, table: str, column: str) -> None:
    sql = f"select coalesce(sum({column}),0) from {table}"
    s = _scalar(con, sql)
    _dprint("non_negative_sum:", sql, "=>", s)
    if s is not None and s < 0:
        raise TestFailure(f"sum({table}.{column}) is negative: {s}")


def row_count_between(con: Any, table: str, min_rows: int = 1, max_rows: int | None = None) -> None:
    sql = f"select count(*) from {table}"
    c = _scalar(con, sql)
    _dprint("row_count_between:", sql, "=>", c)
    if c is None or c < min_rows:
        raise TestFailure(f"{table} has too few rows: {c} < {min_rows}")
    if max_rows is not None and c > max_rows:
        raise TestFailure(f"{table} has too many rows: {c} > {max_rows}")


def freshness(con: Any, table: str, ts_col: str, max_delay_minutes: int) -> None:
    # Straightforward for DuckDB/PG; BQ would need a TIMESTAMPDIFF variant.
    sql = f"select date_part('epoch', now() - max({ts_col})) / 60.0 as delay_min from {table}"
    # Note: DuckDB has different date_diff signatures; for DuckDB-only you could use:
    # sql_duckdb =
    # f\"\"\"select date_diff('minute', max({ts_col}), now()) as delay_min from {table}\"\"\"
    delay = _scalar(con, sql)
    _dprint("freshness:", sql, "=>", delay)
    if delay is None or delay > max_delay_minutes:
        raise TestFailure(
            f"freshness of {table}.{ts_col} too old: {delay} min > {max_delay_minutes} min"
        )


# ===== Cross-table reconciliations (FF-310) ======================================


def _scalar_where(con: Any, table: str, expr: str, where: str | None = None) -> Any:
    """Return the first scalar from `SELECT {expr} FROM {table} [WHERE ...]`."""
    sql = f"select {expr} from {table}" + (f" where {where}" if where else "")
    _dprint("reconcile:", sql)
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
    _dprint("reconcile_coverage:", sql, "=>", missing)
    if missing and missing != 0:
        raise TestFailure(f"Coverage failed: {missing} source keys missing in target")

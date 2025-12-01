# fastflowtransform/stdlib/dates.py
from __future__ import annotations

from .engine import normalize_engine


def _clean_expr(expr: str) -> str:
    """
    Treat `expr` as a raw SQL snippet (column name, expression, etc.)
    and strip surrounding whitespace.
    """
    return expr.strip()


def sql_date_trunc(expr: str, part: str = "day", *, engine: str | None = None) -> str:
    """
    Build an engine-aware DATE_TRUNC expression.

    Parameters
    ----------
    expr:
        SQL expression / column reference, e.g. "order_date" or "CAST(ts AS TIMESTAMP)".
    part:
        Date part like "day", "month", "year", "week", ...
    engine:
        Engine key/hint (e.g. "duckdb", "postgres", "bigquery").
        If omitted, "generic" semantics are used.

    Examples (golden SQL)
    ---------------------
    DuckDB / Postgres / Redshift / Snowflake / Spark:
        sql_date_trunc("order_date", "day", engine="duckdb")
        -> "date_trunc('day', order_date)"

    BigQuery:
        sql_date_trunc("order_date", "day", engine="bigquery")
        -> "DATE_TRUNC(order_date, DAY)"

    Generic:
        sql_date_trunc("created_at", "month")
        -> "date_trunc('month', created_at)"
    """
    eng = normalize_engine(engine)
    expr_sql = _clean_expr(expr)
    part_norm = part.strip().lower()
    if not part_norm:
        raise ValueError("date part must be a non-empty string")

    # Engines like DuckDB want date_trunc('<part>', <TIMESTAMP>)
    if eng in {"duckdb", "postgres", "redshift", "snowflake", "spark", "generic"}:
        return f"date_trunc('{part_norm}', CAST({expr_sql} AS TIMESTAMP))"

    if eng == "bigquery":
        # DATE_TRUNC(timestamp_expression, date_part)
        part_upper = part_norm.upper()
        return f"DATE_TRUNC(CAST({expr_sql} AS TIMESTAMP), {part_upper})"

    # Fallback: ANSI-ish
    return f"date_trunc('{part_norm}', CAST({expr_sql} AS TIMESTAMP))"


def sql_date_add(
    expr: str,
    part: str,
    amount: int,
    *,
    engine: str | None = None,
) -> str:
    """
    Build an engine-aware date / timestamp addition expression.

    Parameters
    ----------
    expr:
        SQL expression / column reference to add to.
    part:
        "day", "month", "year", ... (engine-specific support may vary).
    amount:
        Integer offset (positive or negative).
    engine:
        Engine key/hint ("duckdb", "postgres", "bigquery", "snowflake", "spark", ...).

    Examples (golden SQL)
    ---------------------
    DuckDB / Postgres / Redshift / Generic:
        sql_date_add("order_date", "day", 3, engine="duckdb")
        -> "CAST(order_date AS TIMESTAMP) + INTERVAL '3 day'"

    Snowflake:
        sql_date_add("created_at", "month", 1, engine="snowflake")
        -> "DATEADD(MONTH, 1, created_at)"

    BigQuery:
        sql_date_add("order_date", "day", -7, engine="bigquery")
        -> "DATE_ADD(order_date, INTERVAL -7 DAY)"
    """
    eng = normalize_engine(engine)
    expr_sql = _clean_expr(expr)
    part_norm = part.strip().lower()
    if not part_norm:
        raise ValueError("date part must be a non-empty string")
    amt = int(amount)

    if eng in {"duckdb", "postgres", "redshift", "generic"}:
        # For these engines we usually want TIMESTAMP + INTERVAL.
        # Heuristic: if the expression already contains a cast, don't wrap it again.
        lower_expr = expr_sql.lower()
        already_cast = (
            "cast(" in lower_expr
            or "::timestamp" in lower_expr
            or "::timestamptz" in lower_expr
            or "::date" in lower_expr
        )
        base_expr = expr_sql if already_cast else f"CAST({expr_sql} AS TIMESTAMP)"
        return f"{base_expr} + INTERVAL '{amt} {part_norm}'"

    if eng == "spark":
        # Spark has DATE_ADD(date, days) for day-precision, but not all parts.
        if part_norm == "day":
            return f"date_add({expr_sql}, {amt})"
        # fall back to ANSI-ish INTERVAL for other parts
        return f"{expr_sql} + INTERVAL {amt} {part_norm.upper()}"

    if eng == "snowflake":
        part_upper = part_norm.upper()
        # Make sure we're not doing VARCHAR + INTERVAL
        expr_ts = f"TO_TIMESTAMP({expr_sql})"
        return f"DATEADD({part_upper}, {amt}, {expr_ts})"

    if eng == "bigquery":
        part_upper = part_norm.upper()

        # If the user already passed a CAST(...) or SAFE_CAST(...), don't double-wrap.
        lower_expr = expr_sql.lower().replace(" ", "")
        already_casted = lower_expr.startswith("cast(") or lower_expr.startswith("safe_cast(")

        expr_for_bq = expr_sql
        if not already_casted:
            # Be permissive: coerce to TIMESTAMP so strings like '2025-10-01T12:00:00'
            # work out of the box.
            expr_for_bq = f"CAST({expr_sql} AS TIMESTAMP)"

        # For dates/timestamps BigQuery supports this signature:
        #   DATE_ADD(timestamp_expr, INTERVAL amt PART)
        return f"DATE_ADD({expr_for_bq}, INTERVAL {amt} {part_upper})"

    # Fallback: ANSI-ish
    return f"{expr_sql} + INTERVAL '{amt} {part_norm}'"

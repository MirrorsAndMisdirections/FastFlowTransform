# fastflowtransform/stdlib/casts.py
from __future__ import annotations

from .engine import normalize_engine


def sql_safe_cast(
    expr: str,
    target_type: str,
    *,
    default: str | None = None,
    engine: str | None = None,
) -> str:
    """
    Engine-aware “safe cast” builder.

    Semantics by engine
    -------------------
    DuckDB:
        TRY_CAST(expr AS type)
    BigQuery:
        SAFE_CAST(expr AS type)
    Spark (3.x+):
        TRY_CAST(expr AS type)
    Snowflake:
        CAST(expr AS type)      # TRY_CAST(FLOAT -> NUMBER) is not supported
    Postgres / Redshift / Generic:
        CAST(expr AS type)

    If `default` is provided, it is treated as a raw SQL snippet and
    wrapped via COALESCE(…, default).
    """
    eng = normalize_engine(engine)
    expr_sql = expr.strip()
    raw_type = target_type.strip()
    if not expr_sql:
        raise ValueError("expr must be a non-empty SQL expression")
    if not raw_type:
        raise ValueError("target_type must be a non-empty SQL type")

    # Normalize logical numeric/decimal types per engine
    norm = raw_type.lower()
    if norm in {"numeric", "number", "decimal"}:
        if eng == "bigquery":
            # BigQuery fixed-precision decimal
            type_sql = "NUMERIC"
        elif eng in {"duckdb", "postgres", "redshift"}:
            type_sql = "NUMERIC"
        elif eng == "snowflake":
            # Use a concrete NUMBER with scale, but via plain CAST (no TRY_CAST)
            type_sql = "NUMBER(38,10)"
        else:
            type_sql = "NUMERIC"
    else:
        type_sql = raw_type

    # Engine-specific safe cast core
    if eng == "bigquery":
        inner = f"SAFE_CAST({expr_sql} AS {type_sql})"
    elif eng == "duckdb":
        inner = f"try_cast({expr_sql} AS {type_sql})"
    elif eng == "spark":
        inner = f"TRY_CAST({expr_sql} AS {type_sql})"
    elif eng == "snowflake":
        # TRY_CAST(FLOAT -> NUMBER(...)) is not allowed, so we use plain CAST
        inner = f"CAST({expr_sql} AS {type_sql})"
    else:
        # Fallback: no TRY_/SAFE_ variant
        inner = f"CAST({expr_sql} AS {type_sql})"

    if default is not None:
        default_sql = default.strip()
        if not default_sql:
            return inner
        return f"COALESCE({inner}, {default_sql})"

    return inner

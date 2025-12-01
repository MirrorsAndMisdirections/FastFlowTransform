# fastflowtransform/stdlib/partitions.py
from __future__ import annotations

from collections.abc import Iterable
from typing import Any

from .engine import normalize_engine
from .sql import sql_literal


def _lit(value: Any) -> str:
    """
    Delegate to core.sql_literal() so Python values become correct SQL literals.
    """
    return sql_literal(value)


def sql_partition_filter(
    column: str,
    start: Any | None = None,
    end: Any | None = None,
    *,
    engine: str | None = None,  # reserved for future engine-specific tweaks
) -> str:
    """
    Build a WHERE predicate for a *range* of partition values.

    Semantics:
      - start only → col >= <start_literal>
      - end only   → col <= <end_literal>
      - both       → col BETWEEN <start_literal> AND <end_literal>
      - neither    → "1=1" (no-op filter)

    `start` and `end` are Python values and will be converted with sql_literal(),
    so you can safely pass `datetime.date`, `datetime.datetime`, strings, ints, etc.

    Parameters
    ----------
    column:
        Partition column name / expression, e.g. "ds" or "DATE(event_time)".
    start, end:
        Python values interpreted as partition bounds.
    engine:
        Currently unused but accepted so callers can pass it consistently.

    Examples (golden SQL)
    ---------------------
    Daily date partition:
        sql_partition_filter("ds", date(2024, 1, 1), date(2024, 1, 31))
        -> "ds BETWEEN '2024-01-01' AND '2024-01-31'"

    Open interval:
        sql_partition_filter("ds", start=date(2024, 1, 1), end=None)
        -> "ds >= '2024-01-01'"
    """
    _ = normalize_engine(engine)  # placeholder for future branching

    col = column.strip()
    if not col:
        raise ValueError("column must be a non-empty SQL expression")

    if start is None and end is None:
        return "1=1"

    conds: list[str] = []
    if start is not None and end is not None:
        conds.append(f"{col} BETWEEN {_lit(start)} AND {_lit(end)}")
    else:
        if start is not None:
            conds.append(f"{col} >= {_lit(start)}")
        if end is not None:
            conds.append(f"{col} <= {_lit(end)}")

    return " AND ".join(conds)


def sql_partition_in(
    column: str,
    values: Iterable[Any],
    *,
    engine: str | None = None,  # reserved for future engine-specific tweaks
) -> str:
    """
    Build an IN() predicate for a set of partition values.

    - Empty values → "1=0" (guaranteed false, useful for guard rails).
    - Non-empty   → col IN (<literal1>, <literal2>, ...)

    Examples (golden SQL)
    ---------------------
    Daily partitions:
        sql_partition_in("ds", [date(2024, 1, 1), date(2024, 1, 2)])
        -> "ds IN ('2024-01-01', '2024-01-02')"

    String partitions:
        sql_partition_in("region", ["EU", "US"])
        -> "region IN ('EU', 'US')"
    """
    _ = normalize_engine(engine)  # placeholder for future branching

    col = column.strip()
    if not col:
        raise ValueError("column must be a non-empty SQL expression")

    vals = list(values or [])
    if not vals:
        return "1=0"

    literals = ", ".join(_lit(v) for v in vals)
    return f"{col} IN ({literals})"

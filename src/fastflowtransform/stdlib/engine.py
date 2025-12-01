# fastflowtransform/stdlib/engine.py
from __future__ import annotations

from typing import Final

# Canonical engine keys we care about. This is intentionally small and focused.
# Unknown values will just be normalized to lower-case and treated as-is.
_ENGINE_ALIASES: Final[dict[str, str]] = {
    # DuckDB
    "duckdb": "duckdb",
    # Postgres family
    "postgres": "postgres",
    "postgresql": "postgres",
    "psql": "postgres",
    # BigQuery
    "bigquery": "bigquery",
    "bq": "bigquery",
    # Snowflake
    "snowflake": "snowflake",
    "snowflake_snowpark": "snowflake",
    "sf": "snowflake",
    # Spark / Databricks
    "spark": "spark",
    "databricks": "spark",
    "databricks_spark": "spark",
}


def normalize_engine(engine: str | None) -> str:
    """
    Normalize an engine string into a canonical key.

    - None / empty → "generic"
    - Known aliases → canonical (e.g. "postgresql" → "postgres")
    - Other values  → lower-case as-is

    Examples
    --------
    >>> normalize_engine("Postgres")
    'postgres'
    >>> normalize_engine("databricks_spark")
    'spark'
    >>> normalize_engine(None)
    'generic'
    """
    if not engine:
        return "generic"
    key = engine.strip().lower()
    if not key:
        return "generic"
    return _ENGINE_ALIASES.get(key, key)


def engine_family(engine: str | None) -> str:
    """
    Return a broad engine *family* key.

    For now this is identical to normalize_engine(), but having a separate
    function makes it easy to distinguish “exact engine” vs “family” later.
    """
    return normalize_engine(engine)


def is_engine(engine: str | None, *candidates: str) -> bool:
    """
    Convenience helper: check if `engine` matches any of the given candidates.

    Examples
    --------
    >>> is_engine("duckdb", "duckdb", "postgres")
    True
    >>> is_engine("bigquery", "duckdb", "postgres")
    False
    """
    norm = normalize_engine(engine)
    cand_norm = {normalize_engine(c) for c in candidates}
    return norm in cand_norm

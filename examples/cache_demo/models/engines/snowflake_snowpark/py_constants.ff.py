from __future__ import annotations

from typing import TYPE_CHECKING, Any

from fastflowtransform import engine_model

if TYPE_CHECKING:  # pragma: no cover - typing only
    from snowflake.snowpark import DataFrame as SnowparkDataFrame
    from snowflake.snowpark import Session as SnowparkSession
else:  # pragma: no cover - runtime fallback
    SnowparkDataFrame = Any
    SnowparkSession = Any


def _ensure_session(df: Any) -> "SnowparkSession":
    try:
        from snowflake.snowpark import Session as _Session
    except Exception as exc:  # pragma: no cover - optional dep guard
        raise RuntimeError(
            "snowflake-snowpark-python is required for this model. "
            "Install fastflowtransform[snowflake]."
        ) from exc

    session = getattr(df, "session", None)
    if session is None:
        raise RuntimeError(
            "Snowpark session missing on upstream DataFrame. "
            "Ensure Snowflake Snowpark is selected via profiles.yml.",
        )
    return session


@engine_model(
    only="snowflake_snowpark",
    name="py_constants",
    deps=["stg_users.ff"],
    materialized="table",
    tags=[
        "example:cache_demo",
        "engine:snowflake_snowpark",
    ],
)
def build(stg_users: SnowparkDataFrame) -> SnowparkDataFrame:
    """Snowpark variant that materializes the constant table."""
    session = _ensure_session(stg_users)
    rows = [("answer", 42)]
    return session.create_dataframe(rows, schema=["k", "v"])

from __future__ import annotations

from typing import TYPE_CHECKING, Any

from fastflowtransform import engine_model

if TYPE_CHECKING:  # pragma: no cover - typing only
    from snowflake.snowpark import DataFrame as SnowparkDataFrame
else:  # pragma: no cover - runtime fallback
    SnowparkDataFrame = Any


def _ensure_session(df: Any):
    session = getattr(df, "session", None)
    if session is None:
        raise RuntimeError(
            "Snowpark session missing on upstream DataFrame. "
            "Ensure Snowflake Snowpark is the active engine."
        )
    return session


def _snowflake_functions():
    try:
        from snowflake.snowpark import functions as F  # type: ignore
    except Exception as exc:  # pragma: no cover - optional dep guard
        raise RuntimeError(
            "snowflake-snowpark-python is required for this model. "
            "Install fastflowtransform[snowflake]."
        ) from exc
    return F


@engine_model(
    only="snowflake_snowpark",
    name="demo_py_emit",
    deps=["dim_customers.ff"],
    tags=["example:materializations_demo", "scope:python", "engine:snowflake_snowpark"],
)
def fetch(dim_customers: SnowparkDataFrame) -> SnowparkDataFrame:
    session = _ensure_session(dim_customers)
    F = _snowflake_functions()
    df = session.create_dataframe([("hello from python (Snowflake)",)], schema=["note"])
    return df.withColumn("emitted_at", F.current_timestamp())

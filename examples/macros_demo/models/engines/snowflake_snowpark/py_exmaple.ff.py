from __future__ import annotations

from typing import TYPE_CHECKING, Any

from fastflowtransform import engine_model

if TYPE_CHECKING:  # pragma: no cover - typing only
    from snowflake.snowpark import DataFrame as SnowparkDataFrame
else:  # pragma: no cover - runtime fallback
    SnowparkDataFrame = Any


def _get_session(df: Any) -> SnowparkDataFrame:
    session = getattr(df, "session", None)
    if session is None:
        raise RuntimeError(
            "Snowpark session missing on upstream DataFrame. "
            "Ensure Snowflake Snowpark is the active engine."
        )
    return session


@engine_model(
    only="snowflake_snowpark",
    name="py_example",
    deps=["fct_user_sales.ff"],
    tags=["example:macros_demo", "scope:engine", "engine:snowflake_snowpark"],
)
def produce(sales_df: SnowparkDataFrame) -> SnowparkDataFrame:
    session = _get_session(sales_df)
    data = [{"note": "Python model ran on Snowflake Snowpark"}]
    return session.create_dataframe(data)

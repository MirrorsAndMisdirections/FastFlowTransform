from __future__ import annotations

from typing import TYPE_CHECKING, Any

from fastflowtransform import engine_model

if TYPE_CHECKING:  # pragma: no cover - typing only
    from snowflake.snowpark import DataFrame as SnowparkDataFrame
else:  # pragma: no cover - runtime fallback
    SnowparkDataFrame = Any


def _get_snowpark_functions():
    try:
        from snowflake.snowpark import functions as _functions
    except Exception as exc:  # pragma: no cover - optional dependency guard
        raise RuntimeError(
            "snowflake-snowpark-python is required for this model. "
            "Install fastflowtransform[snowflake]."
        ) from exc
    return _functions


@engine_model(
    only="snowflake_snowpark",
    name="fct_events_py_incremental",
    deps=["events_base.ff"],
    tags=[
        "example:incremental_demo",
        "scope:engine",
        "engine:snowflake_snowpark",
        "kind:python",
        "kind:incremental",
    ],
)
def build(events_df: SnowparkDataFrame) -> SnowparkDataFrame:
    """Snowpark variant of the incremental Python model."""
    F = _get_snowpark_functions()
    df = events_df.withColumn("value_x10", F.col("value") * F.lit(10))
    return df.select("event_id", "updated_at", "value", "value_x10")

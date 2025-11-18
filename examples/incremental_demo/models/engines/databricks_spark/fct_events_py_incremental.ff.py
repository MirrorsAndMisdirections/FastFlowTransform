from typing import TYPE_CHECKING, Any

from fastflowtransform import engine_model

if TYPE_CHECKING:
    from pyspark.sql import DataFrame as SparkDataFrame
    from pyspark.sql import functions as F
else:
    SparkDataFrame = Any
    F = Any


def _get_functions() -> Any:
    try:
        from pyspark.sql import functions as _F
    except Exception as exc:  # pragma: no cover - optional dep guard
        raise RuntimeError(
            "pyspark is required for this model. Install fastflowtransform[spark]."
        ) from exc
    return _F


@engine_model(
    only="databricks_spark",
    name="fct_events_py_incremental",
    deps=["events_base.ff"],
    tags=[
        "example:incremental_demo",
        "scope:engine",
        "engine:databricks_spark",
        "kind:python",
        "kind:incremental",
    ],
)
def build(events_df: SparkDataFrame) -> SparkDataFrame:
    """
    Python-Incremental-Example (Databricks Spark).
    """
    _F = _get_functions()
    return events_df.withColumn("value_x10", _F.col("value") * _F.lit(10)).select(
        "event_id", "updated_at", "value", "value_x10"
    )

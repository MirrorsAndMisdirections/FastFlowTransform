from fastflowtransform import engine_model

try:
    from pyspark.sql import DataFrame as SparkDataFrame
    from pyspark.sql import functions as F
except Exception:  # pragma: no cover - optional dep guard
    from typing import Any

    SparkDataFrame = Any  # type: ignore[misc]
    F = None  # type: ignore[assignment]
    _spark_import_error = RuntimeError(
        "pyspark is required for this model. Install fastflowtransform[spark]."
    )
else:
    _spark_import_error = None


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
    if _spark_import_error:
        raise _spark_import_error

    return events_df.withColumn("value_x10", F.col("value") * F.lit(10)).select(
        "event_id", "updated_at", "value", "value_x10"
    )

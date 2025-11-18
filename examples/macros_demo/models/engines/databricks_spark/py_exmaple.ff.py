from typing import TYPE_CHECKING, Any

from fastflowtransform import engine_model

if TYPE_CHECKING:
    from pyspark.sql import DataFrame as SparkDataFrame
else:

    class SparkDataFrame:  # pragma: no cover - placeholder for runtime type hints
        ...


def _ensure_spark_session(df: Any):
    try:
        from pyspark.sql import SparkSession as _SparkSession
    except Exception as exc:  # pragma: no cover - optional dep guard
        raise RuntimeError(
            "pyspark is required for this model. Install fastflowtransform[spark]."
        ) from exc

    session = getattr(df, "sparkSession", None)
    if session is None:
        session = _SparkSession.getActiveSession()
    if session is None:
        session = _SparkSession.builder.getOrCreate()
    return session


@engine_model(
    only="databricks_spark",
    name="py_example",
    deps=["fct_user_sales.ff"],
    tags=["example:macros_demo", "scope:engine", "engine:databricks_spark"],
)
def produce(df: SparkDataFrame) -> SparkDataFrame:
    # Use the incoming Spark session to return a simple marker row
    spark = _ensure_spark_session(df)
    return spark.createDataFrame([{"note": "Python model ran on Databricks Spark"}])

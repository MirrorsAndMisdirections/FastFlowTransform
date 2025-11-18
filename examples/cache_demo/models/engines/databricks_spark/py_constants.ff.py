try:
    from pyspark.sql import SparkSession
except Exception:  # pragma: no cover - optional dep guard
    SparkSession = None  # type: ignore[assignment]
    _spark_import_error = RuntimeError(
        "pyspark is required for this model. Install fastflowtransform[spark]."
    )
else:
    _spark_import_error = None

from fastflowtransform import engine_model


@engine_model(
    only="databricks_spark",
    name="py_constants",
    materialized="table",
    tags=[
        "example:cache_demo",
        "engine:databricks_spark",
    ],
)
def build():
    """Spark version returning a Spark DataFrame."""
    if _spark_import_error:
        raise _spark_import_error

    spark = SparkSession.getActiveSession() or SparkSession.builder.getOrCreate()
    return spark.createDataFrame([{"k": "answer", "v": 42}])

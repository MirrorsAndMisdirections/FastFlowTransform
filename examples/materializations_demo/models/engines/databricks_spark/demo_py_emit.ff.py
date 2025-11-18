from fastflowtransform import engine_model

try:
    from pyspark.sql import SparkSession
    from pyspark.sql import DataFrame as SparkDataFrame
except Exception:  # pragma: no cover - optional dep guard
    from typing import Any

    SparkSession = None  # type: ignore[assignment]
    SparkDataFrame = Any  # type: ignore[misc]
    _spark_import_error = RuntimeError(
        "pyspark is required for this model. Install fastflowtransform[spark]."
    )
else:
    _spark_import_error = None


@engine_model(
    only="databricks_spark",
    name="demo_py_emit",
    deps=["dim_customers.ff"],
    tags=["example:materializations_demo", "scope:python", "engine:databricks_spark"],
)
def fetch(_: SparkDataFrame) -> SparkDataFrame:
    if _spark_import_error:
        raise _spark_import_error

    spark = SparkSession.getActiveSession() or SparkSession.builder.getOrCreate()
    return spark.createDataFrame(
        [{"note": "hello from python", "emitted_at": "2020-01-01T00:00:00Z"}]
    )

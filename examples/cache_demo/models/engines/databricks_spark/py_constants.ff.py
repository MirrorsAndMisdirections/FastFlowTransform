from fastflowtransform import engine_model
from typing import TYPE_CHECKING, Any

if TYPE_CHECKING:
    from pyspark.sql import SparkSession
else:

    class SparkSession:  # pragma: no cover - placeholder for runtime type hints
        ...


def _ensure_spark_session() -> "SparkSession":
    try:
        from pyspark.sql import SparkSession as _SparkSession
    except Exception as exc:  # pragma: no cover - optional dep guard
        raise RuntimeError(
            "pyspark is required for this model. Install fastflowtransform[spark]."
        ) from exc
    return _SparkSession.getActiveSession() or _SparkSession.builder.getOrCreate()


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
    spark = _ensure_spark_session()
    return spark.createDataFrame([{"k": "answer", "v": 42}])

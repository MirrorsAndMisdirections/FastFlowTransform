from typing import TYPE_CHECKING, Any

from fastflowtransform import engine_model

if TYPE_CHECKING:
    from pyspark.sql import SparkSession
    from pyspark.sql import DataFrame as SparkDataFrame
else:

    class SparkSession:  # pragma: no cover - placeholder for runtime type hints
        ...

    class SparkDataFrame:  # pragma: no cover - placeholder for runtime type hints
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
    name="demo_py_emit",
    deps=["dim_customers.ff"],
    tags=["example:materializations_demo", "scope:python", "engine:databricks_spark"],
)
def fetch(_: SparkDataFrame) -> SparkDataFrame:
    spark = _ensure_spark_session()
    return spark.createDataFrame(
        [{"note": "hello from python", "emitted_at": "2020-01-01T00:00:00Z"}]
    )

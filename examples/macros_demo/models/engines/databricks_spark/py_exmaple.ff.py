from fastflowtransform import engine_model

try:
    from pyspark.sql import DataFrame
except Exception:  # pragma: no cover - optional dep guard
    from typing import Any

    DataFrame = Any  # type: ignore[misc]
    _spark_import_error = RuntimeError(
        "pyspark is required for this model. Install fastflowtransform[spark]."
    )
else:
    _spark_import_error = None


@engine_model(
    only="databricks_spark",
    name="py_example",
    deps=["fct_user_sales.ff"],
    tags=["example:macros_demo", "scope:engine", "engine:databricks_spark"],
)
def produce(df: DataFrame) -> DataFrame:
    # Use the incoming Spark session to return a simple marker row
    if _spark_import_error:
        raise _spark_import_error
    return df.sparkSession.createDataFrame([{"note": "Python model ran on Databricks Spark"}])

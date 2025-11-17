from pyspark.sql import SparkSession

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
    spark = SparkSession.getActiveSession() or SparkSession.builder.getOrCreate()
    return spark.createDataFrame([{"k": "answer", "v": 42}])

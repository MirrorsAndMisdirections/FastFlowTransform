from fastflowtransform import engine_model
from pyspark.sql import SparkSession
from pyspark.sql import DataFrame as SparkDataFrame


@engine_model(
    only="databricks_spark",
    name="demo_py_emit",
    deps=["dim_customers.ff"],
    tags=["example:materializations_demo", "scope:python", "engine:databricks_spark"],
)
def fetch(_: SparkDataFrame) -> SparkDataFrame:
    spark = SparkSession.getActiveSession() or SparkSession.builder.getOrCreate()
    return spark.createDataFrame(
        [{"note": "hello from python", "emitted_at": "2020-01-01T00:00:00Z"}]
    )

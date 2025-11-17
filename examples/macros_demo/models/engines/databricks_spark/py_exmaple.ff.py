from pyspark.sql import DataFrame

from fastflowtransform import engine_model


@engine_model(
    only="databricks_spark",
    name="py_example",
    deps=["fct_user_sales.ff"],
    tags=["example:macros_demo", "scope:engine", "engine:databricks_spark"],
)
def produce(df: DataFrame) -> DataFrame:
    # Use the incoming Spark session to return a simple marker row
    return df.sparkSession.createDataFrame([{"note": "Python model ran on Databricks Spark"}])

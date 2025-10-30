from fastflowtransform import engine_model
from fastflowtransform.api.http import get_df
from pyspark.sql import DataFrame as SparkDataFrame
from pyspark.sql import SparkSession


@engine_model(
    only="databricks_spark",
    name="api_users_http",
    deps=["users.ff"],
    tags=["example:api_demo", "scope:engine", "engine:databricks_spark"],
)
def fetch(users_df: SparkDataFrame) -> SparkDataFrame:
    """
    Fetch demo users via the FFT HTTP helper and return a Spark DataFrame.
    Leverages get_df(..., output='spark') to stay entirely in Spark.
    """
    spark = (
        users_df.sparkSession
        if isinstance(users_df, SparkDataFrame)
        else SparkSession.getActiveSession()
    )
    if spark is None:
        spark = SparkSession.builder.getOrCreate()

    df = get_df(
        url="https://jsonplaceholder.typicode.com/users",
        record_path=None,
        normalize=True,
        output="spark",
        session=spark,
    )
    return df.select("id", "email", "username", "name").withColumnRenamed("id", "api_user_id")

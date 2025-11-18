from fastflowtransform import engine_model
from pyspark.sql import DataFrame as SparkDataFrame
from pyspark.sql import SparkSession

try:
    import httpx
except Exception as _e:  # pragma: no cover
    raise RuntimeError("Please install 'httpx' to run this model") from _e


@engine_model(
    only="databricks_spark",
    name="api_users_requests",
    deps=["users.ff"],
    tags=["example:api_demo", "scope:engine", "engine:databricks_spark"],
)
def fetch(users_df: SparkDataFrame) -> SparkDataFrame:
    """
    Plain requests-based HTTP fetch that returns a Spark DataFrame.
    Useful when you need full control over authentication, retries, etc.
    """
    spark = (
        users_df.sparkSession
        if isinstance(users_df, SparkDataFrame)
        else SparkSession.getActiveSession()
    )
    if spark is None:
        spark = SparkSession.builder.getOrCreate()

    resp = httpx.get("https://jsonplaceholder.typicode.com/users", timeout=30.0)
    resp.raise_for_status()
    rows = resp.json()

    # Select a stable subset of columns and rename id -> api_user_id
    projected = [
        {
            "api_user_id": row.get("id"),
            "email": row.get("email"),
            "username": row.get("username"),
            "name": row.get("name"),
        }
        for row in rows
    ]

    return spark.createDataFrame(projected)

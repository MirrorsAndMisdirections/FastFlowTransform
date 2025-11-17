from pyspark.sql import DataFrame
from pyspark.sql.functions import col

from fastflowtransform import engine_model
from fastflowtransform.api.http import get_df


@engine_model(
    only="databricks_spark",
    name="http_users",
    deps=["stg_users.ff"],  # just to show a dependency; not used in code
    meta={
        "materialized": "table",
        "tags": ["example:cache_demo", "engine:databricks_spark"],
    },
)
def fetch(_: DataFrame) -> DataFrame:
    df = get_df(
        url="https://jsonplaceholder.typicode.com/users",
        record_path=None,
        normalize=True,
        output="spark",
    )
    return df.select(col("id").alias("api_user_id"), col("email"), col("username"))

from pyspark.sql import DataFrame
from pyspark.sql import Window
from pyspark.sql import functions as F

from fastflowtransform import engine_model


@engine_model(
    only="databricks_spark",
    name="mart_latest_signup",
    materialized="table",
    tags=[
        "example:basic_demo",
        "scope:mart",
        "engine:databricks_spark",
    ],
    deps=["users_clean.ff"],
    require={"users_clean.ff": ["user_id", "email", "email_domain", "signup_date"]},
)
def build(users_clean: DataFrame) -> DataFrame:
    """Return the latest signup per email domain using PySpark DataFrame operations."""
    window = Window.partitionBy("email_domain").orderBy(F.col("signup_date").desc())

    latest = (
        users_clean.withColumn("row_number", F.row_number().over(window))
        .filter(F.col("row_number") == 1)
        .select(
            F.col("email_domain"),
            F.col("user_id").alias("latest_user_id"),
            F.col("email").alias("latest_email"),
            F.col("signup_date").alias("latest_signup_date"),
        )
    )
    return latest

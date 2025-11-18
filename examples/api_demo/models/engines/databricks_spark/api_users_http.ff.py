from typing import TYPE_CHECKING, Any

from fastflowtransform import engine_model
from fastflowtransform.api.http import get_df

if TYPE_CHECKING:
    from pyspark.sql import DataFrame as SparkDataFrame
    from pyspark.sql import SparkSession
else:

    class SparkDataFrame:  # pragma: no cover - placeholder for runtime type hints
        ...

    class SparkSession:  # pragma: no cover - placeholder for runtime type hints
        ...


def _ensure_spark_session(users_df: Any) -> "SparkSession":
    try:
        from pyspark.sql import SparkSession as _SparkSession
    except Exception as exc:  # pragma: no cover - optional dep guard
        raise RuntimeError(
            "pyspark is required for this model. Install fastflowtransform[spark]."
        ) from exc

    session: _SparkSession | None = getattr(users_df, "sparkSession", None)
    if session is None:
        session = _SparkSession.getActiveSession()
    if session is None:
        session = _SparkSession.builder.getOrCreate()
    return session


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
    spark = _ensure_spark_session(users_df)
    df = get_df(
        url="https://jsonplaceholder.typicode.com/users",
        record_path=None,
        normalize=True,
        output="spark",
        session=spark,
    )
    return df.select("id", "email", "username", "name").withColumnRenamed("id", "api_user_id")

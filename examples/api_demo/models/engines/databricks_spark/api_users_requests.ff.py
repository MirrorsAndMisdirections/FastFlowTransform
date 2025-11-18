from typing import TYPE_CHECKING, Any

from fastflowtransform import engine_model

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
    spark = _ensure_spark_session(users_df)
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

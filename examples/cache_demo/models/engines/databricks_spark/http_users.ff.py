try:
    from pyspark.sql import DataFrame
    from pyspark.sql.functions import col
except Exception:  # pragma: no cover - optional dep guard
    from typing import Any

    DataFrame = Any  # type: ignore[misc]
    col = None  # type: ignore[assignment]
    _spark_import_error = RuntimeError(
        "pyspark is required for this model. Install fastflowtransform[spark]."
    )
else:
    _spark_import_error = None

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
    if _spark_import_error:
        raise _spark_import_error

    df = get_df(
        url="https://jsonplaceholder.typicode.com/users",
        record_path=None,
        normalize=True,
        output="spark",
    )
    return df.select(col("id").alias("api_user_id"), col("email"), col("username"))

from fastflowtransform import engine_model
from fastflowtransform.api.http import get_df
from typing import TYPE_CHECKING, Any

if TYPE_CHECKING:
    from pyspark.sql import DataFrame
    from pyspark.sql.functions import col
else:
    DataFrame = Any

    def col(*args: Any, **kwargs: Any):  # pragma: no cover - placeholder
        ...


def _get_col():
    try:
        from pyspark.sql.functions import col as _col
    except Exception as exc:  # pragma: no cover - optional dep guard
        raise RuntimeError(
            "pyspark is required for this model. Install fastflowtransform[spark]."
        ) from exc
    return _col


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
    col_fn = _get_col()
    df = get_df(
        url="https://jsonplaceholder.typicode.com/users",
        record_path=None,
        normalize=True,
        output="spark",
    )
    return df.select(col_fn("id").alias("api_user_id"), col_fn("email"), col_fn("username"))

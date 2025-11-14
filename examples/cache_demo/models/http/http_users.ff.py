from fastflowtransform import model
from fastflowtransform.api.http import get_df
import pandas as pd


@model(
    name="http_users",
    deps=["stg_users.ff"],  # just to show a dependency; not used in code
    meta={
        "materialized": "table",
        "tags": ["example:cache_demo", "engine:duckdb"],
    },
)
def fetch(_: pd.DataFrame) -> pd.DataFrame:
    df = get_df(
        url="https://jsonplaceholder.typicode.com/users",
        record_path=None,
        normalize=True,
    )
    cols = [c for c in df.columns if c in ("id", "email", "username")]
    return df[cols].rename(columns={"id": "api_user_id"})

from fastflowtransform import engine_model
from fastflowtransform.api.http import get_df
import pandas as pd


@engine_model(
    only="postgres",
    name="http_users",
    deps=["stg_users.ff"],  # just to show a dependency; not used in code
    meta={
        "materialized": "table",
        "tags": ["example:cache_demo", "engine:postgres"],
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

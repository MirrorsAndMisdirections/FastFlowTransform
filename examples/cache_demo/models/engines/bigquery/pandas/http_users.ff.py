from fastflowtransform import engine_model
from fastflowtransform.api.http import get_df
import pandas as pd


@engine_model(
    env_match={
        "FF_ENGINE": "bigquery",
        "FF_ENGINE_VARIANT": "pandas",
    },
    name="http_users",
    deps=["stg_users.ff"],  # dependency for cache invalidation symmetry
    meta={
        "materialized": "table",
        "tags": ["example:cache_demo", "engine:bigquery"],
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

from __future__ import annotations

import bigframes.pandas as bpd
from fastflowtransform import engine_model
from fastflowtransform.api.http import get_df

BFDataFrame = bpd.DataFrame


@engine_model(
    env_match={
        "FF_ENGINE": "bigquery",
        "FF_ENGINE_VARIANT": "bigframes",
    },
    name="http_users",
    deps=["stg_users.ff"],  # dependency for cache invalidation symmetry
    meta={
        "materialized": "table",
        "tags": ["example:cache_demo", "engine:bigquery"],
    },
)
def fetch(_: BFDataFrame) -> BFDataFrame:
    df = get_df(
        url="https://jsonplaceholder.typicode.com/users",
        record_path=None,
        normalize=True,
        output="bigframes",
    )
    return df.loc[:, ["id", "email", "username"]].rename(columns={"id": "api_user_id"})  # type: ignore[arg-type]

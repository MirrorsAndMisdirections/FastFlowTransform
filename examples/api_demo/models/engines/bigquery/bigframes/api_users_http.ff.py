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
    name="api_users_http",
    deps=["users.ff"],
    tags=["example:api_demo", "scope:engine", "engine:bigquery"],
)
def fetch(_: BFDataFrame) -> BFDataFrame:
    """
    Fetch users via the FFT HTTP helper and return a BigFrames DataFrame.
    """
    df = get_df(
        url="https://jsonplaceholder.typicode.com/users",
        record_path=None,
        normalize=True,
        output="bigframes",
    )
    return df.loc[:, ["id", "email", "username", "name"]].rename(columns={"id": "api_user_id"})  # type: ignore[arg-type]

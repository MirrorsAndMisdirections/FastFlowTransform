from __future__ import annotations

import bigframes.pandas as bpd
from fastflowtransform import engine_model

BFDataFrame = bpd.DataFrame

try:
    import requests
except Exception as _e:  # pragma: no cover
    raise RuntimeError("Please install 'requests' to run this model") from _e


@engine_model(
    env_match={
        "FF_ENGINE": "bigquery",
        "FF_ENGINE_VARIANT": "bigframes",
    },
    name="api_users_requests",
    deps=["users.ff"],
    tags=["example:api_demo", "scope:engine", "engine:bigquery"],
)
def fetch(_: BFDataFrame) -> BFDataFrame:
    """Fetch users via plain requests and return a BigFrames DataFrame."""
    resp = requests.get("https://jsonplaceholder.typicode.com/users", timeout=30)
    resp.raise_for_status()
    df = bpd.DataFrame(resp.json())  # accepts a JSON-serialisable list of dicts
    return df.loc[:, ["id", "email", "username", "name"]].rename(  # type: ignore[arg-type]
        columns={"id": "api_user_id"}
    )

from __future__ import annotations

from fastflowtransform import engine_model

try:
    import bigframes.pandas as bpd
except Exception:  # pragma: no cover - optional dep guard
    bpd = None  # type: ignore[assignment]
    BFDataFrame = object  # type: ignore[misc,assignment]
    _bf_import_error = RuntimeError(
        "bigframes is required for this model. Install fastflowtransform[bigquery_bf]."
    )
else:
    BFDataFrame = bpd.DataFrame
    _bf_import_error = None

try:
    import httpx
except Exception as _e:  # pragma: no cover
    raise RuntimeError("Please install 'httpx' to run this model") from _e


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
    """Fetch users via plain httpx and return a BigFrames DataFrame."""
    if _bf_import_error:
        raise _bf_import_error

    resp = httpx.get("https://jsonplaceholder.typicode.com/users", timeout=30.0)
    resp.raise_for_status()
    df = bpd.DataFrame(resp.json())  # accepts a JSON-serialisable list of dicts
    return df.loc[:, ["id", "email", "username", "name"]].rename(  # type: ignore[arg-type]
        columns={"id": "api_user_id"}
    )

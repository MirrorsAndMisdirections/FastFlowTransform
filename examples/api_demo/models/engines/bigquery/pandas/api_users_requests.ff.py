from fastflowtransform import engine_model
import pandas as pd

try:
    import httpx
except Exception as _e:  # pragma: no cover
    raise RuntimeError("Please install 'httpx' to run this model") from _e


@engine_model(
    env_match={
        "FF_ENGINE": "bigquery",
        "FF_ENGINE_VARIANT": "pandas",
    },
    name="api_users_requests",
    deps=["users.ff"],
    tags=["example:api_demo", "scope:engine", "engine:bigquery"],
)
def fetch(_: pd.DataFrame) -> pd.DataFrame:
    """Fetch users via plain httpx (pandas client)."""
    url = "https://jsonplaceholder.typicode.com/users"
    resp = httpx.get(url, timeout=30.0)
    resp.raise_for_status()
    data = resp.json()

    df = pd.DataFrame(data)
    cols = [c for c in df.columns if c in ("id", "email", "username", "name")]
    return df[cols].rename(columns={"id": "api_user_id"})

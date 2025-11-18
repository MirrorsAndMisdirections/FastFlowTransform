from fastflowtransform import engine_model
from fastflowtransform.api.http import get_df
import pandas as pd


@engine_model(
    env_match={
        "FF_ENGINE": "bigquery",
        "FF_ENGINE_VARIANT": "pandas",
    },
    name="api_users_http",
    deps=["users.ff"],  # at least one dependency is required by the executor contract
    tags=["example:api_demo", "scope:engine", "engine:bigquery"],
)
def fetch(_: pd.DataFrame) -> pd.DataFrame:
    """
    Fetch users from a public demo API using the built-in HTTP wrapper (pandas client).
    """
    df = get_df(
        url="https://jsonplaceholder.typicode.com/users",
        record_path=None,  # the outer JSON is already a list
        normalize=True,  # flatten objects to columns (address.*, company.*)
    )

    cols = [c for c in df.columns if c in ("id", "email", "username", "name")]
    return df[cols].rename(columns={"id": "api_user_id"})

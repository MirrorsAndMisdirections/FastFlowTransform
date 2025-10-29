from fastflowtransform import model
from fastflowtransform.api.http import get_df
import pandas as pd


@model(
    name="api_users_http",
    deps=["users.ff"],  # at least one dependency is required by the executor contract
    tags=["example:api_demo", "scope:engine", "engine:duckdb"],
)
def fetch(_: pd.DataFrame) -> pd.DataFrame:
    """
    Fetch users from a public demo API using the built-in HTTP wrapper.
    Pros: caching, offline mode, telemetry in run_results.json.
    """
    # Example endpoint (JSON Placeholder); replace with your real API.
    # For paginated APIs you can add a `paginator` function.
    df = get_df(
        url="https://jsonplaceholder.typicode.com/users",
        record_path=None,  # the outer JSON is already a list
        normalize=True,  # flatten objects to columns (address.*, company.*)
    )

    # Keep only a few columns to make joins simpler
    cols = [c for c in df.columns if c in ("id", "email", "username", "name")]
    return df[cols].rename(columns={"id": "api_user_id"})

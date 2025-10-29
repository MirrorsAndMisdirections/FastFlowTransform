# NOTE: Plain Python variant (requests/httpx). No built-in FFT telemetry or HTTP cache here.
from fastflowtransform import model
import pandas as pd

try:
    import requests  # you can swap this with httpx if you prefer
except Exception as _e:  # pragma: no cover
    raise RuntimeError("Please install 'requests' to run this model") from _e


@model(
    name="api_users_requests",
    deps=["users.ff"],  # keep a dependency for executor contract
    tags=["example:api_demo", "scope:engine", "engine:duckdb"],
)
def fetch(_: pd.DataFrame) -> pd.DataFrame:
    """
    Fetch users from the same demo API using plain Python code.
    Pros: ultimate flexibility (custom auth, retry, shaping).
    Cons: no built-in FFT telemetry or cache (unless you add it manually).
    """
    url = "https://jsonplaceholder.typicode.com/users"
    headers = {
        # Add your auth headers here if needed:
        # "Authorization": f"Bearer {os.getenv('MY_TOKEN')}",
    }
    resp = requests.get(url, headers=headers, timeout=30)
    resp.raise_for_status()
    data = resp.json()  # list[dict]

    # Example shaping
    df = pd.DataFrame(data)
    cols = [c for c in df.columns if c in ("id", "email", "username", "name")]
    return df[cols].rename(columns={"id": "api_user_id"})

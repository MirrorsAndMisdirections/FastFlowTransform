from fastflowtransform import model
import pandas as pd


@model(
    name="users_enriched",
    deps=["users.ff"],
    require={"users": {"id", "email"}},
)
def enrich(df: pd.DataFrame) -> pd.DataFrame:
    out = df.copy()
    out["is_gmail"] = out["email"].str.endswith("@gmail.com")
    return out

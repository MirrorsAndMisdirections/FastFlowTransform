import pandas as pd

from fastflowtransform import model


@model(name="users_enriched", deps=["users.ff"], require={"users.ff": ["id", "email"]})
def enrich(df: pd.DataFrame) -> pd.DataFrame:
    out = df.copy()
    out["is_gmail"] = out["email"].str.endswith("@gmail.com")
    return out

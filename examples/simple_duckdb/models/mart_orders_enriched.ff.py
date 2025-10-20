# examples/simple_duckdb/models/mart_orders_enriched.ff.py

import pandas as pd

from fastflowtransform import model


@model(
    name="mart_orders_enriched",
    deps=["orders.ff", "users_enriched"],
    require={
        "orders.ff": ["order_id", "user_id", "amount"],  # logical name works
        "users_enriched": ["id", "email", "is_gmail"],  # physical relation works too
    },
)
def build(dfs: dict[str, pd.DataFrame]) -> pd.DataFrame:
    # Keys already resolve to physical relations via relation_for(): "orders", "users_enriched"
    orders = dfs["orders"]
    users = dfs["users_enriched"]
    out = orders.merge(users, left_on="user_id", right_on="id", how="left").assign(
        valid_amt=lambda x: x["amount"] >= 0
    )
    return out

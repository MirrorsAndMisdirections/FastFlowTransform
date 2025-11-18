from __future__ import annotations

import pandas as pd

from fastflowtransform import engine_model


@engine_model(
    env_match={
        "FF_ENGINE": "bigquery",
        "FF_ENGINE_VARIANT": "pandas",
    },
    name="mart_orders_agg",
    materialized="table",
    tags=[
        "example:dq_demo",
        "scope:mart",
        "engine:bigquery",
    ],
    deps=["orders.ff", "customers.ff"],
    require={
        "orders.ff": ["order_id", "customer_id", "amount", "order_ts"],
        "customers.ff": ["customer_id", "name", "status"],
    },
)
def build(orders: pd.DataFrame, customers: pd.DataFrame) -> pd.DataFrame:
    """Aggregate orders per customer for reconciliation checks."""
    base = orders.merge(customers, on="customer_id", how="inner", suffixes=("", "_cust"))

    grouped = (
        base.groupby(["customer_id", "name", "status"], dropna=False)
        .agg(
            order_count=("order_id", "count"),
            total_amount=("amount", "sum"),
            first_order_ts=("order_ts", "min"),
            last_order_ts=("order_ts", "max"),
        )
        .reset_index()
    )

    grouped.rename(
        columns={
            "name": "customer_name",
        },
        inplace=True,
    )
    return grouped[
        [
            "customer_id",
            "customer_name",
            "status",
            "order_count",
            "total_amount",
            "first_order_ts",
            "last_order_ts",
        ]
    ]

from __future__ import annotations

import bigframes.pandas as bpd

from fastflowtransform import engine_model

BFDataFrame = bpd.DataFrame


@engine_model(
    env_match={
        "FF_ENGINE": "bigquery",
        "FF_ENGINE_VARIANT": "bigframes",
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
def build(orders: BFDataFrame, customers: BFDataFrame) -> BFDataFrame:
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

    grouped = grouped.rename(columns={"name": "customer_name"})  # type: ignore[arg-type]
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

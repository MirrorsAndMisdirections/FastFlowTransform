from __future__ import annotations

from typing import TYPE_CHECKING, Any

from fastflowtransform import engine_model

if TYPE_CHECKING:
    import bigframes.pandas as bpd
    from bigframes.pandas import DataFrame as BFDataFrame
else:
    bpd: Any = None

    class BFDataFrame:  # pragma: no cover - placeholder for runtime type hints
        ...


def _get_bigframes() -> Any:
    try:
        import bigframes.pandas as bpd_mod
    except Exception as exc:  # pragma: no cover - optional dep guard
        raise RuntimeError(
            "bigframes is required for this model. Install fastflowtransform[bigquery_bf]."
        ) from exc
    return bpd_mod


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
    _get_bigframes()
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

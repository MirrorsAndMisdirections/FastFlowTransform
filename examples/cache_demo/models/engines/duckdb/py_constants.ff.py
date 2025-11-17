import pandas as pd

from fastflowtransform import engine_model


@engine_model(
    only="duckdb",
    name="py_constants",
    materialized="table",
    tags=[
        "example:cache_demo",
        "engine:duckdb",
    ],
)
def build() -> pd.DataFrame:
    """DuckDB/Postgres-friendly version using pandas."""
    return pd.DataFrame([{"k": "answer", "v": 42}])

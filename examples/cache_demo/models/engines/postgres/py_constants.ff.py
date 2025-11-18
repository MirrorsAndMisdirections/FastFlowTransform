import pandas as pd

from fastflowtransform import engine_model


@engine_model(
    only="postgres",
    name="py_constants",
    materialized="table",
    tags=[
        "example:cache_demo",
        "engine:postgres",
    ],
)
def build() -> pd.DataFrame:
    """Postgres version using pandas."""
    return pd.DataFrame([{"k": "answer", "v": 42}])

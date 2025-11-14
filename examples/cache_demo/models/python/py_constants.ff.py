from fastflowtransform import model
import pandas as pd


@model(
    name="py_constants",
    deps=[],  # independent
    meta={
        "materialized": "table",
        "tags": ["example:cache_demo", "engine:duckdb"],
    },
)
def build() -> pd.DataFrame:
    # Change this constant to trigger a fingerprint change for a pure Python model.
    CONSTANT = 42
    return pd.DataFrame([{"k": "answer", "v": CONSTANT}])

import pandas as pd

from fastflowtransform import engine_model


@engine_model(
    env_match={
        "FF_ENGINE": "bigquery",
        "FF_ENGINE_VARIANT": "pandas",
    },
    name="py_constants",
    materialized="table",
    tags=[
        "example:cache_demo",
        "engine:bigquery",
    ],
)
def build() -> pd.DataFrame:
    """BigQuery (pandas) version returning a pandas DataFrame."""
    return pd.DataFrame([{"k": "answer", "v": 42}])

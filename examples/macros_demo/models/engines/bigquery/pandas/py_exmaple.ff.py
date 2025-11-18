import pandas as pd

from fastflowtransform import engine_model


@engine_model(
    env_match={
        "FF_ENGINE": "bigquery",
        "FF_ENGINE_VARIANT": "pandas",
    },
    name="py_example",
    deps=["fct_user_sales.ff"],
    tags=["example:macros_demo", "scope:engine", "engine:bigquery"],
)
def produce(_: pd.DataFrame) -> pd.DataFrame:
    # In a real project, you might fetch extra metadata here or post-process
    return pd.DataFrame([{"note": "Python model ran on BigQuery (pandas)"}])

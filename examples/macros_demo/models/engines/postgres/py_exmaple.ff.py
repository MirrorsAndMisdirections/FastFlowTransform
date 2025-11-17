import pandas as pd

from fastflowtransform import engine_model


@engine_model(
    only="postgres",
    name="py_example",
    deps=["fct_user_sales.ff"],
    tags=["example:macros_demo", "scope:engine", "engine:postgres"],
)
def produce(_: pd.DataFrame) -> pd.DataFrame:
    # In a real project, you might fetch extra metadata here or post-process
    return pd.DataFrame([{"note": "Python model ran on Postgres"}])

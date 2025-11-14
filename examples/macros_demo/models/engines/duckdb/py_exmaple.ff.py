from fastflowtransform import engine_model
import pandas as pd


@engine_model(
    only="duckdb",
    name="py_example",
    deps=["fct_user_sales.ff"],
    tags=["example:macros_demo", "scope:engine", "engine:duckdb"],
)
def produce(_: pd.DataFrame) -> pd.DataFrame:
    # In a real project, you might fetch extra metadata here or post-process
    return pd.DataFrame([{"note": "Python model ran on DuckDB"}])

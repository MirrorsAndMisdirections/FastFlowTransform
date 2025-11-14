from fastflowtransform import engine_model
import pandas as pd


@engine_model(
    only="postgres",
    name="demo_py_emit",
    deps=["dim_customers.ff"],
    tags=["example:materializations_demo", "scope:python", "engine:postgres"],
)
def fetch(_: pd.DataFrame) -> pd.DataFrame:
    return pd.DataFrame([{"note": "hello from python", "emitted_at": pd.Timestamp.utcnow()}])

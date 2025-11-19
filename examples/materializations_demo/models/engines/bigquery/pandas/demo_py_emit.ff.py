from fastflowtransform import engine_model
import pandas as pd


@engine_model(
    env_match={"FF_ENGINE": "bigquery", "FF_ENGINE_VARIANT": "pandas"},
    name="demo_py_emit",
    deps=["dim_customers.ff"],
    tags=["example:materializations_demo", "scope:python", "engine:bigquery"],
)
def fetch(_: pd.DataFrame) -> pd.DataFrame:
    return pd.DataFrame(
        [{"note": "hello from python (BigQuery pandas)", "emitted_at": pd.Timestamp.utcnow()}]
    )

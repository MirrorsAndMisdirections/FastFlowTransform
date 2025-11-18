from __future__ import annotations

import pandas as pd
from fastflowtransform import engine_model


@engine_model(
    env_match={
        "FF_ENGINE": "bigquery",
        "FF_ENGINE_VARIANT": "pandas",
    },
    name="fct_events_py_incremental",
    deps=["events_base.ff"],
    tags=[
        "example:incremental_demo",
        "scope:engine",
        "engine:bigquery",
        "kind:python",
        "kind:incremental",
    ],
)
def build(events_df: pd.DataFrame) -> pd.DataFrame:
    """
    Python incremental example for BigQuery (pandas client).

    Computes the delta dataset; merge/update behaviour is configured via
    project.yml → models.incremental.fct_events_py_incremental.ff.
    """
    df = events_df.copy()
    df["value_x10"] = df["value"] * 10
    return df[["event_id", "updated_at", "value", "value_x10"]]

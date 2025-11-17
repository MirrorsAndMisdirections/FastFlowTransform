from __future__ import annotations

import bigframes.pandas as bpd
from fastflowtransform import engine_model

BFDataFrame = bpd.DataFrame


@engine_model(
    env_match={
        "FF_ENGINE": "bigquery",
        "FF_ENGINE_VARIANT": "bigframes",
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
def build(events_df: BFDataFrame) -> BFDataFrame:
    """
    Python incremental example for BigQuery (BigFrames client).

    Applies a small transformation; incremental merge/insert behaviour is
    governed by the project.yml incremental config.
    """
    df = events_df.copy()
    df["value_x10"] = df["value"] * 10
    return df[["event_id", "updated_at", "value", "value_x10"]]

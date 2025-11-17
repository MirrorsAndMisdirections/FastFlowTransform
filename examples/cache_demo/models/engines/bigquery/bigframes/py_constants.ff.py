from __future__ import annotations

import bigframes.pandas as bpd
from fastflowtransform import engine_model

BFDataFrame = bpd.DataFrame


@engine_model(
    env_match={
        "FF_ENGINE": "bigquery",
        "FF_ENGINE_VARIANT": "bigframes",
    },
    name="py_constants",
    materialized="table",
    tags=[
        "example:cache_demo",
        "engine:bigquery",
    ],
)
def build() -> BFDataFrame:
    """BigQuery (BigFrames) version returning a BigFrames DataFrame."""
    return bpd.DataFrame([{"k": "answer", "v": 42}])

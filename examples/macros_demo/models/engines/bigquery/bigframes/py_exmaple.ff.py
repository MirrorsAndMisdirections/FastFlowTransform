from __future__ import annotations

import bigframes.pandas as bpd
from fastflowtransform import engine_model

BFDataFrame = bpd.DataFrame


@engine_model(
    env_match={
        "FF_ENGINE": "bigquery",
        "FF_ENGINE_VARIANT": "bigframes",
    },
    name="py_example",
    deps=["fct_user_sales.ff"],
    tags=["example:macros_demo", "scope:engine", "engine:bigquery"],
)
def produce(_: BFDataFrame) -> BFDataFrame:
    # In a real project, you might fetch extra metadata here or post-process
    return bpd.DataFrame([{"note": "Python model ran on BigQuery (BigFrames)"}])

from __future__ import annotations

from fastflowtransform import engine_model

try:
    import bigframes.pandas as bpd
except Exception:  # pragma: no cover - optional dep guard
    bpd = None  # type: ignore[assignment]
    BFDataFrame = object  # type: ignore[misc,assignment]
    _bf_import_error = RuntimeError(
        "bigframes is required for this model. Install fastflowtransform[bigquery_bf]."
    )
else:
    BFDataFrame = bpd.DataFrame
    _bf_import_error = None


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
    if _bf_import_error:
        raise _bf_import_error
    return bpd.DataFrame([{"note": "Python model ran on BigQuery (BigFrames)"}])

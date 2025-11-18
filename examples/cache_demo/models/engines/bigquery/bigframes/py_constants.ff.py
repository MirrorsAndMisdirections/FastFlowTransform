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
    name="py_constants",
    materialized="table",
    tags=[
        "example:cache_demo",
        "engine:bigquery",
    ],
)
def build() -> BFDataFrame:
    """BigQuery (BigFrames) version returning a BigFrames DataFrame."""
    if _bf_import_error:
        raise _bf_import_error
    return bpd.DataFrame([{"k": "answer", "v": 42}])

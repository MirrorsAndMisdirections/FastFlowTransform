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
    if _bf_import_error:
        raise _bf_import_error

    df = events_df.copy()
    df["value_x10"] = df["value"] * 10
    return df[["event_id", "updated_at", "value", "value_x10"]]

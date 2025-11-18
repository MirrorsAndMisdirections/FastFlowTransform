from __future__ import annotations

from typing import TYPE_CHECKING, Any

from fastflowtransform import engine_model

if TYPE_CHECKING:
    import bigframes.pandas as bpd
    from bigframes.pandas import DataFrame as BFDataFrame
else:
    bpd: Any = None

    class BFDataFrame:  # pragma: no cover - placeholder for runtime type hints
        ...


def _get_bigframes() -> Any:
    try:
        import bigframes.pandas as bpd_mod
    except Exception as exc:  # pragma: no cover - optional dep guard
        raise RuntimeError(
            "bigframes is required for this model. Install fastflowtransform[bigquery_bf]."
        ) from exc
    return bpd_mod


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
    _get_bigframes()
    df = events_df.copy()
    df["value_x10"] = df["value"] * 10
    return df[["event_id", "updated_at", "value", "value_x10"]]

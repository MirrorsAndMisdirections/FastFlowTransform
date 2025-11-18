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
    name="py_constants",
    materialized="table",
    tags=[
        "example:cache_demo",
        "engine:bigquery",
    ],
)
def build() -> BFDataFrame:
    """BigQuery (BigFrames) version returning a BigFrames DataFrame."""
    bpd_mod = _get_bigframes()
    return bpd_mod.DataFrame([{"k": "answer", "v": 42}])

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
    name="py_example",
    deps=["fct_user_sales.ff"],
    tags=["example:macros_demo", "scope:engine", "engine:bigquery"],
)
def produce(_: BFDataFrame) -> BFDataFrame:
    # In a real project, you might fetch extra metadata here or post-process
    bpd_mod = _get_bigframes()
    return bpd_mod.DataFrame([{"note": "Python model ran on BigQuery (BigFrames)"}])

from __future__ import annotations

from typing import TYPE_CHECKING, Any

from fastflowtransform import engine_model
import pandas as pd

if TYPE_CHECKING:
    from bigframes.dataframe import DataFrame as BFDataFrame
else:
    BFDataFrame = Any


def _ensure_bigframes():
    try:
        import bigframes.pandas as bpd  # type: ignore
    except Exception as exc:  # pragma: no cover
        raise RuntimeError(
            "bigframes is required for this model. Install fastflowtransform[bigquery_bf]."
        ) from exc
    return bpd


@engine_model(
    env_match={"FF_ENGINE": "bigquery", "FF_ENGINE_VARIANT": "bigframes"},
    name="demo_py_emit",
    deps=["dim_customers.ff"],
    tags=["example:materializations_demo", "scope:python", "engine:bigquery"],
)
def fetch(_: BFDataFrame) -> BFDataFrame:
    bpd = _ensure_bigframes()
    return bpd.DataFrame(
        [{"note": "hello from python (BigQuery BigFrames)", "emitted_at": pd.Timestamp.utcnow()}]
    )

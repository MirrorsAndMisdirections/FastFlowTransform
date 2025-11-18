from __future__ import annotations

from fastflowtransform import engine_model
from fastflowtransform.api.http import get_df

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
    name="http_users",
    deps=["stg_users.ff"],  # dependency for cache invalidation symmetry
    meta={
        "materialized": "table",
        "tags": ["example:cache_demo", "engine:bigquery"],
    },
)
def fetch(_: BFDataFrame) -> BFDataFrame:
    if _bf_import_error:
        raise _bf_import_error

    df = get_df(
        url="https://jsonplaceholder.typicode.com/users",
        record_path=None,
        normalize=True,
        output="bigframes",
    )
    return df.loc[:, ["id", "email", "username"]].rename(columns={"id": "api_user_id"})  # type: ignore[arg-type]

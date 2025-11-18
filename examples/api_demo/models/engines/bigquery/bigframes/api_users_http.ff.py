from __future__ import annotations

from typing import TYPE_CHECKING, Any

from fastflowtransform import engine_model
from fastflowtransform.api.http import get_df

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
    name="api_users_http",
    deps=["users.ff"],
    tags=["example:api_demo", "scope:engine", "engine:bigquery"],
)
def fetch(_: BFDataFrame) -> BFDataFrame:
    """
    Fetch users via the FFT HTTP helper and return a BigFrames DataFrame.
    """
    _get_bigframes()
    df = get_df(
        url="https://jsonplaceholder.typicode.com/users",
        record_path=None,
        normalize=True,
        output="bigframes",
    )
    return df.loc[:, ["id", "email", "username", "name"]].rename(columns={"id": "api_user_id"})  # type: ignore[arg-type]

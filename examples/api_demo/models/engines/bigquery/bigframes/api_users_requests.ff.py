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


try:
    import httpx
except Exception as _e:  # pragma: no cover
    raise RuntimeError("Please install 'httpx' to run this model") from _e


@engine_model(
    env_match={
        "FF_ENGINE": "bigquery",
        "FF_ENGINE_VARIANT": "bigframes",
    },
    name="api_users_requests",
    deps=["users.ff"],
    tags=["example:api_demo", "scope:engine", "engine:bigquery"],
)
def fetch(_: BFDataFrame) -> BFDataFrame:
    """Fetch users via plain httpx and return a BigFrames DataFrame."""
    bpd_mod = _get_bigframes()
    resp = httpx.get("https://jsonplaceholder.typicode.com/users", timeout=30.0)
    resp.raise_for_status()
    df = bpd_mod.DataFrame(resp.json())  # accepts a JSON-serialisable list of dicts
    return df.loc[:, ["id", "email", "username", "name"]].rename(  # type: ignore[arg-type]
        columns={"id": "api_user_id"}
    )

from __future__ import annotations

from typing import TYPE_CHECKING, Any

import pandas as pd

from fastflowtransform import engine_model
from fastflowtransform.api.http import get_df

if TYPE_CHECKING:  # pragma: no cover - typing only
    from snowflake.snowpark import DataFrame as SnowparkDataFrame
    from snowflake.snowpark import Session as SnowparkSession
else:  # pragma: no cover - runtime fallback
    SnowparkDataFrame = Any
    SnowparkSession = Any


def _ensure_session(df: Any) -> "SnowparkSession":
    try:
        from snowflake.snowpark import Session as _Session
    except Exception as exc:  # pragma: no cover - optional dependency guard
        raise RuntimeError(
            "snowflake-snowpark-python is required for this model. "
            "Install fastflowtransform[snowflake]."
        ) from exc

    session = getattr(df, "session", None)
    if session is None:
        raise RuntimeError(
            "Snowpark session missing on upstream DataFrame. "
            "Ensure Snowflake Snowpark is the active engine."
        )
    return session


@engine_model(
    only="snowflake_snowpark",
    name="api_users_http",
    deps=["users.ff"],
    tags=["example:api_demo", "scope:engine", "engine:snowflake_snowpark"],
)
def fetch(users_df: SnowparkDataFrame) -> SnowparkDataFrame:
    """
    Fetch demo users via the FFT HTTP helper and return a Snowpark DataFrame.
    """
    session = _ensure_session(users_df)
    pdf = get_df(
        url="https://jsonplaceholder.typicode.com/users",
        record_path=None,
        normalize=True,
    )

    projected = (
        pdf.loc[:, ["id", "email", "username", "name"]]
        .rename(columns={"id": "api_user_id"})
        .astype({"api_user_id": pd.Int64Dtype()}, copy=False)
    )
    return session.create_dataframe(projected)

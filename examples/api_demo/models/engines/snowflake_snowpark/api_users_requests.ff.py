from __future__ import annotations

from typing import TYPE_CHECKING, Any

from fastflowtransform import engine_model

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
            "Ensure Snowflake Snowpark is selected via profiles.yml."
        )
    return session


try:
    import httpx
except Exception as _e:  # pragma: no cover - optional dep
    raise RuntimeError("Please install 'httpx' to run this model") from _e


@engine_model(
    only="snowflake_snowpark",
    name="api_users_requests",
    deps=["users.ff"],
    tags=["example:api_demo", "scope:engine", "engine:snowflake_snowpark"],
)
def fetch(users_df: SnowparkDataFrame) -> SnowparkDataFrame:
    """
    Fetch demo users via plain httpx and return a Snowpark DataFrame.
    """
    session = _ensure_session(users_df)
    resp = httpx.get("https://jsonplaceholder.typicode.com/users", timeout=30.0)
    resp.raise_for_status()
    rows = resp.json()

    projected = [
        (
            row.get("id"),
            row.get("email"),
            row.get("username"),
            row.get("name"),
        )
        for row in rows
    ]

    schema = ["api_user_id", "email", "username", "name"]
    return session.create_dataframe(projected, schema=schema)

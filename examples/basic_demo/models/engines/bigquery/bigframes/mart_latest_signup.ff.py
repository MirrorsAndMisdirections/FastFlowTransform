from __future__ import annotations

from typing import Any

from fastflowtransform import engine_model

try:
    import bigframes.pandas as bpd
except Exception:  # pragma: no cover - optional dep guard
    bpd = None  # type: ignore[assignment]
    BFDataFrame = Any  # type: ignore[assignment,misc]
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
    name="mart_latest_signup",
    materialized="table",
    tags=[
        "example:basic_demo",
        "scope:mart",
        "engine:bigquery",
    ],
    deps=["users_clean.ff"],
    require={"users_clean.ff": ["user_id", "email", "email_domain", "signup_date"]},
)
def build(users_clean: BFDataFrame) -> BFDataFrame:
    if _bf_import_error:
        raise _bf_import_error

    latest = (
        users_clean.sort_values("signup_date", ascending=False)
        .drop_duplicates(subset="email_domain")
        .loc[:, ["email_domain", "user_id", "email", "signup_date"]]
        .rename(
            columns={
                "user_id": "latest_user_id",
                "email": "latest_email",
                "signup_date": "latest_signup_date",
            }
        )  # type: ignore[arg-type]
    )
    return latest

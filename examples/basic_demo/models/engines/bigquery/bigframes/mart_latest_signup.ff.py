from __future__ import annotations

from typing import Any

import bigframes.pandas as bpd
from fastflowtransform import engine_model

BFDataFrame = bpd.DataFrame


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

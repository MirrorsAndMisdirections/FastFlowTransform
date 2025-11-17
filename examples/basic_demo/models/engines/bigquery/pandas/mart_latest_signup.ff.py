import pandas as pd

from fastflowtransform import engine_model


@engine_model(
    env_match={
        "FF_ENGINE": "bigquery",
        "FF_ENGINE_VARIANT": "pandas",
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
def build(users_clean: pd.DataFrame) -> pd.DataFrame:
    """Return the latest signup per email domain using pandas (BigQuery)."""
    latest = (
        users_clean.sort_values("signup_date", ascending=False)
        .drop_duplicates("email_domain")
        .loc[:, ["email_domain", "user_id", "email", "signup_date"]]
        .rename(
            columns={
                "user_id": "latest_user_id",
                "email": "latest_email",
                "signup_date": "latest_signup_date",
            }
        )
        .reset_index(drop=True)
    )
    return latest

# models/marts/mart_latest_signup.ff.py
import pandas as pd
from fastflowtransform import engine_model


@engine_model(
    # Register this model for both DuckDB (local) and BigQuery (pandas backend)
    only=("duckdb", "bigquery"),
    name="mart_latest_signup",
    materialized="table",
    deps=["stg_users.ff"],  # SQL model from earlier in the article
    tags=["scope:mart", "engine:duckdb", "engine:bigquery"],
    requires={
        # Columns produced by stg_users.ff.sql:
        #   id, email, signup_date
        "stg_users.ff": {"id", "email", "signup_date"},
    },
)
def build(stg_users: pd.DataFrame) -> pd.DataFrame:
    """Return the latest signup per email domain using pandas."""

    # Derive an email_domain column in Python
    users = stg_users.copy()
    users["email_domain"] = users["email"].str.split("@").str[-1]

    latest = (
        users.sort_values("signup_date", ascending=False)
        .drop_duplicates("email_domain")  # keep the newest per domain
        .loc[:, ["email_domain", "id", "email", "signup_date"]]
        .rename(
            columns={
                "id": "latest_user_id",
                "email": "latest_email",
                "signup_date": "latest_signup_date",
            }
        )
        .reset_index(drop=True)
    )

    return latest

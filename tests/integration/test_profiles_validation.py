# tests/integration/test_profiles_validation.py
from __future__ import annotations

import os
import textwrap
from pathlib import Path

import pytest

from fastflowtransform.errors import ProfileConfigError
from fastflowtransform.settings import EnvSettings, resolve_profile


def _write_profiles(tmp_path: Path, yaml_text: str) -> None:
    (tmp_path / "profiles.yml").write_text(
        textwrap.dedent(yaml_text).strip() + "\n", encoding="utf-8"
    )


@pytest.mark.parametrize(
    "case_name,profiles_yml,env_kwargs,expect_error,expect_substring",
    [
        # --- OK cases -------------------------------------------------------
        (
            "duckdb_ok",
            """
            dev:
              engine: duckdb
              duckdb:
                path: ":memory:"
            """,
            {},
            False,
            None,
        ),
        (
            "pg_ok_via_env_override",
            """
            dev:
              engine: postgres
              postgres:
                dsn: ""            # will be overridden by EnvSettings.PG_DSN
                db_schema: public
            """,
            {"PG_DSN": "postgresql+psycopg://u:p@h:5432/db"},
            False,
            None,
        ),
        (
            "bq_ok_dataset_in_profiles",
            """
            dev:
              engine: bigquery
              bigquery:
                project: my-proj
                dataset: my_ds
            """,
            {},
            False,
            None,
        ),
        # --- Error cases (check single-line hint + substring) ---------------
        (
            "pg_missing_dsn",
            """
            dev:
              engine: postgres
              postgres:
                db_schema: public
            """,
            {},
            True,
            "Postgres profile missing DSN. Hint: set profiles.yml → postgres.dsn or env FF_PG_DSN.",
        ),
        (
            "pg_empty_schema",
            """
            dev:
              engine: postgres
              postgres:
                dsn: postgresql+psycopg://u:p@h:5432/db
                db_schema: ""
            """,
            {},
            True,
            "Postgres profile has empty schema. "
            "Hint: set profiles.yml → postgres.db_schema or env FF_PG_SCHEMA.",
        ),
        (
            "bq_missing_dataset",
            """
            dev:
              engine: bigquery
              bigquery:
                project: my-proj
            """,
            {},
            True,
            "BigQuery profile missing dataset. Hint: set profiles.yml → bigquery.dataset "
            "or env FF_BQ_DATASET.",
        ),
        (
            "sf_missing_many",
            """
            dev:
              engine: snowflake_snowpark
              snowflake_snowpark:
                account: ""
                user: ""
                password: ""
                warehouse: ""
                database: ""
                schema: ""
            """,
            {},
            True,
            "Snowflake profile missing:",
        ),
    ],
)
def test_profiles_validation(
    tmp_path: Path,
    case_name: str,
    profiles_yml: str,
    env_kwargs: dict,
    expect_error: bool,
    expect_substring: str | None,
    monkeypatch,
):
    _write_profiles(tmp_path, profiles_yml)

    # Ensure FF_* env vars from the outer environment do not affect expectations.
    for key in [k for k in os.environ if k.startswith("FF_")]:
        monkeypatch.delenv(key, raising=False)

    env = EnvSettings(**env_kwargs)

    if expect_error:
        with pytest.raises(ProfileConfigError) as exc:
            resolve_profile(tmp_path, "dev", env)
        msg = str(exc.value)
        # single-line hint (no embedded newlines)
        assert "\n" not in msg, f"{case_name}: error message must be single-line"
        if expect_substring:
            assert expect_substring in msg, f"{case_name}: expected hint not found:\n{msg}"
    else:
        prof = resolve_profile(tmp_path, "dev", env)
        # sanity: returns the right engine type
        assert prof.engine in {
            "duckdb",
            "postgres",
            "bigquery",
            "databricks_spark",
            "snowflake_snowpark",
        }

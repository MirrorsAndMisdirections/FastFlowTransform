from __future__ import annotations

import os
from collections.abc import Callable
from pathlib import Path
from typing import Annotated, Any, Literal, cast

import yaml
from pydantic import BaseModel, Field, TypeAdapter
from pydantic_settings import BaseSettings, SettingsConfigDict

from fastflowtransform.errors import ProfileConfigError

EngineType = Literal["duckdb", "postgres", "bigquery", "databricks_spark", "snowflake_snowpark"]


class DuckDBConfig(BaseModel):
    path: str = ":memory:"  # file path or ":memory:"


class PostgresConfig(BaseModel):
    dsn: str | None = None  # e.g. postgresql+psycopg://user:pass@host:5432/db
    db_schema: str = "public"


class BigQueryConfig(BaseModel):
    project: str | None = None
    dataset: str | None = None
    location: str | None = None
    use_bigframes: bool = True


class DatabricksSparkConfig(BaseModel):
    master: str = "local[*]"
    app_name: str = "fastflowtransform"
    extra_conf: dict[str, Any] | None = None


class SnowflakeSnowparkConfig(BaseModel):
    account: str
    user: str
    password: str
    warehouse: str
    database: str
    db_schema: str | None = None
    role: str | None = None


class DuckDBProfile(BaseModel):
    engine: Literal["duckdb"]
    duckdb: DuckDBConfig


class PostgresProfile(BaseModel):
    engine: Literal["postgres"]
    postgres: PostgresConfig


class BigQueryProfile(BaseModel):
    engine: Literal["bigquery"]
    bigquery: BigQueryConfig


class DatabricksSparkProfile(BaseModel):
    engine: Literal["databricks_spark"]
    databricks_spark: DatabricksSparkConfig


class SnowflakeSnowparkProfile(BaseModel):
    engine: Literal["snowflake_snowpark"]
    snowflake_snowpark: SnowflakeSnowparkConfig


Profile = Annotated[
    DuckDBProfile
    | PostgresProfile
    | BigQueryProfile
    | DatabricksSparkProfile
    | SnowflakeSnowparkProfile,
    Field(discriminator="engine"),
]


class ProjectConfig(BaseModel):
    name: str
    version: str
    models_dir: str = "models"


class EnvSettings(BaseSettings):
    model_config = SettingsConfigDict(env_prefix="FF_", env_file=".env", extra="ignore")

    # generic
    ENV: str = Field(default="dev", description="active environment: dev/stg/prod")
    PROJECT_DIR: str | None = None

    # Engine selection (overrides profiles)
    ENGINE: EngineType | None = None

    # DuckDB
    DUCKDB_PATH: str | None = None

    # Postgres
    PG_DSN: str | None = None
    PG_SCHEMA: str | None = None

    # bigquery
    BQ_PROJECT: str | None = None
    BQ_DATASET: str | None = None
    BQ_LOCATION: str | None = None

    # databricks spark
    DBR_MASTER: str | None = None
    DBR_APPNAME: str | None = None

    # snowflake snowpark
    SF_ACCOUNT: str | None = None
    SF_USER: str | None = None
    SF_PASSWORD: str | None = None
    SF_ROLE: str | None = None
    SF_WAREHOUSE: str | None = None
    SF_DATABASE: str | None = None
    SF_SCHEMA: str | None = None


def load_project_config(project_dir: Path) -> ProjectConfig:
    cfg_path = project_dir / "project.yml"
    data = yaml.safe_load(cfg_path.read_text(encoding="utf-8"))
    return ProjectConfig(**data)


# ---------- Loader ----------
def load_profiles(project_dir: Path) -> dict:
    """
    Read project.yml/profiles.yml and return a dict per environment.
    Expected format:
      { "dev": {"engine":"duckdb", "duckdb":{"path":":memory:"}}, ... }
    """
    pf_path = project_dir / "profiles.yml"
    if not pf_path.exists():
        return {}
    return yaml.safe_load(pf_path.read_text(encoding="utf-8")) or {}


# ---------- Resolver ----------
# def resolve_profile(project_dir: Path, env_name: str, env: EnvSettings) -> Profile:
#     profiles: dict[str, dict[str, Any]] = load_profiles(project_dir)  # type: ignore[name-defined]
#     raw: dict[str, Any] = (
#         profiles.get(env_name)
#         or profiles.get("default")
#         or {"engine": "duckdb", "duckdb": {"path": ":memory:"}}
#     )

#     # --------------------------
#     # 1) Apply ENV overrides before parsing
#     # --------------------------

#     # Engine-Override
#     if env.ENGINE:
#         raw["engine"] = env.ENGINE

#     eng = str(raw.get("engine", "duckdb")).lower()

#     # DuckDB
#     if eng == "duckdb":
#         raw.setdefault("duckdb", {})
#         if env.DUCKDB_PATH is not None:
#             raw["duckdb"]["path"] = env.DUCKDB_PATH

#     # Postgres
#     elif eng == "postgres":
#         raw.setdefault("postgres", {})
#         if env.PG_DSN:
#             raw["postgres"]["dsn"] = env.PG_DSN
#         if env.PG_SCHEMA:
#             raw["postgres"]["db_schema"] = env.PG_SCHEMA

#     # BigQuery (pandas or BigFrames; flag in config)
#     elif eng == "bigquery":
#         raw.setdefault("bigquery", {})
#         if env.BQ_PROJECT is not None:
#             raw["bigquery"]["project"] = env.BQ_PROJECT
#         if env.BQ_DATASET:
#             raw["bigquery"]["dataset"] = env.BQ_DATASET
#         if env.BQ_LOCATION is not None:
#             raw["bigquery"]["location"] = env.BQ_LOCATION
#         # optional ENV flag for BigFrames
#         uf = os.getenv("FF_BQ_USE_BIGFRAMES")
#         if uf is not None:
#             raw["bigquery"]["use_bigframes"] = uf.lower() in ("1", "true", "yes", "on")

#     # Databricks Spark (local or via Connect)
#     elif eng == "databricks_spark":
#         raw.setdefault("databricks_spark", {})
#         if env.DBR_MASTER is not None:
#             raw["databricks_spark"]["master"] = env.DBR_MASTER
#         if env.DBR_APPNAME is not None:
#             raw["databricks_spark"]["app_name"] = env.DBR_APPNAME
#         # optionally set additional fields (connect params) via env here

#     # Snowflake Snowpark
#     elif eng == "snowflake_snowpark":
#         raw.setdefault("snowflake_snowpark", {})
#         # Important: field name is consistently "schema" (not "db_schema")
#         if env.SF_ACCOUNT:
#             raw["snowflake_snowpark"]["account"] = env.SF_ACCOUNT
#         if env.SF_USER:
#             raw["snowflake_snowpark"]["user"] = env.SF_USER
#         if env.SF_PASSWORD:
#             raw["snowflake_snowpark"]["password"] = env.SF_PASSWORD
#         if env.SF_WAREHOUSE:
#             raw["snowflake_snowpark"]["warehouse"] = env.SF_WAREHOUSE
#         if env.SF_DATABASE:
#             raw["snowflake_snowpark"]["database"] = env.SF_DATABASE
#         if env.SF_SCHEMA:
#             raw["snowflake_snowpark"]["schema"] = env.SF_SCHEMA
#         if env.SF_ROLE is not None:
#             raw["snowflake_snowpark"]["role"] = env.SF_ROLE

#     # --------------------------
#     # 2) Parse via discriminated union
#     # --------------------------
#     prof: Profile = TypeAdapter(Profile).validate_python(raw)

#     # --------------------------
#     # 3) Sanity checks per engine
#     # --------------------------
#     if prof.engine == "postgres":
#         if not prof.postgres.dsn:
#             raise ProfileConfigError(
#                 "Postgres profile missing DSN. "
#                 + "Hint: set profiles.yml → postgres.dsn or env FF_PG_DSN."
#             )
#         if prof.postgres.db_schema == "":
#             raise ProfileConfigError(
#                 "Postgres profile has empty schema. "
#                 + "Hint: set profiles.yml → postgres.db_schema or env FF_PG_SCHEMA."
#             )

#     elif prof.engine == "bigquery":
#         if not prof.bigquery.dataset:
#             raise ProfileConfigError(
#                 "BigQuery profile missing dataset. "
#                 + "Hint: set profiles.yml → bigquery.dataset or env FF_BQ_DATASET."
#             )
#         # project can come via ADC → no hard failure, only optional hint

#     elif prof.engine == "snowflake_snowpark":
#         sf = prof.snowflake_snowpark
#         missing = [
#             k
#             for k in ("account", "user", "password", "warehouse", "database", "schema")
#             if not getattr(sf, k)
#         ]
#         if missing:
#             miss = ", ".join(missing)
#             raise ProfileConfigError(
#                 f"Snowflake profile missing: {miss}. "
#                 + "Hint: set profiles.yml → snowflake_snowpark.* or env FF_SF_*."
#             )

#     # databricks_spark: usually fine (local[*]); additional checks optional

#     return prof


# ---------- Resolver (schlank) ----------
def resolve_profile(project_dir: Path, env_name: str, env: EnvSettings) -> Profile:
    profiles: dict[str, dict[str, Any]] = load_profiles(project_dir)
    raw: dict[str, Any] = (
        profiles.get(env_name)
        or profiles.get("default")
        or {"engine": "duckdb", "duckdb": {"path": ":memory:"}}
    )

    _apply_env_overrides(raw, env)
    prof: Profile = TypeAdapter(Profile).validate_python(raw)
    _sanity_check_profile(prof)
    return prof


# ---------- ENV-Overrides ----------
def _apply_env_overrides(raw: dict[str, Any], env: EnvSettings) -> None:
    if getattr(env, "ENGINE", None):
        raw["engine"] = env.ENGINE

    eng = str(raw.get("engine", "duckdb")).lower()
    handlers = {
        "duckdb": _ov_duckdb,
        "postgres": _ov_postgres,
        "bigquery": _ov_bigquery,
        "databricks_spark": _ov_databricks_spark,
        "snowflake_snowpark": _ov_snowflake_snowpark,
    }
    raw.setdefault(eng, {})
    handler = handlers.get(eng)
    if handler:
        handler(raw, env)


def _set_if(d: dict[str, Any], key: str, value: Any | None) -> None:
    if value is not None:
        d[key] = value


def _ov_duckdb(raw: dict[str, Any], env: EnvSettings) -> None:
    duck = raw.setdefault("duckdb", {})
    _set_if(duck, "path", getattr(env, "DUCKDB_PATH", None))


def _ov_postgres(raw: dict[str, Any], env: EnvSettings) -> None:
    pg = raw.setdefault("postgres", {})
    _set_if(pg, "dsn", getattr(env, "PG_DSN", None))
    if getattr(env, "PG_SCHEMA", None):
        pg["db_schema"] = env.PG_SCHEMA


def _ov_bigquery(raw: dict[str, Any], env: EnvSettings) -> None:
    bq = raw.setdefault("bigquery", {})
    _set_if(bq, "project", getattr(env, "BQ_PROJECT", None))
    if getattr(env, "BQ_DATASET", None):
        bq["dataset"] = env.BQ_DATASET
    _set_if(bq, "location", getattr(env, "BQ_LOCATION", None))
    uf = os.getenv("FF_BQ_USE_BIGFRAMES")
    if uf is not None:
        bq["use_bigframes"] = uf.lower() in ("1", "true", "yes", "on")


def _ov_databricks_spark(raw: dict[str, Any], env: EnvSettings) -> None:
    dbr = raw.setdefault("databricks_spark", {})
    _set_if(dbr, "master", getattr(env, "DBR_MASTER", None))
    _set_if(dbr, "app_name", getattr(env, "DBR_APPNAME", None))
    # ggf. weitere Connect-Parameter hier setzen


def _ov_snowflake_snowpark(raw: dict[str, Any], env: EnvSettings) -> None:
    sf = raw.setdefault("snowflake_snowpark", {})
    # Feld heißt überall "schema"
    _set_if(sf, "account", getattr(env, "SF_ACCOUNT", None))
    _set_if(sf, "user", getattr(env, "SF_USER", None))
    _set_if(sf, "password", getattr(env, "SF_PASSWORD", None))
    _set_if(sf, "warehouse", getattr(env, "SF_WAREHOUSE", None))
    _set_if(sf, "database", getattr(env, "SF_DATABASE", None))
    _set_if(sf, "schema", getattr(env, "SF_SCHEMA", None))
    _set_if(sf, "role", getattr(env, "SF_ROLE", None))


# ---------- Sanity Checks ----------
CheckFn = Callable[[Profile], None]


def _sanity_check_profile(prof: Profile) -> None:
    checks: dict[str, CheckFn] = {
        "postgres": lambda p: _check_postgres(cast(PostgresProfile, p)),
        "bigquery": lambda p: _check_bigquery(cast(BigQueryProfile, p)),
        "snowflake_snowpark": lambda p: _check_snowflake_snowpark(
            cast(SnowflakeSnowparkProfile, p)
        ),
        # "databricks_spark": optional
        # "duckdb": keine Checks erforderlich
    }
    fn = checks.get(prof.engine)
    if fn:
        fn(prof)


def _check_postgres(prof: PostgresProfile) -> None:
    if not prof.postgres.dsn:
        raise ProfileConfigError(
            "Postgres profile missing DSN. Hint: set profiles.yml → postgres.dsn or env FF_PG_DSN."
        )
    if prof.postgres.db_schema == "":
        raise ProfileConfigError(
            "Postgres profile has empty schema. "
            "Hint: set profiles.yml → postgres.db_schema or env FF_PG_SCHEMA."
        )


def _check_bigquery(prof: BigQueryProfile) -> None:
    if not prof.bigquery.dataset:
        raise ProfileConfigError(
            "BigQuery profile missing dataset. "
            "Hint: set profiles.yml → bigquery.dataset or env FF_BQ_DATASET."
        )
    # project kann via ADC kommen → kein Hard-Fail


def _check_snowflake_snowpark(prof: SnowflakeSnowparkProfile) -> None:
    sf = prof.snowflake_snowpark
    missing = [
        k
        for k in ("account", "user", "password", "warehouse", "database", "schema")
        if not getattr(sf, k)
    ]
    if missing:
        miss = ", ".join(missing)
        raise ProfileConfigError(
            f"Snowflake profile missing: {miss}. "
            "Hint: set profiles.yml → snowflake_snowpark.* or env FF_SF_*."
        )

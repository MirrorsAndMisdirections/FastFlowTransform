# fastflowtransform/cli/bootstrap.py
from __future__ import annotations

import os
from collections.abc import Callable
from contextlib import suppress
from dataclasses import dataclass
from pathlib import Path
from types import SimpleNamespace
from typing import Any, NoReturn, cast

import typer
import yaml
from dotenv import load_dotenv
from jinja2 import Environment

from fastflowtransform.core import REGISTRY
from fastflowtransform.errors import DependencyNotFoundError
from fastflowtransform.executors import (
    BigQueryBFExecutor,
    BigQueryExecutor,
    DatabricksSparkExecutor,
    DuckExecutor,
    PostgresExecutor,
    SnowflakeSnowparkExecutor,
)
from fastflowtransform.executors._shims import BigQueryConnShim, SAConnShim
from fastflowtransform.executors.base import BaseExecutor
from fastflowtransform.incremental import run_or_dispatch
from fastflowtransform.logging import echo
from fastflowtransform.settings import (
    EngineType,
    EnvSettings,
    Profile,
    resolve_profile as _resolve_profile_impl,
)


@dataclass
class CLIContext:
    project: Path
    jinja_env: Environment
    env_settings: EnvSettings
    profile: Profile

    def make_executor(self) -> tuple[Any, Callable, Callable]:
        return _make_executor(self.profile, self.jinja_env)


def _resolve_project_path(project_arg: str) -> Path:
    """
    Validate a FastFlowTransform project path:
      - must exist
      - must be a directory
      - must contain 'models/'
    """
    p = Path(project_arg).expanduser().resolve()
    if not p.exists():
        raise typer.BadParameter(
            f"Project path not found: {p}\n"
            "Tip: Benutze einen absoluten Pfad oder '.' im Projekt-Root."
        )
    if not p.is_dir():
        raise typer.BadParameter(
            f"Project path is not a directory: {p}\n"
            "Tip: Übergebe das Verzeichnis, nicht eine Datei."
        )
    models = p / "models"
    if not models.exists() or not models.is_dir():
        raise typer.BadParameter(
            f"Invalid project at {p}\n"
            "Erwartet ein Unterverzeichnis 'models/'.\n"
            "Tip: Wechsle ins Projekt und nutze '.'."
        )
    return p


def _die(msg: str, code: int = 1) -> NoReturn:
    echo(f"\n❌ {msg}")
    raise typer.Exit(code)


def _load_project_and_env(project_arg: str) -> tuple[Path, Environment]:
    proj = _resolve_project_path(project_arg)
    try:
        REGISTRY.load_project(proj)
    except DependencyNotFoundError as e:
        echo(str(e))
        raise typer.Exit(1) from e

    jenv = REGISTRY.env
    if jenv is None:
        _die("Internal error: Jinja Environment not initialized after load_project()", code=99)
    return proj, cast(Environment, jenv)


def _load_dotenv_layered(project_dir: Path, env_name: str) -> None:
    """
    Load .env in layers (lowest to highest precedence):
      1) <repo>/.env
      2) <project>/.env
      3) <project>/.env.local
      4) <project>/.env.<env_name>
      5) <project>/.env.<env_name>.local
    """

    def _safe_load(p: Path, override: bool) -> None:
        with suppress(Exception):
            load_dotenv(dotenv_path=p, override=override)

    # 1) Repo root defaults
    _safe_load(Path.cwd() / ".env", override=False)
    # 2) Project defaults
    _safe_load(project_dir / ".env", override=True)
    # 3) Project local (gitignored)
    _safe_load(project_dir / ".env.local", override=True)
    # 4) Env-specific
    _safe_load(project_dir / f".env.{env_name}", override=True)
    # 5) Env-specific local (gitignored)
    _safe_load(project_dir / f".env.{env_name}.local", override=True)


def _resolve_profile(
    env_name: str, engine: EngineType | None, proj: Path
) -> tuple[EnvSettings, Profile]:
    env = EnvSettings()
    if engine is not None:
        env = env.model_copy(update={"ENGINE": engine})
    # Prefer the safe resolver (it may already default to duckdb). On any failure,
    # fall back to a minimal DuckDB ':memory:' profile instead of exiting.
    try:
        prof = _resolve_profile_impl(proj, env_name, env)
        return env, prof
    except Exception:
        # Defensive fallback for tmp projects without profiles.yml (or misconfig).
        db_path = os.environ.get("FF_DUCKDB_PATH", ":memory:")
        # Return a SimpleNamespace with the attributes _make_executor expects.
        # This avoids any pydantic validation issues entirely.
        prof_ns = SimpleNamespace(
            engine="duckdb",
            duckdb=SimpleNamespace(path=db_path),
        )
        # Optional: concise note for visibility in tests (stderr), no exit.
        with suppress(Exception):
            echo("ℹ︎ Falling back to DuckDB (:memory:) profile.", err=True)  # Noqa RUF001
        # Type hint says Profile, aber alles was _make_executor braucht ist .engine und .duckdb.path
        return env, cast(Profile, prof_ns)


def _prepare_context(
    project_arg: str,
    env_name: str,
    engine: EngineType | None,
    vars_opt: list[str] | None,
) -> CLIContext:
    proj_raw, jenv = _load_project_and_env(project_arg)
    proj = Path(proj_raw)
    _load_dotenv_layered(proj, env_name)
    REGISTRY.set_cli_vars(_parse_cli_vars(vars_opt or []))
    env_settings, prof = _resolve_profile(env_name, engine, proj)
    return CLIContext(project=proj, jinja_env=jenv, env_settings=env_settings, profile=prof)


def _parse_cli_vars(pairs: list[str]) -> dict[str, object]:
    """
    Parse --vars key=value pairs. Values are YAML-parsed for light typing:
    --vars day='2025-10-01' limit=5 enabled=true tags='[a,b]'
    """
    out: dict[str, object] = {}
    for item in pairs:
        if "=" not in item:
            raise typer.BadParameter(f"--vars expects key=value, got: {item}")
        k, v = item.split("=", 1)
        try:
            out[k] = yaml.safe_load(v)
        except Exception:
            out[k] = v
    return out


def _get_test_con(executor: Any) -> Any:
    """
    Return a connection with .execute(...) that understands sequences and (sql, params).
    Reuse shims on the executor or build an appropriate one when needed.
    """
    if hasattr(executor, "engine"):
        try:
            return SAConnShim(executor.engine, schema=getattr(executor, "schema", None))
        except Exception:
            pass
    if hasattr(executor, "client") and hasattr(executor, "dataset"):
        try:
            return BigQueryConnShim(executor.client, executor.dataset, executor.location)
        except Exception:
            try:
                return BigQueryConnShim(executor.client, getattr(executor, "location", None))
            except Exception:
                pass
    if hasattr(executor, "con") and hasattr(executor.con, "execute"):
        return executor.con
    return executor


def _make_executor(prof: Profile, jenv: Environment) -> tuple[Any, Callable, Callable]:
    ex: BaseExecutor
    if prof.engine == "duckdb":
        ex = DuckExecutor(db_path=prof.duckdb.path)
        return ex, (lambda n: run_or_dispatch(ex, n, jenv)), ex.run_python

    if prof.engine == "postgres":
        if prof.postgres.dsn is None:
            raise RuntimeError("Postgres DSN must be set")

        ex = PostgresExecutor(dsn=prof.postgres.dsn, schema=prof.postgres.db_schema)
        return ex, (lambda n: run_or_dispatch(ex, n, jenv)), ex.run_python

    if prof.engine == "bigquery":
        if prof.bigquery.dataset is None:
            raise RuntimeError("BigQuery dataset must be set")

        if prof.bigquery.use_bigframes:
            ex = BigQueryBFExecutor(
                project=prof.bigquery.project or "",
                dataset=prof.bigquery.dataset,
                location=prof.bigquery.location,
            )
        else:
            ex = BigQueryExecutor(
                project=prof.bigquery.project or "",
                dataset=prof.bigquery.dataset,
                location=prof.bigquery.location,
            )
        return ex, (lambda n: run_or_dispatch(ex, n, jenv)), ex.run_python

    if prof.engine == "databricks_spark":
        ex = DatabricksSparkExecutor(
            master=prof.databricks_spark.master,
            app_name=prof.databricks_spark.app_name,
        )
        return ex, (lambda n: run_or_dispatch(ex, n, jenv)), ex.run_python

    if prof.engine == "snowflake_snowpark":
        cfg = {
            "account": prof.snowflake_snowpark.account,
            "user": prof.snowflake_snowpark.user,
            "password": prof.snowflake_snowpark.password,
            "warehouse": prof.snowflake_snowpark.warehouse,
            "database": prof.snowflake_snowpark.database,
            "schema": prof.snowflake_snowpark.db_schema,
        }
        if prof.snowflake_snowpark.role:
            cfg["role"] = prof.snowflake_snowpark.role
        ex = SnowflakeSnowparkExecutor(cfg)
        return ex, (lambda n: run_or_dispatch(ex, n, jenv)), ex.run_python

    _die(f"Unbekannter Engine-Typ: {getattr(prof, 'engine', None)}", code=1)
    raise AssertionError("unreachable")

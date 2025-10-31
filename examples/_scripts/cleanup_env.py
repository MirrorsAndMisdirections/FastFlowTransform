from __future__ import annotations

import argparse
import os
import shutil
import sys
from contextlib import suppress
from pathlib import Path
from typing import Any, Iterable

from dotenv import dotenv_values

SCRIPT_DIR = Path(__file__).resolve().parent
PROJECT_ROOT = SCRIPT_DIR.parents[1]
SRC_DIR = PROJECT_ROOT / "src"
if SRC_DIR.exists() and str(SRC_DIR) not in sys.path:
    sys.path.insert(0, str(SRC_DIR))

from fastflowtransform.logging import LOG_PREFIX
from fastflowtransform.settings import EnvSettings, resolve_profile


def _log(msg: str) -> None:
    if LOG_PREFIX:
        print(f"{LOG_PREFIX} {msg}")
    else:
        print(msg)


def _coerce_path(value: str | None, project: Path) -> Path | None:
    if not value:
        return None
    p = Path(value)
    if not p.is_absolute():
        p = (project / p).resolve()
    return p


def _remove_paths(paths: Iterable[Path], *, dry_run: bool) -> None:
    for path in paths:
        if not path:
            continue
        if not path.exists():
            continue
        if dry_run:
            _log(f"[dry-run] Would remove {path}")
            continue
        if path.is_dir():
            shutil.rmtree(path, ignore_errors=True)
            _log(f"Removed directory {path}")
        else:
            with suppress(Exception):
                path.unlink()
                _log(f"Removed file {path}")


def cleanup_duckdb(*, project: Path, db_path: str | None, dry_run: bool) -> None:
    candidates: list[Path] = []
    env_path = os.getenv("FF_DUCKDB_PATH")
    for raw in [db_path, env_path]:
        candidate = _coerce_path(raw, project)
        if candidate and candidate not in candidates:
            candidates.append(candidate)
            wal = candidate.with_suffix(candidate.suffix + ".wal")
            if wal not in candidates:
                candidates.append(wal)
    if candidates:
        _log("Cleaning DuckDB files")
        _remove_paths(candidates, dry_run=dry_run)


def cleanup_postgres(*, dsn: str | None, schema: str | None, dry_run: bool) -> None:
    if not dsn:
        raise ValueError("Postgres cleanup requires FF_PG_DSN or --postgres-dsn")
    if not schema:
        raise ValueError("Postgres cleanup requires FF_PG_SCHEMA or --postgres-schema")
    if dry_run:
        _log(f"[dry-run] Would drop and recreate schema '{schema}' on {dsn}")
        return
    from sqlalchemy import (
        create_engine,
        text,
    )  # local import to avoid optional dependency at import

    engine = create_engine(dsn, isolation_level="AUTOCOMMIT")
    _log(f"Dropping schema '{schema}' on {dsn}")
    with engine.begin() as conn:
        conn.execute(text(f'DROP SCHEMA IF EXISTS "{schema}" CASCADE'))
        conn.execute(text(f'CREATE SCHEMA "{schema}"'))


def _env_flag(name: str, default: bool = False) -> bool:
    val = os.getenv(name)
    if val is None:
        return default
    return val.strip().lower() in {"1", "true", "yes", "on"}


def cleanup_databricks(
    *,
    project: Path,
    master: str | None,
    app_name: str | None,
    warehouse_dir: str | None,
    database: str | None,
    catalog: str | None,
    extra_conf: dict[str, Any] | None,
    use_hive: bool,
    dry_run: bool,
) -> Path | None:
    master = master or os.getenv("FF_DBR_MASTER", "local[*]")
    app_name = app_name or os.getenv("FF_DBR_APPNAME", "cleanup")
    warehouse = warehouse_dir or os.getenv("FF_DBR_WAREHOUSE_DIR")
    database = database or os.getenv("FF_DBR_DATABASE")
    catalog = catalog or os.getenv("FF_DBR_CATALOG")
    enable_hive = use_hive or _env_flag("FF_DBR_ENABLE_HIVE", False)

    if dry_run:
        _log(
            "[dry-run] Would reset Databricks/Spark environment "
            f"(master={master}, database={database}, warehouse={warehouse})"
        )
        return None

    try:
        from pyspark.sql import SparkSession
    except ModuleNotFoundError as exc:
        raise RuntimeError("Databricks cleanup requires pyspark to be installed") from exc

    builder = SparkSession.builder.master(master).appName(app_name)

    warehouse_path: Path | None = None
    if warehouse:
        warehouse_path = Path(warehouse).expanduser()
        if not warehouse_path.is_absolute():
            warehouse_path = (project / warehouse_path).resolve()
        _log(f"Resetting warehouse directory {warehouse_path}")
        warehouse_path.mkdir(parents=True, exist_ok=True)
        builder = builder.config("spark.sql.warehouse.dir", str(warehouse_path))

    if catalog:
        builder = builder.config("spark.sql.catalog.spark_catalog", catalog)

    if extra_conf:
        for key, value in extra_conf.items():
            if value is not None:
                builder = builder.config(str(key), str(value))

    if enable_hive:
        builder = builder.config("spark.sql.catalogImplementation", "hive")
        builder = builder.enableHiveSupport()

    spark = builder.getOrCreate()
    try:
        if database:
            _log(f"Dropping database `{database}`")
            spark.sql(f"DROP DATABASE IF EXISTS `{database}` CASCADE")
            spark.sql(f"CREATE DATABASE `{database}`")
        elif catalog:
            _log(f"Clearing catalog `{catalog}` tables")
            tables = spark.sql(f"SHOW TABLES IN `{catalog}`").collect()
            for row in tables:
                db = row["database"]
                tbl = row["tableName"]
                if db:
                    spark.sql(f"DROP TABLE IF EXISTS `{catalog}`.`{db}`.`{tbl}`")
                else:
                    spark.sql(f"DROP TABLE IF EXISTS `{catalog}`.`{tbl}`")
    finally:
        with suppress(Exception):
            spark.stop()

    if warehouse_path:
        _log(f"Resetting warehouse directory {warehouse_path}")
        shutil.rmtree(warehouse_path, ignore_errors=True)
        warehouse_path.mkdir(parents=True, exist_ok=True)
    return warehouse_path


def cleanup_common_artifacts(
    *, project: Path, dry_run: bool, extra_paths: Iterable[Path] | None = None
) -> None:
    targets = [
        project / ".fastflowtransform",
        project / "docs",
        project / "site",
        project / "dist",
        project / "build",
        project / ".local",
    ]
    if extra_paths:
        for path in extra_paths:
            if path:
                targets.append(Path(path))
    _remove_paths(targets, dry_run=dry_run)
    extra = [p for p in project.glob("*.egg-info")]
    _remove_paths(extra, dry_run=dry_run)


def _load_dotenv_layered(project_dir: Path, env_name: str) -> None:
    original_env = dict(os.environ)
    merged: dict[str, str] = {}

    def _merge(p: Path) -> None:
        try:
            if not p.exists():
                return
            data = dotenv_values(p)
            for key, value in (data or {}).items():
                if value is not None:
                    merged[key] = value
        except Exception:
            pass

    _merge(Path.cwd() / ".env")
    _merge(project_dir / ".env")
    _merge(project_dir / ".env.local")
    _merge(project_dir / f".env.{env_name}")
    _merge(project_dir / f".env.{env_name}.local")

    for key, value in merged.items():
        if key not in original_env and value is not None:
            os.environ.setdefault(key, value)


def _load_profile(project: Path, env_name: str, engine: str | None):
    env_settings = EnvSettings()
    if engine:
        env_settings = env_settings.model_copy(update={"ENGINE": engine})
    env_settings = env_settings.model_copy(update={"ENV": env_name})
    return resolve_profile(project, env_name, env_settings)


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description="Reset FastFlowTransform example environments.")
    parser.add_argument(
        "--engine", required=True, choices=["duckdb", "postgres", "databricks_spark"]
    )
    parser.add_argument("--project", default=".")
    parser.add_argument("--env", help="Profile environment name (e.g. dev_duckdb).")
    parser.add_argument("--duckdb-path")
    parser.add_argument("--postgres-dsn")
    parser.add_argument("--postgres-schema")
    parser.add_argument("--spark-master")
    parser.add_argument("--spark-app-name")
    parser.add_argument("--spark-warehouse")
    parser.add_argument("--spark-database")
    parser.add_argument("--spark-catalog")
    parser.add_argument(
        "--spark-use-hive", action="store_true", help="Force Hive metastore enablement for cleanup."
    )
    parser.add_argument("--dry-run", action="store_true")
    parser.add_argument(
        "--skip-artifacts",
        action="store_true",
        help="Do not remove local artifacts (.fastflowtransform, docs, site, .local).",
    )

    args = parser.parse_args(argv)
    project = Path(args.project).expanduser().resolve()

    env_name = (
        args.env
        or os.getenv("FFT_ACTIVE_ENV")
        or ("dev_" + args.engine if args.engine in {"duckdb", "postgres"} else "dev")
    )

    os.environ["FFT_ACTIVE_ENV"] = env_name
    _load_dotenv_layered(project, env_name)

    profile = None
    try:
        profile = _load_profile(project, env_name, args.engine)
    except Exception as exc:  # pragma: no cover - best-effort logging
        _log(
            f"Warning: failed to resolve profile '{env_name}' for engine '{args.engine}': {exc}. "
            "Continuing with environment variables only."
        )

    warehouse_path: Path | None = None
    try:
        if args.engine == "duckdb":
            profile_duckdb = (
                getattr(getattr(profile, "duckdb", None), "path", None) if profile else None
            )
            db_path = args.duckdb_path or os.getenv("FF_DUCKDB_PATH") or profile_duckdb
            cleanup_duckdb(project=project, db_path=db_path, dry_run=args.dry_run)
        elif args.engine == "postgres":
            profile_pg = getattr(profile, "postgres", None) if profile else None
            profile_dsn = getattr(profile_pg, "dsn", None) if profile_pg else None
            profile_schema = getattr(profile_pg, "db_schema", None) if profile_pg else None
            dsn = args.postgres_dsn or os.getenv("FF_PG_DSN") or profile_dsn
            schema = args.postgres_schema or os.getenv("FF_PG_SCHEMA") or profile_schema
            cleanup_postgres(dsn=dsn, schema=schema, dry_run=args.dry_run)
        elif args.engine == "databricks_spark":
            profile_db = getattr(profile, "databricks_spark", None) if profile else None
            profile_master = getattr(profile_db, "master", None) if profile_db else None
            profile_app = getattr(profile_db, "app_name", None) if profile_db else None
            profile_warehouse = getattr(profile_db, "warehouse_dir", None) if profile_db else None
            profile_database = getattr(profile_db, "database", None) if profile_db else None
            profile_catalog = getattr(profile_db, "catalog", None) if profile_db else None
            profile_use_hive = (
                getattr(profile_db, "use_hive_metastore", False) if profile_db else False
            )
            profile_extra_conf = getattr(profile_db, "extra_conf", None) if profile_db else None
            warehouse_path = cleanup_databricks(
                project=project,
                master=args.spark_master or profile_master,
                app_name=args.spark_app_name or profile_app,
                warehouse_dir=args.spark_warehouse or profile_warehouse,
                database=args.spark_database or profile_database,
                catalog=args.spark_catalog or profile_catalog,
                extra_conf=profile_extra_conf,
                use_hive=args.spark_use_hive or bool(profile_use_hive),
                dry_run=args.dry_run,
            )
    except Exception as exc:
        _log(f"Cleanup failed: {exc}")
        return 1

    if not args.skip_artifacts:
        extra_paths: list[Path] = []
        if args.engine == "databricks_spark":
            configured = (
                args.spark_warehouse
                or os.getenv("FF_DBR_WAREHOUSE_DIR")
                or getattr(getattr(profile, "databricks_spark", None), "warehouse_dir", None)
            )
            if configured:
                p = Path(configured).expanduser()
                if not p.is_absolute():
                    p = (project / p).resolve()
                extra_paths.append(p)
            if warehouse_path:
                extra_paths.append(warehouse_path)
            extra_paths.append((project / "spark-warehouse").resolve())
        cleanup_common_artifacts(project=project, dry_run=args.dry_run, extra_paths=extra_paths)

    return 0


if __name__ == "__main__":
    sys.exit(main())

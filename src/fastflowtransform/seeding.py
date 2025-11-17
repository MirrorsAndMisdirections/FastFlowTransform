# src/fastflowtransform/seeding.py
from __future__ import annotations

import math
import shutil
import uuid
from collections.abc import Callable, Iterable
from contextlib import suppress
from pathlib import Path
from time import perf_counter
from typing import Any, NamedTuple, cast
from urllib.parse import unquote, urlparse

import pandas as pd
from pyspark.sql import DataFrame as SDF, SparkSession

from fastflowtransform import storage
from fastflowtransform.config.seeds import SeedsSchemaConfig, load_seeds_schema
from fastflowtransform.logging import echo
from fastflowtransform.settings import EngineType

try:  # Optional Spark dependency
    from pyspark.errors.exceptions.base import AnalysisException as _SparkAnalysisException
except Exception:  # pragma: no cover - Spark not installed
    _SparkAnalysisException = Exception  # type: ignore


# ----------------------------- File I/O & Schema (dtypes) -----------------------------


def _read_seed_file(path: Path) -> pd.DataFrame:
    """Read a seed file (.csv, .parquet, .pq) into a pandas DataFrame."""
    if path.suffix.lower() == ".csv":
        return pd.read_csv(path)
    if path.suffix.lower() in (".parquet", ".pq"):
        return pd.read_parquet(path)
    raise ValueError(f"Unsupported seed file format: {path.name}")


def _apply_schema(
    df: pd.DataFrame,
    table: str,
    schema_cfg: SeedsSchemaConfig | None,
) -> pd.DataFrame:
    """
    Apply optional pandas dtypes from seeds/schema.yml for a given table key.

    The validated configuration is:

      dtypes:
        <table_key>:
          col_a: string
          col_b: int64

    Casting errors are swallowed on purpose to avoid blocking seed loads.
    """
    if not schema_cfg:
        return df
    dtypes: dict[str, str] = schema_cfg.dtypes.get(table) or {}
    if not dtypes:
        return df

    cast_map = {col: dtype for col, dtype in dtypes.items() if col in df.columns}
    try:
        return df.astype(cast_map)
    except Exception:
        # Prefer loading data over failing the run; you may log a warning here.
        return df


# -------------------------------- Identifier utilities --------------------------------


def _dq(ident: str) -> str:
    """Double-quote an SQL identifier (DuckDB/Postgres compatible)."""
    return '"' + ident.replace('"', '""') + '"'


def _is_qualified(name: str) -> bool:
    """Return True if the provided table name appears to be schema-qualified."""
    return "." in name


def _qualify(table: str, schema: str | None, catalog: str | None = None) -> str:
    """
    Return a safely quoted, optionally schema-qualified identifier.
    - Respects already-qualified names like raw.users or "raw"."users".
    - Quotes each identifier part individually.
    """
    if _is_qualified(table):
        return ".".join(_dq(p) for p in table.split("."))
    catalog_part = catalog.strip() if isinstance(catalog, str) and catalog.strip() else None
    if schema:
        schema_part = schema.strip()
        parts: list[str] = []
        if catalog_part:
            parts.append(_dq(catalog_part))
        parts.append(_dq(schema_part))
        parts.append(_dq(table))
        return ".".join(parts)
    if catalog_part:
        return f"{_dq(catalog_part)}.{_dq(table)}"
    return _dq(table)


def _spark_warehouse_base(spark: Any) -> Path | None:
    """Resolve the Spark warehouse directory if it points to the local filesystem."""
    try:
        conf_val = spark.conf.get("spark.sql.warehouse.dir", "spark-warehouse")
    except Exception:
        conf_val = "spark-warehouse"

    if not isinstance(conf_val, str):
        conf_val = str(conf_val)
    parsed = urlparse(conf_val)
    scheme = (parsed.scheme or "").lower()

    if scheme and scheme != "file":
        return None

    if scheme == "file":
        # Treat file:// URIs as local filesystem paths.
        if parsed.netloc and parsed.netloc not in {"", "localhost"}:
            return None
        raw_path = unquote(parsed.path or "")
        if not raw_path:
            return None
        base = Path(raw_path)
    else:
        base = Path(conf_val)

    if not base.is_absolute():
        base = Path.cwd() / base
    return base


def _spark_table_location(parts: list[str], spark: Any) -> Path | None:
    """
    Best-effort guess of the filesystem location for a managed Spark table.
    Works for default schema, schema.table, and catalog.schema.table patterns.
    """
    base = _spark_warehouse_base(spark)
    if base is None or not parts:
        return None

    filtered = [p for p in parts if p]
    if not filtered:
        return None

    # Drop common catalog prefixes while retaining the schema name.
    catalog_cutoff = 3
    if len(filtered) >= catalog_cutoff and filtered[0].lower() in {"spark_catalog", "spark"}:
        filtered = filtered[1:]

    table = filtered[-1]
    schema_cutoff = 2
    schema = filtered[-2] if len(filtered) >= schema_cutoff else None

    location = base
    if schema:
        location = location / f"{schema}.db"
    return location / table


# -------------------------------- Pretty echo helpers ---------------------------------


def _human_int(n: int) -> str:
    """Format integers with thin-space grouping (12 345)."""
    return f"{n:,}".replace(",", " ")


def _human_bytes(n: int) -> str:
    """Coarse byte-size formatting for user hints."""
    mb_threshold = 1024
    if n < mb_threshold:
        return f"{n} B"
    units = ["KB", "MB", "GB", "TB", "PB"]
    exp = min(int(math.log(n, 1024)), len(units))
    val = n / (1024**exp)
    unit = units[exp - 1] if exp > 0 else "KB"
    return f"{val:.1f} {unit}"


def _echo_seed_line(
    full_name: str,
    rows: int,
    cols: int,
    engine: str,
    ms: int,
    created_schema: bool = False,
    action: str = "replaced",
    extra: str | None = None,
) -> None:
    """
    Emit a single pretty seed log line, e.g.:
      — ✓ raw.users • 12 345x6 • 1.2 MB • 138 ms [duckdb] (+schema)
    """
    size_hint: str | None = None
    with suppress(Exception):
        # Heuristic: 8 bytes per cell. Good enough as a hint, not exact.
        size_hint = _human_bytes(rows * max(cols, 1) * 8)

    parts = [
        f"✓ {full_name}",
        f"{_human_int(rows)}×{cols}",  # Noqa RUF001
        *([size_hint] if size_hint else []),
        f"{ms} ms",
        f"[{engine}]",
        *(["(+schema)"] if created_schema else []),
        *([extra] if extra else []),
    ]
    echo("— " + " • ".join(parts))


# ------------------------------ Target resolution (CFG) -------------------------------


class SeedTarget(NamedTuple):
    """Resolved seed target (schema, table)."""

    schema: str | None
    table: str


def _engine_name_from_executor(executor: Any) -> str:
    """
    Infer a canonical engine name from the executor object.

    Preference:
      1) executor.engine_name (BaseExecutor-derived)
      2) Spark hint → "databricks_spark"
      3) SQLAlchemy dialect ("postgresql" → "postgres", "bigquery" → "bigquery")
      4) DuckDB heuristic (executor.con present) → "duckdb"
      5) "unknown" as last resort
    """
    # 1) BaseExecutor-style engine_name
    engine_name = getattr(executor, "engine_name", None)
    if isinstance(engine_name, str) and engine_name.strip():
        return engine_name.strip()

    # 2) Spark-style executor
    if getattr(executor, "spark", None) is not None:
        return "databricks_spark"

    # 3) SQLAlchemy-based executors
    eng = getattr(executor, "engine", None)
    if eng is not None:
        dialect_name = getattr(getattr(eng, "dialect", None), "name", None)
        if isinstance(dialect_name, str) and dialect_name:
            low = dialect_name.lower()
            if low.startswith("postgres"):
                return "postgres"
            if low.startswith("bigquery"):
                return "bigquery"
            return low

    # 4) DuckDB-ish: has a .con (DuckDBPyConnection or similar)
    if getattr(executor, "con", None) is not None:
        return "duckdb"

    # 5) Fallback
    return "unknown"


def _seed_id(seeds_dir: Path, path: Path) -> str:
    """
    Build a unique seed ID from the path relative to `seeds/`, without the extension.
    Examples:
      seeds/raw/users.csv      -> "raw/users"
      seeds/staging/users.parquet -> "staging/users"
      seeds/users.csv          -> "users"
    """
    rel = path.relative_to(seeds_dir)
    return rel.with_suffix("").as_posix()


def _resolve_schema_and_table_by_cfg(
    seed_id: str,
    stem: str,
    schema_cfg: SeedsSchemaConfig | None,
    executor: Any,
    default_schema: str | None,
) -> tuple[str | None, str]:
    """
    Resolve (schema, table) using seeds/schema.yml with a clear priority:
      1) targets[<seed_id>] (recommended; path-based ID e.g. "raw/users")
      2) targets[<seed_id with dots>] (optional convenience; "raw.users")
      3) targets[<stem>] (legacy; only safe if the stem is unique)
      4) default_schema (profile/executor-supplied)
    Supports:
      targets:
        raw/users:
          schema: raw
          table: users
          schema_by_engine:
            postgres: raw
            duckdb: main
    """
    schema = default_schema
    table = stem
    if not schema_cfg:
        return schema, table

    targets = schema_cfg.targets
    engine = _engine_name_from_executor(executor)

    entry = targets.get(seed_id)
    if not entry:
        # Optional "raw.users" style key as a convenience
        dotted_id = seed_id.replace("/", ".")
        entry = targets.get(dotted_id)

    # stem-based only if present (uniqueness checked by caller)
    if not entry and stem in targets:
        entry = targets[stem]

    if not entry:
        return schema, table

    table = entry.table or table
    engine = _engine_name_from_executor(executor)
    engine_key = cast(EngineType, engine)
    schema = entry.schema_by_engine.get(engine_key) or entry.schema_ or schema
    return schema, table


# ------------------------------ Materialization (engines) ------------------------------

# ------------------------------------------------------------
# Engine-specifig Handlers
# ------------------------------------------------------------


def _handle_duckdb(table: str, df: pd.DataFrame, executor: Any, schema: str | None) -> bool:
    con = getattr(executor, "con", None)
    if con is None:
        return False

    try:
        import duckdb as _dd  # Noqa PLC0415

        is_duck_con = isinstance(con, _dd.DuckDBPyConnection)
    except Exception:
        is_duck_con = all(hasattr(con, m) for m in ("register", "execute"))

    if not is_duck_con:
        return False

    catalog = getattr(executor, "catalog", None)
    full_name = _qualify(table, schema, catalog)
    created_schema = False
    if schema and not _is_qualified(table):
        con.execute(f"create schema if not exists {_dq(schema)}")
        created_schema = True

    t0 = perf_counter()
    tmp = f"_ff_seed_{uuid.uuid4().hex[:8]}"
    con.register(tmp, df)
    try:
        con.execute(f"create or replace table {full_name} as select * from {_dq(tmp)}")
    finally:
        with suppress(Exception):
            con.unregister(tmp)  # duckdb >= 0.8
        with suppress(Exception):
            con.execute(f"drop view if exists {_dq(tmp)}")

    dt_ms = int((perf_counter() - t0) * 1000)
    _echo_seed_line(
        full_name=full_name,
        rows=len(df),
        cols=df.shape[1],
        engine="duckdb",
        ms=dt_ms,
        created_schema=created_schema,
        action="replaced",
    )
    return True


def _handle_sqlalchemy(table: str, df: pd.DataFrame, executor: Any, schema: str | None) -> bool:
    eng = getattr(executor, "engine", None)
    if eng is None:
        return False
    if "sqlalchemy" not in getattr(eng.__class__, "__module__", ""):
        return False

    full_name = _qualify(table, schema)
    dialect_name = getattr(getattr(eng, "dialect", None), "name", "") or ""
    if dialect_name.lower() == "postgresql":
        # Postgres blocks DROP TABLE when dependent views exist (e.g. stg_* views).
        drop_sql = f"DROP TABLE IF EXISTS {full_name} CASCADE"
        with eng.begin() as conn:
            conn.exec_driver_sql(drop_sql)

    t0 = perf_counter()
    df.to_sql(table, eng, if_exists="replace", index=False, schema=schema, method="multi")
    dt_ms = int((perf_counter() - t0) * 1000)

    dialect = dialect_name or getattr(getattr(eng, "dialect", None), "name", "sqlalchemy")
    _echo_seed_line(
        full_name=full_name,
        rows=len(df),
        cols=df.shape[1],
        engine=dialect,
        ms=dt_ms,
        created_schema=False,
        action="replaced",
    )
    return True


def _handle_bigquery(table: str, df: pd.DataFrame, executor: Any, schema: str | None) -> bool:
    """
    Handle seeding for the BigQuery executor using the official client.

    We detect BigQuery by the presence of an attribute named ``client``
    that behaves like ``google.cloud.bigquery.Client``. The target dataset
    is resolved as:

      1) the provided ``schema`` argument (preferred; allows seeds/schema.yml
         to control datasets explicitly), or
      2) an executor attribute such as ``dataset`` / ``dataset_id``.

    Notes:
    - The dataset must already exist; this function does not create it.
    - We use WRITE_TRUNCATE semantics (replace) to mirror the behavior of
      the DuckDB / SQLAlchemy handlers.
    """
    client = getattr(executor, "client", None)
    if client is None:
        return False

    # Prefer explicit schema from the caller / seeds/schema.yml.
    dataset_id = (
        schema or getattr(executor, "dataset", None) or getattr(executor, "dataset_id", None)
    )
    if not isinstance(dataset_id, str) or not dataset_id.strip():
        # Not a BigQuery executor we know how to handle.
        return False

    dataset_id = dataset_id.strip()

    # Project: executor may expose it explicitly; otherwise fall back to the
    # client project (Application Default Credentials, etc.).
    project_id = getattr(executor, "project", None)
    if not isinstance(project_id, str) or not project_id.strip():
        project_id = getattr(client, "project", None)

    if isinstance(project_id, str) and project_id.strip():
        table_id = f"{project_id.strip()}.{dataset_id}.{table}"
        full_name = table_id
    else:
        # Dataset-qualified ID still works if a default project is set on the client.
        table_id = f"{dataset_id}.{table}"
        full_name = table_id

    try:
        from google.cloud import bigquery  # noqa PLC0415 type: ignore  # pragma: no cover
    except Exception as exc:  # pragma: no cover - missing optional dependency
        raise RuntimeError(
            "google-cloud-bigquery is required for seeding into BigQuery, "
            "but it is not installed. Install the BigQuery extras for "
            "FastFlowTransform or add google-cloud-bigquery to your environment."
        ) from exc

    job_config = bigquery.LoadJobConfig(write_disposition=bigquery.WriteDisposition.WRITE_TRUNCATE)

    t0 = perf_counter()
    # Let the BigQuery client infer the schema from the pandas DataFrame.
    load_job = client.load_table_from_dataframe(df, table_id, job_config=job_config)
    load_job.result()  # Wait for completion
    dt_ms = int((perf_counter() - t0) * 1000)

    _echo_seed_line(
        full_name=full_name,
        rows=len(df),
        cols=df.shape[1],
        engine="bigquery",
        ms=dt_ms,
        created_schema=False,
        action="replaced",
    )
    return True


def _spark_ident(name: str) -> str:
    """Return a Spark-safe identifier (escapes backticks)."""
    return name.replace("`", "``")


def _prepare_spark_target(
    table: str,
    schema: str | None,
    executor: Any,
    spark: Any,
) -> tuple[str, str, Any, bool]:
    """
    Build Spark target identifiers and detect the table location.

    Returns:
        target_identifier: unquoted table identifier (db.table or table)
        target_sql: quoted SQL identifier with backticks
        target_location: filesystem location (may be None)
        created_schema: whether a database schema was created implicitly
    """
    created_schema = False

    if schema and not _is_qualified(table):
        spark.sql(f"CREATE DATABASE IF NOT EXISTS `{_spark_ident(schema)}`")
        created_schema = True
        parts = [schema, table]
    else:
        parts = table.split(".")

    parts = [p for p in parts if p]
    target_identifier = ".".join(parts)
    target_sql = ".".join(f"`{_spark_ident(p)}`" for p in parts)
    target_location = _spark_table_location(parts, spark)
    return target_identifier, target_sql, target_location, created_schema


def _write_spark_seed_to_path(
    spark: Any,
    target_identifier: str,
    sdf: Any,
    storage_meta: dict[str, Any],
    table_format: str | None,
    table_options: dict[str, Any],
) -> str:
    """Write the seed via a custom storage path configuration."""
    storage.spark_write_to_path(
        spark,
        target_identifier,
        sdf,
        storage=storage_meta,
        default_format=table_format,
        default_options=table_options,
    )
    return "custom path"


def _write_spark_seed_managed(
    executor: Any,
    spark: SparkSession,
    target_identifier: str,
    sdf: SDF,
    table_format: str | None,
    table_options: dict[str, Any],
) -> str | None:
    """
    Write the seed as a *managed* Spark table (no custom storage path).

    For engines like DatabricksSparkExecutor this ensures that the
    table_format handler (Delta / Iceberg / etc.) is used, so Iceberg
    seeds become proper Iceberg tables.
    """
    try:
        # Prefer engine-specific helper when available (e.g. DatabricksSparkExecutor)
        save_df = getattr(executor, "_save_df_as_table", None)
        if callable(save_df):
            # Pass a truthy storage dict with path=None so we do *not* get
            # redirected back into path-based storage again.
            save_df(
                target_identifier,
                sdf,
                storage={"path": None, "options": table_options or {}},
            )
        else:
            # Generic Spark fallback: may not be format-aware, but keeps behavior
            writer = sdf.write.mode("overwrite")
            if table_format:
                writer = writer.format(table_format)
            if table_options:
                writer = writer.options(**table_options)
            writer.saveAsTable(target_identifier)
    except Exception as exc:
        raise RuntimeError(
            f"Failed to materialize Spark seed '{target_identifier}' as managed table: {exc}"
        ) from exc
    # No temporary path to clean up for managed tables
    return None


def _spark_write_table(
    sdf: Any,
    target_identifier: str,
    table_format: str | None,
    table_options: dict[str, Any],
) -> None:
    """Perform the actual Spark saveAsTable call with configured options."""
    writer = sdf.write.mode("overwrite")
    if table_format:
        writer = writer.format(table_format)
    if table_options:
        writer = writer.options(**table_options)
    writer.saveAsTable(target_identifier)


def _reset_spark_table_and_location(
    spark: Any,
    target_sql: str,
    target_location: Any,
) -> str | None:
    """
    Drop the Spark table and remove the underlying location if possible.

    Returns:
        A cleanup hint string (e.g. 'reset location') or None.
    """
    with suppress(Exception):
        spark.sql(f"DROP TABLE IF EXISTS {target_sql}")

    cleanup_hint: str | None = None
    if target_location and target_location.exists():
        with suppress(Exception):
            shutil.rmtree(target_location, ignore_errors=True)
            cleanup_hint = "reset location"
    return cleanup_hint


def _write_spark_seed_to_table(
    spark: Any,
    sdf: Any,
    target_identifier: str,
    target_sql: str,
    target_location: Any,
    table_format: str | None,
    table_options: dict[str, Any],
) -> str | None:
    """
    Write the seed as a managed Spark table, handling common location issues.

    Returns:
        A cleanup hint string describing corrective actions, or None.
    """
    cleanup_hint = _reset_spark_table_and_location(spark, target_sql, target_location)

    try:
        _spark_write_table(sdf, target_identifier, table_format, table_options)
        return cleanup_hint
    except _SparkAnalysisException as exc:
        message = str(exc)
        if target_location and "LOCATION_ALREADY_EXISTS" in message.upper():
            # Attempt to fix by resetting the table location and retrying once.
            with suppress(Exception):
                shutil.rmtree(target_location, ignore_errors=True)
            cleanup_hint = "reset location"
            _spark_write_table(sdf, target_identifier, table_format, table_options)
            return cleanup_hint
        raise RuntimeError(f"Spark seed load failed for {target_sql}: {exc}") from exc
    except Exception as exc:  # pragma: no cover - generic safety net
        raise RuntimeError(f"Spark seed load failed for {target_sql}: {exc}") from exc


def _detect_spark_storage_format(
    storage_meta: Any,
    table_format: Any,
) -> str:
    """
    Determine an effective storage format label (e.g. 'delta') from storage
    metadata or executor configuration.
    """
    storage_format = ""
    if isinstance(storage_meta, dict):
        raw_fmt = storage_meta.get("format")
        if isinstance(raw_fmt, str) and raw_fmt.strip():
            storage_format = raw_fmt.strip().lower()

    if not storage_format and isinstance(table_format, str) and table_format.strip():
        storage_format = table_format.strip().lower()

    return storage_format


def _handle_spark(
    table: str,
    df: pd.DataFrame,
    executor: Any,
    schema: str | None,
) -> bool:
    """Try to detect and handle Spark/Databricks for seeding."""
    spark = getattr(executor, "spark", None)
    if spark is None:
        return False

    target_identifier, target_sql, target_location, created_schema = _prepare_spark_target(
        table=table,
        schema=schema,
        executor=executor,
        spark=spark,
    )

    table_format = getattr(executor, "spark_table_format", None)
    table_options = getattr(executor, "spark_table_options", None) or {}
    format_handler = getattr(executor, "_format_handler", None)

    storage_meta = storage.get_seed_storage(target_identifier)

    t0 = perf_counter()
    sdf = spark.createDataFrame(df)

    cleanup_hint: str | None = None
    allows_unmanaged = bool(getattr(format_handler, "allows_unmanaged_paths", lambda: True)())

    if storage_meta.get("path") and allows_unmanaged:
        # Behavior for parquet/delta/etc: respect custom path.
        cleanup_hint = _write_spark_seed_to_path(
            spark=spark,
            target_identifier=target_identifier,
            sdf=sdf,
            storage_meta=storage_meta,
            table_format=table_format,
            table_options=table_options,
        )
    else:
        # Behavior when no path is configured: table-based seed via executor handler
        try:
            if hasattr(executor, "_save_df_as_table"):
                executor._save_df_as_table(target_identifier, sdf, storage={"path": None})
                cleanup_hint = None
            else:
                cleanup_hint = _write_spark_seed_to_table(
                    spark=spark,
                    sdf=sdf,
                    target_identifier=target_identifier,
                    target_sql=target_sql,
                    target_location=target_location,
                    table_format=table_format,
                    table_options=table_options,
                )
        except Exception as exc:  # pragma: no cover
            raise RuntimeError(f"Spark seed load failed for {target_sql}: {exc}") from exc

    dt_ms = int((perf_counter() - t0) * 1000)

    storage_format = _detect_spark_storage_format(storage_meta, table_format)
    engine_label = f"spark/{storage_format}" if storage_format else "spark"

    _echo_seed_line(
        full_name=target_sql,
        rows=len(df),
        cols=df.shape[1],
        engine=engine_label,
        ms=dt_ms,
        created_schema=created_schema,
        action="replaced",
        extra=cleanup_hint,
    )
    return True


# ------------------------------------------------------------
# Dispatcher
# ------------------------------------------------------------

Handler = Callable[[str, pd.DataFrame, Any, str | None], bool]

_HANDLERS: Iterable[Handler] = (_handle_duckdb, _handle_sqlalchemy, _handle_spark, _handle_bigquery)


def materialize_seed(
    table: str, df: pd.DataFrame, executor: Any, schema: str | None = None
) -> None:
    """
    Materialize a DataFrame as a database table across engines.
    """
    for handler in _HANDLERS:
        if handler(table, df, executor, schema):
            return

    raise RuntimeError("No compatible executor connection for seeding found.")


# ----------------------------------- Seeding runner -----------------------------------


def seed_project(project_dir: Path, executor: Any, default_schema: str | None = None) -> int:
    """
    Load every seed file under <project>/seeds recursively and materialize it.

    Supports configuration in seeds/schema.yml (validated via Pydantic):

      targets:
        <seed-id>:                 # e.g., "raw/users" (path-based, recommended)
          schema: <schema-name>    # global target schema
          table: <table-name>      # optional rename
          schema_by_engine:        # optional engine overrides (EngineType keys)
            postgres: raw
            duckdb: main

      dtypes:
        <table-key>:
          column_a: string
          column_b: int64

    Resolution priority for (schema, table):
      1) targets[<seed-id>]  (e.g., "raw/users")
      2) targets[<seed-id with dots>] (e.g., "raw.users")
      3) targets[<stem>] (*only* if stem is unique)
      4) executor.schema or default_schema

    Returns:
      Number of successfully materialized seed tables.

    Raises:
      ValueError: if schema.yml uses a plain stem key while multiple files share that stem.
    """
    seeds_dir = project_dir / "seeds"
    if not seeds_dir.exists():
        return 0

    # Pydantic-validated seeds/schema.yml (or None if not present)
    schema_cfg = load_seeds_schema(project_dir)

    # Collect seed files recursively to allow folder-based schema conventions.
    paths: list[Path] = [
        p
        for p in sorted(seeds_dir.rglob("*"))
        if p.is_file() and p.suffix.lower() in (".csv", ".parquet", ".pq")
    ]
    if not paths:
        return 0

    # Check for ambiguous stems (same filename in different folders).
    stem_counts: dict[str, int] = {}
    for p in paths:
        stem_counts[p.stem] = stem_counts.get(p.stem, 0) + 1

    count = 0
    for path in paths:
        seedid = _seed_id(seeds_dir, path)
        stem = path.stem

        # Default schema may come from executor or caller.
        base_schema = getattr(executor, "schema", None) or default_schema
        schema, table = _resolve_schema_and_table_by_cfg(
            seedid, stem, schema_cfg, executor, base_schema
        )

        # If schema.yml uses a bare stem while that stem exists multiple times,
        # force disambiguation.
        if schema_cfg and stem in schema_cfg.targets and stem_counts.get(stem, 0) > 1:
            raise ValueError(
                f'Seed stem "{stem}" appears multiple times. '
                f"Please configure using the path-based seed ID "
                f'(e.g., "{seedid}") in seeds/schema.yml.'
            )

        df = _read_seed_file(path)
        # Use the resolved *table* key for dtypes (allows rename-aware dtype mapping in cfg).
        df = _apply_schema(df, table, schema_cfg)

        materialize_seed(table, df, executor, schema=schema)
        count += 1

    return count

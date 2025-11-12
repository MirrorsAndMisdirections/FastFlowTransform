# src/fastflowtransform/executors/databricks_spark_exec.py
from __future__ import annotations

from collections.abc import Iterable
from contextlib import suppress
from pathlib import Path
from typing import Any
from urllib.parse import unquote, urlparse

from pyspark.sql import DataFrame as SDF, SparkSession
from pyspark.sql.types import DataType

try:
    # Enable Delta Lake via delta-spark when available
    from delta import configure_spark_with_delta_pip
except Exception:  # pragma: no cover
    configure_spark_with_delta_pip = None  # type: ignore[assignment]

from fastflowtransform import storage
from fastflowtransform.core import REGISTRY, Node, relation_for
from fastflowtransform.errors import ModelExecutionError
from fastflowtransform.executors.base import BaseExecutor
from fastflowtransform.meta import ensure_meta_table, upsert_meta

_DELTA_EXTENSION = "io.delta.sql.DeltaSparkSessionExtension"
_DELTA_CATALOG = "org.apache.spark.sql.delta.catalog.DeltaCatalog"


def _csv_tokens(value: str | None) -> list[str]:
    if not value:
        return []
    return [part.strip() for part in value.split(",") if part and part.strip()]


def _ensure_csv_token(value: str | None, token: str) -> tuple[str | None, bool]:
    tokens = _csv_tokens(value)
    if token in tokens:
        return value, False
    tokens.append(token)
    return ",".join(tokens), True


def _as_nonempty_str(value: Any | None) -> str | None:
    if value is None:
        return None
    text = str(value).strip()
    return text or None


class DatabricksSparkExecutor(BaseExecutor[SDF]):
    ENGINE_NAME = "databricks_spark"
    """Spark/Databricks executor without pandas: Python models operate on Spark DataFrames."""

    def __init__(
        self,
        master: str = "local[*]",
        app_name: str = "fastflowtransform",
        *,
        extra_conf: dict[str, Any] | None = None,
        warehouse_dir: str | None = None,
        use_hive_metastore: bool = False,
        catalog: str | None = None,
        database: str | None = None,
        table_format: str | None = "parquet",
        table_options: dict[str, Any] | None = None,
    ):
        extra_conf = dict(extra_conf or {})
        builder = SparkSession.builder.master(master).appName(app_name)

        warehouse_path: Path | None = None
        if warehouse_dir:
            warehouse_path = Path(warehouse_dir).expanduser()
            if not warehouse_path.is_absolute():
                warehouse_path = Path.cwd() / warehouse_path
            warehouse_path.mkdir(parents=True, exist_ok=True)
            builder = builder.config("spark.sql.warehouse.dir", str(warehouse_path))

        catalog_value = _as_nonempty_str(catalog)
        if catalog_value:
            builder = builder.config("spark.sql.catalog.spark_catalog", catalog_value)

        if extra_conf:
            for key, value in extra_conf.items():
                if value is not None:
                    builder = builder.config(str(key), str(value))

        if use_hive_metastore:
            builder = builder.config("spark.sql.catalogImplementation", "hive")
            builder = builder.enableHiveSupport()

        # Apply Delta configuration last, after all Spark configs are set.
        wants_delta = (table_format or "").strip().lower() == "delta"
        if wants_delta:
            if configure_spark_with_delta_pip is None:
                raise RuntimeError(
                    "Delta table_format requested for DatabricksSparkExecutor, "
                    "but 'delta-spark' is not installed. "
                    "Install it with: pip install delta-spark"
                )
            builder = configure_spark_with_delta_pip(builder)
            ext_key = "spark.sql.extensions"
            existing_ext = _as_nonempty_str(extra_conf.get(ext_key))
            merged_ext, changed = _ensure_csv_token(existing_ext, _DELTA_EXTENSION)
            if changed or existing_ext is None:
                builder = builder.config(ext_key, merged_ext)

            catalog_key = "spark.sql.catalog.spark_catalog"
            extra_catalog = _as_nonempty_str(extra_conf.get(catalog_key))
            catalog_overridden = bool(catalog_value) or bool(extra_catalog)
            if not catalog_overridden:
                builder = builder.config(catalog_key, _DELTA_CATALOG)

        self.spark = builder.getOrCreate()
        # Lightweight testing shim so tests can call executor.con.execute("SQL")
        self.con = _SparkConnShim(self.spark)
        self._registered_path_sources: dict[str, dict[str, Any]] = {}
        self.warehouse_dir = warehouse_path
        self.catalog = catalog
        self.database = database
        self.schema = database
        if database:
            self.spark.sql(f"CREATE DATABASE IF NOT EXISTS `{database}`")
            with suppress(Exception):
                self.spark.catalog.setCurrentDatabase(database)

        fmt = (table_format or "").strip().lower()
        self.spark_table_format: str | None = fmt or None
        if table_options:
            self.spark_table_options = {str(k): str(v) for k, v in table_options.items()}
        else:
            self.spark_table_options = {}

    # ---------- Frame hooks (required) ----------
    def _read_relation(self, relation: str, node: Node, deps: Iterable[str]) -> SDF:
        # relation may optionally be "db.table" (via source()/ref())
        return self.spark.table(relation)

    def _materialize_relation(self, relation: str, df: SDF, node: Node) -> None:
        if not self._is_frame(df):
            raise TypeError("Spark model must return a Spark DataFrame")
        storage_meta = self._storage_meta(node, relation)
        if storage_meta.get("path"):
            self._write_to_storage_path(relation, df, storage_meta)
            return
        # write as a table in Hive/Unity/Delta environments
        self._save_df_as_table(relation, df, storage=storage_meta)

    def _create_view_over_table(self, view_name: str, backing_table: str, node: Node) -> None:
        self.spark.sql(f"CREATE OR REPLACE VIEW `{view_name}` AS SELECT * FROM `{backing_table}`")

    def _validate_required(
        self, node_name: str, inputs: Any, requires: dict[str, set[str]]
    ) -> None:
        if not requires:
            return

        def cols(df: SDF) -> set[str]:
            return set(df.schema.fieldNames())

        errors: list[str] = []
        # Single dependency: requires typically contains exactly one entry (ignore the key)
        if isinstance(inputs, SDF):
            need = next(iter(requires.values()), set())
            missing = need - cols(inputs)
            if missing:
                errors.append(f"- missing columns: {sorted(missing)} | have={sorted(cols(inputs))}")
        else:
            # Multiple dependencies: keys in requires = physical relations (relation_for(dep))
            for rel, need in requires.items():
                if rel not in inputs:
                    errors.append(f"- missing dependency key '{rel}'")
                    continue
                missing = need - cols(inputs[rel])
                if missing:
                    errors.append(
                        f"- [{rel}] missing: {sorted(missing)} | have={sorted(cols(inputs[rel]))}"
                    )

        if errors:
            raise ValueError(
                "Required columns check failed for Spark model "
                f"'{node_name}'.\n" + "\n".join(errors)
            )

    def _columns_of(self, frame: SDF) -> list[str]:  # pragma: no cover
        return frame.schema.fieldNames()

    def _is_frame(self, obj: Any) -> bool:  # pragma: no cover
        return isinstance(obj, SDF)

    def _frame_name(self) -> str:  # pragma: no cover
        return "Spark"

    # ---- Helpers ----
    @staticmethod
    def _q_ident(value: str | None) -> str:
        if value is None:
            return ""
        return f"`{value.replace('`', '``')}`"

    def _storage_meta(self, node: Node | None, relation: str) -> dict[str, Any]:
        """
        Retrieve configured storage overrides for the logical node backing `relation`.
        """
        rel_clean = self._strip_quotes(relation)
        if node is not None:
            meta = dict((node.meta or {}).get("storage") or {})
            if meta:
                return meta
            lookup = storage.get_model_storage(node.name)
            if lookup:
                return lookup
        for cand in getattr(REGISTRY, "nodes", {}).values():
            try:
                if self._strip_quotes(relation_for(cand.name)) == rel_clean:
                    meta = dict((cand.meta or {}).get("storage") or {})
                    if meta:
                        return meta
                    lookup = storage.get_model_storage(cand.name)
                    if lookup:
                        return lookup
            except Exception:
                continue
        return storage.get_model_storage(rel_clean)

    def _write_to_storage_path(self, relation: str, df: SDF, storage_meta: dict[str, Any]) -> None:
        parts = self._identifier_parts(relation)
        identifier = ".".join(parts)

        storage.spark_write_to_path(
            self.spark,
            identifier,
            df,
            storage=storage_meta,
            default_format=self.spark_table_format,
            default_options=self.spark_table_options,
        )

        path = storage_meta.get("path")
        if path:
            with suppress(Exception):
                self.spark.catalog.refreshByPath(path)

    # ---- SQL hooks ----
    def _format_relation_for_ref(self, name: str) -> str:
        return self._q_ident(relation_for(name))

    def _format_source_reference(
        self, cfg: dict[str, Any], source_name: str, table_name: str
    ) -> str:
        location = cfg.get("location")
        identifier = cfg.get("identifier")

        if location:
            alias = identifier or f"__ff_src_{source_name}_{table_name}"
            fmt = cfg.get("format")
            if not fmt:
                raise KeyError(
                    f"Source {source_name}.{table_name} requires 'format' when using a location"
                )

            options = dict(cfg.get("options") or {})
            descriptor = {
                "location": location,
                "format": fmt,
                "options": options,
            }
            existing = self._registered_path_sources.get(alias)
            if existing != descriptor:
                reader = self.spark.read.format(fmt)
                if options:
                    reader = reader.options(**options)
                df = reader.load(location)
                df.createOrReplaceTempView(alias)
                self._registered_path_sources[alias] = descriptor
            return self._q_ident(alias)

        if not identifier:
            raise KeyError(f"Source {source_name}.{table_name} missing identifier")

        catalog = cfg.get("catalog") or cfg.get("database")
        schema = cfg.get("schema")
        parts = [p for p in (catalog, schema, identifier) if p]
        if not parts:
            parts = [identifier]
        return ".".join(self._q_ident(str(part)) for part in parts)

    # ---- Spark table helpers ----
    @staticmethod
    def _strip_quotes(identifier: str) -> str:
        return identifier.replace("`", "").replace('"', "")

    def _identifier_parts(self, identifier: str) -> list[str]:
        cleaned = self._strip_quotes(identifier)
        return [part for part in cleaned.split(".") if part]

    def _warehouse_base(self) -> Path | None:
        try:
            conf_val = self.spark.conf.get("spark.sql.warehouse.dir", "spark-warehouse")
        except Exception:
            conf_val = "spark-warehouse"

        if not isinstance(conf_val, str):
            conf_val = str(conf_val)
        parsed = urlparse(conf_val)
        scheme = (parsed.scheme or "").lower()

        if scheme and scheme != "file":
            return None

        if scheme == "file":
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

    def _table_location(self, parts: list[str]) -> Path | None:
        base = self._warehouse_base()
        if base is None or not parts:
            return None

        filtered = [p for p in parts if p]
        if not filtered:
            return None

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

    def _save_df_as_table(
        self, identifier: str, df: SDF, *, storage: dict[str, Any] | None = None
    ) -> None:
        parts = self._identifier_parts(identifier)
        if not parts:
            raise ValueError(f"Invalid Spark table identifier: {identifier}")

        storage_meta = storage or self._storage_meta(None, identifier)
        if storage_meta.get("path"):
            self._write_to_storage_path(identifier, df, storage_meta)
            return

        table_name = ".".join(parts)

        writer = df.write.mode("overwrite")
        if self.spark_table_format:
            writer = writer.format(self.spark_table_format)
        if self.spark_table_options:
            writer = writer.options(**self.spark_table_options)

        writer.saveAsTable(table_name)

    def _create_or_replace_view(self, target_sql: str, select_body: str, node: Node) -> None:
        self.spark.sql(f"CREATE OR REPLACE VIEW {target_sql} AS {select_body}")

    def _create_or_replace_table(self, target_sql: str, select_body: str, node: Node) -> None:
        preview = f"-- target={target_sql}\n{select_body}"
        try:
            df = self.spark.sql(select_body)
            storage_meta = self._storage_meta(node, target_sql)
            self._save_df_as_table(target_sql, df, storage=storage_meta)
        except Exception as exc:
            raise ModelExecutionError(node.name, target_sql, str(exc), sql_snippet=preview) from exc

    def _create_or_replace_view_from_table(
        self, view_name: str, backing_table: str, node: Node
    ) -> None:
        self.spark.sql(f"CREATE OR REPLACE VIEW `{view_name}` AS SELECT * FROM `{backing_table}`")

    # ---- Meta hook ----
    def on_node_built(self, node: Node, relation: str, fingerprint: str) -> None:
        """After successful materialization, upsert _ff_meta (best-effort)."""
        try:
            ensure_meta_table(self)
            upsert_meta(self, node.name, relation, fingerprint, "databricks_spark")
        except Exception:
            pass

    # ── Incremental API (parity) ─────────────────────────────────────────
    def exists_relation(self, relation: str) -> bool:
        """Check whether a table/view exists (optionally qualified with database)."""
        db, tbl = _split_db_table(relation)
        if db:
            return bool(self.spark.catalog._jcatalog.tableExists(db, tbl))
        return self.spark.catalog.tableExists(tbl)

    def create_table_as(self, relation: str, select_sql: str) -> None:
        """CREATE TABLE AS with cleaned SELECT body."""
        body = self._selectable_body(select_sql).strip().rstrip(";\n\t ")
        df = self.spark.sql(body)
        self._save_df_as_table(relation, df)

    def full_refresh_table(self, relation: str, select_sql: str) -> None:
        """
        Engine-specific full refresh for incremental fallbacks.
        Important: NO 'REPLACE TABLE' SQL, but DataFrame path + saveAsTable instead.
        """
        body = self._selectable_body(select_sql).strip().rstrip(";\n\t ")
        df = self.spark.sql(body)
        self._save_df_as_table(relation, df)

    def incremental_insert(self, relation: str, select_sql: str) -> None:
        """INSERT INTO with cleaned SELECT body."""
        body = self._selectable_body(select_sql).strip().rstrip(";\n\t ")
        self.spark.sql(f"INSERT INTO {relation} {body}")

    def incremental_merge(self, relation: str, select_sql: str, unique_key: list[str]) -> None:
        body = self._selectable_body(select_sql).strip().rstrip(";\n\t ")
        pred = " AND ".join([f"t.{k}=s.{k}" for k in unique_key]) or "FALSE"

        # 1) Happy Path: native MERGE (Delta / Unity Catalog)
        try:
            self.spark.sql(
                f"""
                MERGE INTO {relation} AS t
                USING ({body}) AS s
                ON {pred}
                WHEN MATCHED THEN UPDATE SET *
                WHEN NOT MATCHED THEN INSERT *
                """
            )
            return
        except Exception:
            # 2) Fallback: Full-Refresh
            df = self.spark.sql(body)
            self._save_df_as_table(relation, df)

    def alter_table_sync_schema(
        self, relation: str, select_sql: str, *, mode: str = "append_new_columns"
    ) -> None:
        """
        Best-effort additive schema sync:
          - infer select schema via LIMIT 0
          - add missing columns as STRING (safe default)
        """
        if mode not in {"append_new_columns", "sync_all_columns"}:
            return
        # Target schema
        try:
            target_df = self.spark.table(relation)
        except Exception:
            return
        existing = {f.name for f in target_df.schema.fields}
        # Output schema from the SELECT
        body = self._first_select_body(select_sql).strip().rstrip(";\n\t ")
        probe = self.spark.sql(f"SELECT * FROM ({body}) q LIMIT 0")
        to_add = [f for f in probe.schema.fields if f.name not in existing]
        if not to_add:
            return

        # Map types best-effort (Spark SQL types); default STRING
        def _spark_sql_type(dt: DataType) -> str:
            # Use simple, portable mapping for documentation UIs & broad compatibility
            return (
                getattr(dt, "simpleString", lambda: "string")().upper()
                if hasattr(dt, "simpleString")
                else "STRING"
            )

        cols_sql = ", ".join([f"`{f.name}` {_spark_sql_type(f.dataType)}" for f in to_add])
        self.spark.sql(f"ALTER TABLE {relation} ADD COLUMNS ({cols_sql})")


# ────────────────────────── local helpers / shim ──────────────────────────
class _SparkResult:
    """Tiny result shim to mimic duckdb/psycopg fetch API in tests."""

    def __init__(self, rows: list[tuple]):
        self._rows = rows

    def fetchall(self) -> list[tuple]:
        return self._rows

    def fetchone(self) -> tuple | None:
        return self._rows[0] if self._rows else None


class _SparkConnShim:  # pragma: no cover
    """Provide .execute(sql) with fetch* for test utilities."""

    def __init__(self, spark: SparkSession):
        self._spark = spark

    def execute(self, sql: str, params: Any | None = None) -> _SparkResult:
        if params:
            # Minimal positional param interpolation for tests is intentionally not implemented.
            # All internal calls use plain SQL strings for Spark.
            raise NotImplementedError("SparkConnShim does not support parametrized SQL")
        df = self._spark.sql(sql)
        rows = [tuple(r) for r in df.collect()]
        return _SparkResult(rows)


def _split_db_table(qualified: str) -> tuple[str | None, str]:
    """Split 'db.table' → (db, table); backticks allowed. Returns (None, name) if unqualified."""
    s = qualified.strip("`")
    parts = s.split(".")
    part_len = 2
    if len(parts) >= part_len:
        return parts[-2], parts[-1]
    return None, s

# fastflowtransform/executors/duckdb.py
from __future__ import annotations

import json
import re
import uuid
from collections.abc import Iterable
from contextlib import suppress
from pathlib import Path
from time import perf_counter
from typing import Any, ClassVar

import duckdb
import pandas as pd
from duckdb import CatalogException
from jinja2 import Environment

from fastflowtransform.core import Node, relation_for
from fastflowtransform.executors.base import BaseExecutor
from fastflowtransform.executors.budget import BudgetGuard
from fastflowtransform.executors.query_stats import QueryStats
from fastflowtransform.logging import echo
from fastflowtransform.meta import ensure_meta_table, upsert_meta
from fastflowtransform.snapshots import resolve_snapshot_config


def _q(ident: str) -> str:
    return '"' + ident.replace('"', '""') + '"'


class DuckExecutor(BaseExecutor[pd.DataFrame]):
    ENGINE_NAME = "duckdb"

    _FIXED_TYPE_SIZES: ClassVar[dict[str, int]] = {
        "boolean": 1,
        "bool": 1,
        "tinyint": 1,
        "smallint": 2,
        "integer": 4,
        "int": 4,
        "bigint": 8,
        "float": 4,
        "real": 4,
        "double": 8,
        "double precision": 8,
        "decimal": 16,
        "numeric": 16,
        "uuid": 16,
        "json": 64,
        "jsonb": 64,
        "timestamp": 8,
        "timestamp_ntz": 8,
        "timestamp_ltz": 8,
        "timestamptz": 8,
        "date": 4,
        "time": 4,
        "interval": 16,
    }
    _VARCHAR_DEFAULT_WIDTH = 64
    _VARCHAR_MAX_WIDTH = 1024
    _DEFAULT_ROW_WIDTH = 128
    _BUDGET_GUARD = BudgetGuard(
        env_var="FF_DUCKDB_MAX_BYTES",
        estimator_attr="_estimate_query_bytes",
        engine_label="DuckDB",
        what="query",
    )

    def __init__(
        self, db_path: str = ":memory:", schema: str | None = None, catalog: str | None = None
    ):
        if db_path and db_path != ":memory:" and "://" not in db_path:
            with suppress(Exception):
                Path(db_path).parent.mkdir(parents=True, exist_ok=True)
        self.db_path = db_path
        self.con = duckdb.connect(db_path)
        self.schema = schema.strip() if isinstance(schema, str) and schema.strip() else None
        catalog_override = catalog.strip() if isinstance(catalog, str) and catalog.strip() else None
        self.catalog = self._detect_catalog()
        self._table_row_width_cache: dict[tuple[str | None, str], int] = {}
        if catalog_override:
            if self._apply_catalog_override(catalog_override):
                self.catalog = catalog_override
            else:
                self.catalog = self._detect_catalog()
        if self.schema:
            safe_schema = _q(self.schema)
            self._execute_sql(f"create schema if not exists {safe_schema}")
            self._execute_sql(f"set schema '{self.schema}'")

    def _execute_sql(self, sql: str, *args: Any, **kwargs: Any) -> duckdb.DuckDBPyConnection:
        """
        Central DuckDB SQL runner.

        All model-driven SQL in this executor should go through here.
        The cost guard may call _estimate_query_bytes(sql) before executing.
        This wrapper also records simple per-query stats for run_results.json.
        """
        estimated_bytes = self._apply_budget_guard(self._BUDGET_GUARD, sql)
        if estimated_bytes is None and not self._is_budget_guard_active():
            with suppress(Exception):
                estimated_bytes = self._estimate_query_bytes(sql)
        t0 = perf_counter()
        cursor = self.con.execute(sql, *args, **kwargs)
        dt_ms = int((perf_counter() - t0) * 1000)

        rows: int | None = None
        try:
            rc = getattr(cursor, "rowcount", None)
            if isinstance(rc, int) and rc >= 0:
                rows = rc
        except Exception:
            rows = None

        # DuckDB doesn't expose bytes-scanned in a simple way yet → rely on the
        # estimate we already collected or the best-effort fallback.
        self._record_query_stats(
            QueryStats(
                bytes_processed=estimated_bytes,
                rows=rows,
                duration_ms=dt_ms,
            )
        )
        return cursor

    # --- Cost estimation for the shared BudgetGuard -----------------

    def _estimate_query_bytes(self, sql: str) -> int | None:
        """
        Estimate query size via DuckDB's EXPLAIN (FORMAT JSON).

        The JSON plan exposes an \"Estimated Cardinality\" per node.
        We walk the parsed tree, take the highest non-zero estimate and
        return it as a byte-estimate surrogate (row count ≈ bytes) so the
        cost guard can still make a meaningful decision without executing
        the query.
        """
        try:
            body = self._selectable_body(sql).strip().rstrip(";\n\t ")
        except AttributeError:
            body = sql.strip().rstrip(";\n\t ")

        lower = body.lower()
        if not lower.startswith(("select", "with")):
            return None

        explain_sql = f"EXPLAIN (FORMAT JSON) {body}"
        try:
            rows = self.con.execute(explain_sql).fetchall()
        except Exception:
            return None

        if not rows:
            return None

        fragments: list[str] = []
        for row in rows:
            for cell in row:
                if cell is None:
                    continue
                fragments.append(str(cell))

        if not fragments:
            return None

        plan_text = "\n".join(fragments).strip()
        start = plan_text.find("[")
        end = plan_text.rfind("]")
        if start == -1 or end == -1 or end <= start:
            return None

        try:
            plan_data = json.loads(plan_text[start : end + 1])
        except Exception:
            return None

        def _to_int(value: Any) -> int | None:
            if value is None:
                return None
            if isinstance(value, (int, float)):
                try:
                    converted = int(value)
                except Exception:
                    return None
                return converted
            text = str(value)
            match = re.search(r"(\d+(?:\.\d+)?)", text)
            if not match:
                return None
            try:
                return int(float(match.group(1)))
            except ValueError:
                return None

        def _walk_node(node: dict[str, Any]) -> int:
            best = 0
            extra = node.get("extra_info") or {}
            for key in (
                "Estimated Cardinality",
                "estimated_cardinality",
                "Cardinality",
                "cardinality",
            ):
                candidate = _to_int(extra.get(key))
                if candidate is not None:
                    best = max(best, candidate)
            candidate = _to_int(node.get("cardinality"))
            if candidate is not None:
                best = max(best, candidate)
            for child in node.get("children") or []:
                if isinstance(child, dict):
                    best = max(best, _walk_node(child))
            return best

        nodes: list[Any]
        nodes = plan_data if isinstance(plan_data, list) else [plan_data]

        estimate = 0
        for entry in nodes:
            if isinstance(entry, dict):
                estimate = max(estimate, _walk_node(entry))

        if estimate <= 0:
            return None

        tables = self._collect_tables_from_plan(nodes)
        row_width = self._row_width_for_tables(tables)
        if row_width <= 0:
            row_width = self._DEFAULT_ROW_WIDTH

        bytes_estimate = int(estimate * row_width)
        return bytes_estimate if bytes_estimate > 0 else None

    def _collect_tables_from_plan(self, nodes: list[dict[str, Any]]) -> set[tuple[str | None, str]]:
        tables: set[tuple[str | None, str]] = set()

        def _walk(entry: dict[str, Any]) -> None:
            extra = entry.get("extra_info") or {}
            table_val = extra.get("Table")
            schema_val = extra.get("Schema") or extra.get("Database") or extra.get("Catalog")
            if isinstance(table_val, str) and table_val.strip():
                schema, table = self._split_identifier(table_val, schema_val)
                if table:
                    tables.add((schema, table))
            for child in entry.get("children") or []:
                if isinstance(child, dict):
                    _walk(child)

        for node in nodes:
            if isinstance(node, dict):
                _walk(node)
        return tables

    def _split_identifier(
        self, identifier: str, explicit_schema: str | None
    ) -> tuple[str | None, str]:
        parts = [part.strip() for part in identifier.split(".") if part.strip()]
        if not parts:
            return explicit_schema, identifier
        if len(parts) >= 2:
            schema_candidate = self._strip_quotes(parts[-2])
            table_candidate = self._strip_quotes(parts[-1])
            return schema_candidate or explicit_schema, table_candidate
        return explicit_schema, self._strip_quotes(parts[-1])

    def _strip_quotes(self, value: str) -> str:
        if value.startswith('"') and value.endswith('"'):
            return value[1:-1]
        return value

    def _row_width_for_tables(self, tables: Iterable[tuple[str | None, str]]) -> int:
        widths: list[int] = []
        for schema, table in tables:
            width = self._row_width_for_table(schema, table)
            if width > 0:
                widths.append(width)
        return max(widths) if widths else 0

    def _row_width_for_table(self, schema: str | None, table: str) -> int:
        key = (schema or "", table.lower())
        cached = self._table_row_width_cache.get(key)
        if cached:
            return cached

        columns = self._columns_for_table(table, schema)
        width = sum(self._estimate_column_width(col) for col in columns)
        if width <= 0:
            width = self._DEFAULT_ROW_WIDTH
        self._table_row_width_cache[key] = width
        return width

    def _columns_for_table(
        self, table: str, schema: str | None
    ) -> list[tuple[str | None, int | None, int | None, int | None]]:
        table_lower = table.lower()
        columns: list[tuple[str | None, int | None, int | None, int | None]] = []
        seen_schemas: set[str | None] = set()
        for candidate in self._schema_candidates(schema):
            if candidate in seen_schemas:
                continue
            seen_schemas.add(candidate)
            if candidate is not None:
                try:
                    rows = self.con.execute(
                        """
                        select lower(data_type) as dtype,
                               character_maximum_length,
                               numeric_precision,
                               numeric_scale
                        from information_schema.columns
                        where lower(table_name)=lower(?)
                          and lower(table_schema)=lower(?)
                        order by ordinal_position
                        """,
                        [table_lower, candidate.lower()],
                    ).fetchall()
                except Exception:
                    continue
            else:
                try:
                    rows = self.con.execute(
                        """
                        select lower(data_type) as dtype,
                               character_maximum_length,
                               numeric_precision,
                               numeric_scale
                        from information_schema.columns
                        where lower(table_name)=lower(?)
                        order by lower(table_schema), ordinal_position
                        """,
                        [table_lower],
                    ).fetchall()
                except Exception:
                    continue
            if rows:
                return rows
        return columns

    def _schema_candidates(self, schema: str | None) -> list[str | None]:
        candidates: list[str | None] = []

        def _add(value: str | None) -> None:
            normalized = self._normalize_schema(value)
            if normalized not in candidates:
                candidates.append(normalized)

        _add(schema)
        _add(self.schema)
        for alt in ("main", "temp"):
            _add(alt)
        _add(None)
        return candidates

    def _normalize_schema(self, schema: str | None) -> str | None:
        if not schema:
            return None
        stripped = schema.strip()
        return stripped or None

    def _estimate_column_width(
        self, column_info: tuple[str | None, int | None, int | None, int | None]
    ) -> int:
        dtype_raw, char_max, numeric_precision, _ = column_info
        dtype = self._normalize_data_type(dtype_raw)
        if dtype and dtype in self._FIXED_TYPE_SIZES:
            return self._FIXED_TYPE_SIZES[dtype]

        if dtype in {"character", "varchar", "char", "text", "string"}:
            if char_max and char_max > 0:
                return min(char_max, self._VARCHAR_MAX_WIDTH)
            return self._VARCHAR_DEFAULT_WIDTH

        if dtype in {"varbinary", "blob", "binary"}:
            if char_max and char_max > 0:
                return min(char_max, self._VARCHAR_MAX_WIDTH)
            return self._VARCHAR_DEFAULT_WIDTH

        if dtype in {"numeric", "decimal"} and numeric_precision and numeric_precision > 0:
            return min(max(int(numeric_precision), 16), 128)

        return 16

    def _normalize_data_type(self, dtype: str | None) -> str | None:
        if not dtype:
            return None
        stripped = dtype.strip().lower()
        if "(" in stripped:
            stripped = stripped.split("(", 1)[0].strip()
        if stripped.endswith("[]"):
            stripped = stripped[:-2]
        return stripped or None

    def _detect_catalog(self) -> str | None:
        try:
            rows = self._execute_sql("PRAGMA database_list").fetchall()
            if rows:
                return str(rows[0][1])
        except Exception:
            return None
        return None

    def _apply_catalog_override(self, name: str) -> bool:
        alias = name.strip()
        if not alias:
            return False
        try:
            if self.db_path != ":memory:":
                resolved = str(Path(self.db_path).resolve())
                with suppress(Exception):
                    self._execute_sql(f"detach database {_q(alias)}")
                self._execute_sql(f"attach database '{resolved}' as {_q(alias)} (READ_ONLY FALSE)")
            self._execute_sql(f"set catalog '{alias}'")
            return True
        except Exception:
            return False

    def clone(self) -> DuckExecutor:
        """
        Generates a new Executor instance with own connection for Thread-Worker.
        """
        return DuckExecutor(self.db_path, schema=self.schema, catalog=self.catalog)

    def _exec_many(self, sql: str) -> None:
        """
        Execute multiple SQL statements separated by ';' on the same connection.
        DuckDB normally accepts one statement per execute(), so we split here.
        """
        for stmt in (part.strip() for part in sql.split(";")):
            if not stmt:
                continue
            self._execute_sql(stmt)

    # ---- Frame hooks ----
    def _qualified(self, relation: str, *, quoted: bool = True) -> str:
        """
        Return (catalog.)schema.relation if schema is set; otherwise just relation.
        When quoted=False, emit bare identifiers for APIs like con.table().
        """
        rel = relation_for(relation) if relation.endswith(".ff") else relation
        rel_part = _q(rel) if quoted else rel
        if not self.schema:
            return rel_part
        parts: list[str] = []
        cat_clean = None
        include_catalog = False
        if isinstance(self.catalog, str):
            cat_trimmed = self.catalog.strip()
            if cat_trimmed and cat_trimmed.lower() == self.schema.strip().lower():
                include_catalog = True
                cat_clean = cat_trimmed
        if include_catalog and cat_clean is not None:
            parts.append(_q(cat_clean) if quoted else cat_clean)
        schema_clean = self.schema.strip()
        parts.append(_q(schema_clean) if quoted else schema_clean)
        parts.append(rel_part)
        return ".".join(parts)

    def _read_relation(self, relation: str, node: Node, deps: Iterable[str]) -> pd.DataFrame:
        try:
            target = self._qualified(relation, quoted=False)
            return self.con.table(target).df()
        except CatalogException as e:
            existing = [
                r[0]
                for r in self._execute_sql(
                    "select table_name from information_schema.tables "
                    "where table_schema in ('main','temp')"
                ).fetchall()
            ]
            raise RuntimeError(
                f"Dependency table not found: '{relation}'\n"
                f"Deps: {list(deps)}\nExisting tables: {existing}\n"
                "Note: Use same File-DB/Connection for Seeding & Run."
            ) from e

    def _materialize_relation(self, relation: str, df: pd.DataFrame, node: Node) -> None:
        tmp = "_ff_py_out"
        try:
            self.con.register(tmp, df)
            target = self._qualified(relation)
            self._execute_sql(f'create or replace table {target} as select * from "{tmp}"')
        finally:
            try:
                self.con.unregister(tmp)
            except Exception:
                # housekeeping only; stats here are not important but harmless if recorded
                self._execute_sql(f'drop view if exists "{tmp}"')

    def _create_or_replace_view_from_table(
        self, view_name: str, backing_table: str, node: Node
    ) -> None:
        view_target = self._qualified(view_name)
        backing = self._qualified(backing_table)
        self._execute_sql(f"create or replace view {view_target} as select * from {backing}")

    def _frame_name(self) -> str:
        return "pandas"

    # ---- SQL hooks ----
    def _format_relation_for_ref(self, name: str) -> str:
        return self._qualified(relation_for(name))

    def _format_source_reference(
        self, cfg: dict[str, Any], source_name: str, table_name: str
    ) -> str:
        location = cfg.get("location")
        if location:
            raise NotImplementedError("DuckDB executor does not support path-based sources yet.")

        identifier = cfg.get("identifier")
        if not identifier:
            raise KeyError(f"Source {source_name}.{table_name} missing identifier")

        catalog_cfg = cfg.get("catalog") or cfg.get("database")
        catalog = (
            catalog_cfg.strip() if isinstance(catalog_cfg, str) and catalog_cfg.strip() else None
        )
        schema_candidate = cfg.get("schema") or self.schema
        schema = (
            schema_candidate.strip()
            if isinstance(schema_candidate, str) and schema_candidate.strip()
            else None
        )
        if catalog is None and schema and isinstance(self.catalog, str):
            cat_clean = self.catalog.strip()
            if cat_clean and cat_clean.lower() == schema.lower():
                catalog = cat_clean
        if catalog is None and schema is None and isinstance(self.catalog, str):
            cat_clean = self.catalog.strip()
            catalog = cat_clean or None
        parts: list[str] = []
        if catalog:
            parts.append(catalog)
        if schema:
            parts.append(schema)
        parts.append(identifier)
        return ".".join(_q(str(part)) for part in parts if part)

    def _create_or_replace_view(self, target_sql: str, select_body: str, node: Node) -> None:
        self._execute_sql(f"create or replace view {target_sql} as {select_body}")

    def _create_or_replace_table(self, target_sql: str, select_body: str, node: Node) -> None:
        self._execute_sql(f"create or replace table {target_sql} as {select_body}")

    # ---- Meta hook ----
    def on_node_built(self, node: Node, relation: str, fingerprint: str) -> None:
        """
        After successful materialization, ensure the meta table exists and upsert the row.
        """
        ensure_meta_table(self)
        upsert_meta(self, node.name, relation, fingerprint, "duckdb")

    # ── Incremental API ────────────────────────────────────────────────────
    def exists_relation(self, relation: str) -> bool:
        where_tables: list[str] = ["lower(table_name) = lower(?)"]
        params: list[str] = [relation]
        if self.catalog:
            where_tables.append("lower(table_catalog) = lower(?)")
            params.append(self.catalog)
        if self.schema:
            where_tables.append("lower(table_schema) = lower(?)")
            params.append(self.schema)
        else:
            where_tables.append("table_schema in ('main','temp')")
        where = " AND ".join(where_tables)
        sql_tables = f"select 1 from information_schema.tables where {where} limit 1"
        if self._execute_sql(sql_tables, params).fetchone():
            return True
        sql_views = f"select 1 from information_schema.views where {where} limit 1"
        return bool(self._execute_sql(sql_views, params).fetchone())

    def create_table_as(self, relation: str, select_sql: str) -> None:
        # Use only the SELECT body and strip trailing semicolons for safety.
        body = self._selectable_body(select_sql).strip().rstrip(";\n\t ")
        self._execute_sql(f"create table {self._qualified(relation)} as {body}")

    def incremental_insert(self, relation: str, select_sql: str) -> None:
        # Ensure the inner SELECT is clean (no trailing semicolon; SELECT body only).
        body = self._selectable_body(select_sql).strip().rstrip(";\n\t ")
        self._execute_sql(f"insert into {self._qualified(relation)} {body}")

    def incremental_merge(self, relation: str, select_sql: str, unique_key: list[str]) -> None:
        """
        Fallback strategy for DuckDB:
        - DELETE collisions via DELETE ... USING (<select>) s
        - INSERT all rows via INSERT ... SELECT * FROM (<select>)
        We intentionally do NOT use a CTE here, because we execute two separate
        statements and DuckDB won't see the CTE from the previous statement.
        """
        # 1) clean inner SELECT
        body = self._selectable_body(select_sql).strip().rstrip(";\n\t ")

        # 2) predicate for DELETE
        keys_pred = " AND ".join([f"t.{k}=s.{k}" for k in unique_key]) or "FALSE"

        # 3) first: delete collisions
        delete_sql = f"delete from {self._qualified(relation)} t using ({body}) s where {keys_pred}"
        self._execute_sql(delete_sql)

        # 4) then: insert fresh rows
        insert_sql = f"insert into {self._qualified(relation)} select * from ({body}) src"
        self._execute_sql(insert_sql)

    def alter_table_sync_schema(
        self, relation: str, select_sql: str, *, mode: str = "append_new_columns"
    ) -> None:
        """
        Best-effort: add new columns with inferred type.
        """
        # Probe: empty projection from the SELECT (cleaned to avoid parser issues).
        body = self._first_select_body(select_sql).strip().rstrip(";\n\t ")
        probe = self._execute_sql(f"select * from ({body}) as q limit 0")
        cols = [c[0] for c in probe.description or []]
        existing = {
            r[0]
            for r in self._execute_sql(
                "select column_name from information_schema.columns "
                + "where lower(table_name)=lower(?)"
                + (" and lower(table_schema)=lower(?)" if self.schema else ""),
                ([relation, self.schema] if self.schema else [relation]),
            ).fetchall()
        }
        add = [c for c in cols if c not in existing]
        for c in add:
            col = _q(c)
            target = self._qualified(relation)
            try:
                self._execute_sql(f"alter table {target} add column {col} varchar")
            except Exception:
                self._execute_sql(f"alter table {target} add column {col} varchar")

    def run_snapshot_sql(self, node: Node, env: Environment) -> None:
        """
        Snapshot materialization for DuckDB.

        Config (node.meta):
          - materialized='snapshot'
          - snapshot: { ... }  # strategy + per-strategy hints
          - unique_key: str | list[str]

        Behaviour:
          - First run: create table with one current row per unique key.
          - Subsequent runs:
              * close changed current rows (set valid_to, is_current=false)
              * insert new current rows for new/changed keys.
        """
        if node.kind != "sql":
            raise TypeError(
                f"Snapshot materialization is only supported for SQL models, "
                f"got kind={node.kind!r} for {node.name}."
            )

        meta = getattr(node, "meta", {}) or {}
        if not self._meta_is_snapshot(meta):
            raise ValueError(f"Node {node.name} is not configured with materialized='snapshot'.")

        # ---- Extract & normalise snapshot config (shared helper) ----
        cfg = resolve_snapshot_config(node, meta)
        strategy = cfg.strategy
        unique_key = cfg.unique_key
        updated_at = cfg.updated_at
        check_cols = cfg.check_cols

        # ---- Render SQL and extract SELECT body ----
        sql_rendered = self.render_sql(
            node,
            env,
            ref_resolver=lambda name: self._resolve_ref(name, env),
            source_resolver=self._resolve_source,
        )
        sql = self._strip_leading_config(sql_rendered).strip()
        body = self._selectable_body(sql).rstrip(" ;\n\t")

        rel_name = relation_for(node.name)
        target = self._qualified(rel_name)

        vf = BaseExecutor.SNAPSHOT_VALID_FROM_COL
        vt = BaseExecutor.SNAPSHOT_VALID_TO_COL
        is_cur = BaseExecutor.SNAPSHOT_IS_CURRENT_COL
        hash_col = BaseExecutor.SNAPSHOT_HASH_COL
        upd_meta = BaseExecutor.SNAPSHOT_UPDATED_AT_COL

        # ---- First run: create snapshot table ----
        if not self.exists_relation(rel_name):
            if strategy == "timestamp":
                # valid_from + updated_at come from the source updated_at column
                create_sql = f"""
create table {target} as
select
    s.*,
    s.{updated_at} as {upd_meta},
    s.{updated_at} as {vf},
    cast(null as timestamp) as {vt},
    true as {is_cur},
    cast(null as varchar) as {hash_col}
from ({body}) as s
"""
            else:  # strategy == "check"
                # Hash over check_cols to detect changes
                col_exprs = [f"coalesce(cast(s.{col} as varchar), '')" for col in check_cols]
                concat_expr = " || '||' || ".join(col_exprs)
                hash_expr = f"cast(md5({concat_expr}) as varchar)"
                upd_expr = f"s.{updated_at}" if updated_at else "current_timestamp"
                create_sql = f"""
create table {target} as
select
    s.*,
    {upd_expr} as {upd_meta},
    current_timestamp as {vf},
    cast(null as timestamp) as {vt},
    true as {is_cur},
    {hash_expr} as {hash_col}
from ({body}) as s
"""
            self._execute_sql(create_sql)
            return

        # ---- Incremental snapshot update ----

        # Stage current source rows in a temp view for reuse
        src_view_name = f"__ff_snapshot_src_{rel_name}".replace(".", "_")
        src_quoted = _q(src_view_name)
        self._execute_sql(f"create or replace temp view {src_quoted} as {body}")

        try:
            # Join predicate on unique keys
            keys_pred = " AND ".join([f"t.{k} = s.{k}" for k in unique_key])

            # Change condition & hash for staging rows
            if strategy == "timestamp":
                change_condition = f"s.{updated_at} > t.{upd_meta}"
                hash_expr_s = "NULL"
                new_upd_expr = f"s.{updated_at}"
                new_valid_from_expr = f"s.{updated_at}"
                new_hash_expr = "NULL"
            else:
                col_exprs_s = [f"coalesce(cast(s.{col} as varchar), '')" for col in check_cols]
                concat_expr_s = " || '||' || ".join(col_exprs_s)
                hash_expr_s = f"cast(md5({concat_expr_s}) as varchar)"
                change_condition = f"coalesce({hash_expr_s}, '') <> coalesce(t.{hash_col}, '')"
                new_upd_expr = f"s.{updated_at}" if updated_at else "current_timestamp"
                new_valid_from_expr = "current_timestamp"
                new_hash_expr = hash_expr_s

            # 1) Close changed current rows
            close_sql = f"""
update {target} as t
set
    {vt} = current_timestamp,
    {is_cur} = false
from {src_quoted} as s
where
    {keys_pred}
    and t.{is_cur} = true
    and {change_condition};
"""
            self._execute_sql(close_sql)

            # 2) Insert new current versions (new keys or changed rows)
            first_key = unique_key[0]
            insert_sql = f"""
insert into {target}
select
    s.*,
    {new_upd_expr} as {upd_meta},
    {new_valid_from_expr} as {vf},
    cast(null as timestamp) as {vt},
    true as {is_cur},
    {new_hash_expr} as {hash_col}
from {src_quoted} as s
left join {target} as t
  on {keys_pred}
  and t.{is_cur} = true
where
    t.{first_key} is null
    or {change_condition};
"""
            self._execute_sql(insert_sql)
        finally:
            with suppress(Exception):
                self._execute_sql(f"drop view if exists {src_quoted}")

    def snapshot_prune(
        self,
        relation: str,
        unique_key: list[str],
        keep_last: int,
        *,
        dry_run: bool = False,
    ) -> None:
        """
        Delete older snapshot versions while keeping the most recent `keep_last`
        rows per business key (including the current row).
        """
        if keep_last <= 0:
            return

        target = self._qualified(relation)
        vf = BaseExecutor.SNAPSHOT_VALID_FROM_COL
        keys = [k for k in unique_key if k]

        if not keys:
            return

        part_by = ", ".join([k for k in keys])
        key_select = ", ".join(keys)

        ranked_sql = f"""
select
  {key_select},
  {vf},
  row_number() over (
    partition by {part_by}
    order by {vf} desc
  ) as rn
from {target}
"""

        if dry_run:
            sql = f"""
with ranked as (
  {ranked_sql}
)
select count(*) as rows_to_delete
from ranked
where rn > {int(keep_last)}
"""
            res = self._execute_sql(sql).fetchone()
            rows = int(res[0]) if res else 0

            echo(
                f"[DRY-RUN] snapshot_prune({relation}): would delete {rows} row(s) "
                f"(keep_last={keep_last})"
            )
            return

        delete_sql = f"""
delete from {target} t
using (
  {ranked_sql}
) r
where
  r.rn > {int(keep_last)}
  and {" AND ".join([f"t.{k} = r.{k}" for k in keys])}
  and t.{vf} = r.{vf};
"""
        self._execute_sql(delete_sql)

    def execute_hook_sql(self, sql: str) -> None:
        """
        Execute one or multiple SQL statements for pre/post/on_run hooks.

        Accepts a string that may contain ';'-separated statements.
        """
        self._exec_many(sql)

        # ---- Unit-test helpers -------------------------------------------------

    def utest_load_relation_from_rows(self, relation: str, rows: list[dict]) -> None:
        """
        Load rows into a DuckDB table for unit tests, fully qualified to
        this executor's schema/catalog.
        """
        df = pd.DataFrame(rows)
        tmp = f"_ff_utest_tmp_{uuid.uuid4().hex[:12]}"
        self.con.register(tmp, df)
        try:
            target = self._qualified(relation)
            self._execute_sql(f"create or replace table {target} as select * from {tmp}")
        finally:
            with suppress(Exception):
                self.con.unregister(tmp)
            # Fallback for older DuckDB where unregister might not exist
            with suppress(Exception):
                self._execute_sql(f'drop view if exists "{tmp}"')

    def utest_read_relation(self, relation: str) -> pd.DataFrame:
        """
        Read a relation as a DataFrame for unit-test assertions.
        """
        target = self._qualified(relation, quoted=False)
        return self.con.table(target).df()

    def utest_clean_target(self, relation: str) -> None:
        """
        Drop any table/view with the given name in this schema/catalog.
        Safe because utest uses its own DB/path.
        """
        target = self._qualified(relation)
        # best-effort; ignore failures
        with suppress(Exception):
            self._execute_sql(f"drop view if exists {target}")
        with suppress(Exception):
            self._execute_sql(f"drop table if exists {target}")

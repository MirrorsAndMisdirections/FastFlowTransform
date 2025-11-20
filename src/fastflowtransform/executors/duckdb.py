# fastflowtransform/executors/duckdb.py
from __future__ import annotations

from collections.abc import Iterable
from contextlib import suppress
from pathlib import Path
from typing import Any

import duckdb
import pandas as pd
from duckdb import CatalogException
from jinja2 import Environment

from fastflowtransform.core import Node, relation_for
from fastflowtransform.executors.base import BaseExecutor
from fastflowtransform.logging import echo
from fastflowtransform.meta import ensure_meta_table, upsert_meta
from fastflowtransform.snapshots import resolve_snapshot_config


def _q(ident: str) -> str:
    return '"' + ident.replace('"', '""') + '"'


class DuckExecutor(BaseExecutor[pd.DataFrame]):
    ENGINE_NAME = "duckdb"

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
        if catalog_override:
            if self._apply_catalog_override(catalog_override):
                self.catalog = catalog_override
            else:
                self.catalog = self._detect_catalog()
        if self.schema:
            safe_schema = _q(self.schema)
            self.con.execute(f"create schema if not exists {safe_schema}")
            self.con.execute(f"set schema '{self.schema}'")

    def _detect_catalog(self) -> str | None:
        try:
            rows = self.con.execute("PRAGMA database_list").fetchall()
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
                    self.con.execute(f"detach database {_q(alias)}")
                self.con.execute(f"attach database '{resolved}' as {_q(alias)} (READ_ONLY FALSE)")
            self.con.execute(f"set catalog '{alias}'")
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
        # very simple splitter - good enough for what we emit in the executor
        for stmt in (part.strip() for part in sql.split(";")):
            if not stmt:
                continue
            self.con.execute(stmt)

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
                for r in self.con.execute(
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
            self.con.execute(f'create or replace table {target} as select * from "{tmp}"')
        finally:
            try:
                self.con.unregister(tmp)
            except Exception:
                self.con.execute(f'drop view if exists "{tmp}"')

    def _create_or_replace_view_from_table(
        self, view_name: str, backing_table: str, node: Node
    ) -> None:
        view_target = self._qualified(view_name)
        backing = self._qualified(backing_table)
        self.con.execute(f"create or replace view {view_target} as select * from {backing}")

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
        self.con.execute(f"create or replace view {target_sql} as {select_body}")

    def _create_or_replace_table(self, target_sql: str, select_body: str, node: Node) -> None:
        self.con.execute(f"create or replace table {target_sql} as {select_body}")

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
        if self.con.execute(sql_tables, params).fetchone():
            return True
        sql_views = f"select 1 from information_schema.views where {where} limit 1"
        return bool(self.con.execute(sql_views, params).fetchone())

    def create_table_as(self, relation: str, select_sql: str) -> None:
        # Use only the SELECT body and strip trailing semicolons for safety.
        body = self._selectable_body(select_sql).strip().rstrip(";\n\t ")
        self.con.execute(f"create table {self._qualified(relation)} as {body}")

    def incremental_insert(self, relation: str, select_sql: str) -> None:
        # Ensure the inner SELECT is clean (no trailing semicolon; SELECT body only).
        body = self._selectable_body(select_sql).strip().rstrip(";\n\t ")
        self.con.execute(f"insert into {self._qualified(relation)} {body}")

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
        self.con.execute(delete_sql)

        # 4) then: insert fresh rows
        insert_sql = f"insert into {self._qualified(relation)} select * from ({body}) src"
        self.con.execute(insert_sql)

    def alter_table_sync_schema(
        self, relation: str, select_sql: str, *, mode: str = "append_new_columns"
    ) -> None:
        """
        Best-effort: add new columns with inferred type.
        """
        # Probe: empty projection from the SELECT (cleaned to avoid parser issues).
        body = self._first_select_body(select_sql).strip().rstrip(";\n\t ")
        probe = self.con.execute(f"select * from ({body}) as q limit 0")
        cols = [c[0] for c in probe.description or []]
        # vorhandene Spalten
        existing = {
            r[0]
            for r in self.con.execute(
                "select column_name from information_schema.columns "
                + "where lower(table_name)=lower(?)"
                + (" and lower(table_schema)=lower(?)" if self.schema else ""),
                ([relation, self.schema] if self.schema else [relation]),
            ).fetchall()
        }
        add = [c for c in cols if c not in existing]
        for c in add:
            # Typ heuristisch: typeof aus einer CAST-Probe; fallback VARCHAR
            col = _q(c)
            target = self._qualified(relation)
            try:
                # Versuche Typ aus Expression abzuleiten (best effort)
                self.con.execute(f"alter table {target} add column {col} varchar")
            except Exception:
                self.con.execute(f"alter table {target} add column {col} varchar")

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
            self.con.execute(create_sql)
            return

        # ---- Incremental snapshot update ----

        # Stage current source rows in a temp view for reuse
        src_view_name = f"__ff_snapshot_src_{rel_name}".replace(".", "_")
        src_quoted = _q(src_view_name)
        self.con.execute(f"create or replace temp view {src_quoted} as {body}")

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
            self.con.execute(close_sql)

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
            self.con.execute(insert_sql)
        finally:
            with suppress(Exception):
                self.con.execute(f"drop view if exists {src_quoted}")

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
            res = self.con.execute(sql).fetchone()
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
        self.con.execute(delete_sql)

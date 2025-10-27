# src/fastflowtransform/executors/snowflake_snowpark_exec.py
from __future__ import annotations

from collections.abc import Iterable
from typing import Any

from snowflake.snowpark import DataFrame as SNDF, Session

from fastflowtransform.core import Node, relation_for
from fastflowtransform.executors.base import BaseExecutor
from fastflowtransform.meta import ensure_meta_table, upsert_meta


class SnowflakeSnowparkExecutor(BaseExecutor[SNDF]):
    """Snowflake executor operating on Snowpark DataFrames (no pandas)."""

    def __init__(self, cfg: dict):
        # cfg: {account, user, password, warehouse, database, schema, role?}
        self.session = Session.builder.configs(cfg).create()
        self.database = cfg["database"]
        self.schema = cfg["schema"]
        # Provide a tiny testing shim so tests can call executor.con.execute("SQL")
        self.con = _SFCursorShim(self.session)

    # ---------- Helpers ----------
    def _q(self, s: str) -> str:
        return '"' + s.replace('"', '""') + '"'

    def _qualified(self, rel: str) -> str:
        # "DB"."SCHEMA"."TABLE"
        return f"{self._q(self.database)}.{self._q(self.schema)}.{self._q(rel)}"

    # ---------- Frame-Hooks ----------
    def _read_relation(self, relation: str, node: Node, deps: Iterable[str]) -> SNDF:
        return self.session.table(self._qualified(relation))

    def _materialize_relation(self, relation: str, df: SNDF, node: Node) -> None:
        if not self._is_frame(df):
            raise TypeError("Snowpark model must return a Snowpark DataFrame")
        df.write.save_as_table(self._qualified(relation), mode="overwrite")

    def _create_view_over_table(self, view_name: str, backing_table: str, node: Node) -> None:
        qv = self._qualified(view_name)
        qb = self._qualified(backing_table)
        self.session.sql(f"CREATE OR REPLACE VIEW {qv} AS SELECT * FROM {qb}").collect()

    def _validate_required(
        self, node_name: str, inputs: Any, requires: dict[str, set[str]]
    ) -> None:
        if not requires:
            return

        def cols(df: SNDF) -> set[str]:
            # Snowpark: schema names
            return set(df.schema.names)

        errors: list[str] = []
        # Single dependency
        if isinstance(inputs, SNDF):
            need = next(iter(requires.values()), set())
            missing = need - cols(inputs)
            if missing:
                errors.append(f"- missing columns: {sorted(missing)} | have={sorted(cols(inputs))}")
        else:
            # Multiple dependencies
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
                "Required columns check failed for Snowpark model "
                f"'{node_name}'.\n" + "\n".join(errors)
            )

    def _columns_of(self, frame: SNDF) -> list[str]:
        return list(frame.schema.names)

    def _is_frame(self, obj: Any) -> bool:
        return isinstance(obj, SNDF)

    def _frame_name(self) -> str:
        return "Snowpark"

    # ---- SQL hooks ----
    def _format_relation_for_ref(self, name: str) -> str:
        return self._qualified(relation_for(name))

    def _format_source_reference(
        self, cfg: dict[str, Any], source_name: str, table_name: str
    ) -> str:
        ident = cfg["identifier"]
        db = cfg.get("database", self.database)
        sch = cfg.get("schema", self.schema)
        return f"{self._q(db)}.{self._q(sch)}.{self._q(ident)}"

    def _create_or_replace_view(self, target_sql: str, select_body: str, node: Node) -> None:
        self.session.sql(f"CREATE OR REPLACE VIEW {target_sql} AS {select_body}").collect()

    def _create_or_replace_table(self, target_sql: str, select_body: str, node: Node) -> None:
        self.session.sql(f"CREATE OR REPLACE TABLE {target_sql} AS {select_body}").collect()

    def _create_or_replace_view_from_table(
        self, view_name: str, backing_table: str, node: Node
    ) -> None:
        view_id = self._qualified(view_name)
        back_id = self._qualified(backing_table)
        self.session.sql(f"CREATE OR REPLACE VIEW {view_id} AS SELECT * FROM {back_id}").collect()

    # ---- Meta hook ----
    def on_node_built(self, node: Node, relation: str, fingerprint: str) -> None:
        """After successful materialization, upsert _ff_meta (best-effort)."""
        try:
            ensure_meta_table(self)
            upsert_meta(self, node.name, relation, fingerprint, "snowflake_snowpark")
        except Exception:
            pass

    # ── Incremental API (parity with DuckDB/PG) ──────────────────────────
    def exists_relation(self, relation: str) -> bool:
        """Check existence via information_schema.tables."""
        db = self._q(self.database)
        q = f"""
          select 1
          from {db}.information_schema.tables
          where table_schema = {self._q(self.schema)}
            and lower(table_name) = lower({self._q(relation)})
          limit 1
        """
        try:
            return bool(self.session.sql(q).collect())
        except Exception:
            return False

    def create_table_as(self, relation: str, select_sql: str) -> None:
        body = self._first_select_body(select_sql).strip().rstrip(";\n\t ")
        self.session.sql(f"CREATE OR REPLACE TABLE {self._qualified(relation)} AS {body}").collect()

    def incremental_insert(self, relation: str, select_sql: str) -> None:
        body = self._first_select_body(select_sql).strip().rstrip(";\n\t ")
        self.session.sql(f"INSERT INTO {self._qualified(relation)} {body}").collect()

    def incremental_merge(self, relation: str, select_sql: str, unique_key: list[str]) -> None:
        """
        Portable fallback without explicit column list:
          - WITH src AS (<body>)
          - DELETE ... USING src ...
          - INSERT ... SELECT * FROM src
        This avoids Snowflake MERGE column listing complexity.
        """
        body = self._first_select_body(select_sql).strip().rstrip(";\n\t ")
        pred = " AND ".join([f"t.{k}=s.{k}" for k in unique_key]) or "FALSE"
        qrel = self._qualified(relation)
        sql = f"""
        WITH src AS ({body})
        DELETE FROM {qrel} AS t USING src AS s WHERE {pred};
        INSERT INTO {qrel} SELECT * FROM src;
        """
        self.session.sql(sql).collect()

    def alter_table_sync_schema(
        self, relation: str, select_sql: str, *, mode: str = "append_new_columns"
    ) -> None:
        """
        Best-effort additive schema sync:
          - infer SELECT schema via LIMIT 0
          - add missing columns as STRING
        """
        if mode not in {"append_new_columns", "sync_all_columns"}:
            return
        qrel = self._qualified(relation)
        try:
            existing = {
                r[0]
                for r in self.session.sql(
                    f"""
                select column_name
                from {self._q(self.database)}.information_schema.columns
                where table_schema={self._q(self.schema)}
                  and lower(table_name)=lower({self._q(relation)})
                """
                ).collect()
            }
        except Exception:
            existing = set()
        # Probe SELECT columns
        body = self._first_select_body(select_sql).strip().rstrip(";\n\t ")
        probe = self.session.sql(f"SELECT * FROM ({body}) q WHERE 1=0")
        probe_cols = list(probe.schema.names)
        to_add = [c for c in probe_cols if c not in existing]
        if not to_add:
            return
        cols_sql = ", ".join(f"{self._q(c)} STRING" for c in to_add)
        self.session.sql(f"ALTER TABLE {qrel} ADD COLUMN {cols_sql}").collect()


# ────────────────────────── local testing shim ───────────────────────────
class _SFCursorShim:
    """Very small shim to expose .execute(...).fetch* for tests."""

    def __init__(self, session: Session):
        self._session = session

    def execute(self, sql: str, params: Any | None = None) -> _SFResult:
        if params:
            # Parametrized SQL not needed in our internal calls
            raise NotImplementedError("Snowflake shim does not support parametrized SQL")
        rows = self._session.sql(sql).collect()
        as_tuples = [tuple(getattr(r, k) for k in r.asDict()) for r in rows] if rows else []
        return _SFResult(as_tuples)


class _SFResult:
    def __init__(self, rows: list[tuple]):
        self._rows = rows

    def fetchall(self) -> list[tuple]:
        return self._rows

    def fetchone(self) -> tuple | None:
        return self._rows[0] if self._rows else None

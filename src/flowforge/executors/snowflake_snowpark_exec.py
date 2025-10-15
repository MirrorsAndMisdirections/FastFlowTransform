# src/flowforge/executors/snowflake_snowpark_exec.py
from __future__ import annotations

from collections.abc import Iterable
from typing import Any

from jinja2 import Environment
from snowflake.snowpark import DataFrame as SNDF, Session

from ..core import Node, relation_for
from .base import BaseExecutor


class SnowflakeSnowparkExecutor(BaseExecutor[SNDF]):
    """Snowflake executor operating on Snowpark DataFrames (no pandas)."""

    def __init__(self, cfg: dict):
        # cfg: {account, user, password, warehouse, database, schema, role?}
        self.session = Session.builder.configs(cfg).create()
        self.database = cfg["database"]
        self.schema = cfg["schema"]

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
        self.session.sql(
            f"CREATE OR REPLACE VIEW {view_id} AS SELECT * FROM {back_id}"
        ).collect()

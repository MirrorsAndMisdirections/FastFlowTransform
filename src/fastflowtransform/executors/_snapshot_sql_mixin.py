from __future__ import annotations

from collections.abc import Callable, Iterable
from contextlib import suppress
from typing import TYPE_CHECKING, Any, cast

from jinja2 import Environment

from fastflowtransform.core import Node, relation_for
from fastflowtransform.logging import echo
from fastflowtransform.snapshots import resolve_snapshot_config

if TYPE_CHECKING:
    # Adjust this import to your actual path
    from fastflowtransform.executors.base import BaseExecutor


class SnapshotSqlMixin:
    """
    Shared SQL snapshot materialization (timestamp + check strategies).

    Engines provide small hooks for identifier qualification, expressions,
    staging, and execution. All column names come from BaseExecutor constants.
    """

    def run_snapshot_sql(self, node: Node, env: Environment) -> None:
        ex = cast("BaseExecutor[Any]", self)

        meta = self._snapshot_validate_node(node)
        cfg = resolve_snapshot_config(node, meta)

        body = self._snapshot_render_body(node, env)
        rel_name = relation_for(node.name)
        target = self._snapshot_target_identifier(rel_name)
        if not cfg.unique_key:
            raise ValueError(f"{node.path}: snapshot models require a non-empty unique_key list.")

        vf = self.SNAPSHOT_VALID_FROM_COL  # type: ignore[attr-defined]
        vt = self.SNAPSHOT_VALID_TO_COL  # type: ignore[attr-defined]
        is_cur = self.SNAPSHOT_IS_CURRENT_COL  # type: ignore[attr-defined]
        hash_col = self.SNAPSHOT_HASH_COL  # type: ignore[attr-defined]
        upd_meta = self.SNAPSHOT_UPDATED_AT_COL  # type: ignore[attr-defined]

        self._snapshot_prepare_target()

        # First run: create snapshot table
        if not ex.exists_relation(rel_name):
            sql = self._snapshot_first_run_sql(
                body=body,
                strategy=cfg.strategy,
                unique_key=cfg.unique_key,
                updated_at=cfg.updated_at,
                check_cols=cfg.check_cols,
                target=target,
                vf=vf,
                vt=vt,
                is_cur=is_cur,
                hash_col=hash_col,
                upd_meta=upd_meta,
            )
            self._snapshot_exec_and_wait(sql)
            return

        # Incremental update
        src_ref, cleanup = self._snapshot_source_ref(rel_name, body)
        try:
            keys_pred = " AND ".join([f"t.{k} = s.{k}" for k in cfg.unique_key]) or "FALSE"

            if cfg.strategy == "timestamp":
                if not cfg.updated_at:
                    raise ValueError(
                        f"{node.path}: strategy='timestamp' snapshots require an updated_at column."
                    )
                change_condition = f"s.{cfg.updated_at} > t.{upd_meta}"
                new_upd_expr = self._snapshot_updated_at_expr(cfg.updated_at, "s")
                new_valid_from_expr = self._snapshot_updated_at_expr(cfg.updated_at, "s")
                new_hash_expr = self._snapshot_null_hash()
            else:
                hash_expr_s = self._snapshot_hash_expr(cfg.check_cols, "s")
                change_condition = f"COALESCE({hash_expr_s}, '') <> COALESCE(t.{hash_col}, '')"
                new_upd_expr = (
                    self._snapshot_updated_at_expr(cfg.updated_at, "s")
                    if cfg.updated_at
                    else self._snapshot_current_timestamp()
                )
                new_valid_from_expr = self._snapshot_current_timestamp()
                new_hash_expr = hash_expr_s

            close_sql = self._snapshot_close_sql(
                target=target,
                src_ref=src_ref,
                keys_pred=keys_pred,
                change_condition=change_condition,
                vt=vt,
                is_cur=is_cur,
            )
            self._snapshot_exec_and_wait(close_sql)

            insert_sql = self._snapshot_insert_sql(
                target=target,
                src_ref=src_ref,
                keys_pred=keys_pred,
                first_key=cfg.unique_key[0],
                new_upd_expr=new_upd_expr,
                new_valid_from_expr=new_valid_from_expr,
                new_hash_expr=new_hash_expr,
                change_condition=change_condition,
                vf=vf,
                vt=vt,
                is_cur=is_cur,
                hash_col=hash_col,
                upd_meta=upd_meta,
            )
            self._snapshot_exec_and_wait(insert_sql)
        finally:
            with suppress(Exception):
                cleanup()

    # ---- Core SQL builders -------------------------------------------------
    def _snapshot_first_run_sql(
        self,
        *,
        body: str,
        strategy: str,
        unique_key: list[str],
        updated_at: str | None,
        check_cols: list[str],
        target: str,
        vf: str,
        vt: str,
        is_cur: str,
        hash_col: str,
        upd_meta: str,
    ) -> str:
        if not unique_key:
            raise ValueError("Snapshot models require a non-empty unique_key list.")

        if strategy == "timestamp":
            if not updated_at:
                raise ValueError("strategy='timestamp' snapshots require an updated_at column.")
            return f"""
{self._snapshot_create_keyword()} {target} AS
SELECT
  s.*,
  {self._snapshot_updated_at_expr(updated_at, "s")} AS {upd_meta},
  {self._snapshot_updated_at_expr(updated_at, "s")} AS {vf},
  {self._snapshot_null_timestamp()} AS {vt},
  TRUE AS {is_cur},
  {self._snapshot_null_hash()} AS {hash_col}
FROM ({body}) AS s
"""

        if not check_cols:
            raise ValueError("strategy='check' snapshots require non-empty check_cols.")

        hash_expr = self._snapshot_hash_expr(check_cols, "s")
        upd_expr = (
            self._snapshot_updated_at_expr(updated_at, "s")
            if updated_at
            else self._snapshot_current_timestamp()
        )
        return f"""
{self._snapshot_create_keyword()} {target} AS
SELECT
  s.*,
  {upd_expr} AS {upd_meta},
  {self._snapshot_current_timestamp()} AS {vf},
  {self._snapshot_null_timestamp()} AS {vt},
  TRUE AS {is_cur},
  {hash_expr} AS {hash_col}
FROM ({body}) AS s
"""

    def _snapshot_close_sql(
        self,
        *,
        target: str,
        src_ref: str,
        keys_pred: str,
        change_condition: str,
        vt: str,
        is_cur: str,
    ) -> str:
        return f"""
UPDATE {target} AS t
SET
  {vt} = {self._snapshot_current_timestamp()},
  {is_cur} = FALSE
FROM {src_ref} AS s
WHERE
  {keys_pred}
  AND t.{is_cur} = TRUE
  AND {change_condition}
"""

    def _snapshot_insert_sql(
        self,
        *,
        target: str,
        src_ref: str,
        keys_pred: str,
        first_key: str,
        new_upd_expr: str,
        new_valid_from_expr: str,
        new_hash_expr: str,
        change_condition: str,
        vf: str,
        vt: str,
        is_cur: str,
        hash_col: str,
        upd_meta: str,
    ) -> str:
        return f"""
INSERT INTO {target}
SELECT
  s.*,
  {new_upd_expr} AS {upd_meta},
  {new_valid_from_expr} AS {vf},
  {self._snapshot_null_timestamp()} AS {vt},
  TRUE AS {is_cur},
  {new_hash_expr} AS {hash_col}
FROM {src_ref} AS s
LEFT JOIN {target} AS t
  ON {keys_pred}
  AND t.{is_cur} = TRUE
WHERE
  t.{first_key} IS NULL
  OR {change_condition}
"""

    # ---- Pruning -----------------------------------------------------------
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
        ex = cast("BaseExecutor[Any]", self)

        if keep_last <= 0:
            return

        keys = [k for k in unique_key if k]
        if not keys:
            return

        target = self._snapshot_target_identifier(relation)
        vf = self.SNAPSHOT_VALID_FROM_COL  # type: ignore[attr-defined]

        key_select = ", ".join(keys)
        part_by = ", ".join(keys)

        ranked_sql = f"""
SELECT
  {key_select},
  {vf},
  ROW_NUMBER() OVER (
    PARTITION BY {part_by}
    ORDER BY {vf} DESC
  ) AS rn
FROM {target}
"""

        if dry_run:
            sql = f"""
WITH ranked AS (
  {ranked_sql}
)
SELECT COUNT(*) AS rows_to_delete
FROM ranked
WHERE rn > {int(keep_last)}
"""
            res = ex._execute_sql(sql)
            count = self._snapshot_fetch_count(res)
            echo(
                f"[DRY-RUN] snapshot_prune({relation}): would delete {count} row(s) "
                f"(keep_last={keep_last})"
            )
            return

        join_pred = " AND ".join([f"t.{k} = r.{k}" for k in keys])
        delete_sql = f"""
DELETE FROM {target} t
USING (
  {ranked_sql}
) r
WHERE
  r.rn > {int(keep_last)}
  AND {join_pred}
  AND t.{vf} = r.{vf}
"""
        ex._execute_sql(delete_sql)

    # ---- Rendering helpers -------------------------------------------------
    def _snapshot_render_body(self, node: Node, env: Environment) -> str:
        ex = cast("BaseExecutor[Any]", self)

        sql_rendered = ex.render_sql(
            node,
            env,
            ref_resolver=lambda name: ex._resolve_ref(name, env),
            source_resolver=ex._resolve_source,
        )
        sql_clean = ex._strip_leading_config(sql_rendered).strip()
        return ex._selectable_body(sql_clean).rstrip(" ;\n\t")

    def _snapshot_validate_node(self, node: Node) -> dict[str, Any]:
        if node.kind != "sql":
            raise TypeError(
                f"Snapshot materialization is only supported for SQL models, "
                f"got kind={node.kind!r} for {node.name}."
            )

        meta = getattr(node, "meta", {}) or {}
        if not self._meta_is_snapshot(meta):  # type: ignore[attr-defined]
            raise ValueError(f"Node {node.name} is not configured with materialized='snapshot'.")
        return meta

    # ---- Staging -----------------------------------------------------------
    def _snapshot_source_ref(
        self, rel_name: str, select_body: str
    ) -> tuple[str, Callable[[], None]]:
        """
        Return (source_ref, cleanup). Default: inline subquery.
        Engines can override to use temp views/tables and cleanup afterward.
        """
        return f"({select_body})", lambda: None

    # ---- Hooks (must be provided by engines) -------------------------------
    def _snapshot_target_identifier(self, rel_name: str) -> str:  # pragma: no cover - abstract
        raise NotImplementedError

    def _snapshot_current_timestamp(self) -> str:  # pragma: no cover - abstract
        raise NotImplementedError

    def _snapshot_null_timestamp(self) -> str:  # pragma: no cover - abstract
        raise NotImplementedError

    def _snapshot_null_hash(self) -> str:  # pragma: no cover - abstract
        raise NotImplementedError

    def _snapshot_hash_expr(self, check_cols: list[str], src_alias: str) -> str:  # pragma: no cover
        raise NotImplementedError

    def _snapshot_updated_at_expr(self, updated_at: str, src_alias: str) -> str:
        return f"{src_alias}.{updated_at}"

    def _snapshot_prepare_target(self) -> None:
        """Hook for engines that need to ensure dataset/schema before writes."""
        return None

    def _snapshot_exec_and_wait(self, sql: str) -> None:
        """
        Execute SQL and, if necessary, wait for completion (jobs, lazy DataFrames).
        """
        res = self._execute_sql(sql)  # type: ignore[attr-defined]
        if res is None:
            return
        for attr in ("result", "collect"):
            fn = getattr(res, attr, None)
            if callable(fn):
                with suppress(Exception):
                    fn()
                break

    # ---- Helpers -----------------------------------------------------------
    def _snapshot_concat_expr(self, columns: list[str], src_alias: str) -> str:
        parts = [
            self._snapshot_coalesce(self._snapshot_cast_as_string(f"{src_alias}.{col}"), "''")
            for col in columns
        ]
        return " || '||' || ".join(parts) if parts else "''"

    def _snapshot_cast_as_string(self, expr: str) -> str:
        return f"CAST({expr} AS STRING)"

    def _snapshot_coalesce(self, expr: str, default: str) -> str:
        return f"COALESCE({expr}, {default})"

    def _snapshot_create_keyword(self) -> str:
        """Hook to allow engines to override CREATE vs CREATE OR REPLACE."""
        return "CREATE TABLE"

    def _snapshot_fetch_count(self, res: Any) -> int:
        """
        Best-effort extraction of a single COUNT(*) value from various result shapes.
        """
        try:
            if hasattr(res, "fetchone"):
                row = res.fetchone()
                if row is not None:
                    return int(row[0])
            if hasattr(res, "fetchall"):
                rows = res.fetchall()
                if rows:
                    return int(rows[0][0])
            result_fn = getattr(res, "result", None)
            if callable(result_fn):
                rows_obj = result_fn()
                if isinstance(rows_obj, Iterable):
                    rows = list(rows_obj)
                    if rows:
                        return int(rows[0][0])
            collect_fn = getattr(res, "collect", None)
            if callable(collect_fn):
                rows_obj = collect_fn()
                if isinstance(rows_obj, Iterable):
                    rows = list(rows_obj)
                    if rows:
                        return int(rows[0][0])
            if isinstance(res, list) and res:
                return int(res[0][0])
        except Exception:
            return 0
        return 0

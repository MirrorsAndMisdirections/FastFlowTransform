# src/flowforge/executors/bigquery_bf_exec.py
from __future__ import annotations

from collections.abc import Iterable
from typing import TYPE_CHECKING, Any

import bigframes  # Package: google-cloud-bigquery-dataframes
from google.api_core.exceptions import BadRequest, NotFound
from google.cloud import bigquery

from ..core import Node, relation_for
from ._bigquery_mixin import BigQueryIdentifierMixin
from ._shims import BigQueryConnShim
from .base import BaseExecutor

if TYPE_CHECKING:
    from bigframes.dataframe import DataFrame as BFDataFrame  # type: ignore
else:
    BFDataFrame = Any


class BigQueryBFExecutor(BigQueryIdentifierMixin, BaseExecutor[BFDataFrame]):
    def __init__(self, project: str, dataset: str, location: str | None = None):
        self.project = project
        self.dataset = dataset
        self.location = location
        self.client = bigquery.Client(project=project, location=location)

        try:
            from bigframes.options import BigQueryOptions  # type: ignore

            ctx = BigQueryOptions(project=project, default_dataset=dataset, location=location)
            self.session = bigframes.Session(context=ctx)
        except Exception:
            # Fallback: session without explicit context (ADC/default project),
            # though you typically use fully qualified table IDs anyway.
            self.session = bigframes.Session()

        self.con = BigQueryConnShim(self.client, location=self.location)

    def _read_relation(self, relation: str, node: Node, deps: Iterable[str]) -> BFDataFrame:
        table_id = f"{self.project}.{self.dataset}.{relation}"
        try:
            return self.session.read_gbq(table_id)
        except NotFound as e:
            existing = [
                t.table_id for t in self.client.list_tables(f"{self.project}.{self.dataset}")
            ]
            raise RuntimeError(
                f"Dependency table not found: {table_id}\n"
                f"Deps: {list(deps)}\nExisting in dataset: {existing}\n"
                "Hinweis: Seeds/Upstream-Modelle erzeugt? DATASET korrekt?"
            ) from e

    def _materialize_relation(self, relation: str, df: BFDataFrame, node: Node) -> None:
        table_id = f"{self.project}.{self.dataset}.{relation}"

        to_gbq = getattr(df, "to_gbq", None)
        if callable(to_gbq):
            to_gbq(table_id, if_exists="replace")
            return

        # Fallback only when it is truly a method (not a column name!)
        mat = getattr(df, "materialize", None)
        if callable(mat):
            mat(table=table_id, mode="overwrite")
            return

        raise RuntimeError(
            "BigQuery DataFrames: Ergebnis nicht materialisierbar. "
            "Erwarte df.to_gbq(...) oder df.materialize(...)."
        )
    
    def _validate_required(
        self, node_name: str, inputs: Any, requires: dict[str, set[str]]
    ) -> None:
        if not requires:
            return

        def cols(bf_df: BFDataFrame) -> set[str]:
            if hasattr(bf_df, "columns"):
                return set(map(str, list(bf_df.columns)))
            if hasattr(bf_df, "schema") and hasattr(bf_df.schema, "names"):
                return set(bf_df.schema.names)
            return set()

        errs: list[str] = []
        if self._is_frame(inputs):
            need = next(iter(requires.values()), set())
            miss = need - cols(inputs)  # type: ignore[arg-type]
            if miss:
                errs.append(f"- missing columns: {sorted(miss)}")
        else:
            for rel, need in requires.items():
                if rel not in inputs:
                    errs.append(f"- missing dependency key '{rel}'")
                    continue
                miss = need - cols(inputs[rel])
                if miss:
                    errs.append(f"- [{rel}] missing: {sorted(miss)}")
        if errs:
            raise ValueError(
                f"Required columns check failed for BigQuery DataFrames model '{node_name}'.\n"
                + "\n".join(errs)
            )

    def _columns_of(self, frame: BFDataFrame) -> list[str]:
        if hasattr(frame, "columns"):
            return [str(c) for c in list(frame.columns)]
        if hasattr(frame, "schema") and hasattr(frame.schema, "names"):
            return list(frame.schema.names)
        return []

    def _is_frame(self, obj: Any) -> bool:
        return bool(obj) and (
            callable(getattr(obj, "to_gbq", None))
            or callable(getattr(obj, "materialize", None))
            or hasattr(obj, "columns")
        )

    def _frame_name(self) -> str:
        return "BigQuery DataFrame (BigFrames)"

    # ---- Helpers ----
    # ---- SQL hooks ----
    def _format_relation_for_ref(self, name: str) -> str:
        return self._qualified_identifier(relation_for(name))

    def _format_source_reference(
        self, cfg: dict[str, Any], source_name: str, table_name: str
    ) -> str:
        ident = cfg["identifier"]
        proj = cfg.get("project", self.project)
        dset = cfg.get("dataset", self.dataset)
        return self._qualified_identifier(ident, project=proj, dataset=dset)

    def _apply_sql_materialization(
        self, node: Node, target_sql: str, select_body: str, materialization: str
    ) -> None:
        self._ensure_dataset()
        try:
            super()._apply_sql_materialization(node, target_sql, select_body, materialization)
        except BadRequest as e:
            raise RuntimeError(
                f"BigQuery SQL failed for {target_sql}:\n{select_body}\n\n{e}"
            ) from e

    def _create_or_replace_view(self, target_sql: str, select_body: str, node: Node) -> None:
        self.client.query(
            f"CREATE OR REPLACE VIEW {target_sql} AS {select_body}",
            location=self.location,
        ).result()

    def _create_or_replace_table(self, target_sql: str, select_body: str, node: Node) -> None:
        self.client.query(
            f"CREATE OR REPLACE TABLE {target_sql} AS {select_body}",
            location=self.location,
        ).result()

    def _create_or_replace_view_from_table(
        self, view_name: str, backing_table: str, node: Node
    ) -> None:
        view_id = self._qualified_identifier(view_name)
        back_id = self._qualified_identifier(backing_table)
        self.client.query(
            f"CREATE OR REPLACE VIEW {view_id} AS SELECT * FROM {back_id}",
            location=self.location,
        ).result()
        raise TypeError(f"Unsupported sql for BigQuery shim: {type(sql_or_stmts)}")

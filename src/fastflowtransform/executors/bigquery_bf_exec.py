# src/fastflowtransform/executors/bigquery_bf_exec.py
from __future__ import annotations

from collections.abc import Iterable
from typing import TYPE_CHECKING, Any

import bigframes  # Package: google-cloud-bigquery-dataframes
from bigframes._config.bigquery_options import BigQueryOptions
from google.api_core.exceptions import BadRequest, NotFound
from google.cloud import bigquery

from fastflowtransform.core import Node, relation_for
from fastflowtransform.executors._bigquery_mixin import BigQueryIdentifierMixin
from fastflowtransform.executors._shims import BigQueryConnShim
from fastflowtransform.executors.base import BaseExecutor
from fastflowtransform.meta import ensure_meta_table, upsert_meta

if TYPE_CHECKING:
    from bigframes.dataframe import DataFrame as BFDataFrame
else:
    BFDataFrame = Any


class BigQueryBFExecutor(BigQueryIdentifierMixin, BaseExecutor[BFDataFrame]):
    ENGINE_NAME = "bigquery_batch"

    def __init__(self, project: str, dataset: str, location: str | None = None):
        self.project = project
        self.dataset = dataset
        self.location = location
        self.client = bigquery.Client(project=project, location=location)

        try:
            ctx = BigQueryOptions(
                project=project,
                # default_dataset=dataset,
                location=location,
            )
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

    # ---- Meta hook ----
    def on_node_built(self, node: Node, relation: str, fingerprint: str) -> None:
        """Mirror DuckDB/PG: write/update _ff_meta after successful build."""
        try:
            ensure_meta_table(self)
            upsert_meta(self, node.name, relation, fingerprint, "bigquery")
        except Exception:
            # Best-effort: meta must not break the run
            pass

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
            miss = need - cols(inputs)
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
        if cfg.get("location"):
            raise NotImplementedError("BigQuery executor does not support path-based sources.")

        ident = cfg.get("identifier")
        if not ident:
            raise KeyError(f"Source {source_name}.{table_name} missing identifier")

        proj = cfg.get("project") or cfg.get("database") or cfg.get("catalog") or self.project
        dset = cfg.get("dataset") or cfg.get("schema") or self.dataset
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

    # ── Incremental API (feature parity with DuckDB/PG) ──────────────────
    def exists_relation(self, relation: str) -> bool:
        """Check presence in TABLES or VIEWS information schema."""
        proj = self.project
        dset = self.dataset
        rel = relation
        q = f"""
        SELECT 1
        FROM `{proj}.{dset}.INFORMATION_SCHEMA.TABLES`
        WHERE LOWER(table_name)=LOWER(@rel)
        UNION ALL
        SELECT 1
        FROM `{proj}.{dset}.INFORMATION_SCHEMA.VIEWS`
        WHERE LOWER(table_name)=LOWER(@rel)
        LIMIT 1
        """
        job = self.client.query(
            q,
            job_config=bigquery.QueryJobConfig(
                query_parameters=[bigquery.ScalarQueryParameter("rel", "STRING", rel)]
            ),
            location=self.location,
        )
        return bool(list(job.result()))

    def create_table_as(self, relation: str, select_sql: str) -> None:
        """CTAS with cleaned SELECT body (no trailing semicolons)."""
        self._ensure_dataset()
        body = self._first_select_body(select_sql).strip().rstrip(";\n\t ")
        target = self._qualified_identifier(relation, project=self.project, dataset=self.dataset)
        self.client.query(
            f"CREATE TABLE {target} AS {body}",
            location=self.location,
        ).result()

    def incremental_insert(self, relation: str, select_sql: str) -> None:
        """INSERT INTO with cleaned SELECT body."""
        self._ensure_dataset()
        body = self._first_select_body(select_sql).strip().rstrip(";\n\t ")
        target = self._qualified_identifier(relation, project=self.project, dataset=self.dataset)
        self.client.query(
            f"INSERT INTO {target} {body}",
            location=self.location,
        ).result()

    def incremental_merge(self, relation: str, select_sql: str, unique_key: list[str]) -> None:
        """
        Portable fallback in BigQuery (without full MERGE):
          - DELETE collisions via WHERE EXISTS against the cleaned SELECT body
          - INSERT all rows from the body
        Executed as two statements to keep error surfaces clean.
        """
        self._ensure_dataset()
        body = self._first_select_body(select_sql).strip().rstrip(";\n\t ")
        target = self._qualified_identifier(relation, project=self.project, dataset=self.dataset)
        pred = " AND ".join([f"t.{k}=s.{k}" for k in unique_key]) or "FALSE"

        # DELETE … WHERE EXISTS (SELECT 1 FROM (body) s WHERE pred)
        delete_sql = f"""
        DELETE FROM {target} t
        WHERE EXISTS (SELECT 1 FROM ({body}) s WHERE {pred})
        """
        self.client.query(delete_sql, location=self.location).result()

        # INSERT new rows
        insert_sql = f"INSERT INTO {target} SELECT * FROM ({body})"
        self.client.query(insert_sql, location=self.location).result()

    def alter_table_sync_schema(
        self, relation: str, select_sql: str, *, mode: str = "append_new_columns"
    ) -> None:
        """
        Best-effort additive schema sync:
          - infer select schema via dry-run (schema on QueryJob)
          - add missing columns as NULLABLE with inferred type
        """
        if mode not in {"append_new_columns", "sync_all_columns"}:
            return

        body = self._first_select_body(select_sql).strip().rstrip(";\n\t ")
        # Infer target schema from the query (no data read)
        probe_job = self.client.query(
            f"SELECT * FROM ({body}) WHERE 1=0",
            job_config=bigquery.QueryJobConfig(dry_run=False, use_query_cache=False),
            location=self.location,
        )
        probe_job.result()
        select_fields = {f.name: f for f in (probe_job.schema or [])}

        # Existing table schema
        table_ref = f"{self.project}.{self.dataset}.{relation}"
        try:
            tbl = self.client.get_table(table_ref)
        except NotFound:
            return
        existing_cols = {f.name for f in (tbl.schema or [])}

        to_add = [name for name in select_fields if name not in existing_cols]
        if not to_add:
            return

        target = self._qualified_identifier(relation, project=self.project, dataset=self.dataset)
        for col in to_add:
            bf = select_fields[col]
            # Use BigQuery standard SQL type string (e.g., STRING, INT64, BOOL, FLOAT64, …)
            typ = str(bf.field_type) if hasattr(bf, "field_type") else "STRING"
            # Nullable by default
            self.client.query(
                f"ALTER TABLE {target} ADD COLUMN {col} {typ}",
                location=self.location,
            ).result()

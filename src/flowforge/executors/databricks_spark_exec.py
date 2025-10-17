# src/flowforge/executors/databricks_spark_exec.py
from __future__ import annotations

from collections.abc import Iterable
from typing import Any

from pyspark.sql import DataFrame as SDF, SparkSession

from ..core import Node, relation_for
from .base import BaseExecutor


class DatabricksSparkExecutor(BaseExecutor[SDF]):
    """Spark/Databricks executor without pandas: Python models operate on Spark DataFrames."""

    def __init__(self, master: str = "local[*]", app_name: str = "flowforge"):
        self.spark = SparkSession.builder.master(master).appName(app_name).getOrCreate()

    # ---------- Frame hooks (required) ----------
    def _read_relation(self, relation: str, node: Node, deps: Iterable[str]) -> SDF:
        # relation may optionally be "db.table" (via source()/ref())
        return self.spark.table(relation)

    def _materialize_relation(self, relation: str, df: SDF, node: Node) -> None:
        if not self._is_frame(df):
            raise TypeError("Spark model must return a Spark DataFrame")
        # write as a table in Hive/Unity/Delta environments
        df.write.mode("overwrite").saveAsTable(relation)

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

    def _columns_of(self, frame: SDF) -> list[str]:
        return frame.schema.fieldNames()

    def _is_frame(self, obj: Any) -> bool:
        return isinstance(obj, SDF)

    def _frame_name(self) -> str:
        return "Spark"

    # ---- Helpers ----
    @staticmethod
    def _q_ident(value: str | None) -> str:
        if value is None:
            return ""
        return f"`{value.replace('`', '``')}`"

    # ---- SQL hooks ----
    def _format_relation_for_ref(self, name: str) -> str:
        return self._q_ident(relation_for(name))

    def _format_source_reference(
        self, cfg: dict[str, Any], source_name: str, table_name: str
    ) -> str:
        ident = cfg["identifier"]
        db = cfg.get("database")
        return f"{self._q_ident(db)}.{self._q_ident(ident)}" if db else self._q_ident(ident)

    def _create_or_replace_view(self, target_sql: str, select_body: str, node: Node) -> None:
        self.spark.sql(f"CREATE OR REPLACE VIEW {target_sql} AS {select_body}")

    def _create_or_replace_table(self, target_sql: str, select_body: str, node: Node) -> None:
        self.spark.sql(f"CREATE OR REPLACE TABLE {target_sql} AS {select_body}")

    def _create_or_replace_view_from_table(
        self, view_name: str, backing_table: str, node: Node
    ) -> None:
        self.spark.sql(f"CREATE OR REPLACE VIEW `{view_name}` AS SELECT * FROM `{backing_table}`")

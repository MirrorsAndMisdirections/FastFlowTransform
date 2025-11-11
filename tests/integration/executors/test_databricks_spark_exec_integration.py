# tests/integration/executors/test_databricks_spark_exec_integration.py
from __future__ import annotations

from pathlib import Path

import pytest

pyspark = pytest.importorskip("pyspark")

from fastflowtransform.core import Node  # noqa: E402
from fastflowtransform.errors import ModelExecutionError  # noqa: E402
from fastflowtransform.executors.databricks_spark_exec import DatabricksSparkExecutor  # noqa: E402


@pytest.mark.integration
@pytest.mark.databricks_spark
def test_create_table_and_exists(spark_exec: DatabricksSparkExecutor):
    spark_exec.create_table_as("default.it_users", "SELECT 1 AS id, 'x' AS name")
    assert spark_exec.exists_relation("default.it_users")
    assert spark_exec.exists_relation("it_users")


@pytest.mark.integration
@pytest.mark.databricks_spark
def test_incremental_insert_integration(spark_exec: DatabricksSparkExecutor):
    spark_exec.create_table_as("it_inc", "SELECT 1 AS id")
    spark_exec.incremental_insert("it_inc", "SELECT 2 AS id")
    rows = [tuple(r) for r in spark_exec.spark.sql("SELECT * FROM it_inc ORDER BY id").collect()]
    assert rows == [(1,), (2,)]


@pytest.mark.integration
@pytest.mark.databricks_spark
def test_incremental_merge_integration(spark_exec: DatabricksSparkExecutor):
    spark_exec.create_table_as("it_merge", "SELECT 1 AS id, 'old' AS v")
    sql = """
    SELECT * FROM (
        SELECT 1 AS id, 'new' AS v
        UNION ALL
        SELECT 2 AS id, 'other' AS v
    ) s
    """
    spark_exec.incremental_merge("it_merge", sql, unique_key=["id"])
    rows = {(r["id"], r["v"]) for r in spark_exec.spark.sql("SELECT * FROM it_merge").collect()}
    assert rows == {(1, "new"), (2, "other")}


@pytest.mark.integration
@pytest.mark.databricks_spark
def test_alter_table_sync_schema_integration(spark_exec: DatabricksSparkExecutor):
    spark_exec.create_table_as("it_schema", "SELECT 1 AS id")
    spark_exec.alter_table_sync_schema("it_schema", "SELECT 1 AS id, 'x' AS extra")
    cols = {f.name for f in spark_exec.spark.table("it_schema").schema.fields}
    assert {"id", "extra"}.issubset(cols)


@pytest.mark.integration
@pytest.mark.databricks_spark
def test_create_or_replace_table_wraps_error(spark_exec: DatabricksSparkExecutor):
    bad_sql = "SELECT * FROM not_there"
    node = Node(name="bad_node", kind="sql", path=Path("dummy"))
    with pytest.raises(ModelExecutionError):
        spark_exec._create_or_replace_table("default.bad_tbl", bad_sql, node)


@pytest.mark.integration
@pytest.mark.databricks_spark
def test_materialize_relation_real(spark_exec: DatabricksSparkExecutor):
    df = spark_exec.spark.createDataFrame([(1, "x")], ["id", "val"])
    node = Node(name="it_node", kind="python", path=Path("x"))
    spark_exec._materialize_relation("default.it_tbl_mr", df, node)
    rows = [tuple(r) for r in spark_exec.spark.sql("SELECT * FROM default.it_tbl_mr").collect()]
    assert rows == [(1, "x")]


@pytest.mark.integration
@pytest.mark.databricks_spark
def test_create_view_over_table_real(spark_exec: DatabricksSparkExecutor):
    """Create a table and a view over it using simple, backtick-safe names."""
    # 1) create a table WITHOUT a dot in the name
    spark_exec.create_table_as("src_tbl", "SELECT 1 AS id")

    # 2) create a view over it
    spark_exec._create_view_over_table(
        "v_src_tbl",
        "src_tbl",  # <- no dot, so backticks are fine
        Node(name="x", kind="sql", path=Path(".")),
    )

    rows = [tuple(r) for r in spark_exec.spark.sql("SELECT * FROM v_src_tbl").collect()]
    assert rows == [(1,)]

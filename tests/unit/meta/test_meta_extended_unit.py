from __future__ import annotations

from types import SimpleNamespace
from unittest.mock import MagicMock

from fastflowtransform.meta import (
    get_meta,
    relation_exists,
    upsert_meta,
)


class _FakeSFRow:
    def __init__(self, data: dict[str, object]):
        self._data = data

    def asDict(self):
        return self._data

    def __getitem__(self, idx: int) -> object:
        return list(self._data.values())[idx]


def _sf_executor():
    session = MagicMock()
    session.sql.return_value.collect.return_value = []
    ex = SimpleNamespace(session=session, database="DBX", schema="SCX")
    return ex, session


def _spark_executor():
    spark = MagicMock()
    spark.sql.return_value.collect.return_value = []
    spark.catalog.tableExists.return_value = True
    ex = SimpleNamespace(spark=spark, database="analytics", spark_table_format=None)
    return ex, spark


def test_snowflake_meta_calls_session_sql():
    ex, session = _sf_executor()
    upsert_meta(ex, "node", '"DBX"."SCX"."TBL"', "fp1", "snowflake_snowpark")
    sql_calls = [call.args[0] for call in session.sql.call_args_list]
    assert any("create table if not exists" in stmt.lower() for stmt in sql_calls)
    assert any("delete from" in stmt.lower() for stmt in sql_calls)
    assert any("insert into" in stmt.lower() for stmt in sql_calls)


def test_snowflake_get_meta_returns_tuple():
    ex, session = _sf_executor()
    row = _FakeSFRow({"FP": "abc", "RELATION": "r", "BUILT_AT": "ts", "ENGINE": "sf"})
    session.sql.return_value.collect.return_value = [row]
    out = get_meta(ex, "node")
    assert out == ("abc", "r", "ts", "sf")


def test_spark_meta_uses_merge_and_get_meta_reads_rows():
    ex, spark = _spark_executor()
    upsert_meta(ex, "node", "analytics.table", "fp2", "databricks_spark")
    merge_stmt = spark.sql.call_args_list[-1].args[0]
    assert "MERGE INTO" in merge_stmt
    spark.sql.return_value.collect.return_value = [
        {"fp": "fp2", "relation": "analytics.table", "built_at": "ts", "engine": "databricks_spark"}
    ]
    assert get_meta(ex, "node") == (
        "fp2",
        "analytics.table",
        "ts",
        "databricks_spark",
    )


def test_spark_relation_exists_uses_catalog(monkeypatch):
    ex, spark = _spark_executor()
    assert relation_exists(ex, "tbl_name") is True
    spark.catalog.tableExists.assert_called_with("analytics", "tbl_name")

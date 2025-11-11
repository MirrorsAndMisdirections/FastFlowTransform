# tests/unit/executors/test_databricks_spark_exec_unit.py
from __future__ import annotations

from pathlib import Path
from types import SimpleNamespace
from unittest.mock import ANY, MagicMock, patch

import pytest

from fastflowtransform.core import REGISTRY, Node
from fastflowtransform.executors import databricks_spark_exec as mod
from fastflowtransform.executors.databricks_spark_exec import (
    _SparkConnShim,
    _split_db_table,
)


@pytest.mark.unit
@pytest.mark.databricks_spark
def test_split_db_table_unit():
    assert _split_db_table("db.tbl") == ("db", "tbl")
    assert _split_db_table("`db`.`tbl`") == ("db`", "`tbl")
    assert _split_db_table("tbl") == (None, "tbl")


@pytest.mark.unit
@pytest.mark.databricks_spark
def test_q_ident_unit(exec_minimal):
    assert exec_minimal._q_ident("foo") == "`foo`"
    assert exec_minimal._q_ident("foo`bar") == "`foo``bar`"
    assert exec_minimal._q_ident(None) == ""


@pytest.mark.unit
@pytest.mark.databricks_spark
def test_validate_required_single_df_unit(exec_minimal):
    # Fake Spark DF
    fake_df = SimpleNamespace(schema=SimpleNamespace(fieldNames=lambda: ["id", "email"]))
    # Call in "multi-input" shape so the executor treats it as a dict
    exec_minimal._validate_required(
        "model_x",
        {"users": fake_df},
        {"users": {"id", "email"}},
    )


@pytest.mark.unit
@pytest.mark.databricks_spark
def test_validate_required_single_df_raises_unit(exec_minimal):
    fake_df = SimpleNamespace(schema=SimpleNamespace(fieldNames=lambda: ["id"]))
    with pytest.raises(ValueError):
        exec_minimal._validate_required(
            "model_x",
            {"users": fake_df},
            {"users": {"id", "email"}},
        )


@pytest.mark.unit
@pytest.mark.databricks_spark
def test_validate_required_multi_dep_unit(exec_minimal):
    fake_users = SimpleNamespace(schema=SimpleNamespace(fieldNames=lambda: ["id", "email"]))
    fake_orders = SimpleNamespace(
        schema=SimpleNamespace(fieldNames=lambda: ["order_id", "user_id"])
    )
    inputs = {"users": fake_users, "orders": fake_orders}
    exec_minimal._validate_required(
        "join_model",
        inputs,
        {
            "users": {"id", "email"},
            "orders": {"order_id", "user_id"},
        },
    )


@pytest.mark.unit
@pytest.mark.databricks_spark
def test_format_source_reference_classic_unit(exec_minimal):
    cfg = {"identifier": "seed_users", "schema": "staging", "catalog": "spark_catalog"}
    ref = exec_minimal._format_source_reference(cfg, "raw", "users")
    assert "spark_catalog" in ref
    assert "staging" in ref
    assert "seed_users" in ref


@pytest.mark.unit
@pytest.mark.databricks_spark
def test_format_source_reference_path_based_unit(exec_minimal):
    # wir patchen spark.read.format(...) Kette
    fake_df = MagicMock()
    fake_spark = exec_minimal.spark
    fake_spark.read.format.return_value = MagicMock(
        options=MagicMock(return_value=MagicMock(load=MagicMock(return_value=fake_df)))
    )
    fake_df.createOrReplaceTempView = MagicMock()

    cfg = {
        "location": "/tmp/somewhere.parquet",
        "format": "parquet",
        "identifier": "my_alias",
        "options": {"mergeSchema": "true"},
    }
    ref = exec_minimal._format_source_reference(cfg, "raw", "tbl")

    assert ref == "`my_alias`"
    fake_spark.read.format.assert_called_with("parquet")
    # und wir sollten ein TempView angelegt haben
    fake_df.createOrReplaceTempView.assert_called_with("my_alias")


@pytest.mark.unit
@pytest.mark.databricks_spark
def test__materialize_relation_uses_save_table_when_no_path(exec_minimal):
    """Executor should call internal table-saving logic when no storage path is configured."""
    df = MagicMock()
    node = Node(name="dummy", kind="python", path=Path("."))

    # force _storage_meta to return empty dict so we hit _save_df_as_table
    exec_minimal._storage_meta = MagicMock(return_value={})
    exec_minimal._save_df_as_table = MagicMock()

    exec_minimal._materialize_relation("default.unit_tbl", df, node)

    exec_minimal._save_df_as_table.assert_called_once()
    exec_minimal._save_df_as_table.assert_called_with("default.unit_tbl", df, storage={})


@pytest.mark.unit
@pytest.mark.databricks_spark
def test__materialize_relation_uses_write_to_storage_path(exec_minimal, tmp_path):
    """Executor should delegate to _write_to_storage_path when storage meta has a path."""
    df = MagicMock()
    node = Node(name="dummy", kind="python", path=Path("."))

    exec_minimal._storage_meta = MagicMock(
        return_value={"path": str(tmp_path), "format": "parquet"}
    )
    exec_minimal._write_to_storage_path = MagicMock()

    exec_minimal._materialize_relation("default.unit_tbl", df, node)

    exec_minimal._write_to_storage_path.assert_called_once()
    exec_minimal._write_to_storage_path.assert_called_with(
        "default.unit_tbl",
        df,
        {"path": str(tmp_path), "format": "parquet"},
    )


@pytest.mark.unit
@pytest.mark.databricks_spark
def test__write_to_storage_path_calls_storage_helper(exec_minimal, monkeypatch, tmp_path):
    """_write_to_storage_path should just be a thin adapter to storage.spark_write_to_path."""
    called = {}

    def fake_write(spark, identifier, df, storage, default_format=None, default_options=None):
        called["spark"] = spark
        called["identifier"] = identifier
        called["storage"] = storage
        called["default_format"] = default_format
        called["default_options"] = default_options

    monkeypatch.setattr(mod.storage, "spark_write_to_path", fake_write)

    df = MagicMock()
    storage_meta = {"path": str(tmp_path), "format": "parquet"}
    exec_minimal.spark_table_format = "parquet"
    exec_minimal.spark_table_options = {"mergeSchema": "true"}

    exec_minimal._write_to_storage_path("default.tbl_x", df, storage_meta)

    assert called["identifier"] == "default.tbl_x"
    assert called["storage"] == storage_meta
    assert called["default_format"] == "parquet"
    assert called["default_options"] == {"mergeSchema": "true"}


@pytest.mark.unit
@pytest.mark.databricks_spark
def test__create_view_over_table_executes_expected_sql(exec_minimal):
    """_create_view_over_table should emit a simple CREATE OR REPLACE VIEW SELECT * statement."""
    exec_minimal.spark.sql = MagicMock()

    exec_minimal._create_view_over_table(
        "v_users", "t_users", Node(name="n", kind="sql", path=Path("."))
    )

    exec_minimal.spark.sql.assert_called_once()
    sql = exec_minimal.spark.sql.call_args[0][0]
    assert "CREATE OR REPLACE VIEW `v_users` AS SELECT * FROM `t_users`" in sql


@pytest.mark.unit
@pytest.mark.databricks_spark
def test_on_node_built_calls_meta_helpers(exec_minimal, monkeypatch):
    """on_node_built should best-effort call ensure_meta_table and upsert_meta."""
    ensure_called = {}
    upsert_called = {}

    def fake_ensure(executor):
        ensure_called["ok"] = True

    def fake_upsert(executor, node_name, relation, fingerprint, engine):
        upsert_called["args"] = (node_name, relation, fingerprint, engine)

    monkeypatch.setattr(mod, "ensure_meta_table", fake_ensure)
    monkeypatch.setattr(mod, "upsert_meta", fake_upsert)

    node = Node(name="demo_node", kind="sql", path=Path("x"))
    exec_minimal.on_node_built(node, "demo_tbl", "abc123")

    assert ensure_called.get("ok") is True
    assert upsert_called["args"] == ("demo_node", "demo_tbl", "abc123", "databricks_spark")


@pytest.mark.unit
@pytest.mark.databricks_spark
def test_spark_conn_shim_execute_runs_select(monkeypatch):
    """_SparkConnShim.execute should return rows collected from spark.sql."""
    fake_spark = MagicMock()
    fake_spark.sql.return_value.collect.return_value = [("a",), ("b",)]
    shim = _SparkConnShim(fake_spark)

    res = shim.execute("SELECT 'a'")
    assert res.fetchall() == [("a",), ("b",)]
    assert res.fetchone() == ("a",)


@pytest.mark.unit
@pytest.mark.databricks_spark
def test_read_relation_uses_spark_table(exec_minimal):
    exec_minimal.spark.table.return_value = "DF"
    out = exec_minimal._read_relation("users", Node(name="n", kind="sql", path=Path(".")), [])
    exec_minimal.spark.table.assert_called_with("users")
    assert out == "DF"


@pytest.mark.unit
@pytest.mark.databricks_spark
def test_validate_required_no_requires_is_noop(exec_minimal):
    # should not raise
    exec_minimal._validate_required("node_x", inputs=MagicMock(), requires={})


@pytest.mark.unit
@pytest.mark.databricks_spark
def test_materialize_relation_rejects_non_frame(exec_minimal, monkeypatch):
    # für diesen Test brauchen wir das echte Verhalten
    monkeypatch.setattr(exec_minimal, "_is_frame", lambda obj: False)
    node = Node(name="x", kind="python", path=Path("."))
    with pytest.raises(TypeError, match="Spark model must return a Spark DataFrame"):
        exec_minimal._materialize_relation("tbl", object(), node)


@pytest.mark.unit
@pytest.mark.databricks_spark
def test_exists_relation_qualified(exec_minimal):
    exec_minimal.spark.catalog._jcatalog.tableExists.return_value = True
    assert exec_minimal.exists_relation("default.my_tbl") is True
    exec_minimal.spark.catalog._jcatalog.tableExists.assert_called_with("default", "my_tbl")


@pytest.mark.unit
@pytest.mark.databricks_spark
def test_exists_relation_unqualified(exec_minimal):
    exec_minimal.spark.catalog.tableExists.return_value = False
    assert exec_minimal.exists_relation("my_tbl") is False
    exec_minimal.spark.catalog.tableExists.assert_called_with("my_tbl")


@pytest.mark.unit
@pytest.mark.databricks_spark
def test_init_makes_relative_warehouse_absolute(exec_factory):
    ex, _, _ = exec_factory(warehouse_dir="rel_dir")
    assert ex.warehouse_dir.is_absolute()


@pytest.mark.unit
@pytest.mark.databricks_spark
def test_init_with_catalog_sets_config(exec_factory):
    _, fake_builder, _ = exec_factory(catalog="hive_metastore")
    fake_builder.config.assert_any_call("spark.sql.catalog.spark_catalog", "hive_metastore")


@pytest.mark.unit
@pytest.mark.databricks_spark
def test_init_with_extra_conf(exec_factory):
    _, fake_builder, _ = exec_factory(extra_conf={"spark.foo": "1", "spark.bar": "2"})
    fake_builder.config.assert_any_call("spark.foo", "1")
    fake_builder.config.assert_any_call("spark.bar", "2")


@pytest.mark.unit
@pytest.mark.databricks_spark
def test_init_with_hive_support(exec_factory):
    _, fake_builder, _ = exec_factory(use_hive_metastore=True)
    fake_builder.config.assert_any_call("spark.sql.catalogImplementation", "hive")
    fake_builder.enableHiveSupport.assert_called_once()


@pytest.mark.unit
@pytest.mark.databricks_spark
def test_init_with_table_options(exec_factory):
    ex, _, _ = exec_factory(table_options={"mergeSchema": True})
    assert ex.spark_table_options == {"mergeSchema": "True"}


@pytest.mark.unit
@pytest.mark.databricks_spark
def test_storage_meta_prefers_node_storage(exec_minimal):
    node = Node(
        name="users.ff", kind="sql", path=Path("x"), meta={"storage": {"path": "/tmp/users"}}
    )
    meta = exec_minimal._storage_meta(node, "users")
    assert meta == {"path": "/tmp/users"}


@pytest.mark.unit
@pytest.mark.databricks_spark
def test_storage_meta_uses_global_lookup_when_node_empty(exec_minimal):
    with patch("fastflowtransform.executors.databricks_spark_exec.storage.get_model_storage") as gm:
        gm.return_value = {"path": "/tmp/global"}
        meta = exec_minimal._storage_meta(None, "some_relation")
    assert meta == {"path": "/tmp/global"}
    gm.assert_called()


@pytest.mark.unit
@pytest.mark.databricks_spark
def test_storage_meta_falls_back_to_registry_scan(exec_minimal, monkeypatch):
    # 1) Fake-Node im Registry, der Storage hat
    reg_node = Node(
        name="orders.ff",
        kind="sql",
        path=Path("x"),
        meta={"storage": {"path": "/tmp/orders"}},
    )
    REGISTRY.nodes = {"orders.ff": reg_node}

    # 2) relation_for(...) so patchen, dass es "orders" ergibt
    with patch("fastflowtransform.executors.databricks_spark_exec.relation_for") as rel_for:
        rel_for.return_value = "orders"

        meta = exec_minimal._storage_meta(None, "orders")

    assert meta == {"path": "/tmp/orders"}


@pytest.mark.unit
@pytest.mark.databricks_spark
def test_storage_meta_registry_scan_then_global(exec_minimal, monkeypatch):
    reg_node = Node(
        name="orders.ff",
        kind="sql",
        path=Path("x"),
        meta={},
    )
    REGISTRY.nodes = {"orders.ff": reg_node}

    with (
        patch("fastflowtransform.executors.databricks_spark_exec.relation_for") as rel_for,
        patch("fastflowtransform.executors.databricks_spark_exec.storage.get_model_storage") as gm,
    ):
        rel_for.return_value = "orders"
        gm.return_value = {"path": "/tmp/from_global"}

        meta = exec_minimal._storage_meta(None, "orders")

    assert meta == {"path": "/tmp/from_global"}


@pytest.mark.unit
@pytest.mark.databricks_spark
def test_format_relation_for_ref(exec_minimal):
    with patch("fastflowtransform.executors.databricks_spark_exec.relation_for") as rel_for:
        rel_for.return_value = "real_table"
        out = exec_minimal._format_relation_for_ref("users.ff")
    assert out == "`real_table`"


@pytest.mark.unit
@pytest.mark.databricks_spark
def test_format_source_reference_location_without_format_raises(exec_minimal):
    cfg = {"location": "/tmp/data", "identifier": "x"}  # no "format"
    with pytest.raises(KeyError, match="requires 'format'"):
        exec_minimal._format_source_reference(cfg, "raw", "events")


@pytest.mark.unit
@pytest.mark.databricks_spark
def test_save_df_as_table_respects_storage_path(exec_minimal):
    df = MagicMock()
    exec_minimal._write_to_storage_path = MagicMock()

    exec_minimal._save_df_as_table(
        "my_tbl",
        df,
        storage={"path": "/tmp/somewhere"},
    )

    exec_minimal._write_to_storage_path.assert_called_once()


@pytest.mark.unit
@pytest.mark.databricks_spark
def test_create_or_replace_table_happy_path_calls_save(exec_minimal):
    # spark.sql soll NICHT werfen, sondern ein DF liefern
    fake_df = MagicMock()
    exec_minimal.spark.sql.return_value = fake_df

    # save beobachten
    exec_minimal._save_df_as_table = MagicMock()

    node = Node(name="my_model", kind="sql", path=Path("."))

    exec_minimal._create_or_replace_table(
        "target_tbl",
        "SELECT 1 AS id",
        node,
    )

    exec_minimal.spark.sql.assert_called_with("SELECT 1 AS id")
    exec_minimal._save_df_as_table.assert_called_once_with("target_tbl", fake_df, storage=ANY)

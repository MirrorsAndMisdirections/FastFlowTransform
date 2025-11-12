# tests/unit/test_seeding_unit.py
from __future__ import annotations

import textwrap
from pathlib import Path
from types import SimpleNamespace
from typing import Any
from unittest.mock import MagicMock

import pandas as pd
import pytest

from fastflowtransform import seeding, storage

# ---------------------------------------------------------------------------
# File I/O helpers
# ---------------------------------------------------------------------------


@pytest.mark.unit
def test_read_seed_file_csv(tmp_path: Path):
    p = tmp_path / "users.csv"
    p.write_text("id,name\n1,A\n2,B\n", encoding="utf-8")

    df = seeding._read_seed_file(p)
    assert list(df.columns) == ["id", "name"]
    expected_row_count = 2
    assert len(df) == expected_row_count


@pytest.mark.unit
def test_read_seed_file_unsupported(tmp_path: Path):
    p = tmp_path / "users.txt"
    p.write_text("nope", encoding="utf-8")
    with pytest.raises(ValueError):
        seeding._read_seed_file(p)


@pytest.mark.unit
def test_apply_schema_happy():
    df = pd.DataFrame({"id": [1, 2], "name": ["a", "b"], "age": [10, 20]})
    cfg_raw = {
        "dtypes": {
            "users": {
                "name": "string",
                "age": "int64",
            }
        }
    }
    schema_cfg = seeding.SeedsSchemaConfig.model_validate(cfg_raw)

    out = seeding._apply_schema(df, "users", schema_cfg)
    assert str(out.dtypes["name"]).startswith("string")
    assert str(out.dtypes["age"]) in ("int64", "Int64")


@pytest.mark.unit
def test_apply_schema_ignores_missing_table_key():
    df = pd.DataFrame({"id": [1]})
    cfg_raw = {"dtypes": {"users": {"id": "int64"}}}
    schema_cfg = seeding.SeedsSchemaConfig.model_validate(cfg_raw)
    out = seeding._apply_schema(df, "other", schema_cfg)
    assert out.equals(df)


@pytest.mark.unit
def test_apply_schema_soft_fails_on_bad_cast():
    df = pd.DataFrame({"id": ["x"]})
    # force bad cast
    cfg_raw = {"dtypes": {"t": {"id": "int64"}}}
    schema_cfg = seeding.SeedsSchemaConfig.model_validate(cfg_raw)
    out = seeding._apply_schema(df, "t", schema_cfg)
    assert len(out) == 1


# ---------------------------------------------------------------------------
# Identifier helpers
# ---------------------------------------------------------------------------


@pytest.mark.unit
def test_dq_quotes_and_escapes():
    assert seeding._dq('a"b') == '"a""b"'
    assert seeding._dq("tbl") == '"tbl"'


@pytest.mark.unit
def test_is_qualified():
    assert seeding._is_qualified("raw.users") is True
    assert seeding._is_qualified("users") is False


@pytest.mark.unit
def test_qualify_unqualified_with_schema():
    out = seeding._qualify("users", "raw")
    assert out == '"raw"."users"'


@pytest.mark.unit
def test_qualify_with_schema_and_catalog():
    out = seeding._qualify("users", "raw", "cat")
    assert out == '"cat"."raw"."users"'


@pytest.mark.unit
def test_qualify_with_catalog_only():
    out = seeding._qualify("users", None, "cat")
    assert out == '"cat"."users"'


@pytest.mark.unit
def test_qualify_already_qualified_preserves_parts():
    out = seeding._qualify("raw.users", None)
    assert out == '"raw"."users"'


# ---------------------------------------------------------------------------
# Spark warehouse helpers
# ---------------------------------------------------------------------------


@pytest.mark.unit
@pytest.mark.databricks_spark
def test_spark_warehouse_base_local(tmp_path: Path):
    fake_spark = SimpleNamespace(
        conf=SimpleNamespace(get=lambda key, default=None: str(tmp_path / "wh"))
    )
    base = seeding._spark_warehouse_base(fake_spark)
    assert base == (tmp_path / "wh")


@pytest.mark.unit
@pytest.mark.databricks_spark
def test_spark_warehouse_base_remote_scheme():
    fake_spark = SimpleNamespace(conf=SimpleNamespace(get=lambda *_: "s3://bucket/warehouse"))
    assert seeding._spark_warehouse_base(fake_spark) is None


@pytest.mark.unit
@pytest.mark.databricks_spark
def test_spark_table_location_strips_catalog(tmp_path: Path):
    # warehouse dir is local
    fake_spark = SimpleNamespace(conf=SimpleNamespace(get=lambda *_: str(tmp_path / "wh")))
    parts = ["spark_catalog", "default", "mytable"]
    loc = seeding._spark_table_location(parts, fake_spark)
    # should resolve to <wh>/default.db/mytable
    assert loc == tmp_path / "wh" / "default.db" / "mytable"


# ---------------------------------------------------------------------------
# Pretty helpers
# ---------------------------------------------------------------------------


@pytest.mark.unit
def test_human_int_formats_with_spaces():
    assert seeding._human_int(1234567) == "1 234 567"
    assert seeding._human_int(0) == "0"


@pytest.mark.unit
def test_human_bytes_formats_reasonably():
    assert seeding._human_bytes(512) == "512 B"
    # just smoke tests
    assert "KB" in seeding._human_bytes(2_000)
    assert "MB" in seeding._human_bytes(2_000_000)


@pytest.mark.unit
def test_echo_seed_line(monkeypatch):
    lines: list[str] = []

    def fake_echo(msg: str) -> None:
        lines.append(msg)

    monkeypatch.setattr(seeding, "echo", fake_echo)

    seeding._echo_seed_line(
        full_name="raw.users",
        rows=1234,
        cols=5,
        engine="duckdb",
        ms=42,
        created_schema=True,
        extra="reset location",
    )

    assert len(lines) == 1
    out = lines[0]
    assert "raw.users" in out
    assert "1 234×5" in out  # noqa RUF001
    assert "[duckdb]" in out
    assert "(+schema)" in out
    assert "reset location" in out


# ---------------------------------------------------------------------------
# Target resolution
# ---------------------------------------------------------------------------


@pytest.mark.unit
def test_engine_name_from_executor_spark():
    ex = SimpleNamespace(spark=object())
    assert seeding._engine_name_from_executor(ex) == "databricks_spark"


@pytest.mark.unit
def test_engine_name_from_executor_sqlalchemy_like():
    eng = SimpleNamespace(dialect=SimpleNamespace(name="postgres"))
    ex = SimpleNamespace(engine=eng)
    assert seeding._engine_name_from_executor(ex) == "postgres"


@pytest.mark.unit
def test_engine_name_from_executor_duckdb_like():
    ex = SimpleNamespace(con=object())
    assert seeding._engine_name_from_executor(ex) == "duckdb"


@pytest.mark.unit
def test_seed_id_simple(tmp_path: Path):
    seeds_dir = tmp_path / "seeds"
    seeds_dir.mkdir()
    p = seeds_dir / "users.csv"
    p.write_text("id\n1\n", encoding="utf-8")
    assert seeding._seed_id(seeds_dir, p) == "users"


@pytest.mark.unit
def test_seed_id_nested(tmp_path: Path):
    seeds_dir = tmp_path / "seeds"
    (seeds_dir / "raw").mkdir(parents=True)
    p = seeds_dir / "raw" / "users.csv"
    p.write_text("id\n1\n", encoding="utf-8")
    assert seeding._seed_id(seeds_dir, p) == "raw/users"


@pytest.mark.unit
def test_resolve_schema_and_table_by_cfg_priority_engine_override():
    schema_cfg_raw = {
        "targets": {
            "raw/users": {
                "schema": "raw",
                "table": "users_final",
                "schema_by_engine": {
                    "postgres": "pg_raw",
                    "duckdb": "main",
                },
            }
        }
    }
    # executor pretending to be postgres
    ex = SimpleNamespace(engine=SimpleNamespace(dialect=SimpleNamespace(name="postgres")))

    schema_cfg = seeding.SeedsSchemaConfig.model_validate(schema_cfg_raw)

    schema, table = seeding._resolve_schema_and_table_by_cfg(
        seed_id="raw/users",
        stem="users",
        schema_cfg=schema_cfg,
        executor=ex,
        default_schema="public",
    )

    assert schema == "pg_raw"
    assert table == "users_final"


@pytest.mark.unit
def test_resolve_schema_and_table_falls_back_to_default_schema():
    ex = SimpleNamespace(engine=None, con=None)
    schema, table = seeding._resolve_schema_and_table_by_cfg(
        seed_id="raw/users",
        stem="users",
        schema_cfg=None,
        executor=ex,
        default_schema="public",
    )
    assert schema == "public"
    assert table == "users"


# ---------------------------------------------------------------------------
# Handlers: DuckDB
# ---------------------------------------------------------------------------


@pytest.mark.unit
@pytest.mark.duckdb
def test_handle_duckdb_returns_false_for_non_duckdb_conn():
    executor = SimpleNamespace(
        con=SimpleNamespace(register=lambda *a, **k: None, execute=lambda *a, **k: None)
    )
    df = pd.DataFrame({"id": [1, 2]})

    handled = seeding._handle_duckdb("users", df, executor, schema="raw")

    assert handled is False


@pytest.mark.unit
def test_handle_duckdb_returns_false_if_no_con():
    executor = SimpleNamespace()
    df = pd.DataFrame({"id": [1]})
    handled = seeding._handle_duckdb("users", df, executor, schema=None)
    assert handled is False


# ---------------------------------------------------------------------------
# Handlers: SQLAlchemy
# ---------------------------------------------------------------------------


@pytest.mark.unit
@pytest.mark.postgres
def test_handle_sqlalchemy_happy(monkeypatch):
    calls = {}

    class FakeEngine:
        __module__ = "sqlalchemy.engine"  # to trigger detection
        dialect = SimpleNamespace(name="postgres")

    class FakeDF(pd.DataFrame):
        def to_sql(self, name, eng, if_exists, index, schema, method):
            calls["name"] = name
            calls["schema"] = schema
            calls["if_exists"] = if_exists

    df = FakeDF({"id": [1, 2]})
    executor = SimpleNamespace(engine=FakeEngine())

    handled = seeding._handle_sqlalchemy("seed_tbl", df, executor, schema="raw")
    assert handled is True
    assert calls["name"] == "seed_tbl"
    assert calls["schema"] == "raw"
    assert calls["if_exists"] == "replace"


@pytest.mark.unit
def test_handle_sqlalchemy_returns_false_if_no_engine():
    df = pd.DataFrame({"id": [1]})
    executor = SimpleNamespace()
    assert seeding._handle_sqlalchemy("t", df, executor, None) is False


@pytest.mark.unit
def test_handle_sqlalchemy_returns_false_if_engine_not_sqlalchemy():
    df = pd.DataFrame({"id": [1]})
    executor = SimpleNamespace(engine=SimpleNamespace(__module__="not.sqlalchemy"))
    assert seeding._handle_sqlalchemy("t", df, executor, None) is False


# ---------------------------------------------------------------------------
# Handlers: Spark
# ---------------------------------------------------------------------------


@pytest.mark.unit
@pytest.mark.databricks_spark
def test_handle_spark_happy_default_table(tmp_path: Path, monkeypatch):
    # fake spark with local warehouse
    fake_spark = MagicMock()
    fake_spark.conf.get.return_value = str(tmp_path / "wh")

    # DataFrame path
    fake_sdf = MagicMock()
    fake_spark.createDataFrame.return_value = fake_sdf

    # writer chain
    writer = MagicMock()
    fake_sdf.write.mode.return_value = writer
    writer.format.return_value = writer
    writer.options.return_value = writer

    executor = SimpleNamespace(
        spark=fake_spark,
        spark_table_format="delta",
        spark_table_options={"mergeSchema": "true"},
    )

    df = pd.DataFrame({"id": [1]})
    handled = seeding._handle_spark("default.seed_tbl", df, executor, schema=None)

    assert handled is True
    # drop table was attempted
    fake_spark.sql.assert_any_call("DROP TABLE IF EXISTS `default`.`seed_tbl`")
    # writer.saveAsTable called with identifier
    writer.saveAsTable.assert_called_once_with("default.seed_tbl")


@pytest.mark.unit
@pytest.mark.databricks_spark
def test_handle_spark_uses_seed_storage(monkeypatch):
    # 1) Seed-Storage Override setzen
    storage.set_seed_storage(
        {"raw.users": {"path": "/tmp/custom", "format": "parquet", "options": {"x": "1"}}}
    )

    # 2) Fake Spark + DataFrame
    fake_spark = MagicMock()
    fake_sdf = MagicMock()
    fake_spark.createDataFrame.return_value = fake_sdf

    writer = MagicMock()
    fake_sdf.write.mode.return_value = writer
    writer.format.return_value = writer
    writer.options.return_value = writer

    # 3) spark_write_to_path stubben, damit kein echtes FS angefasst wird
    called: dict[str, Any] = {}

    def _fake_write_to_path(
        spark, identifier, df, *, storage: dict, default_format, default_options
    ):
        called["spark"] = spark
        called["identifier"] = identifier
        called["df"] = df
        called["storage"] = storage
        called["default_format"] = default_format
        called["default_options"] = default_options

    monkeypatch.setattr(seeding.storage, "spark_write_to_path", _fake_write_to_path)

    # 4) Executor-Stub
    executor = SimpleNamespace(
        spark=fake_spark,
        spark_table_format=None,
        spark_table_options=None,
    )

    df = pd.DataFrame({"id": [1]})

    # 5) Aufruf
    handled = seeding._handle_spark("raw.users", df, executor, schema=None)
    assert handled is True

    # 6) Asserts
    fake_spark.createDataFrame.assert_called_once_with(df)
    assert called["spark"] is fake_spark
    assert called["identifier"] == "raw.users"
    assert called["storage"]["path"] == "/tmp/custom"
    assert called["storage"]["format"] == "parquet"


# ---------------------------------------------------------------------------
# Dispatcher
# ---------------------------------------------------------------------------


@pytest.mark.unit
def test_materialize_seed_tries_all_and_raises(monkeypatch):
    df = pd.DataFrame({"id": [1]})
    # executor without con/engine/spark
    executor = SimpleNamespace()

    with pytest.raises(RuntimeError) as exc:
        seeding.materialize_seed("t", df, executor, schema=None)
    assert "No compatible executor" in str(exc.value)


@pytest.mark.unit
@pytest.mark.duckdb
def test_materialize_seed_stops_at_first_handler(monkeypatch):
    df = pd.DataFrame({"id": [1]})

    # first handler claims success
    def h1(table, df, ex, schema):
        return True

    # second handler should not be called
    called = {"h2": False}

    def h2(table, df, ex, schema):
        called["h2"] = True
        return True

    monkeypatch.setattr(seeding, "_HANDLERS", (h1, h2))

    seeding.materialize_seed("t", df, SimpleNamespace(), schema=None)
    assert called["h2"] is False


# ---------------------------------------------------------------------------
# seed_project
# ---------------------------------------------------------------------------


@pytest.mark.unit
@pytest.mark.duckdb
def test_seed_project_happy_duckdb(tmp_path: Path, monkeypatch):
    # project structure
    seeds_dir = tmp_path / "seeds"
    seeds_dir.mkdir()
    (seeds_dir / "raw").mkdir()
    (seeds_dir / "raw" / "users.csv").write_text("id,name\n1,A\n", encoding="utf-8")

    # fake duckdb-like executor
    exec_calls = []

    class FakeCon:
        def register(self, name, df):
            exec_calls.append(("register", name))

        def execute(self, sql):
            exec_calls.append(("execute", sql))

        def unregister(self, name):
            exec_calls.append(("unregister", name))

    executor = SimpleNamespace(con=FakeCon(), schema="public")

    # IMPORTANT: in environments where duckdb is installed, _handle_duckdb()
    # does an isinstance(...) against the real DuckDB connection type.
    # That would make our FakeCon fail. So we just force the handler to succeed.
    def fake_handle_duckdb(table, df, ex, schema):
        # simulate the real duckdb handler a bit
        full_name = seeding._qualify(table, schema)
        ex.con.register("_tmp", df)
        ex.con.execute(f'create or replace table {full_name} as select * from "_tmp"')
        ex.con.unregister("_tmp")
        return True

    handlers = tuple(seeding._HANDLERS)
    monkeypatch.setattr(
        seeding,
        "_HANDLERS",
        (fake_handle_duckdb, *handlers[1:]),
    )

    count = seeding.seed_project(tmp_path, executor, default_schema=None)
    assert count == 1
    # we should have a create or replace in there
    assert any("create or replace table" in sql for (op, sql) in exec_calls if op == "execute")


@pytest.mark.unit
def test_seed_project_no_seeds_dir(tmp_path: Path):
    executor = SimpleNamespace()
    count = seeding.seed_project(tmp_path, executor, default_schema=None)
    assert count == 0


@pytest.mark.unit
def test_seed_project_ambiguous_stems_raises(tmp_path: Path):
    seeds_dir = tmp_path / "seeds"
    (seeds_dir / "a").mkdir(parents=True)
    (seeds_dir / "b").mkdir(parents=True)
    (seeds_dir / "a" / "users.csv").write_text("id\n1\n", encoding="utf-8")
    (seeds_dir / "b" / "users.csv").write_text("id\n2\n", encoding="utf-8")

    # schema.yml that uses bare "users"
    (seeds_dir / "schema.yml").write_text(
        textwrap.dedent(
            """
            targets:
              users:
                schema: raw
            """
        ),
        encoding="utf-8",
    )

    executor = SimpleNamespace(schema="public")

    with pytest.raises(ValueError) as exc:
        seeding.seed_project(tmp_path, executor, default_schema=None)

    assert "appears multiple times" in str(exc.value)
    assert "Please configure using the path-based seed ID" in str(exc.value)

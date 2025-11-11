# tests/unit/test_storage_unit.py
from __future__ import annotations

from pathlib import Path
from unittest.mock import MagicMock

import pytest

from fastflowtransform import storage


@pytest.fixture(autouse=True)
def reset_storage():
    """Reset global storage registry between tests."""
    storage.set_model_storage({})
    storage.set_seed_storage({})
    yield
    storage.set_model_storage({})
    storage.set_seed_storage({})


# ---------------------------------------------------------------------------
# _sanitize_key
# ---------------------------------------------------------------------------


@pytest.mark.unit
def test_sanitize_key_strips_quotes_and_spaces():
    assert storage._sanitize_key("  `foo.bar`  ") == "foo.bar"
    assert storage._sanitize_key(' "foo" ') == "foo"
    assert storage._sanitize_key("foo") == "foo"


# ---------------------------------------------------------------------------
# normalize_storage_map
# ---------------------------------------------------------------------------


@pytest.mark.unit
def test_normalize_storage_map_resolves_relative_paths(tmp_path: Path):
    raw = {
        "m1": {
            "path": "data/out",
            "format": "parquet",
            "options": {"k": "v"},
        },
        # invalid entry → should be skipped
        "bad": "not-a-mapping",
    }

    norm = storage.normalize_storage_map(raw, project_dir=tmp_path)

    assert "m1" in norm
    m1 = norm["m1"]
    # path must be absolute and under project_dir
    assert Path(m1["path"]).is_absolute()
    assert str(tmp_path) in m1["path"]
    assert m1["format"] == "parquet"
    assert m1["options"] == {"k": "v"}

    # "bad" must be ignored
    assert "bad" not in norm


@pytest.mark.unit
def test_normalize_storage_map_empty_input(tmp_path: Path):
    assert storage.normalize_storage_map(None, project_dir=tmp_path) == {}
    assert storage.normalize_storage_map({}, project_dir=tmp_path) == {}


@pytest.mark.unit
def test_normalize_storage_map_keeps_absolute_path(tmp_path: Path):
    abs_dir = tmp_path / "absdir"
    raw = {
        "model_x": {
            "path": str(abs_dir),
            "format": "delta",
        }
    }
    norm = storage.normalize_storage_map(raw, project_dir=tmp_path)
    assert norm["model_x"]["path"] == str(abs_dir.resolve())
    assert norm["model_x"]["format"] == "delta"


# ---------------------------------------------------------------------------
# set_model_storage / get_model_storage
# ---------------------------------------------------------------------------


@pytest.mark.unit
def test_get_model_storage_exact_match():
    storage.set_model_storage({"m1": {"path": "/tmp/m1"}})
    meta = storage.get_model_storage("m1")
    assert meta == {"path": "/tmp/m1"}


@pytest.mark.unit
def test_get_model_storage_accepts_ff_suffix():
    # registry contains name without .ff
    storage.set_model_storage({"m_model": {"format": "parquet"}})

    # ask with .ff → should find it
    meta = storage.get_model_storage("m_model.ff")
    assert meta == {"format": "parquet"}

    # ask without .ff → should also find it (because we add .ff as candidate)
    meta2 = storage.get_model_storage("m_model")
    assert meta2 == {"format": "parquet"}


@pytest.mark.unit
def test_get_model_storage_dotted_name_uses_last_part():
    # registry only knows the short name
    storage.set_model_storage({"short": {"path": "/opt/data"}})

    # caller asks with db.schema.short
    meta = storage.get_model_storage("db.schema.short")
    assert meta == {"path": "/opt/data"}


@pytest.mark.unit
def test_get_model_storage_returns_empty_if_not_found():
    storage.set_model_storage({"other": {"x": 1}})
    assert storage.get_model_storage("not-there") == {}


# ---------------------------------------------------------------------------
# set_seed_storage / get_seed_storage
# ---------------------------------------------------------------------------


@pytest.mark.unit
def test_get_seed_storage_exact_and_last_part():
    storage.set_seed_storage(
        {
            "schema.seed_tbl": {"path": "/tmp/seed1"},
            "pure": {"path": "/tmp/seed2"},
        }
    )

    # exact
    assert storage.get_seed_storage("schema.seed_tbl") == {"path": "/tmp/seed1"}

    # only last part
    assert storage.get_seed_storage("schema.other.pure") == {"path": "/tmp/seed2"}

    # missing
    assert storage.get_seed_storage("nothing") == {}


# ---------------------------------------------------------------------------
# spark_write_to_path
# ---------------------------------------------------------------------------


@pytest.mark.unit
@pytest.mark.databricks_spark
def test_spark_write_to_path_happy(tmp_path: Path, monkeypatch):
    # fake spark + df.write chain
    fake_spark = MagicMock()
    fake_df = MagicMock()

    writer = MagicMock()
    # df.write.mode("overwrite") → writer
    fake_df.write.mode.return_value = writer
    # .format(...) → writer
    writer.format.return_value = writer
    # .options(...) → writer
    writer.options.return_value = writer

    def _save_side_effect(path_str: str):
        p = Path(path_str)
        p.mkdir(parents=True, exist_ok=True)

    writer.save.side_effect = _save_side_effect

    # storage entry with local path
    target_dir = tmp_path / "out"
    storage_meta = {
        "path": str(target_dir),
        "format": "parquet",
        "options": {"compression": "snappy"},
    }

    storage.spark_write_to_path(
        fake_spark,
        "db.tbl",
        fake_df,
        storage=storage_meta,
        default_format=None,
        default_options={"mergeSchema": "true"},
    )

    # 1) DROP TABLE IF EXISTS `db`.`tbl`
    fake_spark.sql.assert_any_call("DROP TABLE IF EXISTS `db`.`tbl`")

    # 2) writer must have been called with format and merged options
    fake_df.write.mode.assert_called_once_with("overwrite")
    writer.format.assert_called_once_with("parquet")

    # merged options: default_options + storage.options
    writer.options.assert_called_once_with(mergeSchema="true", compression="snappy")

    # 3) save() called with path
    assert writer.save.call_count == 1
    tmp_save_path = writer.save.call_args[0][0]
    assert ".ff_tmp_" in tmp_save_path
    assert str(target_dir.parent) in tmp_save_path

    assert target_dir.exists()

    # 4) create table ... location ...
    # fmt is known → USING parquet
    create_calls = [c.args[0] for c in fake_spark.sql.call_args_list if "CREATE TABLE" in c.args[0]]
    assert len(create_calls) == 1
    assert "CREATE TABLE `db`.`tbl` USING parquet LOCATION" in create_calls[0]
    assert str(target_dir) in create_calls[0]


@pytest.mark.unit
@pytest.mark.databricks_spark
def test_spark_write_to_path_without_format_uses_default(tmp_path: Path):
    fake_spark = MagicMock()
    fake_df = MagicMock()
    writer = MagicMock()
    fake_df.write.mode.return_value = writer
    writer.format.return_value = writer

    writer.save.side_effect = lambda p: Path(p).mkdir(parents=True, exist_ok=True)

    target_dir = tmp_path / "x"
    storage_meta = {
        "path": str(target_dir),
        # no "format" here
    }

    storage.spark_write_to_path(
        fake_spark,
        "tbl_only",
        fake_df,
        storage=storage_meta,
        default_format="delta",
        default_options=None,
    )

    writer.format.assert_called_once_with("delta")


@pytest.mark.unit
@pytest.mark.databricks_spark
def test_spark_write_to_path_requires_path():
    fake_spark = MagicMock()
    fake_df = MagicMock()

    with pytest.raises(ValueError) as exc:
        storage.spark_write_to_path(
            fake_spark,
            "db.tbl",
            fake_df,
            storage={},
            default_format="parquet",
            default_options=None,
        )
    assert "requires 'path'" in str(exc.value)


@pytest.mark.unit
@pytest.mark.databricks_spark
def test_spark_write_to_path_rejects_empty_identifier(tmp_path: Path):
    fake_spark = MagicMock()
    fake_df = MagicMock()
    writer = MagicMock()
    fake_df.write.mode.return_value = writer

    storage_meta = {"path": str(tmp_path / "out")}

    with pytest.raises(ValueError) as exc:
        storage.spark_write_to_path(
            fake_spark,
            "",
            fake_df,
            storage=storage_meta,
            default_format=None,
            default_options=None,
        )
    assert "Invalid Spark identifier" in str(exc.value)

# tests/unit/cli/test_bootstrap_unit.py
from __future__ import annotations

import datetime
from pathlib import Path
from types import SimpleNamespace
from typing import cast

import pytest
from jinja2 import Environment
from tests.common.mock.profiles import (
    fake_bigquery_profile,
    fake_databricks_spark_profile,
    fake_duckdb_profile,
    fake_postgres_profile,
    fake_snowflake_snowpark_profile,
)

from fastflowtransform.cli import bootstrap
from fastflowtransform.settings import (
    SnowflakeSnowparkProfile,
)

# ---------------------------------------------------------------------------
# helpers
# ---------------------------------------------------------------------------


class _FakeRegistry:
    """Tiny fake of fastflowtransform.core.REGISTRY for tests."""

    def __init__(self) -> None:
        self._active_engine: str | None = None
        self._cli_vars: dict[str, object] = {}
        self.env: Environment = Environment()

    def set_active_engine(self, name: str | None) -> None:
        self._active_engine = name

    def set_cli_vars(self, d: dict[str, object]) -> None:
        self._cli_vars = d

    def load_project(self, p: Path) -> None:
        # in unit tests we do nothing - real REGISTRY would scan models
        return


# ---------------------------------------------------------------------------
# _validate_profile_params
# ---------------------------------------------------------------------------


@pytest.mark.unit
def test_validate_profile_params_bigquery_ok():
    prof = fake_bigquery_profile(dataset="analytics")
    # should not raise
    bootstrap._validate_profile_params("dev", prof)


@pytest.mark.unit
def test_validate_profile_params_bigquery_missing_dataset():
    prof = fake_bigquery_profile(dataset="")
    with pytest.raises(bootstrap.typer.BadParameter):
        bootstrap._validate_profile_params("dev", prof)


@pytest.mark.unit
def test_validate_profile_params_duckdb_ok():
    prof = fake_duckdb_profile(path=":memory:")
    bootstrap._validate_profile_params("dev", prof)


@pytest.mark.unit
def test_validate_profile_params_duckdb_missing_path():
    prof = fake_duckdb_profile(path="")
    with pytest.raises(bootstrap.typer.BadParameter):
        bootstrap._validate_profile_params("dev", prof)


@pytest.mark.unit
def test_validate_profile_params_postgres_ok():
    prof = fake_postgres_profile(dsn="postgres://...", schema="public")
    bootstrap._validate_profile_params("dev", prof)


@pytest.mark.unit
def test_validate_profile_params_postgres_missing_dsn():
    prof = fake_postgres_profile(dsn="", schema="public")
    with pytest.raises(bootstrap.typer.BadParameter):
        bootstrap._validate_profile_params("dev", prof)


@pytest.mark.unit
def test_validate_profile_params_postgres_missing_schema():
    prof = fake_postgres_profile(dsn="postgres://...", schema="")
    with pytest.raises(bootstrap.typer.BadParameter):
        bootstrap._validate_profile_params("dev", prof)


@pytest.mark.unit
def test_validate_profile_params_databricks_ok():
    prof = fake_databricks_spark_profile(master="local[*]", app_name="fft-test")
    bootstrap._validate_profile_params("dev", prof)


@pytest.mark.unit
def test_validate_profile_params_databricks_missing_master():
    prof = fake_databricks_spark_profile(master="", app_name="fft-test")
    with pytest.raises(bootstrap.typer.BadParameter):
        bootstrap._validate_profile_params("dev", prof)


@pytest.mark.unit
def test_validate_profile_params_snowflake_ok():
    prof = fake_snowflake_snowpark_profile()
    bootstrap._validate_profile_params("dev", prof)


@pytest.mark.unit
def test_validate_profile_params_snowflake_missing_required():
    prof = fake_snowflake_snowpark_profile()
    sf_prof = cast(SnowflakeSnowparkProfile, prof)
    # break one required field
    sf_prof.snowflake_snowpark.account = ""
    with pytest.raises(bootstrap.typer.BadParameter):
        bootstrap._validate_profile_params("dev", sf_prof)


# ---------------------------------------------------------------------------
# _parse_cli_vars
# ---------------------------------------------------------------------------


@pytest.mark.unit
def test_parse_cli_vars_parses_yaml_values():
    # note: the quotes around [a,b] make YAML return a *string*, not a list
    out = bootstrap._parse_cli_vars(["day=2025-10-01", "limit=5", "enabled=true", "tags='[a,b]'"])

    # day: yaml parses ISO dates as datetime.date
    day_val = out["day"]
    if isinstance(day_val, datetime.date):
        assert day_val == datetime.date(2025, 10, 1)
    else:
        # fallback, in case the YAML loader changes
        assert day_val == "2025-10-01"

    # numeric
    assert out["limit"] == 5

    # bool
    assert out["enabled"] is True

    # because of the extra quotes, YAML keeps this as a plain string
    assert out["tags"] == "[a,b]"


@pytest.mark.unit
def test_parse_cli_vars_raises_on_missing_equal():
    with pytest.raises(bootstrap.typer.BadParameter):
        bootstrap._parse_cli_vars(["justkey"])


# ---------------------------------------------------------------------------
# _make_executor
# ---------------------------------------------------------------------------


@pytest.mark.unit
def test_make_executor_bigquery_uses_correct_executor(monkeypatch):
    # we don't want to hit the real bigquery imports
    # so we just monkeypatch the executors the bootstrap imports
    class _FakeBQExec:
        def __init__(self, *a, **k):
            self.args = a
            self.kw = k

        def run_python(self, *a, **k):
            pass

    # patch BOTH BF and normal - code branches on use_bigframes
    monkeypatch.setattr(bootstrap, "BigQueryExecutor", _FakeBQExec, raising=True)
    monkeypatch.setattr(bootstrap, "BigQueryBFExecutor", _FakeBQExec, raising=True)

    prof = fake_bigquery_profile(use_bigframes=False)
    jenv = Environment()

    ex, run_fn, py_fn = bootstrap._make_executor(prof, jenv)

    assert isinstance(ex, _FakeBQExec)
    assert callable(run_fn)
    assert callable(py_fn)


@pytest.mark.unit
def test_make_executor_duckdb(monkeypatch, tmp_path: Path):
    class _FakeDuckExec:
        def __init__(self, db_path: str):
            self.db_path = db_path

        def run_python(self, *a, **k):
            pass

    monkeypatch.setattr(bootstrap, "DuckExecutor", _FakeDuckExec, raising=True)

    prof = fake_duckdb_profile(path=str(tmp_path / "test.duckdb"))
    jenv = Environment()

    ex, run_fn, py_fn = bootstrap._make_executor(prof, jenv)
    assert isinstance(ex, _FakeDuckExec)
    assert ex.db_path.endswith("test.duckdb")
    assert callable(run_fn)
    assert callable(py_fn)


# ---------------------------------------------------------------------------
# _resolve_project_path - minimal check with temp dir
# ---------------------------------------------------------------------------


@pytest.mark.unit
def test_resolve_project_path_happy(tmp_path: Path):
    # create models/ folder so bootstrap accepts the dir
    (tmp_path / "models").mkdir()
    p = bootstrap._resolve_project_path(str(tmp_path))
    assert p == tmp_path.resolve()


@pytest.mark.unit
def test_resolve_project_path_missing_models(tmp_path: Path):
    # no models/ → should raise
    with pytest.raises(bootstrap.typer.BadParameter):
        bootstrap._resolve_project_path(str(tmp_path))


# ---------------------------------------------------------------------------
# _get_test_con - just smoke test
# ---------------------------------------------------------------------------


@pytest.mark.unit
def test_get_test_con_prefers_executor_con():
    class ExecWithCon:
        def __init__(self):
            self.con = SimpleNamespace(execute=lambda *_: "ok")

    ex = ExecWithCon()
    con = bootstrap._get_test_con(ex)
    assert con.execute("SELECT 1") == "ok"


@pytest.mark.unit
def test_get_test_con_falls_back_to_executor():
    class ExecSimple:
        def run(self):
            return "ran"

    ex = ExecSimple()
    con = bootstrap._get_test_con(ex)
    # we just get the executor back
    assert con is ex

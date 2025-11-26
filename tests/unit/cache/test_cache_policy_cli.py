from __future__ import annotations

import importlib
import time
from pathlib import Path
from types import SimpleNamespace

import pytest
from jinja2 import Environment
from typer.testing import CliRunner

from fastflowtransform.cli import app
from fastflowtransform.core import REGISTRY, Node
from fastflowtransform.run_executor import ScheduleResult

cli_bootstrap = importlib.import_module("fastflowtransform.cli.bootstrap")
cli_run = importlib.import_module("fastflowtransform.cli.run")

# ----------------------------- Helpers -----------------------------


def _mk_node(tmp: Path, name: str, *, kind: str = "sql", mat: str = "table", deps=None) -> Node:
    """
    Create a minimal Node and backing file so REGISTRY.load_project() isn't required.
    """
    deps = deps or []
    models_dir = tmp / "models"
    models_dir.mkdir(parents=True, exist_ok=True)
    suffix = ".ff.sql" if kind == "sql" else ".ff.py"
    filename = f"{name}{'.sql' if name.endswith('.ff') else suffix}"
    path = models_dir / filename
    path.write_text("-- test", encoding="utf-8")
    n = Node(name=name, kind=kind, path=path, deps=list(deps), meta={"materialized": mat})
    return n


class _FakeCache:
    """
    Minimal fake for FingerprintCache used by CLI.
    """

    def __init__(self, _project: Path, profile: str, engine: str):
        self._data: dict[str, str] = {}
        self.loaded = False
        self.updated = False
        self.saved = False
        self.profile = profile
        self.engine = engine

    def load(self) -> None:
        self.loaded = True

    def get(self, key: str) -> str | None:
        return self._data.get(key)

    def update_many(self, mapping: dict[str, str]) -> None:
        # Mark but do not overwrite unless test configured it to
        self._data.update(mapping)
        self.updated = True

    def save(self) -> None:
        self.saved = True


def _stub_schedule(monkeypatch: pytest.MonkeyPatch):
    """
    Replace schedule() with a deterministic, synchronous runner.
    """

    def schedule(
        levels,
        jobs,
        fail_policy,
        run_node,
        *,
        before=None,
        on_error=None,
        logger=None,
        engine_abbr="",
        name_width=28,
        name_formatter=None,
        durations_s=None,
    ):
        start = time.perf_counter()
        per: dict[str, float] = {}
        for lvl in levels:
            # level barrier
            for name in lvl:
                t0 = time.perf_counter()
                try:
                    if before:
                        # neue Arity (name, level_idx) supporten; fallback auf (name)
                        try:
                            before(name, 1)
                        except TypeError:
                            before(name)
                    if name_formatter:
                        # exercise formatter for parity with real schedule
                        name_formatter(name)
                    run_node(name)
                except BaseException as e:
                    if on_error:
                        on_error(name, e)
                    per[name] = time.perf_counter() - t0
                    # failed erwartet dict[str, BaseException]
                    return ScheduleResult(
                        failed={name: e},
                        per_node_s=per,
                        total_s=time.perf_counter() - start,
                    )
                per[name] = time.perf_counter() - t0
        # Success → empty Dict
        return ScheduleResult(
            failed={},
            per_node_s=per,
            total_s=time.perf_counter() - start,
        )

    monkeypatch.setattr(cli_run, "schedule", schedule)


def _stub_project_and_profile(monkeypatch: pytest.MonkeyPatch, tmp_path: Path):
    """
    Avoids filesystem discovery and real profiles.
    """

    def fake_load_project_and_env(project_arg: str):
        env = Environment()
        REGISTRY.env = env
        return tmp_path, env

    def fake_resolve_profile(env_name, engine, proj):
        env = SimpleNamespace()
        prof = SimpleNamespace(engine="duckdb", duckdb=SimpleNamespace(path=":memory:"))
        return env, prof

    monkeypatch.setattr(cli_bootstrap, "_load_project_and_env", fake_load_project_and_env)
    monkeypatch.setattr(cli_bootstrap, "_resolve_profile", fake_resolve_profile)


def _stub_executor(monkeypatch: pytest.MonkeyPatch, calls: dict[str, list]):
    """
    Make executor factory return a dummy executor with call-recording run_sql/run_py.
    """

    class DummyEx:
        pass

    def fake_make_executor(prof, jenv):
        ex = DummyEx()

        def run_sql(node):
            calls["run_sql"].append(node.name)

        def run_py(node):
            calls["run_py"].append(node.name)

        return ex, run_sql, run_py

    monkeypatch.setattr(cli_bootstrap, "_make_executor", fake_make_executor)


def _stub_levels(monkeypatch: pytest.MonkeyPatch, levels):
    """
    Force deterministic levels for the run.
    """
    monkeypatch.setattr(cli_run, "dag_levels", lambda _nodes: levels)


def _stub_fingerprints(monkeypatch: pytest.MonkeyPatch):
    """
    Force deterministic per-node fingerprints independent of executor internals.
    """
    monkeypatch.setattr(
        cli_run._RunEngine, "_maybe_fingerprint", lambda self, node, ex: f"fp::{node.name}"
    )


def _stub_cache_class(monkeypatch: pytest.MonkeyPatch, cache_obj: _FakeCache):
    """
    Ensure CLI uses our fake cache instance.
    """

    def ctor(project: Path, profile: str, engine: str):
        # Reconfigure the provided cache_obj with the runtime keys
        cache_obj.profile = profile
        cache_obj.engine = engine
        return cache_obj

    monkeypatch.setattr(cli_run, "FingerprintCache", ctor)


# ------------------------------ Tests -------------------------------


@pytest.mark.unit
def test_cache_rw_noop_skips_all(tmp_path: Path, monkeypatch: pytest.MonkeyPatch):
    """
    RW mode: when cache hits for all nodes, a no-op run builds 0 nodes (all skipped).
    """
    # Registry with two sql nodes
    a = _mk_node(tmp_path, "users", mat="table")
    b = _mk_node(tmp_path, "orders.ff", mat="view")
    REGISTRY.nodes = {x.name: x for x in (a, b)}

    # Stubs
    _stub_project_and_profile(monkeypatch, tmp_path)
    _stub_schedule(monkeypatch)
    _stub_levels(monkeypatch, [["users", "orders.ff"]])
    _stub_fingerprints(monkeypatch)

    calls = {"run_sql": [], "run_py": []}
    _stub_executor(monkeypatch, calls)

    # Fake cache pre-populated with matching fps
    fake_cache = _FakeCache(tmp_path, profile="dev", engine="duckdb")
    fake_cache._data = {"users": "fp::users", "orders.ff": "fp::orders.ff"}
    _stub_cache_class(monkeypatch, fake_cache)

    # Avoid relation existence checks interfering
    monkeypatch.setattr("fastflowtransform.cache.relation_exists", lambda _ex, _rel: True)

    runner = CliRunner()
    res = runner.invoke(app, ["run", str(tmp_path), "--cache", "rw"])
    assert res.exit_code == 0, res.output
    # No node should be executed
    assert calls["run_sql"] == []
    assert calls["run_py"] == []


@pytest.mark.unit
def test_cache_ro_build_no_write(tmp_path: Path, monkeypatch: pytest.MonkeyPatch):
    """
    RO mode: cache miss -> build models but do not write cache.
    """
    a = _mk_node(tmp_path, "users", mat="table")
    b = _mk_node(tmp_path, "orders.ff", mat="view", deps=["users"])
    REGISTRY.nodes = {x.name: x for x in (a, b)}

    _stub_project_and_profile(monkeypatch, tmp_path)
    _stub_schedule(monkeypatch)
    _stub_levels(monkeypatch, [["users"], ["orders.ff"]])
    _stub_fingerprints(monkeypatch)

    calls = {"run_sql": [], "run_py": []}
    _stub_executor(monkeypatch, calls)

    fake_cache = _FakeCache(tmp_path, profile="dev", engine="duckdb")
    # empty cache -> misses
    _stub_cache_class(monkeypatch, fake_cache)

    monkeypatch.setattr("fastflowtransform.cache.relation_exists", lambda _ex, _rel: True)

    runner = CliRunner()
    res = runner.invoke(app, ["run", str(tmp_path), "--cache", "ro"])
    assert res.exit_code == 0, res.output
    # Both nodes should have been executed (misses)
    assert calls["run_sql"] == ["users", "orders.ff"]
    assert calls["run_py"] == []
    # In RO mode, cache must not be written
    assert fake_cache.updated is False or fake_cache.saved is False, "RO must not persist cache"


@pytest.mark.unit
def test_rebuild_selected_builds_even_on_hit(tmp_path: Path, monkeypatch: pytest.MonkeyPatch):
    """
    --rebuild with a selection: selected node is built even if cache matches.
    """
    raw = _mk_node(tmp_path, "raw_sales", mat="table")
    marts = _mk_node(tmp_path, "marts_daily.ff", mat="table", deps=["raw_sales"])
    REGISTRY.nodes = {x.name: x for x in (raw, marts)}

    _stub_project_and_profile(monkeypatch, tmp_path)
    _stub_schedule(monkeypatch)
    # Two logical levels, but we'll select only the second
    _stub_levels(monkeypatch, [["raw_sales"], ["marts_daily.ff"]])
    _stub_fingerprints(monkeypatch)

    calls = {"run_sql": [], "run_py": []}
    _stub_executor(monkeypatch, calls)

    fake_cache = _FakeCache(tmp_path, profile="dev", engine="duckdb")
    fake_cache._data = {"raw_sales": "fp::raw_sales", "marts_daily.ff": "fp::marts_daily.ff"}
    _stub_cache_class(monkeypatch, fake_cache)

    monkeypatch.setattr("fastflowtransform.cache.relation_exists", lambda _ex, _rel: True)

    runner = CliRunner()
    # Select only marts_daily.ff and force rebuild
    res = runner.invoke(
        app, ["run", str(tmp_path), "--select", "marts_daily.ff", "--cache", "rw", "--rebuild"]
    )
    assert res.exit_code == 0, res.output
    # Only the selected node should be executed, even though cache hits exist
    assert calls["run_sql"] == ["marts_daily.ff"]
    assert calls["run_py"] == []

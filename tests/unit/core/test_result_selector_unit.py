# tests/unit/core/test_result_selector_unit.py
from __future__ import annotations

import json
from pathlib import Path

import pytest

from fastflowtransform.cli.selectors import _compile_selector
from fastflowtransform.core import REGISTRY, Node


def _write_run_results(base: Path, results: list[dict]) -> None:
    tgt = base / ".fastflowtransform" / "target"
    tgt.mkdir(parents=True, exist_ok=True)
    payload = {
        "metadata": {"tool": "fastflowtransform", "generated_at": "2025-01-01T00:00:00Z"},
        "run_started_at": "2025-01-01T00:00:00Z",
        "run_finished_at": "2025-01-01T00:00:01Z",
        "results": results,
    }
    (tgt / "run_results.json").write_text(
        json.dumps(payload, ensure_ascii=False, sort_keys=True), encoding="utf-8"
    )


@pytest.fixture(autouse=True)
def clean_registry():
    # Ensure each test starts with a clean registry
    REGISTRY.nodes.clear()
    REGISTRY.py_funcs.clear()
    REGISTRY.py_requires.clear()
    REGISTRY.macros.clear()
    REGISTRY.sources = {}
    REGISTRY.project_vars = {}
    REGISTRY.cli_vars = {}
    yield
    REGISTRY.nodes.clear()


@pytest.mark.unit
def test_result_tokens_ok_error_fail_warn(tmp_path: Path):
    # Minimal project structure so selectors can resolve project dir
    (tmp_path / "models").mkdir(parents=True)
    REGISTRY.project_dir = tmp_path  # allow selectors to find .fastflowtransform/target

    # Register minimal nodes (paths are dummies; selectors don't read files)
    REGISTRY.nodes.update(
        {
            "ok_node": Node("ok_node", "sql", path=tmp_path / "models" / "ok.sql", deps=[]),
            "err_node": Node("err_node", "sql", path=tmp_path / "models" / "err.sql", deps=[]),
            "warn_node": Node("warn_node", "sql", path=tmp_path / "models" / "warn.sql", deps=[]),
            "other": Node("other", "sql", path=tmp_path / "models" / "other.sql", deps=[]),
        }
    )

    # Simulate a previous run with one success, one error, one success+warning
    _write_run_results(
        tmp_path,
        results=[
            {
                "name": "ok_node",
                "status": "success",
                "started_at": "2025-01-01T00:00:00Z",
                "finished_at": "2025-01-01T00:00:00Z",
                "duration_ms": 5,
            },
            {
                "name": "err_node",
                "status": "error",
                "started_at": "2025-01-01T00:00:00Z",
                "finished_at": "2025-01-01T00:00:00Z",
                "duration_ms": 7,
                "message": "boom",
            },
            {
                "name": "warn_node",
                "status": "success",
                "started_at": "2025-01-01T00:00:00Z",
                "finished_at": "2025-01-01T00:00:00Z",
                "duration_ms": 3,
                # Either of these should trigger "warn" detection. We'll set both for clarity.
                "message": "WARN: threshold crossed",
                "warnings": 1,
            },
        ],
    )

    # result:ok
    _, pred = _compile_selector(["result:ok"])
    assert pred(REGISTRY.nodes["ok_node"])
    assert not pred(REGISTRY.nodes["err_node"])
    assert not pred(REGISTRY.nodes["warn_node"])

    # result:error
    _, pred = _compile_selector(["result:error"])
    assert pred(REGISTRY.nodes["err_node"])
    assert not pred(REGISTRY.nodes["ok_node"])
    assert not pred(REGISTRY.nodes["warn_node"])

    # result:fail (alias for error)
    _, pred = _compile_selector(["result:fail"])
    assert pred(REGISTRY.nodes["err_node"])
    assert not pred(REGISTRY.nodes["ok_node"])

    # result:warn
    _, pred = _compile_selector(["result:warn"])
    assert pred(REGISTRY.nodes["warn_node"])
    assert not pred(REGISTRY.nodes["ok_node"])
    assert not pred(REGISTRY.nodes["err_node"])

    # Combination: result:ok + nameglob
    _, pred = _compile_selector(["result:ok", "ok_*"])
    assert pred(REGISTRY.nodes["ok_node"])
    assert not pred(REGISTRY.nodes["warn_node"])
    assert not pred(REGISTRY.nodes["err_node"])

    # Unknown token: result:maybe
    _, pred = _compile_selector(["result:maybe"])
    assert not pred(REGISTRY.nodes["ok_node"])
    assert not pred(REGISTRY.nodes["err_node"])
    assert not pred(REGISTRY.nodes["warn_node"])


@pytest.mark.unit
def test_result_tokens_without_file_are_noops(tmp_path: Path):
    (tmp_path / "models").mkdir(parents=True)
    REGISTRY.project_dir = tmp_path

    REGISTRY.nodes.update(
        {
            "a": Node("a", "sql", path=tmp_path / "models" / "a.sql", deps=[]),
            "b": Node("b", "sql", path=tmp_path / "models" / "b.sql", deps=[]),
        }
    )

    for token in ("result:ok", "result:error", "result:fail", "result:warn"):
        _, pred = _compile_selector([token])
        assert not pred(REGISTRY.nodes["a"])
        assert not pred(REGISTRY.nodes["b"])


@pytest.mark.unit
def test_result_tokens_coexist_with_other_filters(tmp_path: Path):
    (tmp_path / "models").mkdir(parents=True)
    REGISTRY.project_dir = tmp_path

    REGISTRY.nodes.update(
        {
            "alpha": Node(
                "alpha",
                "sql",
                path=tmp_path / "models" / "alpha.sql",
                deps=[],
                meta={"tags": ["core"]},
            ),
            "beta": Node(
                "beta",
                "sql",
                path=tmp_path / "models" / "beta.sql",
                deps=[],
                meta={"tags": ["extra"]},
            ),
        }
    )

    _write_run_results(
        tmp_path,
        results=[
            {
                "name": "alpha",
                "status": "success",
                "started_at": "2025-01-01T00:00:00Z",
                "finished_at": "2025-01-01T00:00:00Z",
                "duration_ms": 1,
            },
            {
                "name": "beta",
                "status": "error",
                "started_at": "2025-01-01T00:00:00Z",
                "finished_at": "2025-01-01T00:00:00Z",
                "duration_ms": 2,
                "message": "boom",
            },
        ],
    )

    # result:ok AND tag:core → only alpha
    _, pred = _compile_selector(["result:ok", "tag:core"])
    assert pred(REGISTRY.nodes["alpha"])
    assert not pred(REGISTRY.nodes["beta"])

    # result:error AND tag:extra → only beta
    _, pred = _compile_selector(["result:error", "tag:extra"])
    assert pred(REGISTRY.nodes["beta"])
    assert not pred(REGISTRY.nodes["alpha"])

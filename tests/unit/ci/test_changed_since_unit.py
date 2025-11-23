# tests/unit/ci/test_changed_since_unit.py
from __future__ import annotations

from pathlib import Path

import pytest

from fastflowtransform.ci.changed_since import (
    compute_affected_models,
    get_changed_models,
)
from fastflowtransform.core import REGISTRY, Node


class DummyProc:
    def __init__(self, returncode: int, stdout: str = "", stderr: str = "") -> None:
        self.returncode = returncode
        self.stdout = stdout
        self.stderr = stderr


def _reset_registry_nodes():
    REGISTRY.nodes.clear()


@pytest.fixture(autouse=True)
def _clean_registry_nodes():
    # Ensure we don't leak nodes across tests
    old_nodes = dict(REGISTRY.nodes)
    try:
        REGISTRY.nodes.clear()
        yield
    finally:
        REGISTRY.nodes = old_nodes


@pytest.mark.unit
def test_get_changed_models_filters_and_maps(monkeypatch, tmp_path):
    """
    get_changed_models should:
      - call 'git diff --name-only <ref> HEAD -- .'
      - only consider files under 'models/'
      - only consider *.ff.sql / *.ff.py
      - map paths to REGISTRY.nodes keys via Path(...).stem
    """
    # Prepare a fake project dir (unused except for cwd)
    project_dir = tmp_path

    # Populate REGISTRY.nodes with model names that match stems
    # of the changed files below.
    REGISTRY.nodes = {
        "customers.ff": Node(
            name="customers.ff", kind="sql", path=Path("models/customers.ff.sql"), deps=[]
        ),
        "orders_agg.ff": Node(
            name="orders_agg.ff",
            kind="sql",
            path=Path("models/marts/orders_agg.ff.sql"),
            deps=["customers.ff"],
        ),
        "util.ff": Node(name="util.ff", kind="python", path=Path("models/py/util.ff.py"), deps=[]),
    }

    def fake_run(cmd, cwd, stdout, stderr, text, check):
        # Sanity-check the git invocation
        assert "git" in cmd[0]
        assert "diff" in cmd[1]
        assert "--name-only" in cmd
        assert cwd == project_dir

        # Mix of relevant & irrelevant paths.
        stdout_text = "\n".join(
            [
                "models/customers.ff.sql",  # → customers.ff
                "models/marts/orders_agg.ff.sql",  # → orders_agg.ff
                "models/py/util.ff.py",  # → util.ff
                "seeds/customers.csv",  # ignored (not under models/)
                "models/readme.md",  # ignored (wrong suffix)
            ]
        )
        return DummyProc(returncode=0, stdout=stdout_text, stderr="")

    monkeypatch.setattr("subprocess.run", fake_run, raising=True)

    changed = get_changed_models(project_dir, git_ref="origin/main")

    # Only matching models should be returned.
    assert changed == {"customers.ff", "orders_agg.ff", "util.ff"}


@pytest.mark.unit
def test_get_changed_models_git_error(monkeypatch, tmp_path):
    """
    If git diff fails (non-zero exit code), get_changed_models should
    return an empty set and not raise.
    """
    project_dir = tmp_path

    def fake_run(cmd, cwd, stdout, stderr, text, check):
        return DummyProc(returncode=1, stdout="", stderr="fatal: not a git repository")

    monkeypatch.setattr("subprocess.run", fake_run, raising=True)

    REGISTRY.nodes = {
        "customers.ff": Node(
            name="customers.ff", kind="sql", path=Path("models/customers.ff.sql"), deps=[]
        )
    }

    changed = get_changed_models(project_dir, git_ref="origin/main")
    assert changed == set()


@pytest.mark.unit
def test_get_changed_models_no_model_files(monkeypatch, tmp_path):
    """
    If git diff returns only non-model paths, get_changed_models should
    return an empty set.
    """
    project_dir = tmp_path

    def fake_run(cmd, cwd, stdout, stderr, text, check):
        stdout_text = "\n".join(
            [
                "README.md",
                "seeds/customers.csv",
                "scripts/some_tool.py",
            ]
        )
        return DummyProc(returncode=0, stdout=stdout_text, stderr="")

    monkeypatch.setattr("subprocess.run", fake_run, raising=True)

    REGISTRY.nodes = {
        "customers.ff": Node(
            name="customers.ff", kind="sql", path=Path("models/customers.ff.sql"), deps=[]
        )
    }

    changed = get_changed_models(project_dir, git_ref="origin/main")
    assert changed == set()


@pytest.mark.unit
def test_compute_affected_models_chain():
    """
    For a simple chain A -> B -> C (B depends on A, C depends on B),
    a change in B should mark {A, B, C} as affected.
    """
    nodes = {
        "A.ff": Node(name="A.ff", kind="sql", path=Path("models/A.ff.sql"), deps=[]),
        "B.ff": Node(name="B.ff", kind="sql", path=Path("models/B.ff.sql"), deps=["A.ff"]),
        "C.ff": Node(name="C.ff", kind="sql", path=Path("models/C.ff.sql"), deps=["B.ff"]),
    }

    changed = {"B.ff"}
    affected = compute_affected_models(changed, nodes)

    # B is changed; A is upstream of B; C is downstream of B.
    assert affected == {"A.ff", "B.ff", "C.ff"}


@pytest.mark.unit
def test_compute_affected_models_disconnected():
    """
    Disconnected components should not leak: changing X only affects its
    own connected component.
    """
    nodes = {
        "A.ff": Node(name="A.ff", kind="sql", path=Path("models/A.ff.sql"), deps=[]),
        "B.ff": Node(name="B.ff", kind="sql", path=Path("models/B.ff.sql"), deps=["A.ff"]),
        "X.ff": Node(name="X.ff", kind="sql", path=Path("models/X.ff.sql"), deps=[]),
        "Y.ff": Node(name="Y.ff", kind="sql", path=Path("models/Y.ff.sql"), deps=["X.ff"]),
    }

    changed = {"X.ff"}
    affected = compute_affected_models(changed, nodes)

    # Only the X↔Y component should be affected.
    assert affected == {"X.ff", "Y.ff"}


@pytest.mark.unit
def test_compute_affected_models_empty_changed():
    """
    If 'changed' is empty, compute_affected_models should return an empty set.
    """
    nodes = {
        "A.ff": Node(name="A.ff", kind="sql", path=Path("models/A.ff.sql"), deps=[]),
    }
    affected = compute_affected_models(set(), nodes)
    assert affected == set()

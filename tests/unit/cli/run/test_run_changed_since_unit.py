# tests/unit/cli/test_run_changed_since_unit.py
from __future__ import annotations

from importlib import import_module
from pathlib import Path

import pytest

# Import the *module* fastflowtransform.cli.run, not the cli.run attribute
run_cmd = import_module("fastflowtransform.cli.run")


class DummyCtx:
    def __init__(self, project: Path | str) -> None:
        self.project = Path(project)


@pytest.mark.unit
def test_apply_changed_since_no_flag_returns_wanted():
    """
    When changed_since is None, _apply_changed_since_filter should return
    'wanted' unchanged.
    """
    ctx = DummyCtx(project=".")
    wanted = {"a.ff", "b.ff"}

    out = run_cmd._apply_changed_since_filter(
        ctx=ctx,
        wanted=wanted,
        select=None,
        exclude=None,
        changed_since=None,
    )

    assert out == wanted


@pytest.mark.unit
def test_apply_changed_since_no_affected(monkeypatch, tmp_path):
    """
    When git-based detection yields no affected models, the filter should
    return an empty set.
    """
    project_dir = tmp_path
    ctx = DummyCtx(project=project_dir)

    def fake_get_changed_models(project_dir_arg, git_ref):
        # We expect to be called with the project dir passed in.
        assert Path(project_dir_arg) == project_dir
        return set()

    def fake_compute_affected_models(changed, nodes):
        # With no changed models, affected is empty.
        assert changed == set()
        return set()

    # Patch the functions on the actual module object
    monkeypatch.setattr(run_cmd, "get_changed_models", fake_get_changed_models, raising=True)
    monkeypatch.setattr(
        run_cmd, "compute_affected_models", fake_compute_affected_models, raising=True
    )

    wanted = {"a.ff", "b.ff"}

    out = run_cmd._apply_changed_since_filter(
        ctx=ctx,
        wanted=wanted,
        select=None,
        exclude=None,
        changed_since="origin/main",
    )

    # Nothing affected => nothing to run
    assert out == set()


@pytest.mark.unit
def test_apply_changed_since_no_select_uses_affected(monkeypatch, tmp_path):
    """
    When there are no --select/--exclude patterns, _apply_changed_since_filter
    returns the full 'affected' set from compute_affected_models.
    """
    project_dir = tmp_path
    ctx = DummyCtx(project=project_dir)

    def fake_get_changed_models(project_dir_arg, git_ref):
        assert Path(project_dir_arg) == project_dir
        assert git_ref == "origin/main"
        return {"m1.ff"}

    def fake_compute_affected_models(changed, nodes):
        # m1.ff changed; pretend m2.ff is downstream
        assert changed == {"m1.ff"}
        return {"m1.ff", "m2.ff"}

    monkeypatch.setattr(run_cmd, "get_changed_models", fake_get_changed_models, raising=True)
    monkeypatch.setattr(
        run_cmd, "compute_affected_models", fake_compute_affected_models, raising=True
    )

    wanted = {"x.ff", "y.ff"}  # initial 'wanted' is ignored when no select/exclude

    out = run_cmd._apply_changed_since_filter(
        ctx=ctx,
        wanted=wanted,
        select=None,
        exclude=None,
        changed_since="origin/main",
    )

    # No select/exclude → affected defines the universe
    assert out == {"m1.ff", "m2.ff"}


@pytest.mark.unit
def test_apply_changed_since_with_select_intersects(monkeypatch, tmp_path):
    """
    When --select or --exclude are present, the filter should intersect
    'wanted' with 'affected'.
    """
    project_dir = tmp_path
    ctx = DummyCtx(project=project_dir)

    def fake_get_changed_models(project_dir_arg, git_ref):
        assert Path(project_dir_arg) == project_dir
        return {"m1.ff"}

    def fake_compute_affected_models(changed, nodes):
        # Suppose m1.ff and m2.ff are affected, but only m2.ff is in 'wanted'
        assert changed == {"m1.ff"}
        return {"m1.ff", "m2.ff"}

    monkeypatch.setattr(run_cmd, "get_changed_models", fake_get_changed_models, raising=True)
    monkeypatch.setattr(
        run_cmd, "compute_affected_models", fake_compute_affected_models, raising=True
    )

    wanted = {"m2.ff", "unrelated.ff"}

    out = run_cmd._apply_changed_since_filter(
        ctx=ctx,
        wanted=wanted,
        select=["tag:dq"],  # any non-empty list triggers intersection logic
        exclude=None,
        changed_since="origin/main",
    )

    # Intersection of wanted and affected
    assert out == {"m2.ff"}

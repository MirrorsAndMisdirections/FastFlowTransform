# tests/unit/cli/test_docgen_cmd_unit.py
from __future__ import annotations

from pathlib import Path
from types import SimpleNamespace
from typing import Any

import pytest
import typer

import fastflowtransform.cli.docgen_cmd as docgen_mod


@pytest.fixture(autouse=True)
def _patch_registry(monkeypatch):
    """Keep REGISTRY small and predictable for all tests in this module."""
    fake_registry = SimpleNamespace(nodes={"model_a": object()})
    monkeypatch.setattr(docgen_mod, "REGISTRY", fake_registry, raising=True)


@pytest.fixture
def fake_ctx(tmp_path: Path):
    """Return a fake context that looks like _prepare_context output."""
    fake_executor = SimpleNamespace(name="fake-exec")

    class FakeCtx:
        def __init__(self, project: Path):
            self.project = project
            self.profile = SimpleNamespace(engine="duckdb")

        def make_executor(self):
            return (fake_executor,)

    return FakeCtx(tmp_path / "proj")


def test_docgen_basic_writes_html_dir(monkeypatch, tmp_path, fake_ctx):
    monkeypatch.setattr(docgen_mod, "_prepare_context", lambda *a, **k: fake_ctx, raising=True)

    dag_dir = tmp_path / "site" / "dag"
    monkeypatch.setattr(
        docgen_mod,
        "_resolve_dag_out_dir",
        lambda project, override: dag_dir,
        raising=True,
    )

    called: dict[str, Any] = {}

    def fake_render(out_dir, nodes, executor=None):
        called["render_out_dir"] = out_dir
        called["nodes"] = nodes
        called["executor"] = executor

    monkeypatch.setattr(docgen_mod, "render_site", fake_render, raising=True)
    monkeypatch.setattr(docgen_mod, "echo", lambda *_: None, raising=True)
    monkeypatch.setattr(docgen_mod, "echo_debug", lambda *_: None, raising=True)

    docgen_mod.docgen(project=".", env_name="dev")

    assert dag_dir.exists()
    assert called["render_out_dir"] == dag_dir
    assert isinstance(called["nodes"], dict)
    assert called["executor"].name == "fake-exec"


def test_docgen_emits_json_when_option_set(monkeypatch, tmp_path, fake_ctx):
    monkeypatch.setattr(docgen_mod, "_prepare_context", lambda *a, **k: fake_ctx, raising=True)

    out_dir = tmp_path / "site" / "dag"
    monkeypatch.setattr(
        docgen_mod,
        "_resolve_dag_out_dir",
        lambda project, override: out_dir,
        raising=True,
    )

    monkeypatch.setattr(docgen_mod, "render_site", lambda *a, **k: None, raising=True)
    monkeypatch.setattr(docgen_mod, "echo", lambda *_: None, raising=True)
    monkeypatch.setattr(docgen_mod, "echo_debug", lambda *_: None, raising=True)

    monkeypatch.setattr(
        docgen_mod,
        "_build_docs_manifest",
        lambda project_dir, nodes, executor, env_name: {
            "project": "fake",
            "models": [],
            "generated_at": "2025-01-01T00:00:00Z",
        },
        raising=True,
    )

    json_path = tmp_path / "docs.json"

    docgen_mod.docgen(project=".", env_name="dev", emit_json=json_path)

    assert json_path.exists()
    txt = json_path.read_text(encoding="utf-8")
    assert '"project": "fake"' in txt


def test_docgen_open_source_opens_browser(monkeypatch, tmp_path, fake_ctx):
    monkeypatch.setattr(docgen_mod, "_prepare_context", lambda *a, **k: fake_ctx, raising=True)
    dag_dir = tmp_path / "site" / "dag"
    monkeypatch.setattr(
        docgen_mod,
        "_resolve_dag_out_dir",
        lambda project, override: dag_dir,
        raising=True,
    )
    monkeypatch.setattr(docgen_mod, "render_site", lambda *a, **k: None, raising=True)
    monkeypatch.setattr(docgen_mod, "echo", lambda *_: None, raising=True)
    monkeypatch.setattr(docgen_mod, "echo_debug", lambda *_: None, raising=True)

    opened: dict[str, Any] = {}

    def fake_open(url, new=0):
        opened["url"] = url
        opened["new"] = new
        return True

    monkeypatch.setattr(docgen_mod.webbrowser, "open", fake_open, raising=True)

    docgen_mod.docgen(project=".", env_name="dev", open_source=True)

    assert "index.html" in opened["url"]
    assert opened["new"] == 2


def test_register_adds_command():
    """
    Typer sometimes sets Command.name to None for decorator-style registration.
    So we assert on the callback instead of the name.
    """
    app = typer.Typer()
    docgen_mod.register(app)

    cmds = app.registered_commands
    assert len(cmds) == 1

    cmd = cmds[0]
    # Typer stores the real function on cmd.callback
    assert cmd.callback is docgen_mod.docgen

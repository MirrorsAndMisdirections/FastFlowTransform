# tests/unit/cli/test_docs_utils_unit.py
from __future__ import annotations

from datetime import datetime
from pathlib import Path
from types import SimpleNamespace

import pytest
import yaml

from fastflowtransform.cli import docs_utils


@pytest.mark.unit
def test_resolve_dag_out_dir_with_override(tmp_path: Path):
    override = tmp_path / "custom"
    out = docs_utils._resolve_dag_out_dir(tmp_path, override)
    assert out == override.resolve()


@pytest.mark.unit
def test_resolve_dag_out_dir_from_project_yml(tmp_path: Path):
    proj = tmp_path
    (proj / "project.yml").write_text(
        yaml.safe_dump({"docs": {"dag_dir": "build/dag"}}),
        encoding="utf-8",
    )
    out = docs_utils._resolve_dag_out_dir(proj, None)
    assert out == (proj / "build" / "dag").resolve()


@pytest.mark.unit
def test_resolve_dag_out_dir_fallback(tmp_path: Path):
    proj = tmp_path
    out = docs_utils._resolve_dag_out_dir(proj, None)
    assert out == (proj / "site" / "dag").resolve()


@pytest.mark.unit
@pytest.mark.parametrize(
    "html,expected",
    [
        (None, None),
        ("", None),
        ("<p>Hello</p>", "Hello"),
        ("<p>Hi <b>you</b></p>", "Hi you"),
        ("<p>Hi</p>\n<p>there</p>", "Hi there"),
        ("Text   with   spaces", "Text with spaces"),
    ],
)
def test_strip_html(html, expected):
    assert docs_utils._strip_html(html) == expected


@pytest.mark.unit
def test_infer_sql_ref_aliases_basic():
    sql = """
        select *
        from public.my_table as t
        join other.tbl o on t.id = o.id
    """
    aliases = docs_utils._infer_sql_ref_aliases(sql)
    # keys must be the aliases
    assert aliases["t"] == "public.my_table"
    assert aliases["o"] == "other.tbl"


@pytest.mark.unit
def test_infer_sql_ref_aliases_quoted_and_backticks():
    sql = """
        SELECT * FROM "raw"."users" u
        JOIN `stg`.`orders` AS o ON u.id = o.user_id
    """
    aliases = docs_utils._infer_sql_ref_aliases(sql)

    # current behavior: only outer quotes/backticks are stripped,
    # inner Punkte + Quotes bleiben erhalten
    assert aliases["u"] == 'raw"."users'
    assert aliases["o"] == "stg`.`orders"


# ---------------------------------------------------------------------------
# _build_docs_manifest
# ---------------------------------------------------------------------------


@pytest.mark.unit
def test_build_docs_manifest_sql_branch(monkeypatch, tmp_path: Path):
    """
    Exercise the SQL-branch: executor.render_sql + lineage.infer_sql_lineage.
    """
    # fake project dir + project.yml
    proj = tmp_path
    (proj / "project.yml").write_text(
        yaml.safe_dump({"name": "MyProj"}),
        encoding="utf-8",
    )

    # fake nodes in REGISTRY
    # node has: name, deps, kind, meta
    n1 = SimpleNamespace(
        name="model_a",
        deps=["model_b"],
        kind="sql",
        meta={"materialized": "table"},
    )
    n2 = SimpleNamespace(
        name="model_b",
        deps=[],
        kind="sql",
        meta={},
    )
    fake_nodes = {"model_a": n1, "model_b": n2}

    # fake REGISTRY
    fake_env = object()

    def fake_relation_for(name: str) -> str:
        # simple predictable mapping
        return f"public.{name}"

    monkeypatch.setattr("fastflowtransform.cli.docs_utils.REGISTRY", SimpleNamespace(env=fake_env))
    monkeypatch.setattr("fastflowtransform.cli.docs_utils.relation_for", fake_relation_for)

    # fake executor with render_sql and _resolve_ref/_resolve_source
    def fake_render_sql(node, env, ref_resolver, source_resolver):
        # we return a SQL that contains aliases for lineage
        return """
            select a.id, b.val
            from public.model_b b
            join public.model_a_src a on a.id = b.id
        """

    fake_executor = SimpleNamespace(
        render_sql=fake_render_sql,
        _resolve_ref=lambda name, env: f"public.{name}",
        _resolve_source=lambda *a, **k: "public.src",
    )

    # fake lineage: return lineage for two cols
    def fake_infer_sql_lineage(rendered_sql: str, alias_map: dict[str, str]):
        return {
            "id": [{"source": "public.model_b", "column": "id"}],
            "val": [{"source": "public.model_a_src", "column": "val"}],
        }

    monkeypatch.setattr(
        "fastflowtransform.cli.docs_utils.lineage_mod.infer_sql_lineage",
        fake_infer_sql_lineage,
    )

    # fake columns: _collect_columns(executor) → relation
    # -> list of objects with .name/.dtype/.nullable
    class Col:
        def __init__(self, name: str, dtype: str = "TEXT", nullable: bool = True):
            self.name = name
            self.dtype = dtype
            self.nullable = nullable

    def fake_collect_columns(executor):
        return {
            "public.model_a": [Col("id"), Col("val")],
            "public.model_b": [Col("id")],
        }

    monkeypatch.setattr(
        "fastflowtransform.cli.docs_utils._collect_columns",
        fake_collect_columns,
    )

    # docs metadata - model level
    def fake_read_docs_metadata(project_dir: Path):
        return {
            "models": {
                "model_a": {
                    "description_html": "<p>Model A</p>",
                    "columns": {"id": "<b>ID</b>"},
                }
            },
            "columns": {
                "public.model_b": {"id": "<p>identifier</p>"},
            },
        }

    monkeypatch.setattr(
        "fastflowtransform.cli.docs_utils.read_docs_metadata",
        fake_read_docs_metadata,
    )

    manifest = docs_utils._build_docs_manifest(
        project_dir=proj,
        nodes=fake_nodes,
        executor=fake_executor,
        env_name="dev",
    )

    # basic shape
    assert manifest["project"] == "MyProj"
    assert "generated_at" in manifest
    # ISO-ish timestamp
    datetime.fromisoformat(manifest["generated_at"].replace("Z", "+00:00"))
    models = manifest["models"]
    # two models
    assert {m["name"] for m in models} == {"model_a", "model_b"}

    m_a = next(m for m in models if m["name"] == "model_a")
    assert m_a["relation"] == "public.model_a"
    assert m_a["description"] == "Model A"
    # depends_on / used_by
    assert m_a["depends_on"] == ["model_b"]
    # model_b should list model_a as used_by
    m_b = next(m for m in models if m["name"] == "model_b")
    assert m_b["used_by"] == ["model_a"]

    # columns should include lineage
    cols_a = {c["name"]: c for c in m_a["columns"]}
    assert "id" in cols_a
    assert cols_a["id"]["lineage"]  # lineage present


@pytest.mark.unit
def test_build_docs_manifest_python_branch(monkeypatch, tmp_path: Path):
    """
    Exercise the python-branch: n.kind == "python" → lineage_mod.infer_py_lineage.
    """
    proj = tmp_path

    # one python model
    n = SimpleNamespace(
        name="py_model",
        deps=[],
        kind="python",
        meta={},
        requires=None,
    )
    fake_nodes = {"py_model": n}

    # fake REGISTRY with env + py_funcs
    def fake_py_func():
        return 1

    REGISTRY_stub = SimpleNamespace(
        env=object(),
        py_funcs={"py_model": fake_py_func},
    )
    monkeypatch.setattr("fastflowtransform.cli.docs_utils.REGISTRY", REGISTRY_stub)

    # relation_for stub
    monkeypatch.setattr(
        "fastflowtransform.cli.docs_utils.relation_for",
        lambda name: f"public.{name}",
    )

    # no columns from executor
    monkeypatch.setattr(
        "fastflowtransform.cli.docs_utils._collect_columns",
        lambda executor: {"public.py_model": []},
    )

    # docs metadata empty
    monkeypatch.setattr(
        "fastflowtransform.cli.docs_utils.read_docs_metadata",
        lambda project_dir: {},
    )

    # fake python-lineage
    monkeypatch.setattr(
        "fastflowtransform.cli.docs_utils.lineage_mod.infer_py_lineage",
        lambda func, requires, _: {"x": [{"source": "input.tbl", "column": "id"}]},
    )

    manifest = docs_utils._build_docs_manifest(
        project_dir=proj,
        nodes=fake_nodes,
        executor=SimpleNamespace(),  # not used in python branch for rendering
        env_name="dev",
    )

    assert manifest["project"] == proj.name
    assert manifest["models"][0]["name"] == "py_model"
    # it should still produce the "columns" field, even if empty
    assert "columns" in manifest["models"][0]

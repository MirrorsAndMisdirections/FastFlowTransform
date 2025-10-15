# tests/common/test_docs_materialization_badges.py
from pathlib import Path
from flowforge.core import Node
from flowforge.docs import render_site

def _mk_node(tmp: Path, name: str, kind: str, materialized: str):
    p = tmp / f"{name}.sql"
    p.write_text("-- stub\n", encoding="utf-8")
    n = Node(name=name, kind=kind, path=p, deps=[])
    # attach meta dynamically (docs.py reads getattr(n, "meta", {}))
    setattr(n, "meta", {"materialized": materialized})
    return n

def test_docs_show_badges_in_index_and_detail(tmp_path: Path):
    # arrange: 3 nodes with different materializations
    a = _mk_node(tmp_path, "a_table", "sql", "table")
    b = _mk_node(tmp_path, "b_view", "sql", "view")
    c = _mk_node(tmp_path, "c_ephemeral", "sql", "ephemeral")
    nodes = {a.name: a, b.name: b, c.name: c}

    out = tmp_path / "site"
    render_site(out, nodes, executor=None)

    index = (out / "index.html").read_text(encoding="utf-8")
    # legend badges present
    assert "badge-table" in index
    assert "badge-view" in index
    assert "badge-ephemeral" in index
    assert "Materialization" in index

    # per-model pages have correct badge class
    assert "badge-table" in (out / "a_table.html").read_text(encoding="utf-8")
    assert "badge-view" in (out / "b_view.html").read_text(encoding="utf-8")
    assert "badge-ephemeral" in (out / "c_ephemeral.html").read_text(encoding="utf-8")

def test_docs_default_materialization_is_table(tmp_path: Path):
    # node without meta → should default to 'table' in docs
    p = tmp_path / "m.sql"
    p.write_text("-- stub\n", encoding="utf-8")
    n = Node(name="m", kind="sql", path=p, deps=[])
    nodes = {"m": n}

    out = tmp_path / "site"
    render_site(out, nodes, executor=None)

    index = (out / "index.html").read_text(encoding="utf-8")
    assert "badge-table" in index

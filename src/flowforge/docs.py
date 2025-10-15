# src/flowforge/docs.py
from __future__ import annotations

import re
from dataclasses import dataclass
from pathlib import Path
from typing import Any

from jinja2 import Environment, FileSystemLoader, select_autoescape
from markupsafe import Markup

from .core import Node, REGISTRY, relation_for
from .dag import mermaid as dag_mermaid


@dataclass
class ModelDoc:
    name: str
    kind: str
    path: str
    relation: str
    deps: list[str]
    materialized: str


def _safe_filename(name: str) -> str:
    """Filename sanitized while keeping dots/slashes meaningful."""
    s = re.sub(r"[^A-Za-z0-9_.-]", "_", name)
    return s or "_model"


def _collect_columns(executor) -> dict[str, list[ColumnInfo]]:
    if hasattr(executor, "con"):  # DuckDB
        return _columns_duckdb(executor.con)
    if hasattr(executor, "engine"):  # Postgres
        return _columns_postgres(executor.engine)
    return {}


def render_site(out_dir: Path, nodes: dict[str, Node], executor: Any | None = None):
    out_dir.mkdir(parents=True, exist_ok=True)

    # Load templates bundled with the package
    tmpl_dir = Path(__file__).parent / "templates"
    env = Environment(
        loader=FileSystemLoader([str(tmpl_dir)]),
        autoescape=select_autoescape(["html", "xml"]),
    )

    # 1) Mermaid source from dag.mermaid (avoid duplication)
    mermaid_src = dag_mermaid(nodes)

    # 2) Model data for table/detail pages
    models = [
        ModelDoc(
            name=n.name,
            kind=n.kind,
            path=str(n.path),
            relation=relation_for(n.name),
            deps=list(n.deps or []),
            materialized=(getattr(n, "meta", {}) or {}).get("materialized", "table"),
        )
        for n in nodes.values()
    ]
    models.sort(key=lambda m: m.name)

    materialization_legend = {
        "table": {"label": "table", "class": "badge-table"},
        "view": {"label": "view", "class": "badge-view"},
        "ephemeral": {"label": "ephemeral", "class": "badge-ephemeral"},
    }

    # 3) Write index.html
    index_tmpl = env.get_template("index.html.j2")
    cols_by_table = _collect_columns(executor) if executor else {}
    index_html = index_tmpl.render(
        mermaid_src=Markup(mermaid_src),
        models=models,
        materialization_legend=materialization_legend,
    )
    
    (out_dir / "index.html").write_text(index_html, encoding="utf-8")

    rev: dict[str, list[str]] = {n: [] for n in nodes}
    for n in nodes.values():
        for d in n.deps or []:
            if d in rev:
                rev[d].append(n.name)

    # add macros inventory for index rendering
    macro_list = sorted(
        [{"name": n, "path": str(REGISTRY.macros[n].relative_to(REGISTRY.get_project_dir()))}
         for n in REGISTRY.macros],
        key=lambda x: x["name"]
    )
    # 4) One detail page per model
    model_tmpl = env.get_template("model.html.j2")
    for m in models:
        phys = relation_for(m.name)
        cols = cols_by_table.get(phys, [])
        html = model_tmpl.render(
            m=m,
            used_by=sorted(rev.get(m.name, [])),
            cols=cols,
            macros=macro_list,
            materialization_legend=materialization_legend,
        )
        (out_dir / f"{_safe_filename(m.name)}.html").write_text(html, encoding="utf-8")


@dataclass
class ColumnInfo:
    name: str
    dtype: str
    nullable: bool


def _columns_duckdb(con) -> dict[str, list[ColumnInfo]]:
    rows = con.execute("""
      select table_name, column_name, data_type, is_nullable
      from information_schema.columns
      where table_schema in ('main','temp')
      order by table_name, ordinal_position
    """).fetchall()
    out: dict[str, list[ColumnInfo]] = {}
    for t, c, dt, null in rows:
        out.setdefault(t, []).append(ColumnInfo(c, str(dt), null in (True, "YES", "Yes")))
    return out


def _columns_postgres(engine) -> dict[str, list[ColumnInfo]]:
    from sqlalchemy import text

    with engine.begin() as conn:
        rows = conn.execute(
            text("""
          select table_name, column_name, data_type, is_nullable
          from information_schema.columns
          where table_schema = current_schema()
          order by table_name, ordinal_position
        """)
        ).fetchall()
    out: dict[str, list[ColumnInfo]] = {}
    for t, c, dt, null in rows:
        out.setdefault(t, []).append(ColumnInfo(c, str(dt), null == "YES"))
    return out

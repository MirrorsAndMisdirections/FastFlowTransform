# fastflowtransform/docs.py
from __future__ import annotations

import re
from dataclasses import dataclass
from pathlib import Path
from typing import Any

import yaml
from jinja2 import Environment, FileSystemLoader, select_autoescape
from markupsafe import Markup
from sqlalchemy import text

from .core import REGISTRY, Node, relation_for
from .dag import mermaid as dag_mermaid
from .lineage import (
    infer_py_lineage,
    infer_sql_lineage,
    merge_lineage,
    parse_sql_lineage_overrides,
)


@dataclass
class ModelDoc:
    name: str
    kind: str
    path: str
    relation: str
    deps: list[str]
    materialized: str
    description_html: str | None = None
    description_short: str | None = None


def _safe_filename(name: str) -> str:
    """Filename sanitized while keeping dots/slashes meaningful."""
    s = re.sub(r"[^A-Za-z0-9_.-]", "_", name)
    return s or "_model"


def _collect_columns(executor: Any) -> dict[str, list[ColumnInfo]]:
    """
    Best-effort schema discovery for supported engines.
    Returns an empty mapping if unsupported or on errors.
    """
    try:
        if hasattr(executor, "con"):  # DuckDB
            return _columns_duckdb(executor.con)
        if hasattr(executor, "engine"):  # Postgres
            return _columns_postgres(executor.engine)
        if hasattr(executor, "session"):
            return _columns_snowflake(executor.session)
    except Exception:
        # Fail-open: no schema info, UI will simply hide the columns card.
        return {}
    return {}


def _read_project_yaml_docs(project_dir: Path) -> dict[str, Any]:
    """
    Read optional docs metadata from project.yml:
      docs:
        models:
          <node_name>:
            description: "..."
            columns:
              <column_name>: "..."
    Returns a dict keyed by logical node name.
    """
    cfg_path = project_dir / "project.yml"
    if not cfg_path.exists():
        return {}
    try:
        cfg = yaml.safe_load(cfg_path.read_text(encoding="utf-8")) or {}
    except Exception:
        return {}
    docs = (cfg or {}).get("docs") or {}
    models = (docs or {}).get("models") or {}
    return models if isinstance(models, dict) else {}


_FRONT_MATTER_RE = re.compile(r"^\s*---\s*\n(.*?)\n---\s*\n?", re.DOTALL)
_LINK_RE = re.compile(r"\[([^\]]+)\]\(([^)]+)\)")
_CODE_RE = re.compile(r"`([^`]+)`")


def _render_minimarkdown(md: str) -> str:
    """
    Very small Markdown-to-HTML converter (no external deps).
    Supports: paragraphs, inline code, links.
    """
    if not md:
        return ""
    # Strip front matter if present (already parsed elsewhere)
    body = _FRONT_MATTER_RE.sub("", md, count=1)
    # Inline code
    body = _CODE_RE.sub(r"<code>\1</code>", body)
    # Links
    body = _LINK_RE.sub(r'<a href="\2" target="_blank" rel="noopener">\1</a>', body)
    # Split into paragraphs on blank lines
    parts = [p.strip() for p in re.split(r"\n\s*\n", body) if p.strip()]
    html = "".join(f"<p>{p.replace('\n', '<br/>')}</p>" for p in parts) if parts else ""
    return html


def _strip_html(text: str) -> str:
    """Remove very simple HTML tags for generating a short preview snippet."""
    if not text:
        return ""
    t = re.sub(r"<[^>]+>", "", text)
    return re.sub(r"\s+", " ", t).strip()


def _read_markdown_file(p: Path) -> tuple[dict[str, Any], str]:
    """
    Read a Markdown file with optional YAML front matter.
    Returns (front_matter_dict, body_text).
    """
    if not p.exists():
        return {}, ""
    raw = p.read_text(encoding="utf-8")
    m = _FRONT_MATTER_RE.match(raw)
    if m:
        try:
            fm = yaml.safe_load(m.group(1)) or {}
        except Exception:
            fm = {}
        body = raw[m.end() :]
        return (fm if isinstance(fm, dict) else {}), body
    return {}, raw


def _init_jinja() -> Environment:
    """Load bundled templates and return a Jinja environment."""
    tmpl_dir = Path(__file__).parent / "templates"
    return Environment(
        loader=FileSystemLoader([str(tmpl_dir)]),
        autoescape=select_autoescape(["html", "xml"]),
    )


def _get_project_dir() -> Path | None:
    """Best-effort resolution of the project dir from the registry."""
    if not hasattr(REGISTRY, "get_project_dir"):
        return None
    try:
        return REGISTRY.get_project_dir()
    except Exception:
        return None


def _materialization_legend() -> dict[str, dict[str, str]]:
    return {
        "table": {"label": "table", "class": "badge-table"},
        "view": {"label": "view", "class": "badge-view"},
        "ephemeral": {"label": "ephemeral", "class": "badge-ephemeral"},
    }


def _build_macro_list(proj_dir: Path | None) -> list[dict[str, str]]:
    macro_list: list[dict[str, str]] = []
    for name, p in getattr(REGISTRY, "macros", {}).items():
        mp = Path(p)
        rel = mp.name
        kind = "python" if p.suffix.lower() == ".py" else "sql"
        if proj_dir:
            try:
                rel = str(mp.relative_to(proj_dir))
            except Exception:
                rel = mp.name
        macro_list.append({"name": name, "path": rel, "kind": kind})
    macro_list.sort(key=lambda x: (x["kind"], x["name"]))
    return macro_list


def _collect_models(nodes: dict[str, Node]) -> list[ModelDoc]:
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
    return models


def _apply_descriptions_to_models(
    models: list[ModelDoc],
    docs_meta: dict[str, Any],
    cols_by_table: dict[str, list[ColumnInfo]],
    *,
    with_schema: bool,
) -> None:
    """Enrich ModelDoc + ColumnInfo mit Description-HTML (bereits gemergt in docs_meta)."""
    for m in models:
        model_meta = (
            (docs_meta.get("models", {}) or {}).get(m.name, {})
            if isinstance(docs_meta, dict)
            else {}
        )
        desc_html: str | None = model_meta.get("description_html")
        m.description_html = desc_html
        char_limit = 160
        if desc_html:
            short = _strip_html(desc_html)
            m.description_short = (short[:char_limit] + "…") if len(short) > char_limit else short
        else:
            m.description_short = None

        if not with_schema or m.relation not in cols_by_table:
            continue
        rel_desc_map = (docs_meta.get("columns", {}) or {}).get(m.relation, {})
        mdl_desc_map = model_meta.get("columns") or {}
        for c in cols_by_table[m.relation]:
            c.description_html = rel_desc_map.get(c.name) or mdl_desc_map.get(c.name)


def _infer_and_attach_lineage(
    models: list[ModelDoc],
    executor: Any | None,
    docs_meta: dict[str, Any],
    cols_by_table: dict[str, list[ColumnInfo]],
    *,
    with_schema: bool,
) -> None:
    """Best-effort Lineage ermitteln (SQL/Python) und auf Columns mappen."""
    for m in models:
        inferred: dict[str, list[dict[str, Any]]] = {}
        try:
            if m.kind == "sql" and executor is not None:
                try:
                    rendered = executor.render_sql(
                        REGISTRY.nodes[m.name],
                        REGISTRY.env,
                        ref_resolver=lambda nm: executor._resolve_ref(nm, REGISTRY.env),
                        source_resolver=executor._resolve_source,
                    )
                except Exception:
                    rendered = None
                if rendered:
                    inferred = infer_sql_lineage(rendered)
                    overrides = parse_sql_lineage_overrides(rendered)
                    inferred = merge_lineage(inferred, overrides)
            elif m.kind == "python":
                func = getattr(REGISTRY, "py_funcs", {}).get(m.name)
                inferred = infer_py_lineage(func)
        except Exception:
            inferred = {}

        # YAML overrides (bereits in docs_meta gemerged)
        model_meta = (
            (docs_meta.get("models", {}) or {}).get(m.name, {})
            if isinstance(docs_meta, dict)
            else {}
        )
        ylin = model_meta.get("lineage") if isinstance(model_meta, dict) else None
        if isinstance(ylin, dict):
            # Normalisieren: { out_col: {from:[{table,col}], transformed:bool} }
            norm: dict[str, list[dict[str, Any]]] = {}
            for out_col, spec in ylin.items():
                if not isinstance(spec, dict):
                    continue
                transformed_flag = bool(spec.get("transformed"))
                items: list[dict[str, Any]] = []
                for s in spec.get("from", []) or []:
                    if isinstance(s, dict) and "table" in s and "column" in s:
                        items.append(
                            {
                                "from_relation": s["table"],
                                "from_column": s["column"],
                                "transformed": transformed_flag,
                            }
                        )
                if items:
                    norm[out_col] = items
            if norm:
                inferred = merge_lineage(inferred, norm)

        if with_schema and (m.relation in cols_by_table) and inferred:
            for c in cols_by_table[m.relation]:
                if c.name in inferred:
                    c.lineage = inferred[c.name]


def _reverse_deps(nodes: dict[str, Node]) -> dict[str, list[str]]:
    rev: dict[str, list[str]] = {n: [] for n in nodes}
    for n in nodes.values():
        for d in n.deps or []:
            if d in rev:
                rev[d].append(n.name)
    return {k: sorted(v) for k, v in rev.items()}


def _render_index(env: Environment, out_dir: Path, **ctx: Any) -> None:
    html = env.get_template("index.html.j2").render(**ctx)
    (out_dir / "index.html").write_text(html, encoding="utf-8")


def _render_model_pages(
    env: Environment,
    out_dir: Path,
    models: list[ModelDoc],
    used_by: dict[str, list[str]],
    cols_by_table: dict[str, list[ColumnInfo]],
    materialization_legend: dict[str, dict[str, str]],
    macros: list[dict[str, str]],
) -> None:
    tmpl = env.get_template("model.html.j2")
    for m in models:
        phys = relation_for(m.name)
        cols = cols_by_table.get(phys, [])
        html = tmpl.render(
            m=m,
            used_by=used_by.get(m.name, []),
            cols=cols,
            macros=macros,
            materialization_legend=materialization_legend,
        )
        (out_dir / f"{_safe_filename(m.name)}.html").write_text(html, encoding="utf-8")


def render_site(
    out_dir: Path,
    nodes: dict[str, Node],
    executor: Any | None = None,
    *,
    with_schema: bool = True,
) -> None:
    out_dir.mkdir(parents=True, exist_ok=True)
    env = _init_jinja()
    mermaid_src = dag_mermaid(nodes)
    proj_dir = _get_project_dir()
    docs_meta = read_docs_metadata(proj_dir) if proj_dir else {"models": {}, "columns": {}}
    models = _collect_models(nodes)
    mat_legend = _materialization_legend()
    macro_list = _build_macro_list(proj_dir)
    cols_by_table = _collect_columns(executor) if (executor and with_schema) else {}

    _apply_descriptions_to_models(models, docs_meta, cols_by_table, with_schema=with_schema)
    _infer_and_attach_lineage(models, executor, docs_meta, cols_by_table, with_schema=with_schema)

    _render_index(
        env,
        out_dir,
        mermaid_src=Markup(mermaid_src),
        models=models,
        materialization_legend=mat_legend,
        macros=macro_list,
    )

    used_by = _reverse_deps(nodes)
    _render_model_pages(
        env,
        out_dir,
        models=models,
        used_by=used_by,
        cols_by_table=cols_by_table,
        materialization_legend=mat_legend,
        macros=macro_list,
    )


# def render_site(
#     out_dir: Path,
#     nodes: dict[str, Node],
#     executor: Any | None = None,
#     *,
#     with_schema: bool = True,
# ) -> None:
#     out_dir.mkdir(parents=True, exist_ok=True)

#     # Load templates bundled with the package
#     tmpl_dir = Path(__file__).parent / "templates"
#     env = Environment(
#         loader=FileSystemLoader([str(tmpl_dir)]),
#         autoescape=select_autoescape(["html", "xml"]),
#     )

#     # 1) Mermaid source from dag.mermaid (avoid duplication)
#     mermaid_src = dag_mermaid(nodes)

#     # 2) Optional docs metadata from project + markdown (Descriptions)
#     proj_dir: Path | None = None
#     if hasattr(REGISTRY, "get_project_dir"):
#         try:
#             proj_dir = REGISTRY.get_project_dir()
#         except Exception:
#             proj_dir = None
#     docs_meta = read_docs_metadata(proj_dir) if proj_dir else {"models": {}, "columns": {}}

#     # 3) Model data for table/detail pages
#     models = [
#         ModelDoc(
#             name=n.name,
#             kind=n.kind,
#             path=str(n.path),
#             relation=relation_for(n.name),
#             deps=list(n.deps or []),
#             materialized=(getattr(n, "meta", {}) or {}).get("materialized", "table"),
#             description_html=None,
#             description_short=None,
#         )
#         for n in nodes.values()
#     ]
#     models.sort(key=lambda m: m.name)

#     materialization_legend = {
#         "table": {"label": "table", "class": "badge-table"},
#         "view": {"label": "view", "class": "badge-view"},
#         "ephemeral": {"label": "ephemeral", "class": "badge-ephemeral"},
#     }

#     # Build macro inventory BEFORE rendering index
#     macro_list: list[dict[str, str]] = []
#     for name, p in getattr(REGISTRY, "macros", {}).items():
#         mp = Path(p)
#         rel = mp.name
#         kind = "python" if p.suffix.lower() == ".py" else "sql"
#         if proj_dir:
#             try:
#                 rel = str(mp.relative_to(proj_dir))
#             except Exception:
#                 rel = mp.name
#         macro_list.append({"name": name, "path": rel, "kind": kind})
#     macro_list.sort(key=lambda x: (x["kind"], x["name"]))

#     # 4) Collect columns (schema) and enrich with descriptions
#     index_tmpl = env.get_template("index.html.j2")
#     cols_by_table = _collect_columns(executor) if (executor and with_schema) else {}

#     # Enrich models with descriptions (already merged: Markdown > YAML)
#     for m in models:
#         model_meta = (
#             (docs_meta.get("models", {}) or {}).get(m.name, {})
#             if isinstance(docs_meta, dict)
#             else {}
#         )

#         # --- Model description (HTML already) ---
#         desc_html = model_meta.get("description_html")
#         m.description_html = desc_html
#         desc_char_limit = 160
#         if desc_html:
#             short = _strip_html(desc_html)
#             m.description_short = (
#                 (short[:desc_char_limit] + "…") if len(short) > desc_char_limit else short
#             )
#         else:
#             m.description_short = None

#         # --- Column descriptions (HTML) ---
#         # Priority: docs/columns/<relation>/<column>.md  >  YAML under docs.models[model].columns
#         rel_desc_map = (docs_meta.get("columns", {}) or {}).get(m.relation, {})
#         mdl_desc_map = model_meta.get("columns") or {}

#         if with_schema and (m.relation in cols_by_table):
#             for c in cols_by_table[m.relation]:
#                 # Both maps contain HTML already; no extra rendering here
#                 c.description_html = rel_desc_map.get(c.name) or mdl_desc_map.get(c.name)

#         # ---- Column lineage (best-effort)
#         inferred_lineage: dict[str, list[dict[str, Any]]] = {}
#         try:
#             if m.kind == "sql" and executor is not None:
#                 # Render SQL and infer FROM/JOIN alias map + select expressions
#                 try:
#                     rendered = executor.render_sql(
#                         REGISTRY.nodes[m.name],
#                         REGISTRY.env,
#                         ref_resolver=lambda nm: executor._resolve_ref(nm, REGISTRY.env),
#                         source_resolver=executor._resolve_source,
#                     )
#                 except Exception:
#                     rendered = None

#                 if rendered:
#                     # Heuristic inference
#                     inferred = infer_sql_lineage(rendered)
#                     # Optional comment-based overrides
#                     overrides = parse_sql_lineage_overrides(rendered)
#                     inferred_lineage = merge_lineage(inferred, overrides)

#             elif m.kind == "python":
#                 func = getattr(REGISTRY, "py_funcs", {}).get(m.name)
#                 inferred_lineage = infer_py_lineage(func)
#         except Exception:
#             inferred_lineage = {}

#         # YAML overrides for lineage (if present) win over inference
#         try:
#             model_meta = (
#                 (docs_meta.get("models", {}) or {}).get(m.name, {})
#                 if isinstance(docs_meta, dict)
#                 else {}
#             )
#             ylin = model_meta.get("lineage") if isinstance(model_meta, dict) else None
#             if isinstance(ylin, dict):
#                 # Normalize YAML shape
#                 norm: dict[str, list[dict[str, Any]]] = {}
#                 for out_col, spec in ylin.items():
#                     # Expect: { from: [{table:.., column:..}], transformed: bool }
#                     items: list[dict[str, Any]] = []
#                     sources = spec.get("from", []) if isinstance(spec, dict) else []
#                     transformed_flag = (
#                         bool(spec.get("transformed")) if isinstance(spec, dict) else False
#                     )
#                     for s in sources:
#                         if isinstance(s, dict) and "table" in s and "column" in s:
#                             items.append(
#                                 {
#                                     "from_relation": s["table"],
#                                     "from_column": s["column"],
#                                     "transformed": transformed_flag,
#                                 }
#                             )
#                     if items:
#                         norm[out_col] = items
#                 if norm:
#                     inferred_lineage = merge_lineage(inferred_lineage, norm)
#         except Exception:
#             pass

#         # Attach lineage to columns (if we know the schema)
#         if with_schema and (m.relation in cols_by_table) and inferred_lineage:
#             for c in cols_by_table[m.relation]:
#                 if c.name in inferred_lineage:
#                     c.lineage = inferred_lineage[c.name]

#     index_html = index_tmpl.render(
#         mermaid_src=Markup(mermaid_src),
#         models=models,
#         materialization_legend=materialization_legend,
#         macros=macro_list,  # ← HIER reinreichen
#     )
#     (out_dir / "index.html").write_text(index_html, encoding="utf-8")

#     rev: dict[str, list[str]] = {n: [] for n in nodes}
#     for n in nodes.values():
#         for d in n.deps or []:
#             if d in rev:
#                 rev[d].append(n.name)

#     # 5) One detail page per model (pass descriptions + column descriptions)
#     model_tmpl = env.get_template("model.html.j2")
#     for m in models:
#         phys = relation_for(m.name)
#         cols = cols_by_table.get(phys, [])
#         html = model_tmpl.render(
#             m=m,
#             used_by=sorted(rev.get(m.name, [])),
#             cols=cols,
#             macros=macro_list,
#             materialization_legend=materialization_legend,
#         )
#         (out_dir / f"{_safe_filename(m.name)}.html").write_text(html, encoding="utf-8")


@dataclass
class ColumnInfo:
    name: str
    dtype: str
    nullable: bool
    description_html: str | None = None
    lineage: list[dict[str, Any]] | None = None


def _columns_duckdb(con: Any) -> dict[str, list[ColumnInfo]]:
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


def _columns_postgres(engine: Any) -> dict[str, list[ColumnInfo]]:
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


def _columns_snowflake(session: Any) -> dict[str, list[ColumnInfo]]:
    rows = session.sql("""
      select table_name, column_name, data_type, is_nullable
      from information_schema.columns
      where table_schema = current_schema()
      order by table_name, ordinal_position
    """).collect()
    out: dict[str, list[ColumnInfo]] = {}
    for r in rows:
        t = r["TABLE_NAME"]
        c = r["COLUMN_NAME"]
        dt = r["DATA_TYPE"]
        null = r["IS_NULLABLE"]
        out.setdefault(t, []).append(ColumnInfo(c, str(dt), null == "YES"))
    return out


def read_docs_metadata(project_dir: Path) -> dict[str, Any]:
    """
    Merge YAML + Markdown descriptions with priority: Markdown > YAML.
    Returns:
      {
        "models": {
          <model>: {
            "description_html": "<p>…</p>" | None,
            "columns": { <col>: "<p>…</p>" }
          },
        },
        "columns": { <relation>: { <col>: "<p>…</p>" } }
      }
    """
    # 1) YAML (from project.yml → docs.models)
    yaml_models = _read_project_yaml_docs(project_dir)  # {model: {description, columns{}}}
    out_models: dict[str, dict[str, Any]] = {}
    for model, meta in yaml_models.items() if isinstance(yaml_models, dict) else []:
        desc = (meta or {}).get("description")
        cols = (meta or {}).get("columns") or {}
        lineage_yaml = (meta or {}).get("lineage")

        out_models[model] = {
            "description_html": _render_minimarkdown(desc) if desc else None,
            "columns": {
                str(k): _render_minimarkdown(str(v))
                for k, v in (cols.items() if isinstance(cols, dict) else [])
            },
        }
        if isinstance(lineage_yaml, dict):
            out_models[model]["lineage"] = lineage_yaml

    # 2) Markdown model overrides: docs/models/<model>.md
    md_models_dir = project_dir / "docs" / "models"
    if md_models_dir.exists():
        for p in md_models_dir.glob("*.md"):
            model_name = p.stem
            _, body = _read_markdown_file(p)
            if body.strip():
                out_models.setdefault(model_name, {"description_html": None, "columns": {}})
                out_models[model_name]["description_html"] = _render_minimarkdown(body)

    # 3) Markdown column overrides: docs/columns/<relation>/<column>.md
    out_columns: dict[str, dict[str, str]] = {}
    cols_root = project_dir / "docs" / "columns"
    if cols_root.exists():
        for rel_dir in cols_root.iterdir():
            if not rel_dir.is_dir():
                continue
            rel = rel_dir.name
            for p in rel_dir.glob("*.md"):
                col = p.stem
                _, body = _read_markdown_file(p)
                if body.strip():
                    out_columns.setdefault(rel, {})[col] = _render_minimarkdown(body)

    return {"models": out_models, "columns": out_columns}

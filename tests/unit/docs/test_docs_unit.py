# tests/unit/docs/test_docs_unit.py
from __future__ import annotations

import textwrap
from pathlib import Path
from types import SimpleNamespace
from typing import Any, cast

import pytest

import fastflowtransform.docs as docs_mod
from fastflowtransform.core import Node

# ---------------------------------------------------------------------------
# Helper fakes
# ---------------------------------------------------------------------------


class _FakeTemplate:
    def __init__(self, content: str):
        self._content = content

    def render(self, **ctx: Any) -> str:
        # handle exactly the two patterns we need in the tests
        if "{m.name}" in self._content and "m" in ctx:
            m = ctx["m"]
            return f"MODEL {getattr(m, 'name', 'UNKNOWN')}"
        if "{mermaid_src}" in self._content and "mermaid_src" in ctx:
            return f"INDEX {ctx['mermaid_src']}"
        # fallback: just dump keys
        return self._content


class _FakeEnv:
    def __init__(self):
        # name -> template
        self._tmpls: dict[str, _FakeTemplate] = {
            "index.html.j2": _FakeTemplate("INDEX {mermaid_src}"),
            "model.html.j2": _FakeTemplate("MODEL {m.name}"),
        }

    def get_template(self, name: str) -> _FakeTemplate:
        return self._tmpls[name]


# ---------------------------------------------------------------------------
# Unit tests for small helpers
# ---------------------------------------------------------------------------


@pytest.mark.unit
def test_safe_filename_keeps_dots_and_slashes_as_underscore():
    assert docs_mod._safe_filename("abc.sql") == "abc.sql"
    assert docs_mod._safe_filename("a b") == "a_b"
    # exotic chars
    assert docs_mod._safe_filename("äöü") == "___"
    # empty → fallback
    assert docs_mod._safe_filename("") == "_model"


@pytest.mark.unit
def test_render_minimarkdown_inline_code_and_links():
    md = textwrap.dedent(
        """
        Some text with `code` and a [link](https://example.com).

        Second paragraph.
        """
    ).strip()
    html = docs_mod._render_minimarkdown(md)
    assert "<code>code</code>" in html
    assert '<a href="https://example.com"' in html
    # 2 paragraphs
    assert "<p>Some text" in html
    assert "<p>Second paragraph." in html


@pytest.mark.unit
def test_strip_html_removes_tags_and_collapses_ws():
    txt = "<p>Hello <b>World</b></p>\n  <span> X </span>"
    assert docs_mod._strip_html(txt) == "Hello World X"


@pytest.mark.unit
def test_read_markdown_file_with_front_matter(tmp_path: Path):
    p = tmp_path / "doc.md"
    p.write_text(
        "---\ntitle: Hello\n---\nBody text\n",
        encoding="utf-8",
    )
    fm, body = docs_mod._read_markdown_file(p)
    assert fm == {"title": "Hello"}
    assert body.strip() == "Body text"


@pytest.mark.unit
def test_read_markdown_file_no_front_matter(tmp_path: Path):
    p = tmp_path / "doc.md"
    p.write_text("Plain body", encoding="utf-8")
    fm, body = docs_mod._read_markdown_file(p)
    assert fm == {}
    assert body == "Plain body"


@pytest.mark.unit
def test_reverse_deps_builds_inverse_graph():
    nodes = {
        "a": SimpleNamespace(name="a", deps=["b", "c"]),
        "b": SimpleNamespace(name="b", deps=["c"]),
        "c": SimpleNamespace(name="c", deps=[]),
    }
    rev = docs_mod._reverse_deps(nodes)  # type: ignore[arg-type]
    # c is used by a and b
    assert rev["c"] == ["a", "b"]
    # b is used by a
    assert rev["b"] == ["a"]
    # a is not used by anyone
    assert rev["a"] == []


@pytest.mark.unit
def test_materialization_legend_has_incremental():
    legend = docs_mod._materialization_legend()
    assert "incremental" in legend
    assert legend["incremental"]["label"] == "incremental"


# ---------------------------------------------------------------------------
# read_docs_metadata
# ---------------------------------------------------------------------------


@pytest.mark.unit
def test_read_docs_metadata_merges_yaml_and_markdown(tmp_path: Path):
    # project.yml with docs
    (tmp_path / "project.yml").write_text(
        textwrap.dedent(
            """
            docs:
              models:
                my_model:
                  description: "YAML desc"
                  columns:
                    col1: "YAML col1"
                  lineage:
                    col1:
                      from:
                        - table: src.t
                          column: x
                      transformed: true
            """
        ),
        encoding="utf-8",
    )

    # Markdown model override (should win over YAML description)
    md_dir = tmp_path / "docs" / "models"
    md_dir.mkdir(parents=True)
    (md_dir / "my_model.md").write_text("Markdown desc", encoding="utf-8")

    # Markdown column override
    col_dir = tmp_path / "docs" / "columns" / "my_model"
    col_dir.mkdir(parents=True)
    (col_dir / "col1.md").write_text("MD col1", encoding="utf-8")

    meta = docs_mod.read_docs_metadata(tmp_path)

    # model present
    assert "my_model" in meta["models"]
    # description_html from MD wins
    assert "Markdown desc" in meta["models"]["my_model"]["description_html"]
    # YAML lineage preserved
    assert "lineage" in meta["models"]["my_model"]
    # column override present
    assert meta["columns"]["my_model"]["col1"].startswith("<p>MD col1")


# ---------------------------------------------------------------------------
# _apply_descriptions_to_models
# ---------------------------------------------------------------------------


@pytest.mark.unit
def test_apply_descriptions_to_models_applies_short_and_column_desc():
    models = [
        docs_mod.ModelDoc(
            name="m1",
            kind="sql",
            path="models/m1.sql",
            relation="db.sc.m1",
            deps=[],
            materialized="table",
        )
    ]
    docs_meta = {
        "models": {
            "m1": {
                "description_html": "<p>Hello world</p>",
                "columns": {"col1": "<p>Col 1</p>"},
            }
        },
        "columns": {
            "db.sc.m1": {
                "col2": "<p>Col 2</p>",
            }
        },
    }
    cols_by_table = {
        "db.sc.m1": [
            docs_mod.ColumnInfo("col1", "STRING", True),
            docs_mod.ColumnInfo("col2", "INT", False),
        ]
    }

    docs_mod._apply_descriptions_to_models(models, docs_meta, cols_by_table, with_schema=True)

    assert models[0].description_html == "<p>Hello world</p>"
    assert models[0].description_short == "Hello world"
    # column 1 desc from model-level
    assert cols_by_table["db.sc.m1"][0].description_html == "<p>Col 1</p>"
    # column 2 desc from relation-level
    assert cols_by_table["db.sc.m1"][1].description_html == "<p>Col 2</p>"


# ---------------------------------------------------------------------------
# render_site (with patched jinja + registry)
# ---------------------------------------------------------------------------


@pytest.mark.unit
def test_render_site_writes_index_and_model_pages(tmp_path: Path, monkeypatch: pytest.MonkeyPatch):
    fake_nodes_raw = {
        "model_a": SimpleNamespace(
            name="model_a",
            kind="sql",
            path=tmp_path / "models" / "model_a.sql",
            deps=["model_b"],
            meta={"materialized": "view"},
        ),
        "model_b": SimpleNamespace(
            name="model_b",
            kind="python",
            path=tmp_path / "models" / "model_b.py",
            deps=[],
            meta={},
        ),
    }

    monkeypatch.setattr(
        docs_mod,
        "REGISTRY",
        SimpleNamespace(
            nodes=fake_nodes_raw,
            macros={},
            get_project_dir=lambda: tmp_path,
        ),
        raising=True,
    )

    monkeypatch.setattr(docs_mod, "_init_jinja", lambda: _FakeEnv(), raising=True)
    fake_nodes = cast(dict[str, Node], fake_nodes_raw)

    docs_mod.render_site(tmp_path, fake_nodes, executor=None, with_schema=False)

    index_file = tmp_path / "index.html"
    assert index_file.exists()
    assert "INDEX" in index_file.read_text(encoding="utf-8")

    model_a_file = tmp_path / "model_a.html"
    model_b_file = tmp_path / "model_b.html"
    assert model_a_file.exists()
    assert model_b_file.exists()

    assert "MODEL model_a" in model_a_file.read_text(encoding="utf-8")
    assert "MODEL model_b" in model_b_file.read_text(encoding="utf-8")


# ---------------------------------------------------------------------------
# _collect_columns engine stubs
# ---------------------------------------------------------------------------


@pytest.mark.unit
def test_collect_columns_prefers_spark():
    class FakeCol:
        def __init__(self, name: str):
            self.name = name
            self.dataType = "INT"
            self.nullable = True

    class FakeTable:
        def __init__(self, name: str):
            self.name = name
            self.database = None
            self.catalog = None

    class FakeSparkCatalog:
        def listTables(self):
            return [FakeTable("T1")]

        def listColumns(self, ident, database=None):
            return [FakeCol("C1"), FakeCol("C2")]

    class FakeSpark:
        catalog = FakeSparkCatalog()

    cols = docs_mod._collect_columns(SimpleNamespace(spark=FakeSpark()))
    assert "T1" in cols
    assert [c.name for c in cols["T1"]] == ["C1", "C2"]


@pytest.mark.unit
def test_collect_columns_with_unknown_executor_returns_empty():
    cols = docs_mod._collect_columns(object())
    assert cols == {}


# ---------------------- _columns_duckdb ----------------------


@pytest.mark.unit
def test_columns_duckdb_collects_tables_and_cols():
    class FakeCursor:
        def __init__(self, rows):
            self._rows = rows

        def fetchall(self):
            return self._rows

    class FakeConn:
        def __init__(self, rows):
            self._rows = rows

        def execute(self, _sql: str):
            return FakeCursor(self._rows)

    rows = [
        # table_name, column_name, data_type, is_nullable
        ("my_table", "id", "INTEGER", "NO"),
        ("my_table", "name", "TEXT", "YES"),
        ("other", "x", "BOOLEAN", "YES"),
    ]
    fake_con = FakeConn(rows)

    cols = docs_mod._columns_duckdb(fake_con)

    assert set(cols.keys()) == {"my_table", "other"}
    mt = cols["my_table"]
    assert [c.name for c in mt] == ["id", "name"]
    assert mt[0].dtype == "INTEGER"
    assert mt[0].nullable is False
    assert mt[1].nullable is True


# ---------------------- _columns_postgres ----------------------


@pytest.mark.unit
def test_columns_postgres_collects_from_engine():
    class FakeResult:
        def __init__(self, rows):
            self._rows = rows

        def fetchall(self):
            return self._rows

    class FakeConn:
        def __init__(self, rows):
            self._rows = rows

        def execute(self, _stmt):
            return FakeResult(self._rows)

        def __enter__(self):
            return self

        def __exit__(self, exc_type, exc, tb):
            return False

    class FakeEngine:
        def __init__(self, rows):
            self._rows = rows

        def begin(self):
            return FakeConn(self._rows)

    rows = [
        # table_name, column_name, data_type, is_nullable
        ("public_tbl", "id", "integer", "YES"),
        ("public_tbl", "email", "text", "NO"),
    ]
    fake_engine = FakeEngine(rows)

    cols = docs_mod._columns_postgres(fake_engine)

    assert "public_tbl" in cols
    tcols = cols["public_tbl"]
    assert [c.name for c in tcols] == ["id", "email"]
    assert tcols[0].dtype == "integer"
    # in deiner Implementierung: nullable == "YES"
    assert tcols[0].nullable is True
    assert tcols[1].nullable is False


# ---------------------- _columns_snowflake ----------------------


@pytest.mark.unit
def test_columns_snowflake_collects_from_session():
    # snowflake .collect() liefert list[Row], aber wir können dicts nehmen
    class FakeDF:
        def __init__(self, rows):
            self._rows = rows

        def collect(self):
            return self._rows

    class FakeSession:
        def __init__(self, rows):
            self._rows = rows

        def sql(self, _sql: str):
            return FakeDF(self._rows)

    rows = [
        {
            "TABLE_NAME": "T1",
            "COLUMN_NAME": "ID",
            "DATA_TYPE": "NUMBER",
            "IS_NULLABLE": "NO",
        },
        {
            "TABLE_NAME": "T1",
            "COLUMN_NAME": "NAME",
            "DATA_TYPE": "TEXT",
            "IS_NULLABLE": "YES",
        },
        {
            "TABLE_NAME": "T2",
            "COLUMN_NAME": "TS",
            "DATA_TYPE": "TIMESTAMP_NTZ",
            "IS_NULLABLE": "YES",
        },
    ]
    fake_session = FakeSession(rows)

    cols = docs_mod._columns_snowflake(fake_session)

    assert set(cols.keys()) == {"T1", "T2"}
    t1 = cols["T1"]
    assert [c.name for c in t1] == ["ID", "NAME"]
    assert t1[0].dtype == "NUMBER"
    assert t1[0].nullable is False
    assert t1[1].nullable is True

    t2 = cols["T2"]
    assert t2[0].name == "TS"
    assert t2[0].dtype == "TIMESTAMP_NTZ"
    assert t2[0].nullable is True


@pytest.mark.unit
def test_build_macro_list_collects_and_sorts(monkeypatch: pytest.MonkeyPatch, tmp_path: Path):
    # Arrange
    project_dir = tmp_path
    macros_dir = project_dir / "macros"
    macros_dir.mkdir(parents=True, exist_ok=True)

    m1 = macros_dir / "util_dates.sql"
    m1.write_text("-- sql macro 1", encoding="utf-8")

    m2 = macros_dir / "cleanup.sql"
    m2.write_text("-- sql macro 2", encoding="utf-8")

    m3 = macros_dir / "py_macro.py"
    m3.write_text("def run(): ...", encoding="utf-8")

    fake_registry = SimpleNamespace(
        macros={
            "util_dates": m1,
            "cleanup": m2,
            "py_macro": m3,
        }
    )
    monkeypatch.setattr(docs_mod, "REGISTRY", fake_registry, raising=True)

    # Act
    res = docs_mod._build_macro_list(project_dir)

    # Assert
    names = [x["name"] for x in res]
    assert names == ["py_macro", "cleanup", "util_dates"]

    paths = {x["name"]: x["path"] for x in res}
    assert paths["cleanup"] == "macros/cleanup.sql"
    assert paths["util_dates"] == "macros/util_dates.sql"
    assert paths["py_macro"] == "macros/py_macro.py"

    kinds = {x["name"]: x["kind"] for x in res}
    assert kinds["py_macro"] == "python"
    assert kinds["cleanup"] == "sql"
    assert kinds["util_dates"] == "sql"


@pytest.mark.unit
def test_infer_and_attach_lineage_sql_branch_is_used(monkeypatch: pytest.MonkeyPatch):
    m = docs_mod.ModelDoc(
        name="model_sql",
        kind="sql",
        path="models/model_sql.sql",
        relation="project.dataset.model_sql",
        deps=["src_table"],
        materialized="table",
    )
    models = [m]

    cols_by_table = {
        "project.dataset.model_sql": [
            docs_mod.ColumnInfo("id", "INT", True),
            docs_mod.ColumnInfo("name", "STRING", True),
        ]
    }

    class FakeExecutor:
        def render_sql(self, node, jenv, ref_resolver=None, source_resolver=None):
            return "select id, name from src_table"

        def _resolve_ref(self, name, env):
            return f"resolved_ref_{name}"

        def _resolve_source(self, source, table):
            return f"resolved_source_{source}_{table}"

    fake_executor = FakeExecutor()

    fake_node = SimpleNamespace(
        name="model_sql",
        kind="sql",
        path=Path("models/model_sql.sql"),
        deps=["src_table"],
        meta={},
    )
    fake_registry = SimpleNamespace(
        nodes={"model_sql": fake_node},
        env=SimpleNamespace(),
    )
    monkeypatch.setattr(docs_mod, "REGISTRY", fake_registry, raising=True)

    monkeypatch.setattr(
        docs_mod,
        "infer_sql_lineage",
        lambda rendered: {"id": [{"from_relation": "src_table", "from_column": "id"}]},
        raising=True,
    )
    monkeypatch.setattr(
        docs_mod,
        "parse_sql_lineage_overrides",
        lambda rendered: {},
        raising=True,
    )
    monkeypatch.setattr(
        docs_mod,
        "merge_lineage",
        lambda base, overrides: base,
        raising=True,
    )
    docs_meta: dict[str, object] = {}

    # ACT
    docs_mod._infer_and_attach_lineage(
        models,
        fake_executor,
        docs_meta,
        cols_by_table,
        with_schema=True,
    )

    # ASSERT
    col_id = cols_by_table["project.dataset.model_sql"][0]
    assert col_id.name == "id"
    assert col_id.lineage == [{"from_relation": "src_table", "from_column": "id"}]


@pytest.mark.unit
def test_infer_and_attach_lineage_yaml_override_branch_is_used(monkeypatch: pytest.MonkeyPatch):
    m = docs_mod.ModelDoc(
        name="model_yml",
        kind="sql",
        path="models/model_yml.sql",
        relation="project.dataset.model_yml",
        deps=[],
        materialized="table",
    )
    models = [m]

    cols_by_table = {
        "project.dataset.model_yml": [
            docs_mod.ColumnInfo("total", "NUMBER", True),
            docs_mod.ColumnInfo("cnt", "NUMBER", True),
        ]
    }

    docs_meta = {
        "models": {
            "model_yml": {
                "description_html": None,
                "columns": {},
                "lineage": {
                    "total": {
                        "from": [
                            {"table": "project.dataset.orders", "column": "amount"},
                        ],
                        "transformed": True,
                    }
                },
            }
        },
        "columns": {},
    }

    monkeypatch.setattr(docs_mod, "infer_sql_lineage", lambda *_a, **_k: {}, raising=True)
    monkeypatch.setattr(docs_mod, "parse_sql_lineage_overrides", lambda *_a, **_k: {}, raising=True)
    monkeypatch.setattr(
        docs_mod,
        "merge_lineage",
        lambda base, overrides: (overrides or base),
        raising=True,
    )

    # ACT
    docs_mod._infer_and_attach_lineage(
        models,
        executor=None,
        docs_meta=docs_meta,
        cols_by_table=cols_by_table,
        with_schema=True,
    )

    # ASSERT
    col_total = cols_by_table["project.dataset.model_yml"][0]
    assert col_total.name == "total"
    assert col_total.lineage == [
        {
            "from_relation": "project.dataset.orders",
            "from_column": "amount",
            "transformed": True,
        }
    ]

# tests/unit/cli/test_sync_db_comments_unit.py
from __future__ import annotations

from pathlib import Path
from types import SimpleNamespace
from unittest.mock import MagicMock

import pytest
import typer

import fastflowtransform.cli.sync_db_comments_cmd as mod

# ---------------------------------------------------------------------------
# helper tests
# ---------------------------------------------------------------------------


@pytest.mark.unit
@pytest.mark.cli
def test_strip_html_for_comment_removes_tags_and_collapses_spaces():
    html = "<p>Hello <b>World</b></p>  <br>   again"
    out = mod._strip_html_for_comment(html)
    assert out == "Hello World again"


@pytest.mark.unit
@pytest.mark.cli
def test_strip_html_for_comment_none():
    assert mod._strip_html_for_comment(None) == ""


@pytest.mark.unit
@pytest.mark.cli
def test_pg_quote_ident_escapes_quotes():
    assert mod._pg_quote_ident('my"table') == '"my""table"'


@pytest.mark.unit
@pytest.mark.cli
@pytest.mark.parametrize(
    "schema,relation,expected",
    [
        ("public", "users", '"public"."users"'),
        (None, "public.users", '"public"."users"'),
        (None, "users", '"users"'),
    ],
)
def test_pg_fq_table(schema, relation, expected):
    assert mod._pg_fq_table(schema, relation) == expected


@pytest.mark.unit
@pytest.mark.cli
def test_sql_literal_escapes_single_quotes():
    assert mod._sql_literal("O'Reilly") == "'O''Reilly'"


# ---------------------------------------------------------------------------
# _sync_comments_postgres
# ---------------------------------------------------------------------------


@pytest.mark.unit
@pytest.mark.cli
def test_sync_comments_postgres_dry_run(capsys):
    intents = [
        {"kind": "table", "relation": "users", "text": "Users table"},
        {"kind": "column", "relation": "users", "column": "id", "text": "Primary key"},
    ]
    fake_exec = SimpleNamespace()  # no .engine -> dry_run only

    mod._sync_comments_postgres(fake_exec, intents, schema="public", dry_run=True)

    out = capsys.readouterr().out
    assert 'COMMENT ON TABLE "public"."users" IS \'Users table\';' in out
    assert 'COMMENT ON COLUMN "public"."users"."id" IS \'Primary key\';' in out


@pytest.mark.unit
@pytest.mark.cli
def test_sync_comments_postgres_executes_on_engine(capsys):
    # fake sqlalchemy engine
    fake_conn = MagicMock()
    fake_engine = MagicMock()
    fake_engine.begin.return_value.__enter__.return_value = fake_conn
    fake_exec = SimpleNamespace(engine=fake_engine)

    intents = [
        {"kind": "table", "relation": "users", "text": "Users table"},
    ]

    mod._sync_comments_postgres(fake_exec, intents, schema="public", dry_run=False)

    # sollte genau 1 statement ausführen
    assert fake_conn.execute.call_count == 1
    stmt_arg = fake_conn.execute.call_args[0][0]  # sa_text(...)
    # sqlalchemy.text hat .text oder .textual?
    assert 'COMMENT ON TABLE "public"."users" IS \'Users table\';' in str(stmt_arg)

    out = capsys.readouterr().out
    assert "applied: 1" in out


# ---------------------------------------------------------------------------
# _sync_comments_snowflake
# ---------------------------------------------------------------------------


@pytest.mark.unit
@pytest.mark.cli
def test_sync_comments_snowflake_dry_run(capsys):
    intents = [
        {"kind": "table", "relation": "MY_DB.MY_TBL", "text": "Some table"},
        {"kind": "column", "relation": "MY_DB.MY_TBL", "column": "C1", "text": "Some column"},
    ]
    fake_exec = SimpleNamespace()

    mod._sync_comments_snowflake(fake_exec, intents, schema=None, dry_run=True)

    out = capsys.readouterr().out
    assert "COMMENT ON TABLE MY_DB.MY_TBL IS 'Some table';" in out
    assert "COMMENT ON COLUMN MY_DB.MY_TBL.C1 IS 'Some column';" in out


@pytest.mark.unit
@pytest.mark.cli
def test_sync_comments_snowflake_with_session():
    fake_session = MagicMock()
    fake_exec = SimpleNamespace(session=fake_session)

    intents = [
        {"kind": "table", "relation": "MY_TBL", "text": "T"},
        {"kind": "column", "relation": "MY_TBL", "column": "C1", "text": "C"},
    ]

    mod._sync_comments_snowflake(fake_exec, intents, schema="PUBLIC", dry_run=False)

    # 2 statements expected
    expected_call_count = 2
    assert fake_session.sql.call_count == expected_call_count
    # each should be collected
    fake_session.sql.return_value.collect.assert_called()


@pytest.mark.unit
@pytest.mark.cli
def test_sync_comments_snowflake_with_execute_method():
    exec_mock = SimpleNamespace(execute=MagicMock())

    intents = [
        {"kind": "table", "relation": "MY_TBL", "text": "T"},
    ]

    mod._sync_comments_snowflake(exec_mock, intents, schema=None, dry_run=False)

    exec_mock.execute.assert_called_once_with("COMMENT ON TABLE MY_TBL IS 'T'")


# ---------------------------------------------------------------------------
# sync_db_comments
# ---------------------------------------------------------------------------


@pytest.mark.unit
@pytest.mark.cli
def test_sync_db_comments_no_intents_exits(monkeypatch):
    """
    Fall: es gibt gar keine Descriptions -> sofort Exit(0) mit gelb.
    """
    # fake context
    fake_ctx = SimpleNamespace(
        project=Path("."),
        profile=SimpleNamespace(engine="postgres", postgres=SimpleNamespace(db_schema="public")),
        make_executor=lambda: (MagicMock(), None, None),
    )
    # REGISTRY ohne Nodes
    monkeypatch.setattr(mod, "REGISTRY", SimpleNamespace(nodes={}))
    # docs metadata -> leer
    monkeypatch.setattr(mod, "read_docs_metadata", lambda _: {})
    # keine Spalten gefunden
    monkeypatch.setattr(mod, "_collect_columns", lambda _: {})

    monkeypatch.setattr(mod, "_prepare_context", lambda *a, **k: fake_ctx)

    with pytest.raises(typer.Exit) as excinfo:
        mod.sync_db_comments(project=".", env_name="dev", dry_run=True)
    assert excinfo.value.exit_code == 0


@pytest.mark.unit
@pytest.mark.cli
def test_sync_db_comments_postgres_path(monkeypatch):
    # 1) Kontext vorbereiten
    fake_exec = MagicMock()
    fake_ctx = SimpleNamespace(
        project=Path("."),
        profile=SimpleNamespace(engine="postgres", postgres=SimpleNamespace(db_schema="public")),
        make_executor=lambda: (fake_exec, None, None),
    )
    monkeypatch.setattr(mod, "_prepare_context", lambda *a, **k: fake_ctx)

    # 2) Registry mit einem Node
    fake_node = SimpleNamespace(name="users.ff")
    monkeypatch.setattr(mod, "REGISTRY", SimpleNamespace(nodes={"users.ff": fake_node}))

    # 3) relation_for -> "users"
    monkeypatch.setattr(mod, "relation_for", lambda name: "users")

    # 4) docs metadata: model-beschreibung + column-beschreibung
    monkeypatch.setattr(
        mod,
        "read_docs_metadata",
        lambda _: {
            "models": {
                "users.ff": {"description_html": "<p>User table</p>", "columns": {"id": "User id"}}
            },
            "columns": {},
        },
    )

    # 5) _collect_columns: table "users" has column "id"
    col = SimpleNamespace(name="id")
    monkeypatch.setattr(mod, "_collect_columns", lambda _: {"users": [col]})

    # 6) _sync_comments_postgres beobachten
    called = {}

    def fake_sync_pg(execu, intents, schema, dry_run):
        called["execu"] = execu
        called["intents"] = intents
        called["schema"] = schema
        called["dry_run"] = dry_run

    monkeypatch.setattr(mod, "_sync_comments_postgres", fake_sync_pg)

    with pytest.raises(typer.Exit) as excinfo:
        mod.sync_db_comments(project=".", env_name="dev", dry_run=True)
    assert excinfo.value.exit_code == 0

    # Assertions
    assert called["execu"] is fake_exec
    assert called["schema"] == "public"
    # wir erwarten 2 intents: table + column
    kinds = {i["kind"] for i in called["intents"]}
    assert kinds == {"table", "column"}


@pytest.mark.unit
@pytest.mark.cli
def test_sync_db_comments_snowflake_path(monkeypatch):
    fake_exec = MagicMock()
    fake_ctx = SimpleNamespace(
        project=Path("."),
        profile=SimpleNamespace(
            engine="snowflake_snowpark",
            snowflake_snowpark=SimpleNamespace(db_schema="PUBLIC"),
        ),
        make_executor=lambda: (fake_exec, None, None),
    )
    monkeypatch.setattr(mod, "_prepare_context", lambda *a, **k: fake_ctx)

    # Registry + relation_for
    monkeypatch.setattr(
        mod, "REGISTRY", SimpleNamespace(nodes={"users.ff": SimpleNamespace(name="users.ff")})
    )
    monkeypatch.setattr(mod, "relation_for", lambda name: "USERS")

    # docs
    monkeypatch.setattr(
        mod,
        "read_docs_metadata",
        lambda _: {"models": {"users.ff": {"description_html": "<b>Users</b>"}}, "columns": {}},
    )
    monkeypatch.setattr(mod, "_collect_columns", lambda _: {})

    called = {}

    def fake_sync_sf(execu, intents, schema, dry_run):
        called["intents"] = intents
        called["schema"] = schema
        called["dry_run"] = dry_run

    monkeypatch.setattr(mod, "_sync_comments_snowflake", fake_sync_sf)

    with pytest.raises(typer.Exit) as excinfo:
        mod.sync_db_comments(project=".", env_name="dev", dry_run=True)
    assert excinfo.value.exit_code == 0

    assert called["schema"] == "PUBLIC"
    assert called["intents"][0]["kind"] == "table"
    assert called["intents"][0]["relation"] == "USERS"


@pytest.mark.unit
@pytest.mark.cli
def test_sync_db_comments_unsupported_engine(monkeypatch, capsys):
    fake_exec = MagicMock()
    fake_ctx = SimpleNamespace(
        project=Path("."),
        profile=SimpleNamespace(engine="duckdb"),
        make_executor=lambda: (fake_exec, None, None),
    )
    monkeypatch.setattr(mod, "_prepare_context", lambda *a, **k: fake_ctx)

    # mindestens ein Node, sonst würden wir vorher returnen
    monkeypatch.setattr(mod, "REGISTRY", SimpleNamespace(nodes={"n": SimpleNamespace(name="n")}))
    monkeypatch.setattr(mod, "relation_for", lambda name: "N")
    monkeypatch.setattr(
        mod, "read_docs_metadata", lambda _: {"models": {"n": {"description_html": "hi"}}}
    )
    monkeypatch.setattr(mod, "_collect_columns", lambda _: {})

    with pytest.raises(typer.Exit) as excinfo:
        mod.sync_db_comments(project=".", env_name="dev", dry_run=True)
    assert excinfo.value.exit_code == 0

    out = capsys.readouterr().out
    assert "not supported for comment sync" in out

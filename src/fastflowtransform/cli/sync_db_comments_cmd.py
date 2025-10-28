from __future__ import annotations

import re
from typing import Annotated, Any

import typer
from sqlalchemy import text as sa_text

from fastflowtransform.cli.bootstrap import _prepare_context
from fastflowtransform.cli.options import EngineOpt, EnvOpt, ProjectArg, VarsOpt
from fastflowtransform.core import REGISTRY, relation_for
from fastflowtransform.docs import _collect_columns, read_docs_metadata
from fastflowtransform.logging import echo, error


def _strip_html_for_comment(s: str | None) -> str:
    if not s:
        return ""
    return " ".join(re.sub(r"<[^>]+>", "", s).split())


def _pg_quote_ident(ident: str) -> str:
    return '"' + ident.replace('"', '""') + '"'


def _pg_fq_table(schema: str | None, relation: str) -> str:
    if schema:
        return f"{_pg_quote_ident(schema)}.{_pg_quote_ident(relation)}"
    if "." in relation:
        s, t = relation.split(".", 1)
        return f"{_pg_quote_ident(s)}.{_pg_quote_ident(t)}"
    return _pg_quote_ident(relation)


def _sql_literal(s: str) -> str:
    return "'" + s.replace("'", "''") + "'"


def _sync_comments_postgres(
    execu: Any, intents: list[dict[str, str]], schema: str | None, *, dry_run: bool
) -> None:
    statements: list[str] = []
    for it in intents:
        kind = it["kind"]
        rel = it["relation"]
        txt = it["text"]
        fqtn = _pg_fq_table(schema, rel)
        if kind == "table":
            statements.append(f"COMMENT ON TABLE {fqtn} IS {_sql_literal(txt)};")
        elif kind == "column":
            col = _pg_quote_ident(it["column"])
            statements.append(f"COMMENT ON COLUMN {fqtn}.{col} IS {_sql_literal(txt)};")

    if dry_run:
        echo("\n-- DRY RUN: Planned Postgres COMMENT statements --")
        for s in statements:
            echo(s)
        return

    if not hasattr(execu, "engine"):
        typer.secho("Postgres executor has no .engine; cannot apply comments.", fg="red")
        raise typer.Exit(1)

    applied, failed = 0, 0
    with execu.engine.begin() as conn:
        for s in statements:
            try:
                conn.execute(sa_text(s))
                applied += 1
            except Exception as e:
                failed += 1
                error("Failed to apply comment: %s  (%s: %s)", s, type(e).__name__, e)

    echo(f"✓ Postgres comments applied: {applied} (failed: {failed})")


def _sf_fq_table(schema: str | None, relation: str) -> str:
    if schema:
        return f"{schema}.{relation}"
    return relation


def _sync_comments_snowflake(
    execu: Any, intents: list[dict[str, str]], schema: str | None, *, dry_run: bool
) -> None:
    statements: list[str] = []
    for it in intents:
        kind = it["kind"]
        rel = it["relation"]
        txt = it["text"]
        fqtn = _sf_fq_table(schema, rel)
        lit = _sql_literal(txt)
        if kind == "table":
            statements.append(f"COMMENT ON TABLE {fqtn} IS {lit}")
        elif kind == "column":
            col = it["column"]
            statements.append(f"COMMENT ON COLUMN {fqtn}.{col} IS {lit}")

    if dry_run:
        echo("\n-- DRY RUN: Planned Snowflake COMMENT statements --")
        for s in statements:
            echo(s + ";")
        return

    applied, failed = 0, 0

    def _run_sql(stmt: str) -> None:
        nonlocal applied, failed
        try:
            if hasattr(execu, "session"):
                execu.session.sql(stmt).collect()
            elif hasattr(execu, "execute"):
                execu.execute(stmt)
            else:
                raise RuntimeError("No execution method available on Snowflake executor.")
            applied += 1
        except Exception as e:
            failed += 1
            error("Failed to apply comment: %s  (%s: %s)", stmt, type(e).__name__, e)

    for s in statements:
        _run_sql(s)

    echo(f"✓ Snowflake comments applied: {applied} (failed: {failed})")


def sync_db_comments(
    project: ProjectArg = ".",
    env_name: EnvOpt = "dev",
    engine: EngineOpt = None,
    vars: VarsOpt = None,
    dry_run: Annotated[bool, typer.Option("--dry-run", help="Print statements only.")] = False,
) -> None:
    ctx = _prepare_context(project, env_name, engine, vars)
    execu, _, _ = ctx.make_executor()

    docs_meta = read_docs_metadata(ctx.project)
    cols_by_table = _collect_columns(execu)

    intents: list[dict[str, str]] = []
    for node in REGISTRY.nodes.values():
        model_name = node.name
        relation = relation_for(model_name)

        model_meta = (
            (docs_meta.get("models", {}) or {}).get(model_name, {})
            if isinstance(docs_meta, dict)
            else {}
        )
        mdesc_html = model_meta.get("description_html")
        mdesc = _strip_html_for_comment(mdesc_html)
        if mdesc:
            intents.append({"kind": "table", "relation": relation, "text": mdesc})

        rel_desc_map = (docs_meta.get("columns", {}) or {}).get(relation, {})
        mdl_desc_map = model_meta.get("columns") or {}
        if relation in cols_by_table:
            for ci in cols_by_table[relation]:
                text = rel_desc_map.get(ci.name) or mdl_desc_map.get(ci.name)
                text = _strip_html_for_comment(text) if text else ""
                if text:
                    intents.append(
                        {"kind": "column", "relation": relation, "column": ci.name, "text": text}
                    )

    if not intents:
        typer.secho("Nothing to sync (no descriptions found).", fg="yellow")
        raise typer.Exit(0)

    eng = ctx.profile.engine
    if eng == "postgres":
        pg_cfg = getattr(ctx.profile, "postgres", None)
        pg_schema = getattr(pg_cfg, "db_schema", None)
        _sync_comments_postgres(execu, intents, pg_schema, dry_run=dry_run)
        raise typer.Exit(0)
    elif eng == "snowflake_snowpark":
        sf_cfg = getattr(ctx.profile, "snowflake_snowpark", None)
        sf_schema = getattr(sf_cfg, "db_schema", None)
        _sync_comments_snowflake(execu, intents, sf_schema, dry_run=dry_run)
        raise typer.Exit(0)
    else:
        typer.secho(f"Engine '{eng}' not supported for comment sync (skipping).", fg="yellow")
        raise typer.Exit(0)


def register(app: typer.Typer) -> None:
    app.command(
        help=(
            "Sync model and column descriptions to database comments (PG & Snowflake).\n\n"
            "Examples:\n  fft sync-db-comments . --env dev --dry-run\n"
            "          fft sync-db-comments . --env stg"
        )
    )(sync_db_comments)


__all__ = ["register", "sync_db_comments"]

from __future__ import annotations

import json
import logging
import webbrowser
from contextlib import suppress
from pathlib import Path
from typing import Annotated

import typer

from fastflowtransform.core import REGISTRY
from fastflowtransform.docs import render_site

from .bootstrap import _prepare_context
from .docs_utils import _build_docs_manifest, _resolve_dag_out_dir
from .logging_utils import LOG
from .options import (
    EngineOpt,
    EnvOpt,
    OutOpt,
    ProjectArg,
    VarsOpt,
)


def docgen(
    project: ProjectArg = ".",
    env_name: EnvOpt = "dev",
    engine: EngineOpt = None,
    vars: VarsOpt = None,
    out: OutOpt = None,
    emit_json: Annotated[
        Path | None, typer.Option("--emit-json", help="Write docs manifest JSON to this file")
    ] = None,
    open_source: Annotated[
        bool, typer.Option("--open-source", help="Open generated index.html in the default browser")
    ] = False,
) -> None:
    if out is not None:
        out = out.resolve()
        out.mkdir(parents=True, exist_ok=True)

    ctx = _prepare_context(project, env_name, engine, vars)
    ex, *_ = ctx.make_executor()
    dag_out = _resolve_dag_out_dir(ctx.project, out)
    dag_out.mkdir(parents=True, exist_ok=True)

    render_site(dag_out, REGISTRY.nodes, executor=ex)
    typer.echo(f"HTML docs written to {dag_out / 'index.html'}")

    if emit_json is not None:
        manifest = _build_docs_manifest(
            project_dir=ctx.project,
            nodes=REGISTRY.nodes,
            executor=ex,
            env_name=env_name,
        )
        emit_json = emit_json.resolve()
        emit_json.parent.mkdir(parents=True, exist_ok=True)
        emit_json.write_text(json.dumps(manifest, indent=2), encoding="utf-8")
        typer.echo(f"Docs manifest JSON written to {emit_json}")

    if open_source:
        index_path = (dag_out / "index.html").as_uri()
        with suppress(Exception):
            webbrowser.open(index_path, new=2)

    if LOG.isEnabledFor(logging.INFO):
        typer.echo(f"Profile: {env_name} | Engine: {ctx.profile.engine}")


def register(app: typer.Typer) -> None:
    app.command(
        help=(
            "Generate documentation site (like 'dag --html') and optionally "
            "emit a JSON manifest.\n\n"
            "Examples:\n"
            "  fft docgen . --env dev --out site/docs\n"
            "  fft docgen . --env dev --out site/docs --emit-json docs_manifest.json "
            "  --open-source\n"
        )
    )(docgen)


__all__ = ["docgen", "register"]

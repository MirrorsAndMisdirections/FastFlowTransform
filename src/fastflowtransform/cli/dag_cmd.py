from __future__ import annotations

import logging

import typer

from fastflowtransform.core import REGISTRY
from fastflowtransform.dag import mermaid
from fastflowtransform.docs import render_site

from .bootstrap import _prepare_context
from .docs_utils import _resolve_dag_out_dir
from .logging_utils import LOG
from .options import (
    EngineOpt,
    EnvOpt,
    HtmlOpt,
    OutOpt,
    ProjectArg,
    SelectOpt,
    VarsOpt,
    WithSchemaOpt,
)
from .selectors import _compile_selector


def dag(
    project: ProjectArg = ".",
    env_name: EnvOpt = "dev",
    html: HtmlOpt = False,
    engine: EngineOpt = None,
    out: OutOpt = None,
    vars: VarsOpt = None,
    select: SelectOpt = None,
    with_schema: WithSchemaOpt = True,
) -> None:
    if out is not None:
        out = out.resolve()
        out.mkdir(parents=True, exist_ok=True)

    ctx = _prepare_context(project, env_name, engine, vars)
    dag_out = _resolve_dag_out_dir(ctx.project, out)
    dag_out.mkdir(parents=True, exist_ok=True)

    _, pred = _compile_selector(select)
    filtered_nodes = {k: v for k, v in REGISTRY.nodes.items() if pred(v)}

    if html:
        ex, *_ = ctx.make_executor()
        try:
            render_site(dag_out, filtered_nodes, executor=ex, with_schema=with_schema)
        except TypeError:
            render_site(dag_out, filtered_nodes, executor=ex)
        typer.echo(f"HTML-DAG written to {dag_out / 'index.html'}")
    else:
        mm = mermaid(filtered_nodes)
        mmd = dag_out / "dag.mmd"
        mmd.write_text(mm, encoding="utf-8")
        typer.echo(f"Mermaid DAG written to {dag_out}")

    if LOG.isEnabledFor(logging.INFO):
        typer.echo(f"Profile: {env_name} | Engine: {ctx.profile.engine}")


def register(app: typer.Typer) -> None:
    app.command(
        help=(
            "Outputs the DAG as Mermaid text or generates an HTML page.\n\nExamples:\n  "
            "fft dag .\n  fft dag . --env dev --html"
        )
    )(dag)


__all__ = ["dag", "register"]

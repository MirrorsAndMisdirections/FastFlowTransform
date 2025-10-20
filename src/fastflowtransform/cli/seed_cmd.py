from __future__ import annotations

import typer

from fastflowtransform.seeding import seed_project

from .bootstrap import _prepare_context
from .options import EngineOpt, EnvOpt, ProjectArg, VarsOpt


def seed(
    project: ProjectArg = ".",
    env_name: EnvOpt = "dev",
    engine: EngineOpt = None,
    vars: VarsOpt = None,
) -> None:
    ctx = _prepare_context(project, env_name, engine, vars)
    execu, _, _ = ctx.make_executor()

    schema: str | None = None
    if ctx.profile.engine == "postgres":
        schema = ctx.profile.postgres.db_schema

    n = seed_project(ctx.project, execu, schema)
    typer.echo(f"✓ Seeded {n} table(s)")


def register(app: typer.Typer) -> None:
    app.command(
        help=(
            "Load seeds from /seeds into the target database.\n\nExamples:\n  fft seed . "
            "--env dev\n  fft seed examples/postgres --env stg"
        )
    )(seed)


__all__ = ["register", "seed"]

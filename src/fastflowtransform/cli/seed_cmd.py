from __future__ import annotations

import typer

from fastflowtransform.cli.bootstrap import _prepare_context
from fastflowtransform.cli.options import EngineOpt, EnvOpt, ProjectArg, VarsOpt
from fastflowtransform.logging import echo
from fastflowtransform.seeding import _human_int, seed_project


def seed(
    project: ProjectArg = ".",
    env_name: EnvOpt = "dev",
    engine: EngineOpt = None,
    vars: VarsOpt = None,
) -> None:
    """
    High-level entry to run seeding for a project:
      1) Prepare the runtime context and executor.
      2) Resolve per-file targets using seeds/schema.yml.
      3) Materialize each seed via the engine-specific path.
    """
    ctx = _prepare_context(project, env_name, engine, vars)
    execu, _, _ = ctx.make_executor()

    # You can still pass a global default schema; per-file CFG will override it.
    default_schema: str | None = None
    if getattr(ctx.profile, "engine", None) == "postgres":
        default_schema = getattr(getattr(ctx.profile, "postgres", None), "db_schema", None)

    n = seed_project(ctx.project, execu, default_schema)
    echo(f"✓ Seeded {_human_int(n)} table(s)")


def register(app: typer.Typer) -> None:
    app.command(
        help=(
            "Load seeds from /seeds into the target database.\n\nExamples:\n  fft seed . "
            "--env dev\n  fft seed examples/postgres --env stg"
        )
    )(seed)


__all__ = ["register", "seed"]

from __future__ import annotations

import typer

from fastflowtransform.cli.bootstrap import _prepare_context
from fastflowtransform.cli.options import (
    CaseOpt,
    EngineOpt,
    EnvOpt,
    ModelOpt,
    PathOpt,
    ProjectArg,
    ReuseMetaOpt,
    UTestCacheMode,
    UTestCacheOpt,
    VarsOpt,
)
from fastflowtransform.logging import echo
from fastflowtransform.utest import discover_unit_specs, run_unit_specs


def utest(
    project: ProjectArg = ".",
    model: ModelOpt = None,
    case: CaseOpt = None,
    env_name: EnvOpt = "dev",
    engine: EngineOpt = None,
    path: PathOpt = None,
    vars: VarsOpt = None,
    cache: UTestCacheOpt = UTestCacheMode.OFF,
    reuse_meta: ReuseMetaOpt = False,
) -> None:
    ctx = _prepare_context(project, env_name, engine, vars, utest=True)
    ex, _, _ = ctx.make_executor()

    specs = discover_unit_specs(ctx.project, path=path, only_model=model)
    if not specs:
        echo("ℹ️  No unit tests found (tests/unit/*.yml).")  # noqa: RUF001
        raise typer.Exit(0)

    failures = run_unit_specs(
        specs,
        ex,
        ctx.jinja_env,
        only_case=case,
        cache_mode=getattr(cache, "value", str(cache)) if cache is not None else "off",
        reuse_meta=bool(reuse_meta),
    )
    raise typer.Exit(code=2 if failures > 0 else 0)


def register(app: typer.Typer) -> None:
    app.command()(utest)


__all__ = ["register", "utest"]

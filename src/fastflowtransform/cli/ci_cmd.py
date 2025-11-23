# fastflowtransform/cli/ci_cmd.py
from __future__ import annotations

import textwrap
from collections.abc import Sequence

import typer

from fastflowtransform.ci.core import CiIssue, CiSummary, run_ci_check
from fastflowtransform.cli.bootstrap import _prepare_context
from fastflowtransform.cli.options import EngineOpt, EnvOpt, ProjectArg, SelectOpt, VarsOpt
from fastflowtransform.logging import bind_context, clear_context, echo

from .selectors import _parse_select


def _format_issue_line(issue: CiIssue) -> str:
    """
    Pretty-print a single CiIssue as a one-line summary.

    Example:
      [E] MISSING_DEP (orders.ff): Missing dependencies for 'orders.ff': customers.ff
    """
    level = (issue.level or "error").lower()
    lvl = "E" if level == "error" else "W"

    target = f" ({issue.obj_name})" if issue.obj_name else ""
    location = ""
    if issue.file:
        location = issue.file
        if issue.line is not None:
            location += f":{issue.line}"
            if issue.column is not None:
                location += f":{issue.column}"
        location = f" [{location}]"

    msg = issue.message or ""
    return f"[{lvl}] {issue.code}{target}: {msg}{location}"


def _print_text_summary(
    summary: CiSummary,
    *,
    project: str,
    select_tokens: Sequence[str],
) -> None:
    """
    Human-friendly text output for `fft ci-check`, similar in spirit to `fft run`.
    """
    echo("CI Check Summary")
    echo("────────────────")
    echo(f"Project: {project}")
    echo(f"Models:  {len(summary.selected_nodes)}/{len(summary.all_nodes)} selected")
    if select_tokens:
        echo(f"Select:  {', '.join(select_tokens)}")

    # Issues section
    echo("\nIssues")
    echo("──────")
    if not summary.issues:
        echo("None 🎉")
    else:
        for issue in summary.issues:
            echo(f"- {_format_issue_line(issue)}")

    # Selection preview
    echo("\nSelected models")
    echo("──────────────")
    if not summary.selected_nodes:
        echo("<none>")
    else:
        for name in summary.selected_nodes:
            echo(f"• {name}")


def ci_check(
    project: ProjectArg = ".",
    env_name: EnvOpt = "dev",
    engine: EngineOpt = None,
    vars: VarsOpt = None,
    select: SelectOpt = None,
) -> None:
    """
    Static CI check: parse project, validate DAG, and preview selection.

    Runs **without a database connection** by default. Intended for PR/CI jobs:

      - Validates that models parse and dependencies are resolvable.
      - Detects dependency cycles.
      - Performs a dry-run selection based on --select (no execution).
      - Returns exit code 1 if any error-level issues are present.
    """
    # Load project + registry, but do NOT create an executor → no DB work.
    ctx = _prepare_context(project, env_name, engine, vars)
    bind_context(engine=ctx.profile.engine, env=env_name)

    # Match run.py: simple profile/engine banner
    echo(f"Profile: {env_name} | Engine: {ctx.profile.engine}")

    # Reuse the same select token parsing as fft run (but purely static)
    select_tokens = _parse_select(select or [])

    # Run CI core checks on the loaded registry
    summary = run_ci_check(select=select_tokens)

    # Text summary (stdout)
    _print_text_summary(
        summary,
        project=str(ctx.project),
        select_tokens=select_tokens,
    )

    # Decide exit code: any error-level issue ⇒ non-zero exit for CI
    has_errors = any(issue.level == "error" for issue in summary.issues)

    clear_context()

    if has_errors:
        raise typer.Exit(1)


def register(app: typer.Typer) -> None:
    """
    Register `fft ci-check` on the main Typer app.
    """
    app.command(
        "ci-check",
        help=textwrap.dedent(
            """\
            Static CI check: parse models, validate DAG, and preview selection
            without touching the database.

            Examples:
              fft ci-check . --env dev
              fft ci-check . --env dev --select tag:finance
            """
        ),
    )(ci_check)


__all__ = ["ci_check", "register"]

# fastflowtransform/cli/options.py
from __future__ import annotations

from enum import Enum
from pathlib import Path
from typing import Annotated, Literal

import click
import typer

from fastflowtransform.settings import EngineType

VarsOpt = Annotated[
    list[str] | None,
    typer.Option("--vars", help="Override template vars: key=value"),
]

CaseOpt = Annotated[str | None, typer.Option("--case", help="Run only a single case")]

EnvOpt = Annotated[str, typer.Option("--env", help="Profile environment")]

EngineOpt = Annotated[
    EngineType | None,
    typer.Option("--engine", help="duckdb|postgres|bigquery (overrides profile)"),
]

PathOpt = Annotated[
    str | None, typer.Option("--path", help="Single YAML file instead of discovery")
]

ProjectArg = Annotated[str, typer.Argument(help="Path to the project (with tests/unit/*.yml)")]

ModelOpt = Annotated[str | None, typer.Option("--model", help="Test a single model")]

SelectOpt = Annotated[
    list[str] | None,
    typer.Option(
        "--select",
        help=(
            "Filter models (name-glob, tag:<t>, type:<view|table|ephemeral>, "
            "kind:<sql|python>) or DQ tags (legacy single token)"
        ),
    ),
]

OutOpt = Annotated[
    Path | None,
    typer.Option("--out", help="Output directory for DAG artifacts"),
]

HtmlOpt = Annotated[
    bool,
    typer.Option("--html", help="Generate HTML DAG and mini documentation"),
]

type Jobs = int | Literal["auto"]


def _jobs_callback(
    ctx: click.Context,
    param: click.Parameter,
    value: str | None,
) -> Jobs | None:
    if value is None:
        return None

    if value == "auto":
        return "auto"

    try:
        n = int(value)
    except ValueError as e:
        raise typer.BadParameter("`--jobs` must be an integer ≥1 or 'auto'.") from e

    if n < 1:
        raise typer.BadParameter("`--jobs` must be ≥1 or 'auto'.")

    return n


JobsOpt = Annotated[
    str,
    typer.Option(
        "--jobs",
        help="Max parallel executions per level (≥1) or 'auto'.",
        show_default=True,
        callback=_jobs_callback,
    ),
]

KeepOpt = Annotated[
    bool,
    typer.Option(
        "--keep-going",
        help=(
            "On errors within a level: do not cancel tasks already running in that level; "
            "subsequent levels still do not start."
        ),
    ),
]

ExcludeOpt = Annotated[
    list[str] | None,
    typer.Option(
        "--exclude",
        help=(
            "Exclude models by the same matcher syntax as --select. "
            "Excluded models and everything downstream of them are removed "
            "from the run subgraph."
        ),
    ),
]

WithSchemaOpt = Annotated[
    bool,
    typer.Option(
        "--with-schema/--no-schema",
        help="Include column schema (types/nullability) in docs if supported by engine.",
        show_default=True,
    ),
]


# ────────────── HTTP / API Flags ──────────────
class HttpCacheMode(str, Enum):
    OFF = "off"  # keine Cache-Nutzung
    RO = "ro"  # read-only: nur lesen, keine Writes
    RW = "rw"  # read-write (Default-Verhalten)


OfflineOpt = Annotated[
    bool,
    typer.Option(
        "--offline",
        help="HTTP offline mode: verbietet Netz-Zugriffe in API-Modellen (nur Cache-Hits erlaubt).",
    ),
]

HttpCacheOpt = Annotated[
    HttpCacheMode | None,
    typer.Option(
        "--http-cache",
        help="HTTP-Cache-Mode for API models: off | ro | rw.",
        case_sensitive=False,
    ),
]


class CacheMode(str, Enum):
    RW = "rw"  # read-write: skip on hit, write on build
    RO = "ro"  # read-only: skip on hit, build on miss (no writes)
    WO = "wo"  # write-only: always build, write
    OFF = "off"  # disabled: always build, no writes


CacheOpt = Annotated[
    CacheMode,
    typer.Option(
        "--cache",
        help="Cache mode: rw (default), ro, wo, off.",
        case_sensitive=False,
        show_default=True,
    ),
]

NoCacheOpt = Annotated[
    bool,
    typer.Option(
        "--no-cache",
        help="Alias for --cache=off (always build, no writes).",
    ),
]

RebuildAllOpt = Annotated[
    bool,
    typer.Option(
        "--rebuild",
        help="Rebuild all selected nodes (ignore cache for them).",
    ),
]

RebuildOnlyOpt = Annotated[
    list[str] | None,
    typer.Option(
        "--rebuild-only",
        "-R",
        help="Rebuild only specific nodes (repeatable).",
    ),
]


class UTestCacheMode(str, Enum):
    OFF = "off"
    RO = "ro"
    RW = "rw"


UTestCacheOpt = Annotated[
    UTestCacheMode,
    typer.Option(
        "--cache",
        help="Unit-test cache mode: off (default), ro, rw.",
        show_default=True,
    ),
]

ReuseMetaOpt = Annotated[
    bool,
    typer.Option(
        "--reuse-meta",
        help="Do not clean or reset meta state between unit tests (reserved; may be ignored).",
    ),
]

SkipBuildOpt = Annotated[
    bool,
    typer.Option(
        "--skip-build",
        help="Do not build models before running tests (use existing tables).",
    ),
]

ChangedSinceOpt = Annotated[
    str | None,
    typer.Option(
        "--changed-since",
        help=(
            "Limit the run to models affected by files changed since the given "
            "git ref (e.g. origin/main)."
        ),
    ),
]


__all__ = [
    "CacheMode",
    "CacheOpt",
    "CaseOpt",
    "ChangedSinceOpt",
    "EngineOpt",
    "EnvOpt",
    "ExcludeOpt",
    "HtmlOpt",
    "JobsOpt",
    "KeepOpt",
    "ModelOpt",
    "NoCacheOpt",
    "OutOpt",
    "PathOpt",
    "ProjectArg",
    "RebuildAllOpt",
    "RebuildOnlyOpt",
    "ReuseMetaOpt",
    "SelectOpt",
    "SkipBuildOpt",
    "UTestCacheMode",
    "UTestCacheOpt",
    "VarsOpt",
    "WithSchemaOpt",
]

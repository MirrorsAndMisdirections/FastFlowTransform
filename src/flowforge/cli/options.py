from __future__ import annotations

from enum import Enum
from pathlib import Path
from typing import Annotated

import typer

from flowforge.settings import EngineType

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

JobsOpt = Annotated[
    int,
    typer.Option(
        "--jobs",
        help="Max parallel executions per level (≥1).",
        min=1,
        show_default=True,
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


__all__ = [
    "CacheMode",
    "CacheOpt",
    "CaseOpt",
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
    "UTestCacheMode",
    "UTestCacheOpt",
    "VarsOpt",
    "WithSchemaOpt",
]

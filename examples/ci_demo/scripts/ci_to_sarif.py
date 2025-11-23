#!/usr/bin/env python
from __future__ import annotations

import argparse
from pathlib import Path

from fastflowtransform.cli.bootstrap import _prepare_context
from fastflowtransform.ci.core import run_ci_check
from fastflowtransform.ci.sarif import write_sarif
from fastflowtransform.logging import bind_context, clear_context, echo

try:
    # Optional – use package version if available
    from fastflowtransform import __version__ as FFT_VERSION
except Exception:  # pragma: no cover
    FFT_VERSION = None


def _parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description=(
            "Run fft ci-check programmatically and emit a SARIF report.\n\n"
            "Typical usage (from examples/ci_demo):\n"
            "  python scripts/ci_to_sarif.py . "
            "--env dev_duckdb --engine duckdb --out ci-scan.sarif"
        )
    )
    parser.add_argument(
        "project",
        nargs="?",
        default=".",
        help="Project directory (default: current directory).",
    )
    parser.add_argument(
        "--env",
        dest="env_name",
        default="dev_duckdb",
        help="Environment/profile name (default: dev_duckdb).",
    )
    parser.add_argument(
        "--engine",
        dest="engine",
        default="duckdb",
        help="Engine hint (duckdb|postgres|bigquery|...). Default: duckdb.",
    )
    parser.add_argument(
        "--select",
        nargs="*",
        default=None,
        help="Optional selectors (simple substrings for now, e.g. customers, .ff, tag:whatever).",
    )
    parser.add_argument(
        "--out",
        dest="out",
        default="ci-scan.sarif",
        help="Output SARIF file path (default: ci-scan.sarif).",
    )
    return parser.parse_args()


def main() -> None:
    args = _parse_args()

    # Load project & registry (no DB work required for run_ci_check)
    ctx = _prepare_context(
        project_arg=str(args.project),
        env_name=args.env_name,
        engine=args.engine,
        vars_opt=None,
    )
    bind_context(engine=ctx.profile.engine, env=args.env_name)

    echo(
        f"[FFT] CI check (project={Path(args.project).resolve()}, "
        f"env={args.env_name}, engine={ctx.profile.engine})"
    )

    # Static CI checks (graph, selection preview, etc.)
    summary = run_ci_check(select=args.select)

    out_path = Path(args.out)
    write_sarif(
        summary,
        out_path,
        tool_name="FastFlowTransform CI",
        tool_version=FFT_VERSION,
    )

    echo(f"[FFT] Wrote SARIF report to {out_path}")
    clear_context()


if __name__ == "__main__":
    main()

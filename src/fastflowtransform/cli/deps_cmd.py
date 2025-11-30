# fastflowtransform/cli/deps_cmd.py
from __future__ import annotations

import typer

from fastflowtransform.cli.bootstrap import _resolve_project_path
from fastflowtransform.cli.options import ProjectArg
from fastflowtransform.logging import echo
from fastflowtransform.packages import resolve_packages


def deps(project: ProjectArg = ".") -> None:
    """
    Show packages configured in packages.yml and basic status checks.

    - Resolves project directory (must contain models/).
    - Parses packages.yml (if present).
    - For each package, resolves its base path and models_dir location.
    - Prints a short report and exits with non-zero status when something is missing.
    """
    proj = _resolve_project_path(project)

    try:
        pkgs = resolve_packages(proj)
    except Exception as exc:  # pragma: no cover - config error path
        raise typer.BadParameter(f"Failed to resolve packages: {exc}") from exc

    echo(f"Project: {proj}")

    if not pkgs:
        echo("No packages configured (packages.yml not found or empty).")
        raise typer.Exit(0)

    echo("Packages:")
    missing = 0

    for pkg in pkgs:
        models_root = pkg.root / pkg.models_dir
        status = "OK"
        if not models_root.exists():
            status = "MISSING: models_dir not found"
            missing += 1

        echo(f"  - {pkg.name} ({pkg.version})")
        echo(f"      kind:       {pkg.source.kind}")
        if pkg.source.kind == "path":
            echo(f"      path:       {pkg.root}")
        else:
            echo(f"      git:        {pkg.source.git}")
            echo(f"      rev:        {pkg.source.rev}")
            if pkg.source.subdir:
                echo(f"      subdir:     {pkg.source.subdir}")
        echo(f"      models_dir: {pkg.models_dir}  -> {models_root}")
        echo(f"      status:     {status}")

    raise typer.Exit(1 if missing else 0)


def register(app: typer.Typer) -> None:
    app.command(
        name="deps",
        help="Show configured packages from packages.yml and their local status.",
    )(deps)


__all__ = ["deps", "register"]

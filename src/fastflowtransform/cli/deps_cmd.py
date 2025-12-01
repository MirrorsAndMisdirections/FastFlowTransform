# fastflowtransform/cli/deps_cmd.py
from __future__ import annotations

import typer

from fastflowtransform.cli.bootstrap import _resolve_project_path
from fastflowtransform.cli.options import ProjectArg
from fastflowtransform.logging import echo
from fastflowtransform.packages import resolve_packages


def deps(project: ProjectArg = ".") -> None:
    """
    Inspect packages declared in packages.yml and show their resolved status.

    For the given project it will:

      - Resolve the project directory.
      - Run the full package resolver (path + git packages):
          * locate or clone/fetch each package
          * load its project.yml manifest (name/version/etc.)
          * enforce version / FFT compatibility / inter-package deps
          * write packages.lock.yml with pinned sources
      - For each resolved package, print:
          * name + version
          * source kind (path | git) and concrete location
          * models_dir and resolved models root
      - Exit with non-zero status if any package's models_dir is missing.
    """
    proj = _resolve_project_path(project)

    try:
        pkgs = resolve_packages(proj)
    except Exception as exc:  # pragma: no cover - resolution error path
        # Keep this as a single, clear error line; resolve_packages already
        # does step-by-step validation (git, refs, manifest, versions, etc.).
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

    # Non-zero exit if any package is structurally broken
    raise typer.Exit(1 if missing else 0)


def register(app: typer.Typer) -> None:
    app.command(
        name="deps",
        help="Show resolved packages (path/git) from packages.yml and their local status.",
    )(deps)


__all__ = ["deps", "register"]

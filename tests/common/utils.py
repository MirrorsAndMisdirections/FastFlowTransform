import os
import subprocess
from pathlib import Path

import pytest


def project_root(start: Path | None = None) -> Path:
    marker_files = {"pyproject.toml", "setup.cfg", ".git"}
    path = (start or Path(__file__)).resolve()
    for parent in [path, *path.parents]:
        if any((parent / marker).exists() for marker in marker_files):
            return parent
    raise RuntimeError("Cannot determine project root; missing marker file?")


ROOT = project_root(Path(__file__))


def run(cmd: list[str], env: dict | None = None) -> subprocess.CompletedProcess:
    e = os.environ.copy()
    if env:
        e.update(env)
    try:
        return subprocess.run(
            cmd,
            cwd=ROOT,
            check=True,
            text=True,
            stdout=subprocess.PIPE,
            stderr=subprocess.STDOUT,
            env=e,
        )
    except subprocess.CalledProcessError as cpe:
        out = cpe.stdout or ""
        pytest.fail(
            f"\n================= flowforge stdout (exit {cpe.returncode}) =================\n"
            f"{out.rstrip()}\n"
            f"============================================================================\n",
            pytrace=False,
        )

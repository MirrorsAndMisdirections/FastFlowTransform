# tests/test_packages_resolver.py
from __future__ import annotations

import subprocess
from pathlib import Path
from typing import Any

import pytest
import yaml

from fastflowtransform import packages as pkgmod
from fastflowtransform.config.packages import PackagesConfig, PackageSpec


def _write_package(
    root: Path,
    *,
    name: str = "shared_package",
    version: str = "0.1.0",
    models_dir: str = "models",
    extra_project_fields: dict[str, Any] | None = None,
) -> None:
    """
    Helper: write a minimal project.yml + models_dir for a package.
    """
    root.mkdir(parents=True, exist_ok=True)
    (root / models_dir).mkdir(parents=True, exist_ok=True)

    data: dict[str, Any] = {
        "name": name,
        "version": version,
        "models_dir": models_dir,
    }
    if extra_project_fields:
        data.update(extra_project_fields)

    (root / "project.yml").write_text(yaml.safe_dump(data, sort_keys=False), encoding="utf-8")


def test_resolve_path_package_basic(tmp_path: Path) -> None:
    project_dir = tmp_path / "proj"
    project_dir.mkdir()

    pkg_root = tmp_path / "shared_package"
    _write_package(pkg_root, name="shared_package", version="0.1.0")

    cfg = PackagesConfig(
        packages=[
            PackageSpec(
                name="shared_package",
                path=str(pkg_root),
                models_dir="models",
            )
        ]
    )

    resolved = pkgmod.resolve_packages(project_dir, cfg)
    assert len(resolved) == 1

    rp = resolved[0]
    assert rp.name == "shared_package"
    assert rp.version == "0.1.0"
    assert rp.root == pkg_root
    assert rp.models_dir == "models"
    assert rp.source.kind == "path"
    assert rp.source.path == str(pkg_root)
    assert rp.source.git is None


def test_resolve_git_package_uses_subdir_and_manifest(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    project_dir = tmp_path / "proj"
    project_dir.mkdir()

    # Fake "repo" with a subdir containing a package
    repo_root = tmp_path / "repo"
    pkg_root = repo_root / "pkg_subdir"
    _write_package(
        pkg_root,
        name="shared_package_git",
        version="0.1.0",
        models_dir="models",
    )

    # 1) monkeypatch _ensure_git_repo to avoid real git
    def fake_ensure_git_repo(cache_dir: Path, spec: PackageSpec) -> Path:
        # we pretend this is the cloned repo root
        return repo_root

    monkeypatch.setattr(pkgmod, "_ensure_git_repo", fake_ensure_git_repo)

    # 2) monkeypatch subprocess.run used by _lock_source_for_spec (rev-parse HEAD)
    def fake_run(args, check, capture_output, text=False):  # type: ignore[override]
        class Result:
            def __init__(self) -> None:
                # match real subprocess.run(..., capture_output=True) behavior: bytes
                self.stdout = b"deadbeefcafebabe\n"
                self.stderr = b""

        return Result()

    monkeypatch.setattr(pkgmod.subprocess, "run", fake_run)

    cfg = PackagesConfig(
        packages=[
            PackageSpec(
                name="shared_package_git",
                git="https://example.com/repo.git",
                subdir="pkg_subdir",
                models_dir="models",
                version=">=0.1.0,<0.2.0",
                ref="main",
            )
        ]
    )

    resolved = pkgmod.resolve_packages(project_dir, cfg)
    assert len(resolved) == 1

    rp = resolved[0]
    assert rp.name == "shared_package_git"
    assert rp.version == "0.1.0"
    assert rp.root == pkg_root
    assert rp.source.kind == "git"
    assert rp.source.git == "https://example.com/repo.git"
    # our fake rev-parse returns a concrete SHA; ref->rev mapping
    # is done in config, then overwritten here
    assert rp.source.rev == "deadbeefcafebabe"
    assert rp.source.subdir == "pkg_subdir"


def test_resolve_packages_version_constraint_failure(tmp_path: Path) -> None:
    project_dir = tmp_path / "proj"
    project_dir.mkdir()

    pkg_root = tmp_path / "shared_package"
    _write_package(pkg_root, name="shared_package", version="0.2.0")

    cfg = PackagesConfig(
        packages=[
            PackageSpec(
                name="shared_package",
                path=str(pkg_root),
                models_dir="models",
                version=">=0.1.0,<0.2.0",  # incompatible
            )
        ]
    )

    with pytest.raises(RuntimeError) as excinfo:
        pkgmod.resolve_packages(project_dir, cfg)

    msg = str(excinfo.value)
    assert "has version 0.2.0" in msg
    assert "requires '>=" in msg


def test_resolve_packages_fft_version_failure(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    project_dir = tmp_path / "proj"
    project_dir.mkdir()

    pkg_root = tmp_path / "shared_package"
    _write_package(
        pkg_root,
        name="shared_package",
        version="0.1.0",
        extra_project_fields={"fft_version": "999.9.9"},
    )

    # Make sure FFT_VERSION is something else
    monkeypatch.setattr(pkgmod, "FFT_VERSION", "0.1.0", raising=False)

    cfg = PackagesConfig(
        packages=[
            PackageSpec(
                name="shared_package",
                path=str(pkg_root),
            )
        ]
    )

    with pytest.raises(RuntimeError) as excinfo:
        pkgmod.resolve_packages(project_dir, cfg)

    msg = str(excinfo.value)
    assert "requires FFT version '999.9.9'" in msg


def test_package_dependency_missing_required_package(tmp_path: Path) -> None:
    project_dir = tmp_path / "proj"
    project_dir.mkdir()

    # Single package that declares a dependency on "other_pkg"
    pkg_root = tmp_path / "pkg1"
    _write_package(
        pkg_root,
        name="pkg1",
        version="0.1.0",
        extra_project_fields={
            "dependencies": [
                {"name": "other_pkg", "version": ">=0.1.0"},
            ]
        },
    )

    cfg = PackagesConfig(
        packages=[
            PackageSpec(
                name="pkg1",
                path=str(pkg_root),
            )
        ]
    )

    with pytest.raises(RuntimeError) as excinfo:
        pkgmod.resolve_packages(project_dir, cfg)

    msg = str(excinfo.value)
    assert "depends on 'other_pkg'" in msg
    assert "no package with that name is declared" in msg


def test_package_dependency_optional_missing_is_ok(tmp_path: Path) -> None:
    project_dir = tmp_path / "proj"
    project_dir.mkdir()

    pkg_root = tmp_path / "pkg1"
    _write_package(
        pkg_root,
        name="pkg1",
        version="0.1.0",
        extra_project_fields={
            "dependencies": [
                {"name": "other_pkg", "optional": True},
            ]
        },
    )

    cfg = PackagesConfig(
        packages=[
            PackageSpec(
                name="pkg1",
                path=str(pkg_root),
            )
        ]
    )

    # optional dependency, so this should succeed and simply log a debug line
    resolved = pkgmod.resolve_packages(project_dir, cfg)
    assert len(resolved) == 1
    assert resolved[0].name == "pkg1"


# -------------------- Semver helpers --------------------


def test_parse_and_compare_versions() -> None:
    assert pkgmod.parse_version("1.2.3") == (1, 2, 3)
    assert pkgmod.compare_versions("1.2.3", "1.2.3") == 0
    assert pkgmod.compare_versions("1.2.3", "1.2.4") < 0
    assert pkgmod.compare_versions("1.3.0", "1.2.9") > 0

    with pytest.raises(ValueError):
        pkgmod.parse_version("not-a-version")


@pytest.mark.parametrize(
    "actual,constraint,expected",
    [
        ("1.2.3", "1.2.3", True),
        ("1.2.3", "==1.2.3", True),
        ("1.2.3", "!=1.2.3", False),
        ("1.2.3", ">=1.2.0,<2.0.0", True),
        ("1.2.3", ">=1.3.0", False),
        ("0.3.1", "^0.3.0", True),
        ("0.4.0", "^0.3.0", False),
        ("1.2.5", "~1.2.3", True),
        ("1.3.0", "~1.2.3", False),
        ("1.2.3", None, True),
        ("1.2.3", "", True),
    ],
)
def test_version_satisfies(actual: str, constraint: str | None, expected: bool) -> None:
    assert pkgmod.version_satisfies(actual, constraint) is expected


# -------------------- git error classification --------------------


def test_run_git_authentication_error(monkeypatch: pytest.MonkeyPatch) -> None:
    def fake_run(*_args, **_kwargs):
        raise subprocess.CalledProcessError(
            1,
            ["git"],
            output="",
            stderr="fatal: Authentication failed for 'https://example.com/repo.git'\n",
        )

    monkeypatch.setattr(pkgmod.subprocess, "run", fake_run)

    with pytest.raises(RuntimeError) as excinfo:
        pkgmod._run_git(["clone", "https://example.com/repo.git"])  # type: ignore[attr-defined]

    msg = str(excinfo.value)
    assert "authentication error" in msg.lower()
    assert "Authentication failed" in msg


def test_run_git_repo_not_found(monkeypatch: pytest.MonkeyPatch) -> None:
    def fake_run(*_args, **_kwargs):
        raise subprocess.CalledProcessError(
            1,
            ["git"],
            output="",
            stderr="fatal: Repository not found.\n",
        )

    monkeypatch.setattr(pkgmod.subprocess, "run", fake_run)

    with pytest.raises(RuntimeError) as excinfo:
        pkgmod._run_git(["clone", "https://example.com/missing.git"])  # type: ignore[attr-defined]

    msg = str(excinfo.value)
    assert "repository not found" in msg.lower()


def test_run_git_bad_ref(monkeypatch: pytest.MonkeyPatch) -> None:
    def fake_run(*_args, **_kwargs):
        raise subprocess.CalledProcessError(
            1,
            ["git"],
            output="",
            stderr="error: pathspec 'nope' did not match any file(s) known to git\n",
        )

    monkeypatch.setattr(pkgmod.subprocess, "run", fake_run)

    with pytest.raises(RuntimeError) as excinfo:
        pkgmod._run_git(["checkout", "nope"])  # type: ignore[attr-defined]

    msg = str(excinfo.value)
    assert "requested ref/branch/tag does not exist" in msg
    assert "did not match any file" in msg

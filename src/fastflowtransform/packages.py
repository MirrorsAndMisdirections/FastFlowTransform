# fastflowtransform/packages.py
from __future__ import annotations

import re
import subprocess
from collections.abc import Mapping
from dataclasses import dataclass, field
from pathlib import Path
from typing import Any

import yaml

from fastflowtransform import __version__ as FFT_VERSION
from fastflowtransform.config.packages import (
    PackagesConfig,
    PackageSpec,
    load_packages_config,
)
from fastflowtransform.logging import echo, get_logger

log = get_logger("packages")


# ---------------------------------------------------------------------------
# Data structures
# ---------------------------------------------------------------------------


@dataclass
class PackageDependency:
    """
    Dependency entry from a package's own manifest (project.yml).

    Example in project.yml inside the package:

        dependencies:
          - name: shared.core
            version: ">=0.8,<1.0"
            optional: false
    """

    name: str
    version_constraint: str | None = None
    optional: bool = False
    raw: Mapping[str, Any] = field(default_factory=dict)


@dataclass
class PackageManifest:
    """
    Package-level metadata loaded from project.yml inside the package.
    """

    name: str
    version: str
    fft_version: str | None
    dependencies: list[PackageDependency] = field(default_factory=list)
    models_dir: str | None = None
    raw: Mapping[str, Any] = field(default_factory=dict)
    root: Path | None = None


@dataclass
class LockedSource:
    """
    Concrete, pinned source info that ends up in packages.lock.yml.
    """

    kind: str  # "path" | "git"
    path: str | None = None
    git: str | None = None
    rev: str | None = None
    subdir: str | None = None

    def to_mapping(self) -> dict[str, Any]:
        out: dict[str, Any] = {"kind": self.kind}
        if self.path is not None:
            out["path"] = self.path
        if self.git is not None:
            out["git"] = self.git
        if self.rev is not None:
            out["rev"] = self.rev
        if self.subdir is not None:
            out["subdir"] = self.subdir
        return out


@dataclass
class LockEntry:
    name: str
    version: str
    source: LockedSource

    def to_mapping(self) -> dict[str, Any]:
        return {
            "name": self.name,
            "version": self.version,
            "source": self.source.to_mapping(),
        }


@dataclass
class LockFile:
    """
    packages.lock.yml structure.

    Right now we only *write* it, we do not use it to drive resolution.
    """

    fft_version: str | None
    entries: list[LockEntry] = field(default_factory=list)

    @classmethod
    def from_mapping(cls, data: Mapping[str, Any]) -> LockFile:
        entries: list[LockEntry] = []
        for row in data.get("packages", []) or []:
            src = row.get("source") or {}
            source = LockedSource(
                kind=src.get("kind", "path"),
                path=src.get("path"),
                git=src.get("git"),
                rev=src.get("rev"),
                subdir=src.get("subdir"),
            )
            entries.append(
                LockEntry(
                    name=row["name"],
                    version=str(row["version"]),
                    source=source,
                )
            )
        return cls(
            fft_version=data.get("fft_version"),
            entries=entries,
        )

    def to_mapping(self) -> dict[str, Any]:
        return {
            "fft_version": self.fft_version,
            "packages": [e.to_mapping() for e in self.entries],
        }


@dataclass
class ResolvedPackage:
    """
    Concrete package that has been:

      - located (path or git checkout)
      - manifest-loaded (project.yml)
      - dependency-validated
    """

    name: str
    version: str
    root: Path  # directory containing project.yml + models/
    models_dir: str  # path inside root where models live
    source: LockedSource
    manifest: PackageManifest


# ---------------------------------------------------------------------------
# Public API
# ---------------------------------------------------------------------------


def resolve_packages(
    project_dir: Path,
    cfg: PackagesConfig | None = None,
) -> list[ResolvedPackage]:
    """
    Resolve all packages declared in packages.yml for the given project:

      - locate local path packages
      - clone/fetch git packages into .fastflowtransform/packages
      - load per-package project.yml as a manifest (name/version/fft_version/dependencies)
      - validate:

          * manifest.name matches spec.name
          * manifest.fft_version is compatible with FFT_VERSION (if declared)
          * spec.version (constraint) matches manifest.version (if declared)
          * inter-package dependencies are satisfied

      - write packages.lock.yml with pinned sources

    Returns a list of ResolvedPackage objects. If packages.yml is missing or
    empty, returns [].
    """
    project_dir = Path(project_dir).expanduser().resolve()

    if cfg is None:
        cfg = load_packages_config(project_dir)

    specs: list[PackageSpec] = list(cfg.packages or [])
    if not specs:
        return []

    cache_dir = project_dir / ".fastflowtransform" / "packages"
    cache_dir.mkdir(parents=True, exist_ok=True)

    manifests_by_name: dict[str, PackageManifest] = {}
    resolved_by_name: dict[str, ResolvedPackage] = {}

    for spec in specs:
        root = _materialize_package_source(project_dir, cache_dir, spec)
        manifest = _load_package_manifest(root)

        # Name check: spec.name must match manifest.name
        if spec.name and manifest.name != spec.name:
            raise RuntimeError(
                f"Package name mismatch for spec '{spec.name}': "
                f"manifest reports '{manifest.name}' in {root / 'project.yml'}."
            )

        # Check FFT core compatibility if declared in manifest
        if manifest.fft_version and not version_satisfies(FFT_VERSION, manifest.fft_version):
            raise RuntimeError(
                f"Package '{manifest.name}' ({manifest.version}) "
                f"requires FFT version '{manifest.fft_version}', "
                f"but running '{FFT_VERSION}'."
            )

        # Check that the spec's own version constraint (if any) is satisfied
        if spec.version and not version_satisfies(manifest.version, spec.version):
            raise RuntimeError(
                f"Package '{manifest.name}' has version {manifest.version} "
                f"but spec requires '{spec.version}'."
            )

        if manifest.name in manifests_by_name:
            other = manifests_by_name[manifest.name]
            raise RuntimeError(
                f"Duplicate package name '{manifest.name}' loaded from:\n"
                f"  - {other.root}\n"
                f"  - {root}\n"
                "Package names must be globally unique."
            )

        manifests_by_name[manifest.name] = manifest

        # Resolve models_dir: packages.yml overrides manifest.models_dir; default "models"
        models_dir = spec.models_dir or manifest.models_dir or "models"

        locked_src = _lock_source_for_spec(spec, root)
        resolved_by_name[manifest.name] = ResolvedPackage(
            name=manifest.name,
            version=manifest.version,
            root=root,
            models_dir=models_dir,
            source=locked_src,
            manifest=manifest,
        )

    # Validate dependencies (only across the set of resolved packages)
    _validate_package_dependencies(resolved_by_name)

    # Write lock file (best-effort; failure shouldn't be fatal)
    _write_lock_file(project_dir, list(resolved_by_name.values()))

    # Return in deterministic order
    return sorted(resolved_by_name.values(), key=lambda p: p.name)


# ---------------------------------------------------------------------------
# Manifest + source handling
# ---------------------------------------------------------------------------


def _load_package_manifest(root: Path) -> PackageManifest:
    """
    Load project.yml from a package root and extract basic fields:

      - name       (required)
      - version    (required)
      - fft_version (optional)
      - dependencies (optional)
      - models_dir (optional override; if absent, we default to 'models')
    """
    path = root / "project.yml"
    if not path.exists():
        raise RuntimeError(f"Package root {root} has no project.yml")

    with path.open("r", encoding="utf8") as f:
        data = yaml.safe_load(f) or {}

    name = str(data.get("name") or "").strip()
    if not name:
        raise RuntimeError(f"{path}: missing 'name' field")

    version = str(data.get("version") or "").strip()
    if not version:
        raise RuntimeError(f"{path}: missing 'version' field")

    fft_version = data.get("fft_version")
    if fft_version is not None:
        fft_version = str(fft_version).strip() or None

    models_dir = data.get("models_dir")
    if models_dir is not None:
        models_dir = str(models_dir).strip() or None

    deps_data = data.get("dependencies") or []
    deps: list[PackageDependency] = []
    if deps_data:
        if not isinstance(deps_data, list):
            raise RuntimeError(f"{path}: 'dependencies' must be a list if present.")
        for d in deps_data:
            if not isinstance(d, Mapping):
                raise RuntimeError(f"{path}: dependency entries must be mappings, got {d!r}")
            dep_name = str(d.get("name") or "").strip()
            if not dep_name:
                raise RuntimeError(f"{path}: dependency entry missing 'name'")
            vc = d.get("version")
            opt = bool(d.get("optional", False))
            deps.append(
                PackageDependency(
                    name=dep_name,
                    version_constraint=str(vc) if vc else None,
                    optional=opt,
                    raw=d,
                )
            )

    return PackageManifest(
        name=name,
        version=version,
        fft_version=fft_version,
        dependencies=deps,
        models_dir=models_dir,
        raw=data,
        root=root,
    )


def _materialize_package_source(
    project_dir: Path,
    cache_dir: Path,
    spec: PackageSpec,
) -> Path:
    """
    Turn a PackageSpec into a concrete directory on disk.

    - path packages → project_dir / path  (must exist)
    - git packages  → cloned/updated repo in cache_dir, then optional subdir
    """
    if spec.path is not None:
        base = Path(spec.path)
        if not base.is_absolute():
            base = (project_dir / base).resolve()
        if not base.exists():
            raise RuntimeError(f"Package '{spec.name}': path not found: {base}")
        if not base.is_dir():
            raise RuntimeError(f"Package '{spec.name}': path is not a directory: {base}")
        log.debug("Using path package '%s' at %s", spec.name, base)
        return base

    # git package
    if not spec.git:
        raise RuntimeError(
            f"Package '{spec.name}' must specify either 'path' or 'git' in packages.yml."
        )

    repo_root = _ensure_git_repo(cache_dir, spec)
    root = (repo_root / spec.subdir).resolve() if spec.subdir else repo_root

    if not root.exists():
        raise RuntimeError(
            f"Package '{spec.name}': subdir '{spec.subdir}' within repo does not exist "
            f"(root: {repo_root})"
        )
    if not root.is_dir():
        raise RuntimeError(
            f"Package '{spec.name}': subdir '{spec.subdir}' is not a directory (root: {repo_root})"
        )

    log.debug("Using git package '%s' at %s", spec.name, root)
    return root


def _ensure_git_repo(cache_dir: Path, spec: PackageSpec) -> Path:
    """
    Ensure we have a local clone for the given git package in cache_dir and
    return the checked-out directory.

    Semantics:

      - path packages: handled in _materialize_package_source (not here)
      - git + rev:    pinned to specific commit (no auto-upgrade)
      - git + tag:    pinned to tag (no auto-upgrade, aside from tag being moved)
      - git + branch: tracks origin/<branch> on each run (auto-upgrade)
      - git with none of rev/tag/branch: just use HEAD (whatever the repo's default is)
    """
    assert spec.git
    git_root = cache_dir / "git"
    git_root.mkdir(parents=True, exist_ok=True)

    repo_slug = _slug_git_url(spec.git, spec.name)
    repo_dir = git_root / repo_slug / "repo"
    repo_dir.parent.mkdir(parents=True, exist_ok=True)

    if not repo_dir.exists():
        echo(f"Cloning package '{spec.name}' from {spec.git} ...")
        _run_git(["clone", "--no-tags", "--quiet", spec.git, str(repo_dir)])
    else:
        # Always try to update remotes; failures are non-fatal (offline etc.).
        try:
            _run_git(
                [
                    "-C",
                    str(repo_dir),
                    "fetch",
                    "--all",
                    "--tags",
                    "--prune",
                    "--quiet",
                ]
            )
        except Exception as exc:  # pragma: no cover
            log.debug("Git fetch failed for %s: %s", spec.git, exc)

    # Decide which selector to use.
    # NOTE: PackageSpec already maps `ref` → `rev` if no rev/tag/branch is set.
    if spec.rev:
        # Pinned commit (or generic ref treated as rev): no auto-upgrade.
        ref = spec.rev
        echo(f"Checking out {spec.name}@{ref} (pinned rev) ...")
        _run_git(["-C", str(repo_dir), "checkout", "--quiet", ref])

    elif spec.tag:
        # Pinned tag: we assume tags are stable; we don't auto-reset anything.
        ref = spec.tag
        echo(f"Checking out {spec.name}@{ref} (tag) ...")
        _run_git(["-C", str(repo_dir), "checkout", "--quiet", ref])

    elif spec.branch:
        # Moving branch: make local <branch> track origin/<branch> and reset to it.
        branch = spec.branch
        echo(f"Checking out {spec.name}@{branch} (tracking origin/{branch}) ...")

        # Create or update local branch to follow origin/<branch>
        # -B: create or reset branch to start-point
        _run_git(
            [
                "-C",
                str(repo_dir),
                "checkout",
                "--quiet",
                "-B",
                branch,
                f"origin/{branch}",
            ]
        )
        # Force working tree to that commit (avoid local drift)
        _run_git(
            [
                "-C",
                str(repo_dir),
                "reset",
                "--hard",
                f"origin/{branch}",
            ]
        )

    else:
        # No explicit selector: just ensure we are on whatever HEAD currently is.
        echo(f"Checking out {spec.name}@HEAD ...")
        _run_git(["-C", str(repo_dir), "checkout", "--quiet", "HEAD"])

    return repo_dir


def _run_git(args: list[str]) -> None:
    try:
        # text=True so stdout/stderr are already str, not bytes
        subprocess.run(
            ["git", *args],
            check=True,
            capture_output=True,
            text=True,
        )
    except FileNotFoundError as exc:  # pragma: no cover
        raise RuntimeError(
            "git executable not found. Git-based packages require git to be installed and on PATH."
        ) from exc
    except subprocess.CalledProcessError as exc:
        stdout = (exc.stdout or "").strip()
        stderr = (exc.stderr or "").strip()

        cmd_str = "git " + " ".join(args)

        # Very rough classification, but enough for common cases
        if "Authentication failed" in stderr or "Permission denied" in stderr:
            raise RuntimeError(f"{cmd_str} failed: authentication error.\n{stderr}") from exc

        if "Repository not found" in stderr:
            raise RuntimeError(f"{cmd_str} failed: repository not found.\n{stderr}") from exc

        if "did not match any file(s) known to git" in stderr or "unknown revision" in stderr:
            raise RuntimeError(
                f"{cmd_str} failed: requested ref/branch/tag does not exist.\n{stderr}"
            ) from exc

        # Fallback: show full stdout/stderr
        raise RuntimeError(
            f"git command failed: {cmd_str}\nstdout:\n{stdout}\nstderr:\n{stderr}"
        ) from exc


def _slug_git_url(url: str, name: str) -> str:
    base = f"{name}@{url}"
    return re.sub(r"[^A-Za-z0-9_.-]", "_", base)


def _lock_source_for_spec(spec: PackageSpec, root: Path) -> LockedSource:
    if spec.path is not None:
        return LockedSource(
            kind="path",
            path=str(root),
        )

    # git - try to figure out the concrete commit SHA at HEAD
    rev = spec.rev
    if spec.git:
        try:
            result = subprocess.run(
                ["git", "-C", str(root), "rev-parse", "HEAD"], check=True, capture_output=True
            )
            head = result.stdout.decode().strip()
            if head:
                rev = head
        except Exception:  # pragma: no cover - best-effort
            pass

    return LockedSource(
        kind="git",
        git=spec.git,
        rev=rev,
        subdir=spec.subdir,
    )


# ---------------------------------------------------------------------------
# Dependency validation
# ---------------------------------------------------------------------------


def _validate_package_dependencies(pkgs: Mapping[str, ResolvedPackage]) -> None:
    """
    Enforce that package-level dependencies are satisfied by the resolved set.

    There is exactly one version per package name in the current project.
    Dependencies simply assert that:

      - a package with `name` exists, and
      - its manifest.version satisfies the declared version constraint.
    """
    for pkg in pkgs.values():
        for dep in pkg.manifest.dependencies:
            target = pkgs.get(dep.name)
            if not target:
                if dep.optional:
                    log.debug(
                        "Optional dependency '%s' of package '%s' not present; skipping.",
                        dep.name,
                        pkg.name,
                    )
                    continue
                raise RuntimeError(
                    f"Package '{pkg.name}' depends on '{dep.name}', "
                    "but no package with that name is declared in packages.yml."
                )

            if dep.version_constraint and not version_satisfies(
                target.version, dep.version_constraint
            ):
                raise RuntimeError(
                    f"Package '{pkg.name}' requires '{dep.name}' "
                    f"with version '{dep.version_constraint}', "
                    f"but resolved version is '{target.version}'."
                )


# ---------------------------------------------------------------------------
# Lock file IO
# ---------------------------------------------------------------------------


def _write_lock_file(project_dir: Path, packages: list[ResolvedPackage]) -> None:
    lock_path = project_dir / "packages.lock.yml"

    entries = [
        LockEntry(
            name=pkg.name,
            version=pkg.version,
            source=pkg.source,
        )
        for pkg in packages
    ]

    lock = LockFile(
        fft_version=FFT_VERSION,
        entries=entries,
    )

    try:
        with lock_path.open("w", encoding="utf8") as f:
            yaml.safe_dump(lock.to_mapping(), f, sort_keys=False)
        log.debug("Wrote packages.lock.yml with %d entries", len(entries))
    except Exception as exc:  # pragma: no cover
        log.warning("Failed to write packages.lock.yml: %s", exc)


# ---------------------------------------------------------------------------
# Very small semver helper (x.y.z only, with optional -suffix)
# ---------------------------------------------------------------------------


_SEMVER_RE = re.compile(r"^(\d+)\.(\d+)\.(\d+)(?:[-+].*)?$")


def parse_version(v: str) -> tuple[int, int, int]:
    """
    Parse a very simple semver string 'MAJOR.MINOR.PATCH'.

    We ignore pre-release / build metadata; they are treated as equal.

    Raises ValueError if we cannot parse.
    """
    m = _SEMVER_RE.match(v.strip())
    if not m:
        raise ValueError(f"Invalid semantic version (expected 'x.y.z'): {v!r}")
    return int(m.group(1)), int(m.group(2)), int(m.group(3))


def compare_versions(a: str, b: str) -> int:
    """
    Compare two version strings.

      <0: a < b
       0: a == b
      >0: a > b
    """
    a_t = parse_version(a)
    b_t = parse_version(b)
    if a_t < b_t:
        return -1
    if a_t > b_t:
        return 1
    return 0


def _expand_caret(v: str) -> str:
    """
    ^1.2.3  →  >=1.2.3,<2.0.0
    ^0.3.0  →  >=0.3.0,<0.4.0
    ^0.0.4  →  >=0.0.4,<0.0.5
    """
    major, minor, patch = parse_version(v)
    if major > 0:
        upper = f"{major + 1}.0.0"
    elif minor > 0:
        upper = f"0.{minor + 1}.0"
    else:
        upper = f"0.0.{patch + 1}"
    return f">={v},<{upper}"


def _expand_tilde(v: str) -> str:
    """
    ~1.2.3  →  >=1.2.3,<1.3.0
    """
    major, minor, patch = parse_version(v)
    upper = f"{major}.{minor + 1}.0"
    norm = f"{major}.{minor}.{patch}"
    return f">={norm},<{upper}"


def _parse_constraints(expr: str) -> list[tuple[str, str]]:
    """
    Parse a constraint expression into (op, version) pairs.

    Supported forms (combined with commas or spaces):

      "1.2.3"          -> ==1.2.3
      ">=1.2.0,<2.0.0"
      ">1.0.0 <=2.0.0"
      "^1.2.3"
      "~1.4.0"

    Returns a list of (op, version), where op ∈ { "==", "!=", ">", "<", ">=", "<=" }.
    """
    expr = expr.strip()
    if not expr:
        return []

    # Expand ^ and ~ first (they return comma-joined ranges)
    if expr.startswith("^"):
        expr = _expand_caret(expr[1:].strip())
    elif expr.startswith("~"):
        expr = _expand_tilde(expr[1:].strip())

    parts = re.split(r"[,\s]+", expr)
    out: list[tuple[str, str]] = []
    for p in parts:
        _p = p.strip()
        if not _p:
            continue
        m = re.match(r"^(>=|<=|==|!=|>|<)?\s*(\d+\.\d+\.\d+)$", _p)
        if not m:
            # bare version "1.2.3" means "==1.2.3"
            if _SEMVER_RE.match(p):
                op = "=="
                v = _p
            else:
                raise ValueError(f"Invalid version constraint token: {_p!r}")
        else:
            op = m.group(1) or "=="
            v = m.group(2)
        out.append((op, v))
    return out


def version_satisfies(actual: str, constraint: str | None) -> bool:
    """
    Return True iff a version string 'actual' satisfies a constraint expression.

    Empty / None constraint always returns True.
    """
    if not constraint:
        return True
    checks = _parse_constraints(constraint)
    for op, target in checks:
        cmp = compare_versions(actual, target)
        if op == "==":
            if cmp != 0:
                return False
        elif op == "!=":
            if cmp == 0:
                return False
        elif op == ">":
            if cmp <= 0:
                return False
        elif op == "<":
            if cmp >= 0:
                return False
        elif op == ">=":
            if cmp < 0:
                return False
        elif op == "<=":
            if cmp > 0:
                return False
        else:  # pragma: no cover
            raise ValueError(f"Unknown operator in version constraint: {op!r}")
    return True

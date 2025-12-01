from __future__ import annotations

from collections.abc import Mapping
from pathlib import Path
from typing import Any

import yaml
from pydantic import BaseModel, ConfigDict, Field, model_validator


class PackageSpec(BaseModel):
    """
    One entry from packages.yml, for example:

      packages:
        - name: fft_utils
          path: "../fft_utils"
          models_dir: "models"

    Or (shorthand mapping form):

      fft_utils: "../fft_utils"

    For git-based packages:

      - name: shared_package_git
        git: "https://github.com/org/repo.git"
        subdir: "path/inside/repo"
        # one of the revision selectors below is optional:
        #   - ref: "main"      (generic alias, mapped to `rev`)
        #   - rev: "abc1234"   (commit SHA)
        #   - tag: "v1.2.3"
        #   - branch: "main"
        # models_dir: "models"  # optional, default "models"
    """

    model_config = ConfigDict(extra="forbid")

    name: str

    # Exactly one of `path` or `git` must be set.
    path: str | None = None
    git: str | None = None

    # Optional git parameters (ignored for path-based packages).
    #
    # "ref" is a user-facing alias (branch/tag/commit); internally we map it to `rev`
    # if no more-specific selector (rev/tag/branch) is provided.
    ref: str | None = None
    rev: str | None = None
    tag: str | None = None
    branch: str | None = None
    subdir: str | None = None

    # Where models live inside the package root (default: "models").
    # This can be overridden by the package's own project.yml (models_dir),
    # but packages.yml always wins if set explicitly.
    models_dir: str = "models"

    # Optional constraint for the package's manifest version (semver expression).
    # Example: ">=1.0.0,<2.0.0"
    version: str | None = None

    @model_validator(mode="after")
    def _validate_source(self) -> PackageSpec:
        """
        Ensure that exactly one of `path` or `git` is set.
        Also treat `ref` as a generic alias for `rev` when no other
        more-specific selector (rev/tag/branch) is given.
        """
        has_path = bool(self.path)
        has_git = bool(self.git)

        if has_path == has_git:
            # Either both set or both unset → error.
            raise ValueError(
                f"Package '{self.name}': exactly one of 'path' or 'git' must be set "
                "in packages.yml."
            )

        # If user provided a generic `ref` but no explicit rev/tag/branch,
        # map it to `rev` so downstream resolver can just look at rev/tag/branch.
        if self.ref and not (self.rev or self.tag or self.branch):
            self.rev = self.ref

        return self


class PackagesConfig(BaseModel):
    """
    Top-level representation of packages.yml.

    We accept two shapes:

      1) Explicit:

         packages:
           - name: fft_utils
             path: "../fft_utils"
             models_dir: "models"

      2) Shorthand mapping:

         fft_utils: "../fft_utils"
         other_pkg:
           path: "../other"
           models_dir: "dbt_models"
    """

    model_config = ConfigDict(extra="forbid")

    packages: list[PackageSpec] = Field(default_factory=list)


def _normalize_raw_packages(raw: Any) -> dict[str, Any]:
    """
    Normalize the various accepted YAML shapes into:

        {"packages": [ {name, path?|git?, models_dir?, ...}, ... ]}
    """
    if raw is None:
        return {"packages": []}

    # Case 1: already a list -> treat as `packages: [...]`
    if isinstance(raw, list):
        return {"packages": raw}

    # Case 2: mapping with explicit 'packages' key
    if isinstance(raw, Mapping):
        if "packages" in raw:
            return {"packages": raw["packages"] or []}

        # Case 3: shorthand mapping name -> path or dict
        pkgs: list[dict[str, Any]] = []
        for name, cfg in raw.items():
            if isinstance(cfg, str):
                # shorthand "pkg: ../path"
                pkgs.append({"name": str(name), "path": cfg})
            elif isinstance(cfg, Mapping):
                d = dict(cfg)
                d.setdefault("name", str(name))
                pkgs.append(d)
        return {"packages": pkgs}

    raise TypeError("packages.yml must be a list or a mapping")


def load_packages_config(project_dir: Path) -> PackagesConfig:
    """
    Read packages.yml under `project_dir` and return a strict PackagesConfig.

    If the file does not exist, we return an empty config (no packages).
    """
    cfg_path = project_dir / "packages.yml"
    if not cfg_path.exists():
        return PackagesConfig()

    raw = yaml.safe_load(cfg_path.read_text(encoding="utf-8")) or {}
    norm = _normalize_raw_packages(raw)
    return PackagesConfig.model_validate(norm)

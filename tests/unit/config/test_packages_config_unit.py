# tests/test_packages_config.py
from __future__ import annotations

from pathlib import Path

import pytest
from pydantic import ValidationError

from fastflowtransform.config.packages import (
    PackagesConfig,
    _normalize_raw_packages,
    load_packages_config,
)


def test_normalize_raw_packages_list_form() -> None:
    raw = [
        {"name": "pkg1", "path": "../pkg1"},
        {"name": "pkg2", "git": "https://example.com/repo.git"},
    ]
    norm = _normalize_raw_packages(raw)
    assert "packages" in norm
    assert len(norm["packages"]) == 2
    assert norm["packages"][0]["name"] == "pkg1"
    assert norm["packages"][1]["name"] == "pkg2"


def test_normalize_raw_packages_mapping_with_packages_key() -> None:
    raw = {
        "packages": [
            {"name": "pkg1", "path": "../pkg1"},
        ]
    }
    norm = _normalize_raw_packages(raw)
    assert len(norm["packages"]) == 1
    assert norm["packages"][0]["name"] == "pkg1"


def test_normalize_raw_packages_shorthand_mapping() -> None:
    raw = {
        "pkg1": "../pkg1",
        "pkg2": {
            "path": "../pkg2",
            "models_dir": "dbt_models",
        },
    }
    norm = _normalize_raw_packages(raw)
    pkgs = {row["name"]: row for row in norm["packages"]}
    assert pkgs["pkg1"]["path"] == "../pkg1"
    assert pkgs["pkg2"]["path"] == "../pkg2"
    assert pkgs["pkg2"]["models_dir"] == "dbt_models"


def test_packages_config_loads_path_package(tmp_path: Path) -> None:
    cfg_text = """
    packages:
      - name: shared_package
        path: "../shared_package"
        models_dir: "models"
    """
    project_dir = tmp_path
    (project_dir / "packages.yml").write_text(cfg_text, encoding="utf-8")

    cfg = load_packages_config(project_dir)
    assert isinstance(cfg, PackagesConfig)
    assert len(cfg.packages) == 1

    spec = cfg.packages[0]
    assert spec.name == "shared_package"
    assert spec.path == "../shared_package"
    assert spec.git is None
    assert spec.models_dir == "models"


def test_git_package_ref_is_mapped_to_rev() -> None:
    data = {
        "packages": [
            {
                "name": "shared_package_git",
                "git": "https://example.com/repo.git",
                "ref": "main",
                "subdir": "pkg",
            }
        ]
    }
    norm = _normalize_raw_packages(data)
    cfg = PackagesConfig.model_validate(norm)
    assert len(cfg.packages) == 1
    spec = cfg.packages[0]

    # `ref` should be preserved, but also mapped to `rev`
    assert spec.git == "https://example.com/repo.git"
    assert spec.ref == "main"
    assert spec.rev == "main"
    assert spec.subdir == "pkg"
    assert spec.path is None


def test_package_spec_requires_exactly_one_of_path_or_git() -> None:
    # both path and git → error
    norm = {
        "packages": [
            {
                "name": "bad",
                "path": "../pkg",
                "git": "https://example.com/repo.git",
            }
        ]
    }
    with pytest.raises(ValidationError):
        PackagesConfig.model_validate(norm)

    # neither path nor git → error
    norm2 = {"packages": [{"name": "bad2"}]}
    with pytest.raises(ValidationError):
        PackagesConfig.model_validate(norm2)

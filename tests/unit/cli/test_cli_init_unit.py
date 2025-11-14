from __future__ import annotations

from pathlib import Path

import pytest
from typer.testing import CliRunner

from fastflowtransform.cli import app


def _read(path: Path) -> str:
    return path.read_text(encoding="utf-8")


@pytest.mark.unit
def test_init_creates_minimal_skeleton(tmp_path: Path):
    runner = CliRunner()
    target = tmp_path / "warehouse"

    result = runner.invoke(app, ["init", str(target), "--engine", "duckdb"])
    assert result.exit_code == 0, result.output

    # Core directories exist
    for rel in ("models", "seeds", "tests/unit", "docs"):
        assert (target / rel).is_dir(), f"missing directory {rel}"

    # Configuration files contain doc references and comments
    project_yaml = _read(target / "project.yml")
    assert "docs/Project_Config.md" in project_yaml
    assert "tests: []" in project_yaml

    profiles_yaml = _read(target / "profiles.yml")
    assert "docs/Profiles.md" in profiles_yaml
    assert "duckdb" in profiles_yaml

    sources_yaml = _read(target / "sources.yml")
    assert "docs/Sources.md" in sources_yaml

    readme = _read(target / "README.md")
    assert "docs/Quickstart.md" in readme

    models_note = _read(target / "models/README.md")
    assert "Config_and_Macros.md" in models_note

    tests_note = _read(target / "tests/unit/README.md")
    assert "fft utest" in tests_note


@pytest.mark.unit
def test_init_refuses_existing_directory(tmp_path: Path):
    runner = CliRunner()
    target = tmp_path / "existing"
    target.mkdir()

    result = runner.invoke(app, ["init", str(target)])
    assert result.exit_code == 1
    assert "already exists" in result.output
    # No files should have been created
    assert not list(target.glob("*"))


@pytest.mark.unit
@pytest.mark.parametrize("engine", ["unknown", "sqlite"])
def test_init_validates_engine(engine: str, tmp_path: Path):
    runner = CliRunner()
    target = tmp_path / engine

    result = runner.invoke(app, ["init", str(target), "--engine", engine])
    expected_exit_code = 2
    assert result.exit_code == expected_exit_code
    assert "Unsupported engine" in result.output
    assert not target.exists()

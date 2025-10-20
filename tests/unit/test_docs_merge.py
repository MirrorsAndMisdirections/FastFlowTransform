from pathlib import Path

import yaml

from flowforge.docs import read_docs_metadata


def _write(p: Path, text: str) -> None:
    p.parent.mkdir(parents=True, exist_ok=True)
    p.write_text(text, encoding="utf-8")


def test_markdown_overrides_yaml(tmp_path: Path, monkeypatch):
    """Markdown model/column descriptions must override YAML (priority: MD > YAML)."""
    proj = tmp_path / "proj"
    proj.mkdir()
    # Minimal project.yml with YAML descriptions
    project_yml = {
        "name": "demo",
        "version": "0.1",
        "models_dir": "models",
        "docs": {
            "models": {
                "users_enriched": {
                    "description": "YAML Desc",
                    "columns": {"email_upper": "YAML Col Desc"},
                }
            }
        },
    }
    _write(proj / "project.yml", yaml.safe_dump(project_yml))

    # Markdown overrides
    _write(proj / "docs" / "models" / "users_enriched.md", "# Title\n\nMD Desc")
    _write(proj / "docs" / "columns" / "users_enriched" / "email_upper.md", "MD Col Desc")

    meta = read_docs_metadata(proj)
    models = meta["models"]
    cols = meta["columns"]

    # Model description overridden by Markdown
    assert "users_enriched" in models
    assert models["users_enriched"]["description_html"] is not None
    assert "MD Desc" in models["users_enriched"]["description_html"]
    assert "YAML Desc" not in models["users_enriched"]["description_html"]

    # Column description overridden by Markdown
    assert "users_enriched" in cols
    assert "email_upper" in cols["users_enriched"]
    assert "MD Col Desc" in cols["users_enriched"]["email_upper"]
    assert "YAML Col Desc" not in cols["users_enriched"]["email_upper"]

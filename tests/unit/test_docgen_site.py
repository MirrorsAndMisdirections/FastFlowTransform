import json
import shutil
from pathlib import Path

import yaml

from tests.common.utils import ROOT, run


def _write(p: Path, text: str) -> None:
    p.parent.mkdir(parents=True, exist_ok=True)
    p.write_text(text, encoding="utf-8")


def test_docgen_generates_descriptions_and_lineage(tmp_path):
    """End-to-end: docgen outputs HTML with Description/Columns/Lineage and a JSON manifest."""
    # Copy example project to an isolated temp dir
    src_proj = ROOT / "examples" / "simple_duckdb"
    proj = tmp_path / "proj"
    shutil.copytree(src_proj, proj)

    # Extend project.yml with docs YAML and a very simple SQL model that guarantees clear lineage
    proj_yml = yaml.safe_load((proj / "project.yml").read_text(encoding="utf-8"))
    proj_yml.setdefault("docs", {}).setdefault("models", {})
    proj_yml["docs"]["models"]["users_enriched"] = {
        "description": "YAML users_enriched description",
        "columns": {"email": "Original email", "is_gmail": "Gmail flag"},
    }
    (proj / "project.yml").write_text(yaml.safe_dump(proj_yml), encoding="utf-8")

    # Markdown override for model description (should win over YAML)
    _write(proj / "docs" / "models" / "users_enriched.md", "MD model description")
    # Markdown column description override (should win over YAML)
    _write(proj / "docs" / "columns" / "users_enriched" / "email.md", "MD column description")

    # Run build first so schema introspection sees physical tables
    env = {"FF_ENGINE": "duckdb", "FF_DUCKDB_PATH": str(proj / ".local" / "demo.duckdb")}
    run(["fft", "seed", str(proj), "--env", "dev"], env)
    run(["fft", "run", str(proj), "--env", "dev"], env)

    out_dir = proj / "site" / "docs"
    manifest_path = out_dir / "docs_manifest.json"
    res = run(
        [
            "fft",
            "docgen",
            str(proj),
            "--env",
            "dev",
            "--out",
            str(out_dir),
            "--emit-json",
            str(manifest_path),
        ],
        env,
    )
    assert res.returncode == 0, res.stdout

    # HTML page exists and includes description + columns + lineage heading
    index_html = (out_dir / "index.html").read_text(encoding="utf-8")
    assert "FastFlowTransform - DAG & Mini Docs" in index_html
    model_html = (out_dir / "users_enriched.html").read_text(encoding="utf-8")
    assert "Description" in model_html
    assert "Columns" in model_html
    assert "Lineage" in model_html
    # Markdown model description took precedence
    assert "MD model description" in model_html

    # JSON manifest exists and has at least one lineage entry somewhere
    data = json.loads(manifest_path.read_text(encoding="utf-8"))
    assert "models" in data and isinstance(data["models"], list) and len(data["models"]) >= 1
    any_lineage = False
    for m in data["models"]:
        for c in m.get("columns", []):
            if c.get("lineage"):
                any_lineage = True
                break
        if any_lineage:
            break
    assert any_lineage, "Expected at least one lineage entry in manifest"

from __future__ import annotations

from pathlib import Path

from fastflowtransform.core import Registry


def test_python_model_tags_propagate_to_node(tmp_path: Path, monkeypatch) -> None:
    project_dir = tmp_path / "proj"
    models_dir = project_dir / "models"
    models_dir.mkdir(parents=True)

    model_file = models_dir / "py_tagged.ff.py"
    model_file.write_text(
        (
            "from fastflowtransform import model\n\n"
            "@model(name='py_tagged', tags=['example', 'demo'], meta={'materialized': 'view'})\n"
            "def build(df=None):\n"
            "    return df\n"
        ),
        encoding="utf-8",
    )

    isolated_registry = Registry()
    monkeypatch.setattr("fastflowtransform.core.REGISTRY", isolated_registry, raising=False)
    monkeypatch.setattr("fastflowtransform.decorators.REGISTRY", isolated_registry, raising=False)
    monkeypatch.setattr("fastflowtransform.REGISTRY", isolated_registry, raising=False)

    isolated_registry.load_project(project_dir)

    node = isolated_registry.get_node("py_tagged")
    assert node.kind == "python"
    assert node.meta.get("materialized") == "view"
    assert set(node.meta.get("tags", [])) == {"example", "demo"}

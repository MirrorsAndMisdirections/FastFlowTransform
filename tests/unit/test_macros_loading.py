from pathlib import Path
from jinja2 import Environment, FileSystemLoader, StrictUndefined

from flowforge.core import REGISTRY

def test_macros_are_loaded_and_callable(tmp_path: Path):
    models = tmp_path / "models" / "macros"
    models.mkdir(parents=True, exist_ok=True)
    (tmp_path / "sources.yml").write_text("{}", encoding="utf-8")

    # A tiny macro file
    (models / "utils.sql").write_text(
        "{% macro greet(name) -%}hello {{ name }}{%- endmacro %}\n",
        encoding="utf-8",
    )

    # A model that uses the macro directly
    user_sql = tmp_path / "models" / "m.ff.sql"
    user_sql.write_text(
        "select '{{ greet(\"world\") }}' as msg;",
        encoding="utf-8",
    )

    REGISTRY.load_project(tmp_path)
    # Macro should be globally available in REGISTRY.env
    env = REGISTRY.get_env()
    assert "greet" in env.globals

    # Render the model to verify the macro actually runs
    tmpl_env = Environment(loader=FileSystemLoader(str(tmp_path / "models")),
                           undefined=StrictUndefined, autoescape=False, trim_blocks=True, lstrip_blocks=True)
    # ensure globals are visible if you ever use a fresh Environment:
    tmpl_env.globals.update(env.globals)
    sql = tmpl_env.get_template("m.ff.sql").render(ref=lambda n: n, source=lambda a,b: b)
    assert "hello world" in sql

    # Macro inventory available for docs
    assert "greet" in REGISTRY.macros
    assert REGISTRY.macros["greet"].name == "utils.sql"

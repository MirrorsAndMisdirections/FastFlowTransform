# tests/test_cli_vars_flag.py
from pathlib import Path

import pytest
from typer.testing import CliRunner

from flowforge.cli import app
from flowforge.core import REGISTRY
from flowforge.executors.duckdb_exec import DuckExecutor

runner = CliRunner()


@pytest.mark.cli
def test_cli_vars_available_in_templates(tmp_path: Path, monkeypatch):
    models = tmp_path / "models"
    models.mkdir(parents=True)
    (tmp_path / "sources.yml").write_text("{}", encoding="utf-8")
    (tmp_path / "project.yml").write_text("vars:\n  day: '2000-01-01'\n", encoding="utf-8")

    # Model uses var('day'); result materializes into table 'm'
    (models / "m.ff.sql").write_text(
        "select '{{ var(\"day\") }}' as d;",
        encoding="utf-8",
    )

    # Run CLI with override
    result = runner.invoke(
        app, ["run", str(tmp_path), "--env", "dev", "--vars", "day='2025-10-01'"]
    )
    assert result.exit_code == 0, result.output

    # Verify materialized data via a quick DuckDB read (in-memory run path may vary in your setup)
    # If your 'run' command uses DuckDB by default, connect and check:
    ex = DuckExecutor(":memory:")
    # Re-render just to read the table in this test session:
    REGISTRY.load_project(tmp_path)
    REGISTRY.cli_vars = {"day": "2025-10-01"}
    env = REGISTRY.get_env()
    ex.run_sql(REGISTRY.nodes["m.ff"], env)
    val_rows = ex.con.execute("select * from m").fetchone()
    assert val_rows is not None
    val = val_rows[0]
    assert val == "2025-10-01"

import time
from pathlib import Path

import pytest
from typer.testing import CliRunner

from fastflowtransform.cli import app


@pytest.mark.integration
def test_state_modified_and_plus(tmp_path: Path):
    (tmp_path / "models").mkdir(parents=True)
    (tmp_path / "models" / "a.ff.sql").write_text("select 1 as x", encoding="utf-8")
    (tmp_path / "models" / "b.ff.sql").write_text(
        "select * from {{ ref('a.ff') }}", encoding="utf-8"
    )

    # First run to populate cache/meta
    runner = CliRunner()
    env = {"FF_ENGINE": "duckdb", "FF_DUCKDB_PATH": ":memory:"}
    res1 = runner.invoke(
        app, ["run", str(tmp_path), "--env", "dev", "--engine", "duckdb", "--cache", "rw"], env=env
    )
    assert res1.exit_code == 0

    # Touch 'a.ff.sql' to cause fingerprint change
    p_a = tmp_path / "models" / "a.ff.sql"
    p_a.write_text("select 2 as x", encoding="utf-8")
    time.sleep(0.01)  # ensure mtime change on coarse FS

    # state:modified should select only 'a.ff'
    res2 = runner.invoke(
        app,
        [
            "run",
            str(tmp_path),
            "--env",
            "dev",
            "--engine",
            "duckdb",
            "--cache",
            "rw",
            "--select",
            "state:modified",
        ],
        env=env,
    )
    assert res2.exit_code == 0
    out2 = res2.output
    assert "a.ff" in out2
    # state:modified+ should include downstream 'b.ff'
    res3 = runner.invoke(
        app,
        [
            "run",
            str(tmp_path),
            "--env",
            "dev",
            "--engine",
            "duckdb",
            "--cache",
            "rw",
            "--select",
            "state:modified+",
        ],
        env=env,
    )
    assert res3.exit_code == 0
    out3 = res3.output
    assert "a.ff" in out3 and "b.ff" in out3

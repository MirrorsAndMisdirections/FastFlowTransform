import pytest
import typer

from fastflowtransform.ci.core import CiIssue, CiSummary
from fastflowtransform.cli import ci_cmd


class DummyProfile:
    def __init__(self, engine: str = "duckdb") -> None:
        self.engine = engine


class DummyCtx:
    def __init__(self, engine: str = "duckdb") -> None:
        self.profile = DummyProfile(engine=engine)
        # Just something printable - run.py also uses ctx.project in logs.
        self.project = "examples/dq_demo"


@pytest.mark.unit
def test_ci_check_no_issues_exit_zero(monkeypatch, capsys):
    """
    When run_ci_check returns no issues, ci_check should:
      - print a summary,
      - exit with code 0 (i.e. not raise typer.Exit).
    """

    # Avoid loading a real project
    def fake_prepare_context(project, env_name, engine, vars):
        assert project == "."
        assert env_name == "dev_duckdb"
        return DummyCtx(engine="duckdb")

    monkeypatch.setattr(ci_cmd, "_prepare_context", fake_prepare_context, raising=True)

    # Make _parse_select deterministic
    def fake_parse_select(tokens):
        # ci_check passes select or [] into _parse_select
        assert tokens == []
        return ["tag:ci"]

    monkeypatch.setattr(ci_cmd, "_parse_select", fake_parse_select, raising=True)

    # run_ci_check returns an all-green summary
    def fake_run_ci_check(select=None):
        # We want to see the parsed tokens arrive here
        assert select == ["tag:ci"]
        return CiSummary(
            issues=[],
            selected_nodes=["model_a", "model_b"],
            all_nodes=["model_a", "model_b", "model_c"],
        )

    monkeypatch.setattr(ci_cmd, "run_ci_check", fake_run_ci_check, raising=True)

    # Should not raise
    ci_cmd.ci_check(
        project=".",
        env_name="dev_duckdb",
        engine=None,
        vars=None,
        select=None,
    )

    out = capsys.readouterr().out

    # Basic smoke checks on the output
    assert "CI Check Summary" in out
    assert "Profile: dev_duckdb | Engine: duckdb" in out
    assert "Models:" in out
    assert "Issues" in out
    assert "None" in out  # "None 🎉" is fine, we just look for "None"
    assert "Selected models" in out
    assert "model_a" in out
    assert "model_b" in out


@pytest.mark.unit
def test_ci_check_warn_only_exit_zero(monkeypatch, capsys):
    """
    If all issues are 'warn'-level, ci_check should still exit with code 0.
    """

    def fake_prepare_context(project, env_name, engine, vars):
        return DummyCtx(engine="duckdb")

    monkeypatch.setattr(ci_cmd, "_prepare_context", fake_prepare_context, raising=True)

    def fake_parse_select(tokens):
        return ["tag:warn-only"]

    monkeypatch.setattr(ci_cmd, "_parse_select", fake_parse_select, raising=True)

    warn_issue = CiIssue(
        code="STYLE",
        level="warn",
        message="Minor style nit",
        obj_name="customers",
        file="models/customers.ff.sql",
        line=12,
        column=3,
    )

    def fake_run_ci_check(select=None):
        assert select == ["tag:warn-only"]
        return CiSummary(
            issues=[warn_issue],
            selected_nodes=["customers"],
            all_nodes=["customers", "orders"],
        )

    monkeypatch.setattr(ci_cmd, "run_ci_check", fake_run_ci_check, raising=True)

    # Should NOT raise typer.Exit
    ci_cmd.ci_check(
        project=".",
        env_name="dev_duckdb",
        engine=None,
        vars=None,
        select=["tag:warn-only"],
    )

    out = capsys.readouterr().out
    assert "CI Check Summary" in out
    # We expect a [W] line for the warning
    assert "[W] STYLE" in out
    assert "Minor style nit" in out


@pytest.mark.unit
def test_ci_check_with_error_issues_exit_one(monkeypatch, capsys):
    """
    If any issue has level='error', ci_check should raise typer.Exit(1).
    """

    def fake_prepare_context(project, env_name, engine, vars):
        return DummyCtx(engine="duckdb")

    monkeypatch.setattr(ci_cmd, "_prepare_context", fake_prepare_context, raising=True)

    def fake_parse_select(tokens):
        # We expect to receive the raw --select tokens here
        assert tokens == ["tag:changed"]
        return ["tag:changed"]

    monkeypatch.setattr(ci_cmd, "_parse_select", fake_parse_select, raising=True)

    err_issue = CiIssue(
        code="MISSING_DEP",
        level="error",
        message="Missing dependencies for 'orders.ff': customers.ff",
        obj_name="orders.ff",
        file="models/orders.ff.sql",
        line=20,
        column=5,
    )

    def fake_run_ci_check(select=None):
        # We should see the parsed tokens passed through to core
        assert select == ["tag:changed"]
        return CiSummary(
            issues=[err_issue],
            selected_nodes=["orders.ff"],
            all_nodes=["orders.ff", "customers.ff"],
        )

    monkeypatch.setattr(ci_cmd, "run_ci_check", fake_run_ci_check, raising=True)

    with pytest.raises(typer.Exit) as excinfo:
        ci_cmd.ci_check(
            project=".",
            env_name="dev_duckdb",
            engine=None,
            vars=None,
            select=["tag:changed"],
        )

    # Exit code should be 1
    assert excinfo.value.exit_code == 1

    out = capsys.readouterr().out
    assert "CI Check Summary" in out
    assert "MISSING_DEP" in out
    assert "orders.ff" in out
    assert "Missing dependencies" in out
    assert "models/orders.ff.sql" in out

import io
import sys

import pytest

from fastflowtransform.cli.test_cmd import DQResult, _print_summary


@pytest.mark.unit
def test_prints_params_and_example_sql():
    buf = io.StringIO()
    old = sys.stdout
    sys.stdout = buf
    try:
        _print_summary(
            [
                DQResult(
                    kind="accepted_values",
                    table="users",
                    column="email",
                    ok=False,
                    msg="x",
                    ms=4,
                    severity="error",
                    param_str="(values=['a','b'], where=flag=1)",
                    example_sql=(
                        "select distinct email from users where email not in ('a','b') limit 5"
                    ),
                ),
                DQResult(
                    kind="not_null",
                    table="users",
                    column="id",
                    ok=True,
                    msg=None,
                    ms=1,
                    severity="error",
                ),
            ]
        )
    finally:
        sys.stdout = old
    out = buf.getvalue()
    assert "(values=" in out and "where=" in out
    assert "e.g. SQL:" in out

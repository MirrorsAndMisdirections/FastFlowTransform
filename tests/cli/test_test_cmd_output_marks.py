import io
import sys

from fastflowtransform.cli.test_cmd import DQResult, _print_summary


def test_output_marks_and_totals():
    results = [
        DQResult(
            kind="not_null", table="t", column="c1", ok=True, msg=None, ms=3, severity="error"
        ),
        DQResult(kind="unique", table="t", column="c2", ok=False, msg="dup", ms=2, severity="warn"),
        DQResult(
            kind="greater_equal",
            table="t",
            column="c3",
            ok=False,
            msg="bad",
            ms=1,
            severity="error",
        ),
    ]
    buf = io.StringIO()
    old_stdout = sys.stdout
    sys.stdout = buf
    try:
        _print_summary(results)
    finally:
        sys.stdout = old_stdout
    out = buf.getvalue()
    assert "✅ not_null" in out
    assert "❕ unique" in out  # warn fail
    assert "❌ greater_equal" in out  # error fail
    assert "! warned:" in out
    assert "✗ failed:" in out

import pandas as pd

from flowforge.lineage import infer_py_lineage


def _dummy():
    # docstring to keep parser calm
    pass


def test_pandas_rename_and_assign():
    """Basic pandas lineage patterns: rename + new column from existing."""

    # Build a small function dynamically to pass to infer_py_lineage
    def fn(df: pd.DataFrame) -> pd.DataFrame:
        out = df.rename(columns={"email": "email_upper"})
        out["flag"] = df["email"]
        return out

    lin = infer_py_lineage(fn)
    # email_upper comes from email (transformed=True due to rename heuristic)
    assert "email_upper" in lin
    assert any(s["from_column"] == "email" for s in lin["email_upper"])
    # flag comes from email (direct/unknown transform
    # -> direct=False or True both acceptable by heuristic,
    # but we require at least mapping to email)
    assert "flag" in lin
    assert any(s["from_column"] == "email" for s in lin["flag"])

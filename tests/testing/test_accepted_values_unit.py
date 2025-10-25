import pytest

from fastflowtransform.executors.duckdb_exec import DuckExecutor
from fastflowtransform.testing import TestFailure, accepted_values


def test_accepted_values_pass_and_fail():
    ex = DuckExecutor(":memory:")
    ex.con.execute("create table t(id int, email varchar)")
    ex.con.execute("insert into t values (1,'a@example.com'),(2,'b@example.com')")
    # Pass
    assert accepted_values(ex.con, "t", "email", values=["a@example.com", "b@example.com"]) is True
    # Fail
    ex.con.execute("insert into t values (3,'bad@example.com')")
    with pytest.raises(TestFailure) as e:
        accepted_values(ex.con, "t", "email", values=["a@example.com", "b@example.com"])
    assert "outside accepted set" in str(e.value)

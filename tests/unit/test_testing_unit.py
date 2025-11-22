# tests/unit/test_testing_unit.py
from __future__ import annotations

from typing import Any

import pytest

from fastflowtransform.testing.base import (
    TestFailure,
    _exec,
    _fail,
    _pretty_sql,
    _scalar,
    accepted_values,
    freshness,
    greater_equal,
    non_negative_sum,
    not_null,
    reconcile_coverage,
    reconcile_diff_within,
    reconcile_equal,
    reconcile_ratio_within,
    relationships,
    row_count_between,
    sql_list,
    unique,
)


class _FakeResult:
    """Tiny fake fetch result for tests."""

    def __init__(self, rows: list[tuple]):
        self._rows = rows

    def fetchone(self) -> tuple | None:
        return self._rows[0] if self._rows else None

    def fetchall(self) -> list[tuple]:
        return self._rows


# ---------------------------------------------------------------------------
# _pretty_sql / _sql_list
# ---------------------------------------------------------------------------


@pytest.mark.unit
def test_pretty_sql_plain():
    assert _pretty_sql(" select 1 ") == "select 1"


@pytest.mark.unit
def test_pretty_sql_tuple():
    out = _pretty_sql(("select 1", {"x": 1}))
    assert out.startswith("select 1")
    assert "params={'x': 1}" in out


@pytest.mark.unit
def test_pretty_sql_sequence():
    out = _pretty_sql(["select 1", "select 2"])
    assert "select 1" in out
    assert "select 2" in out
    assert out.startswith("[")
    assert out.endswith("]")


@pytest.mark.unit
def test_sql_list_various_types():
    assert sql_list([1, 2, 3]) == "1, 2, 3"
    assert sql_list(["a", "b"]) == "'a', 'b'"
    assert sql_list([None, "O'Reilly"]) == "NULL, 'O''Reilly'"


# ---------------------------------------------------------------------------
# _exec: branch 1 - connection has .execute
# ---------------------------------------------------------------------------


@pytest.mark.unit
def test_exec_direct_non_sqlalchemy():
    calls: list[Any] = []

    class FakeCon:
        def execute(self, sql):
            calls.append(sql)
            return _FakeResult([(1,)])

    con = FakeCon()
    res = _exec(con, "select 1")
    assert isinstance(res, _FakeResult)
    assert calls == ["select 1"]


@pytest.mark.unit
def test_exec_direct_sqlalchemy_like_string(monkeypatch):
    # simulate a SA-like connection (module name contains "sqlalchemy")
    class FakeSACon:
        __module__ = "sqlalchemy.engine.mock"

        def __init__(self):
            self.calls: list[Any] = []

        def execute(self, stmt, params=None):
            # sqlalchemy.text(...) should have been called
            self.calls.append((stmt, params))
            return _FakeResult([(1,)])

    con = FakeSACon()
    res = _exec(con, "select 1")
    assert isinstance(res, _FakeResult)
    # first arg should be a TextClause
    assert len(con.calls) == 1
    assert str(con.calls[0][0]).strip().lower().startswith("select 1")


@pytest.mark.unit
def test_exec_direct_sqlalchemy_like_tuple_params():
    class FakeSACon:
        __module__ = "sqlalchemy.engine.mock"

        def __init__(self):
            self.calls: list[Any] = []

        def execute(self, stmt, params=None):
            self.calls.append((stmt, params))
            return _FakeResult([(1,)])

    con = FakeSACon()
    res = _exec(con, ("select :x", {"x": 10}))
    assert isinstance(res, _FakeResult)
    assert len(con.calls) == 1
    sql_obj, params = con.calls[0]
    assert "select :x" in str(sql_obj).lower()
    assert params == {"x": 10}


# ---------------------------------------------------------------------------
# _exec: branch 2 - no .execute, but .begin() (SQLAlchemy fallback)
# ---------------------------------------------------------------------------


@pytest.mark.unit
def test_exec_fallback_begin_with_sequence():
    executed: list[str] = []

    class FakeCtx:
        def __init__(self, outer):
            self.outer = outer

        def __enter__(self):
            return self

        def __exit__(self, exc_type, exc, tb):
            return False

        def execute(self, stmt, params=None):
            # stmt may be TextClause
            if hasattr(stmt, "text"):
                executed.append(stmt.text)
            else:
                executed.append(str(stmt))
            return _FakeResult([(1,)])

    class FakeCon:
        def begin(self):
            return FakeCtx(self)

    con = FakeCon()
    res = _exec(con, ["select 1", "select 2"])
    assert isinstance(res, _FakeResult)
    assert executed == ["select 1", "select 2"]


@pytest.mark.unit
def test_exec_fallback_unsupported_type_raises():
    class FakeCtx:
        def __enter__(self):
            """Enter."""
            return self

        def __exit__(self, exc_type, exc, tb):
            """Exit."""
            return False

        def execute(self, *_a, **_k):
            return _FakeResult([])

    class FakeCon:
        def begin(self):
            return FakeCtx()

    con = FakeCon()
    with pytest.raises(TypeError):
        _exec(con, object())


# ---------------------------------------------------------------------------
# _scalar
# ---------------------------------------------------------------------------


@pytest.mark.unit
def test_scalar_returns_first_value():
    class FakeCon:
        def execute(self, sql):
            return _FakeResult([(42, "x")])

    v = _scalar(FakeCon(), "select 42")
    expected_value = 42
    assert v == expected_value


@pytest.mark.unit
def test_scalar_returns_none_on_empty():
    class FakeCon:
        def execute(self, sql):
            return _FakeResult([])

    v = _scalar(FakeCon(), "select 42")
    assert v is None


# ---------------------------------------------------------------------------
# accepted_values
# ---------------------------------------------------------------------------


@pytest.mark.unit
def test_accepted_values_ok():
    # first call: count(*) = 0 → ok
    # second call (sample) should not be executed
    class FakeCon:
        def __init__(self):
            self.calls = 0

        def execute(self, sql):
            self.calls += 1
            if "count(*)" in sql:
                return _FakeResult([(0,)])
            return _FakeResult([])

    con = FakeCon()
    accepted_values(con, "tbl", "col", values=["a", "b"])
    assert con.calls == 1


@pytest.mark.unit
def test_accepted_values_fail_collects_samples():
    class FakeCon:
        def __init__(self):
            self.queries: list[str] = []

        def execute(self, sql):
            self.queries.append(sql)
            if "count(*)" in sql:
                return _FakeResult([(3,)])
            if "distinct" in sql:
                return _FakeResult([("X",), ("Y",)])
            return _FakeResult([])

    con = FakeCon()
    with pytest.raises(TestFailure) as exc:
        accepted_values(con, "x.tbl", "kind", values=["A", "B"])
    msg = str(exc.value)
    assert "x.tbl.kind has 3 value(s) outside accepted set" in msg
    # should include sample values
    assert "X" in msg or "Y" in msg


# ---------------------------------------------------------------------------
# not_null / unique
# ---------------------------------------------------------------------------


@pytest.mark.unit
def test_not_null_ok():
    class FakeCon:
        def execute(self, sql):
            return _FakeResult([(0,)])

    # should not raise
    not_null(FakeCon(), "tbl", "col")


@pytest.mark.unit
def test_not_null_fails_on_nulls():
    class FakeCon:
        def execute(self, sql):
            return _FakeResult([(2,)])

    with pytest.raises(TestFailure) as exc:
        not_null(FakeCon(), "tbl", "col")
    assert "has 2 NULL-values" in str(exc.value)


@pytest.mark.unit
def test_not_null_wraps_db_error():
    class FakeCon:
        def execute(self, sql):
            raise RuntimeError("undefinedcolumn: foo HAVING")

    with pytest.raises(TestFailure) as exc:
        not_null(FakeCon(), "tbl", "col")
    msg = str(exc.value).lower()
    assert "error in tbl.col" in msg
    assert "undefinedcolumn" in msg
    assert "having" in msg or "note: postgres does not permit alias usage" in msg


@pytest.mark.unit
def test_unique_ok():
    class FakeCon:
        def execute(self, sql):
            return _FakeResult([(0,)])

    unique(FakeCon(), "tbl", "col")


@pytest.mark.unit
def test_unique_fails():
    class FakeCon:
        def execute(self, sql):
            return _FakeResult([(5,)])

    with pytest.raises(TestFailure) as exc:
        unique(FakeCon(), "tbl", "col")
    assert "contains 5 duplicates" in str(exc.value)


# ---------------------------------------------------------------------------
# numeric checks
# ---------------------------------------------------------------------------


@pytest.mark.unit
def test_greater_equal_ok():
    class FakeCon:
        def execute(self, sql):
            # no rows with < threshold
            return _FakeResult([(0,)])

    greater_equal(FakeCon(), "tbl", "amount", threshold=10)


@pytest.mark.unit
def test_greater_equal_fails():
    class FakeCon:
        def execute(self, sql):
            return _FakeResult([(3,)])

    with pytest.raises(TestFailure) as exc:
        greater_equal(FakeCon(), "tbl", "amount", threshold=10)
    assert "has 3 values < 10" in str(exc.value)


@pytest.mark.unit
def test_non_negative_sum_ok():
    class FakeCon:
        def execute(self, sql):
            return _FakeResult([(0,)])

    non_negative_sum(FakeCon(), "tbl", "amount")


@pytest.mark.unit
def test_non_negative_sum_fails():
    class FakeCon:
        def execute(self, sql):
            return _FakeResult([(-5,)])

    with pytest.raises(TestFailure) as exc:
        non_negative_sum(FakeCon(), "tbl", "amount")
    assert "is negative: -5" in str(exc.value)


@pytest.mark.unit
def test_row_count_between_ok():
    class FakeCon:
        def execute(self, sql):
            return _FakeResult([(5,)])

    row_count_between(FakeCon(), "tbl", min_rows=1, max_rows=10)


@pytest.mark.unit
def test_row_count_between_too_few():
    class FakeCon:
        def execute(self, sql):
            return _FakeResult([(0,)])

    with pytest.raises(TestFailure):
        row_count_between(FakeCon(), "tbl", min_rows=1)


@pytest.mark.unit
def test_row_count_between_too_many():
    class FakeCon:
        def execute(self, sql):
            return _FakeResult([(50,)])

    with pytest.raises(TestFailure):
        row_count_between(FakeCon(), "tbl", min_rows=1, max_rows=10)


@pytest.mark.unit
def test_freshness_ok():
    class FakeCon:
        def execute(self, sql):
            # pretend last update was 3 min ago
            return _FakeResult([(3.0,)])

    freshness(FakeCon(), "tbl", "ts", max_delay_minutes=5)


@pytest.mark.unit
def test_freshness_too_old():
    class FakeCon:
        def execute(self, sql):
            return _FakeResult([(99.0,)])

    with pytest.raises(TestFailure):
        freshness(FakeCon(), "tbl", "ts", max_delay_minutes=10)


# ---------------------------------------------------------------------------
# reconcile_* helpers
# ---------------------------------------------------------------------------


@pytest.mark.unit
def test_reconcile_equal_exact_ok():
    class FakeCon:
        def execute(self, sql):
            # both scalar_where calls will read this
            return _FakeResult([(10,)])

    reconcile_equal(
        FakeCon(),
        left={"table": "a", "expr": "sum(x)"},
        right={"table": "b", "expr": "sum(y)"},
    )


@pytest.mark.unit
def test_reconcile_equal_abs_tolerance_ok():
    class FakeCon:
        def __init__(self):
            self.calls = 0

        def execute(self, sql):
            self.calls += 1
            if self.calls == 1:
                return _FakeResult([(10.0,)])
            return _FakeResult([(11.0,)])

    reconcile_equal(
        FakeCon(),
        left={"table": "a", "expr": "v"},
        right={"table": "b", "expr": "v"},
        abs_tolerance=1.5,
    )


@pytest.mark.unit
def test_reconcile_equal_fails():
    class FakeCon:
        def __init__(self):
            self.calls = 0

        def execute(self, sql):
            self.calls += 1
            if self.calls == 1:
                return _FakeResult([(10.0,)])
            return _FakeResult([(20.0,)])

    with pytest.raises(TestFailure):
        reconcile_equal(
            FakeCon(),
            left={"table": "a", "expr": "v"},
            right={"table": "b", "expr": "v"},
        )


@pytest.mark.unit
def test_reconcile_ratio_within_ok():
    class FakeCon:
        def __init__(self):
            self.calls = 0

        def execute(self, sql):
            self.calls += 1
            if self.calls == 1:
                return _FakeResult([(100.0,)])
            return _FakeResult([(50.0,)])

    # ratio = 100 / 50 = 2.0
    reconcile_ratio_within(
        FakeCon(),
        left={"table": "l", "expr": "x"},
        right={"table": "r", "expr": "y"},
        min_ratio=1.5,
        max_ratio=2.5,
    )


@pytest.mark.unit
def test_reconcile_ratio_within_fails():
    class FakeCon:
        def execute(self, sql):
            if "from l" in sql:
                return _FakeResult([(10.0,)])
            return _FakeResult([(100.0,)])

    with pytest.raises(TestFailure):
        reconcile_ratio_within(
            FakeCon(),
            left={"table": "l", "expr": "x"},
            right={"table": "r", "expr": "y"},
            min_ratio=0.5,
            max_ratio=0.8,
        )


@pytest.mark.unit
def test_reconcile_diff_within_ok():
    class FakeCon:
        def __init__(self):
            self.calls = 0

        def execute(self, sql):
            self.calls += 1
            if self.calls == 1:
                return _FakeResult([(50.0,)])
            return _FakeResult([(53.0,)])

    reconcile_diff_within(
        FakeCon(),
        left={"table": "l", "expr": "x"},
        right={"table": "r", "expr": "y"},
        max_abs_diff=5.0,
    )


@pytest.mark.unit
def test_reconcile_diff_within_fails():
    class FakeCon:
        def execute(self, sql):
            if "from l" in sql:
                return _FakeResult([(10.0,)])
            return _FakeResult([(25.0,)])

    with pytest.raises(TestFailure):
        reconcile_diff_within(
            FakeCon(),
            left={"table": "l", "expr": "x"},
            right={"table": "r", "expr": "y"},
            max_abs_diff=5.0,
        )


@pytest.mark.unit
def test_reconcile_coverage_ok():
    class FakeCon:
        def execute(self, sql):
            # anti-join count(*) == 0
            return _FakeResult([(0,)])

    reconcile_coverage(
        FakeCon(),
        source={"table": "src", "key": "id"},
        target={"table": "tgt", "key": "id"},
    )


@pytest.mark.unit
def test_reconcile_coverage_fails():
    class FakeCon:
        def execute(self, sql):
            return _FakeResult([(3,)])

    with pytest.raises(TestFailure):
        reconcile_coverage(
            FakeCon(),
            source={"table": "src", "key": "id"},
            target={"table": "tgt", "key": "id"},
        )


@pytest.mark.unit
def test_relationships_ok():
    class FakeCon:
        def execute(self, sql):
            return _FakeResult([(0,)])

    relationships(
        FakeCon(),
        table="fact_events",
        field="user_id",
        to_table="dim_users",
        to_field="id",
    )


@pytest.mark.unit
def test_relationships_fails_on_orphans():
    class FakeCon:
        def execute(self, sql):
            return _FakeResult([(5,)])

    with pytest.raises(TestFailure):
        relationships(
            FakeCon(),
            table="fact_events",
            field="user_id",
            to_table="dim_users",
            to_field="id",
        )


@pytest.mark.unit
def test_relationships_wraps_db_errors():
    class FakeCon:
        def execute(self, sql):
            raise RuntimeError("no such column")

    with pytest.raises(TestFailure) as exc:
        relationships(
            FakeCon(),
            table="fact_events",
            field="user_id",
            to_table="dim_users",
            to_field="id",
        )
    assert "[relationships]" in str(exc.value)


# ---------------------------------------------------------------------------
# _fail
# ---------------------------------------------------------------------------


@pytest.mark.unit
def test_fail_builds_message():
    with pytest.raises(TestFailure) as exc:
        _fail("check_x", "tbl", "col", "select 1", "oops")
    msg = str(exc.value)
    assert "[check_x] tbl.col: oops" in msg
    assert "select 1" in msg

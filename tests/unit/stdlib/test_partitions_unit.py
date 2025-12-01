from datetime import date, datetime

import pytest

from fastflowtransform.stdlib.partitions import sql_partition_filter, sql_partition_in


@pytest.mark.unit
def test_sql_partition_filter_between_dates():
    s = date(2025, 1, 1)
    e = date(2025, 1, 31)
    sql = sql_partition_filter("ds", s, e)
    assert sql == "ds BETWEEN '2025-01-01' AND '2025-01-31'"


@pytest.mark.unit
def test_sql_partition_filter_start_only():
    s = date(2025, 1, 1)
    sql = sql_partition_filter("ds", start=s, end=None)
    assert sql == "ds >= '2025-01-01'"


@pytest.mark.unit
def test_sql_partition_filter_end_only():
    e = date(2025, 1, 31)
    sql = sql_partition_filter("ds", start=None, end=e)
    assert sql == "ds <= '2025-01-31'"


@pytest.mark.unit
def test_sql_partition_filter_no_bounds_is_noop():
    sql = sql_partition_filter("ds", None, None)
    assert sql == "1=1"


@pytest.mark.unit
def test_sql_partition_filter_raises_on_empty_column():
    with pytest.raises(ValueError):
        sql_partition_filter("   ", "2025-01-01", "2025-01-31")


@pytest.mark.unit
def test_sql_partition_filter_datetime_literal():
    ts = datetime(2025, 1, 1, 12, 30, 45)
    sql = sql_partition_filter("event_ts", ts, None)
    # default sql_literal(str(dt)) → "YYYY-MM-DD HH:MM:SS"
    assert "event_ts >= '" in sql
    assert "2025-01-01" in sql


@pytest.mark.unit
def test_sql_partition_in_dates():
    vals = [date(2025, 1, 1), date(2025, 1, 2)]
    sql = sql_partition_in("ds", vals)
    assert sql == "ds IN ('2025-01-01', '2025-01-02')"


@pytest.mark.unit
def test_sql_partition_in_strings():
    vals = ["EU", "US"]
    sql = sql_partition_in("region", vals)
    assert sql == "region IN ('EU', 'US')"


@pytest.mark.unit
def test_sql_partition_in_empty_values_is_false_guard():
    sql = sql_partition_in("ds", [])
    assert sql == "1=0"


@pytest.mark.unit
def test_sql_partition_in_raises_on_empty_column():
    with pytest.raises(ValueError):
        sql_partition_in("   ", ["a", "b"])

import pytest

from fastflowtransform.stdlib.dates import sql_date_add, sql_date_trunc


@pytest.mark.parametrize(
    "engine, expr, part, expected",
    [
        (
            "duckdb",
            "order_ts",
            "day",
            "date_trunc('day', CAST(order_ts AS TIMESTAMP))",
        ),
        (
            "postgres",
            "created_at",
            "month",
            "date_trunc('month', CAST(created_at AS TIMESTAMP))",
        ),
        (
            "snowflake",
            "event_time",
            "year",
            "date_trunc('year', CAST(event_time AS TIMESTAMP))",
        ),
        (
            "spark",
            "ts",
            "day",
            "date_trunc('day', CAST(ts AS TIMESTAMP))",
        ),
        (
            "bigquery",
            "order_ts",
            "day",
            "DATE_TRUNC(CAST(order_ts AS TIMESTAMP), DAY)",
        ),
        (
            None,  # generic
            "ts",
            "day",
            "date_trunc('day', CAST(ts AS TIMESTAMP))",
        ),
    ],
)
@pytest.mark.unit
def test_sql_date_trunc(engine, expr, part, expected):
    assert sql_date_trunc(expr, part, engine=engine) == expected


@pytest.mark.unit
def test_sql_date_trunc_default_part_is_day():
    out = sql_date_trunc("order_ts", engine="duckdb")
    assert out == "date_trunc('day', CAST(order_ts AS TIMESTAMP))"


@pytest.mark.unit
def test_sql_date_trunc_invalid_part_raises():
    with pytest.raises(ValueError):
        sql_date_trunc("order_ts", "  ", engine="duckdb")


@pytest.mark.parametrize(
    "engine, expr, part, amount, expected",
    [
        (
            "duckdb",
            "order_ts",
            "day",
            1,
            "CAST(order_ts AS TIMESTAMP) + INTERVAL '1 day'",
        ),
        (
            "duckdb",
            "CAST(order_ts AS TIMESTAMP)",
            "day",
            3,
            # already casted, we don't wrap again
            "CAST(order_ts AS TIMESTAMP) + INTERVAL '3 day'",
        ),
        (
            "postgres",
            "created_at",
            "month",
            -2,
            "CAST(created_at AS TIMESTAMP) + INTERVAL '-2 month'",
        ),
        (
            "spark",
            "order_date",
            "day",
            7,
            "date_add(order_date, 7)",
        ),
        (
            "spark",
            "order_ts",
            "month",
            1,
            "order_ts + INTERVAL 1 MONTH",
        ),
        (
            "snowflake",
            "order_ts",
            "day",
            1,
            "DATEADD(DAY, 1, TO_TIMESTAMP(order_ts))",
        ),
        (
            "bigquery",
            "order_ts",
            "day",
            1,
            "DATE_ADD(CAST(order_ts AS TIMESTAMP), INTERVAL 1 DAY)",
        ),
        (
            "bigquery",
            "CAST(order_ts AS TIMESTAMP)",
            "day",
            2,
            # already casted -> no extra CAST wrapper
            "DATE_ADD(CAST(order_ts AS TIMESTAMP), INTERVAL 2 DAY)",
        ),
        (
            None,  # generic
            "ts",
            "day",
            1,
            "CAST(ts AS TIMESTAMP) + INTERVAL '1 day'",
        ),
    ],
)
@pytest.mark.unit
def test_sql_date_add(engine, expr, part, amount, expected):
    assert sql_date_add(expr, part, amount, engine=engine) == expected


@pytest.mark.unit
def test_sql_date_add_invalid_part_raises():
    with pytest.raises(ValueError):
        sql_date_add("ts", "   ", 1, engine="duckdb")

import pytest

from fastflowtransform.stdlib.casts import sql_safe_cast


@pytest.mark.parametrize(
    "engine, type_str, expected_inner",
    [
        ("bigquery", "numeric", "SAFE_CAST(amount AS NUMERIC)"),
        ("bigquery", "NUMERIC", "SAFE_CAST(amount AS NUMERIC)"),
        ("duckdb", "numeric", "try_cast(amount AS NUMERIC)"),
        ("duckdb", "INTEGER", "try_cast(amount AS INTEGER)"),
        ("postgres", "numeric", "CAST(amount AS NUMERIC)"),
        ("redshift", "numeric", "CAST(amount AS NUMERIC)"),
        ("snowflake", "numeric", "CAST(amount AS NUMBER(38,10))"),
        ("spark", "numeric", "TRY_CAST(amount AS NUMERIC)"),
        (None, "numeric", "CAST(amount AS NUMERIC)"),
    ],
)
@pytest.mark.unit
def test_sql_safe_cast_numeric_normalization(engine, type_str, expected_inner):
    sql = sql_safe_cast("amount", type_str, engine=engine)
    assert sql == expected_inner


@pytest.mark.unit
def test_sql_safe_cast_default_coalesce():
    sql = sql_safe_cast("raw_col", "INTEGER", default="0", engine="duckdb")
    assert sql == "COALESCE(try_cast(raw_col AS INTEGER), 0)"


@pytest.mark.unit
def test_sql_safe_cast_default_blank_is_ignored():
    sql = sql_safe_cast("raw_col", "INTEGER", default="   ", engine="duckdb")
    # same as without default
    assert sql == "try_cast(raw_col AS INTEGER)"


@pytest.mark.unit
def test_sql_safe_cast_raises_on_empty_expr():
    with pytest.raises(ValueError):
        sql_safe_cast("   ", "INTEGER", engine="duckdb")


@pytest.mark.unit
def test_sql_safe_cast_raises_on_empty_type():
    with pytest.raises(ValueError):
        sql_safe_cast("amount", "   ", engine="duckdb")

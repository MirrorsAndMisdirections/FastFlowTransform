import pytest

from fastflowtransform.stdlib.engine import engine_family, is_engine, normalize_engine


@pytest.mark.parametrize(
    "raw, expected",
    [
        (None, "generic"),
        ("", "generic"),
        ("  ", "generic"),
        ("duckdb", "duckdb"),
        ("DuckDB", "duckdb"),
        ("postgres", "postgres"),
        ("postgresql", "postgres"),
        ("psql", "postgres"),
        ("bigquery", "bigquery"),
        ("bq", "bigquery"),
        ("snowflake", "snowflake"),
        ("snowflake_snowpark", "snowflake"),
        ("sf", "snowflake"),
        ("spark", "spark"),
        ("databricks", "spark"),
        ("databricks_spark", "spark"),
        ("MyCustomEngine", "mycustomengine"),
    ],
)
@pytest.mark.unit
def test_normalize_engine(raw, expected):
    assert normalize_engine(raw) == expected


@pytest.mark.unit
def test_engine_family_alias_for_now():
    # Today engine_family == normalize_engine, but kept as separate API.
    assert engine_family("Postgres") == normalize_engine("Postgres")


@pytest.mark.parametrize(
    "engine, candidates, expected",
    [
        ("duckdb", ["duckdb", "postgres"], True),
        ("postgresql", ["duckdb", "postgres"], True),
        ("bigquery", ["duckdb", "postgres"], False),
        ("snowflake_snowpark", ["snowflake"], True),
        (None, ["generic"], True),
        ("unknown", ["duckdb", "postgres"], False),
    ],
)
@pytest.mark.unit
def test_is_engine(engine, candidates, expected):
    assert is_engine(engine, *candidates) == expected

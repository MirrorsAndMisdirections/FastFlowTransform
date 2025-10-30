# tests/unit/test_validation_unit.py
from __future__ import annotations

import pandas as pd
import pytest

from fastflowtransform.validation import validate_required_columns


@pytest.mark.unit
def test_validate_required_columns_no_requires_returns():
    # nothing to validate → no exception
    df = pd.DataFrame({"id": [1]})
    validate_required_columns("node_x", df, {})
    # if we got here: ok


@pytest.mark.unit
def test_single_dataframe_all_columns_present():
    df = pd.DataFrame(
        {
            "id": [1, 2],
            "email": ["a@example.com", "b@example.com"],
        }
    )
    requires = {"users": {"id", "email"}}

    # should not raise
    validate_required_columns("users_enriched", df, requires)


@pytest.mark.unit
def test_single_dataframe_missing_column_raises():
    df = pd.DataFrame(
        {
            "id": [1, 2],
        }
    )
    requires = {"users": {"id", "email"}}

    with pytest.raises(ValueError) as excinfo:
        validate_required_columns("users_enriched", df, requires)

    msg = str(excinfo.value)
    assert "Required columns check failed for Python model 'users_enriched'." in msg
    # the detail
    assert "- missing columns: ['email'] | have=['id']" in msg
    assert "Hint: define/adjust `require=`" in msg


@pytest.mark.unit
def test_multi_inputs_all_good():
    inputs = {
        "users": pd.DataFrame({"id": [1], "email": ["a@example.com"]}),
        "orders": pd.DataFrame({"order_id": [10], "user_id": [1]}),
    }
    requires = {
        "users": {"id", "email"},
        "orders": {"order_id", "user_id"},
    }

    # should not raise
    validate_required_columns("mart_orders_enriched", inputs, requires)


@pytest.mark.unit
def test_multi_inputs_missing_dep_key():
    inputs = {
        "users": pd.DataFrame({"id": [1], "email": ["a@example.com"]}),
        # "orders" fehlt
    }
    requires = {
        "users": {"id", "email"},
        "orders": {"order_id", "user_id"},
    }

    with pytest.raises(ValueError) as excinfo:
        validate_required_columns("mart_orders_enriched", inputs, requires)

    msg = str(excinfo.value)
    assert "- missing dependency key 'orders' in inputs dict" in msg
    assert "mart_orders_enriched" in msg


@pytest.mark.unit
def test_multi_inputs_missing_columns_in_one_dep():
    inputs = {
        "users": pd.DataFrame({"id": [1], "email": ["a@example.com"]}),
        "orders": pd.DataFrame({"order_id": [10]}),  # user_id fehlt
    }
    requires = {
        "users": {"id", "email"},
        "orders": {"order_id", "user_id"},
    }

    with pytest.raises(ValueError) as excinfo:
        validate_required_columns("mart_orders_enriched", inputs, requires)

    msg = str(excinfo.value)
    assert "- [orders] missing columns: ['user_id'] | have=['order_id']" in msg
    assert "Hint:" in msg


@pytest.mark.unit
def test_multi_inputs_multiple_errors_are_combined():
    inputs = {
        # users fehlt komplett
        "orders": pd.DataFrame({"order_id": [10]}),
    }
    requires = {
        "users": {"id"},
        "orders": {"order_id", "user_id"},
    }

    with pytest.raises(ValueError) as excinfo:
        validate_required_columns("mart_orders_enriched", inputs, requires)

    msg = str(excinfo.value)
    # beide Fehler sollten drin stehen
    assert "- missing dependency key 'users' in inputs dict" in msg
    assert "- [orders] missing columns: ['user_id'] | have=['order_id']" in msg

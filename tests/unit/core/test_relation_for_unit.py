import pytest

from fastflowtransform.core import relation_for


@pytest.mark.unit
def test_relation_for_strips_ff_suffix():
    assert relation_for("users.ff") == "users"
    assert relation_for("mart_users.ff") == "mart_users"


@pytest.mark.unit
def test_relation_for_passthrough_other_names():
    assert relation_for("users") == "users"
    assert relation_for("users_enriched") == "users_enriched"

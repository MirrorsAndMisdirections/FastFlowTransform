from flowforge.core import relation_for


def test_relation_for_strips_ff_suffix():
    assert relation_for("users.ff") == "users"
    assert relation_for("marts_daily.ff") == "marts_daily"


def test_relation_for_passthrough_other_names():
    assert relation_for("users") == "users"
    assert relation_for("users_enriched") == "users_enriched"

# tests/unit/test_source_freshness_unit.py
from __future__ import annotations

import pytest

from fastflowtransform.core import REGISTRY
from fastflowtransform.source_freshness import (
    _relation_for_source,
    run_source_freshness,
)
from fastflowtransform.testing.base import TestFailure


def test_relation_for_source_duckdb(monkeypatch):
    """
    _relation_for_source should build schema.table for DuckDB/Postgres-like engines.
    """

    entry = {"base": {"identifier": "seed_customers", "schema": "dq_demo"}}
    dummy_exec = object()

    # Patch resolve_source_entry so we don't depend on its internals here.
    def fake_resolve_source_entry(e, engine, default_identifier=None):
        assert e is entry
        assert engine == "duckdb"
        return {"identifier": "seed_customers", "schema": "dq_demo"}

    monkeypatch.setattr(
        "fastflowtransform.source_freshness.resolve_source_entry",
        fake_resolve_source_entry,
    )

    rel = _relation_for_source(
        entry,
        source_name="crm",
        table_name="customers",
        executor=dummy_exec,
        engine="duckdb",
    )
    assert rel == "dq_demo.seed_customers"


def test_relation_for_source_bigquery(monkeypatch):
    """
    For BigQuery, we expect a `project.dataset.table` style identifier.
    """

    entry = {"base": {"identifier": "seed_customers"}}
    dummy_exec = object()

    def fake_resolve_source_entry(e, engine, default_identifier=None):
        assert engine == "bigquery"
        return {
            "identifier": "seed_customers",
            "project": "myproj",
            "dataset": "dq_demo",
        }

    monkeypatch.setattr(
        "fastflowtransform.source_freshness.resolve_source_entry",
        fake_resolve_source_entry,
    )

    rel = _relation_for_source(
        entry,
        source_name="crm",
        table_name="customers",
        executor=dummy_exec,
        engine="bigquery",
    )
    assert rel == "myproj.dq_demo.seed_customers"


def test_relation_for_source_snowflake(monkeypatch):
    """
    For Snowflake, we currently use DB.SCHEMA.TABLE (unquoted).
    """

    entry = {"base": {"identifier": "SEED_CUSTOMERS"}}
    dummy_exec = object()

    def fake_resolve_source_entry(e, engine, default_identifier=None):
        assert "snowflake" in engine
        return {
            "identifier": "SEED_CUSTOMERS",
            "database": "DQ_DEMO",
            "schema": "CRM",
        }

    monkeypatch.setattr(
        "fastflowtransform.source_freshness.resolve_source_entry",
        fake_resolve_source_entry,
    )

    rel = _relation_for_source(
        entry,
        source_name="crm",
        table_name="customers",
        executor=dummy_exec,
        engine="snowflake_snowpark",
    )
    assert rel == "DQ_DEMO.CRM.SEED_CUSTOMERS"


def test_run_source_freshness_classification(monkeypatch):
    """
    run_source_freshness should classify PASS / WARN / ERROR based on
    the parsed delay vs warn/error thresholds.
    """

    # Build a fake normalized REGISTRY.sources
    sources = {
        "crm": {
            # PASS case: customers - freshness test does not raise
            "customers": {
                "base": {"identifier": "seed_customers", "schema": "dq_demo"},
                "freshness": {
                    "loaded_at_field": "loaded_at",
                    "warn_after": {"count_in_minutes": 60},
                    "error_after": {"count_in_minutes": 240},
                },
            },
            # WARN case: orders_warn - delay just above warn_after but below error_after
            "orders_warn": {
                "base": {"identifier": "seed_orders_warn", "schema": "dq_demo"},
                "freshness": {
                    "loaded_at_field": "loaded_at",
                    "warn_after": {"count_in_minutes": 60},
                    "error_after": {"count_in_minutes": 240},
                },
            },
            # ERROR case: orders_error - delay above error_after
            "orders_error": {
                "base": {"identifier": "seed_orders_error", "schema": "dq_demo"},
                "freshness": {
                    "loaded_at_field": "loaded_at",
                    "warn_after": {"count_in_minutes": 60},
                    "error_after": {"count_in_minutes": 240},
                },
            },
        }
    }

    # Patch REGISTRY.sources (monkeypatch will restore afterwards)
    monkeypatch.setattr(REGISTRY, "sources", sources, raising=False)

    # Patch resolve_source_entry to just return the base config; we only
    # care about identifier + schema for the relation string.
    def fake_resolve_source_entry(entry, engine, default_identifier=None):
        base = entry.get("base", {})
        return {
            "identifier": base.get("identifier"),
            "schema": base.get("schema"),
        }

    monkeypatch.setattr(
        "fastflowtransform.source_freshness.resolve_source_entry",
        fake_resolve_source_entry,
    )

    # Now patch the underlying _freshness_test used by run_source_freshness so
    # we can control pass/warn/error without hitting a real DB.
    calls: list[tuple[str, str, int]] = []

    def fake_freshness(con, relation, loaded_at_field, max_delay_minutes):
        # record for sanity
        calls.append((relation, loaded_at_field, max_delay_minutes))

        if "seed_customers" in relation:
            # PASS: no exception
            return

        if "seed_orders_warn" in relation:
            # Simulate delay = 90 min -> WARN (since 60 < 90 < 240)
            delay = 90
            raise TestFailure(
                f"freshness of {relation}.{loaded_at_field} too old: "
                f"{delay} min > {max_delay_minutes} min"
            )

        if "seed_orders_error" in relation:
            # Simulate delay = 300 min -> ERROR (since 300 > 240)
            delay = 300
            raise TestFailure(
                f"freshness of {relation}.{loaded_at_field} too old: "
                f"{delay} min > {max_delay_minutes} min"
            )

        raise AssertionError(f"Unexpected relation in fake_freshness: {relation}")

    monkeypatch.setattr(
        "fastflowtransform.source_freshness._freshness_test",
        fake_freshness,
    )

    # Dummy connection object; fake_freshness ignores it
    dummy_con = object()

    results = run_source_freshness(dummy_con, engine="duckdb")

    # Basic shape: 3 entries
    assert {r.table_name for r in results} == {
        "customers",
        "orders_warn",
        "orders_error",
    }

    by_tbl = {r.table_name: r for r in results}

    # PASS
    r_pass = by_tbl["customers"]
    assert r_pass.status == "pass"
    assert r_pass.delay_minutes is None

    # WARN
    r_warn = by_tbl["orders_warn"]
    assert r_warn.status == "warn"
    assert r_warn.delay_minutes == pytest.approx(90)

    # ERROR
    r_err = by_tbl["orders_error"]
    assert r_err.status == "error"
    assert r_err.delay_minutes == pytest.approx(300)

    # Sanity: underlying function was called for each relation
    assert len(calls) == 3
    rels = {c[0] for c in calls}
    assert rels == {
        "dq_demo.seed_customers",
        "dq_demo.seed_orders_warn",
        "dq_demo.seed_orders_error",
    }

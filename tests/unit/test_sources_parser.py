from __future__ import annotations

import yaml

from fastflowtransform.core import _parse_sources_yaml, resolve_source_entry


def test_parse_sources_and_overrides_merge():
    doc = """version: 2

sources:
  - name: raw
    schema: staging
    overrides:
      databricks_spark:
        schema: bronze
    tables:
      - name: users
        identifier: seed_users
        overrides:
          databricks_spark:
            format: delta
            location: "/mnt/delta/raw/users"
"""

    parsed = _parse_sources_yaml(yaml.safe_load(doc))
    entry = parsed["raw"]["users"]

    default_cfg = resolve_source_entry(entry, "duckdb")
    assert default_cfg["identifier"] == "seed_users"
    assert default_cfg["schema"] == "staging"
    assert default_cfg["location"] is None

    spark_cfg = resolve_source_entry(entry, "databricks_spark")
    assert spark_cfg["schema"] == "bronze"
    assert spark_cfg["format"] == "delta"
    assert spark_cfg["location"] == "/mnt/delta/raw/users"


def test_wildcard_override_applied_before_engine_specific():
    doc = """version: 2

sources:
  - name: crm
    tables:
      - name: users
        identifier: seed_users
        overrides:
          "*":
            schema: shared
          postgres:
            schema: analytics
"""

    parsed = _parse_sources_yaml(yaml.safe_load(doc))
    entry = parsed["crm"]["users"]

    duck_cfg = resolve_source_entry(entry, "duckdb")
    assert duck_cfg["schema"] == "shared"

    pg_cfg = resolve_source_entry(entry, "postgres")
    assert pg_cfg["schema"] == "analytics"


def test_missing_identifier_falls_back_to_table_name():
    doc = """version: 2

sources:
  - name: ext
    tables:
      - name: events
        overrides:
          postgres:
            schema: external
"""

    parsed = _parse_sources_yaml(yaml.safe_load(doc))
    entry = parsed["ext"]["events"]

    pg_cfg = resolve_source_entry(entry, "postgres", default_identifier="events")
    assert pg_cfg["identifier"] == "events"

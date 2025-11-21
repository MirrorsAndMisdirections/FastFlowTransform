# src/fastflowtransform/config/seeds.py
from __future__ import annotations

from pathlib import Path
from typing import Any

import yaml
from pydantic import BaseModel, ConfigDict, Field, ValidationError, field_validator, model_validator

from fastflowtransform.settings import EngineType


class SeedTargetConfig(BaseModel):
    """
    Configuration for a single seed target entry in seeds/schema.yml.

    Example:
      targets:
        raw/users:
          schema: raw
          table: seed_users
          schema_by_engine:
            duckdb: main
            postgres: raw
    """

    model_config = ConfigDict(extra="forbid", populate_by_name=True)

    schema_: str | None = Field(default=None, alias="schema")
    table: str | None = None
    schema_by_engine: dict[EngineType, str] = Field(default_factory=dict)

    @field_validator("schema_")
    @classmethod
    def _strip_schema(cls, value: str | None) -> str | None:
        if isinstance(value, str):
            value = value.strip()
            return value or None
        return value

    @field_validator("schema_by_engine")
    @classmethod
    def _strip_schema_by_engine(cls, value: dict[str, str]) -> dict[str, str]:
        out: dict[str, str] = {}
        for eng, sch in (value or {}).items():
            if not isinstance(sch, str):
                continue
            sch_clean = sch.strip()
            if sch_clean:
                out[eng] = sch_clean
        return out

    @model_validator(mode="after")
    def _allow_empty_schema(self) -> SeedTargetConfig:
        # At the moment we allow targets without schema / schema_by_engine,
        # so that the executor/default schema can still be used.
        # If you want to enforce at least one schema, uncomment the check below.
        #
        # if not self.schema and not self.schema_by_engine:
        #     raise ValueError(
        #         "Either 'schema' or 'schema_by_engine' must be set for a seed target"
        #     )
        return self


class SeedColumnConfig(BaseModel):
    """Column typing metadata for seeds/schema.yml."""

    model_config = ConfigDict(extra="forbid", populate_by_name=True)

    type_: str | None = Field(default=None, alias="type")
    engines: dict[EngineType, str] = Field(default_factory=dict)

    @field_validator("type_")
    @classmethod
    def _strip_type(cls, value: str | None) -> str | None:
        if isinstance(value, str):
            cleaned = value.strip()
            return cleaned or None
        return value

    @field_validator("engines")
    @classmethod
    def _strip_engines(cls, value: dict[str, str]) -> dict[str, str]:
        out: dict[str, str] = {}
        for eng, typ in (value or {}).items():
            if not isinstance(typ, str):
                continue
            typ_clean = typ.strip()
            if typ_clean:
                out[eng] = typ_clean
        return out


class SeedsSchemaConfig(BaseModel):
    """
    Top-level configuration for seeds/schema.yml.

    Structure:
      targets:
        <seed-id>:
          schema: ...
          table: ...
          schema_by_engine: { duckdb: ..., postgres: ... }

      dtypes:
        <table-key>:
          column_a: string
          column_b: int64

      columns:
        <table-key>:
          <column-name>:
            type: string|integer|timestamp|...
            engines:
              postgres: timestamptz
    """

    model_config = ConfigDict(extra="forbid")

    targets: dict[str, SeedTargetConfig] = Field(default_factory=dict)
    dtypes: dict[str, dict[str, str]] = Field(default_factory=dict)
    columns: dict[str, dict[str, Any]] = Field(default_factory=dict)

    @field_validator("dtypes")
    @classmethod
    def _normalize_dtypes(cls, value: dict[str, dict[str, Any]]) -> dict[str, dict[str, str]]:
        out: dict[str, dict[str, str]] = {}
        for table_key, cols in (value or {}).items():
            if not isinstance(cols, dict):
                continue
            clean_cols: dict[str, str] = {}
            for col, dtype in cols.items():
                if not isinstance(col, str) or not isinstance(dtype, str):
                    continue
                col_clean = col.strip()
                dtype_clean = dtype.strip()
                if col_clean and dtype_clean:
                    clean_cols[col_clean] = dtype_clean
            if clean_cols:
                out[table_key] = clean_cols
        return out

    @field_validator("columns")
    @classmethod
    def _normalize_columns(
        cls, value: dict[str, dict[str, Any]]
    ) -> dict[str, dict[str, SeedColumnConfig]]:
        out: dict[str, dict[str, SeedColumnConfig]] = {}
        for table_key, cols in (value or {}).items():
            if not isinstance(cols, dict):
                continue
            clean_cols: dict[str, SeedColumnConfig] = {}
            for col_name, payload in cols.items():
                if not isinstance(col_name, str):
                    continue
                col_clean = col_name.strip()
                if not col_clean:
                    continue
                if isinstance(payload, dict):
                    clean_cols[col_clean] = SeedColumnConfig.model_validate(payload)
                elif isinstance(payload, str):
                    clean_cols[col_clean] = SeedColumnConfig(type=payload)
            if clean_cols:
                out[table_key] = clean_cols
        return out


def load_seeds_schema(project_dir: Path, seeds_dir: Path | None = None) -> SeedsSchemaConfig | None:
    """
    Load and validate seeds/schema.yml for a given project.

    Returns:
      - SeedsSchemaConfig instance when the file exists and is valid
      - None when no file is present

    Raises:
      ValueError: when YAML is present but does not match the expected schema.
    """
    seeds_dir = seeds_dir or project_dir / "seeds"
    cfg_path = seeds_dir / "schema.yml"
    if not cfg_path.exists():
        return None

    raw = yaml.safe_load(cfg_path.read_text(encoding="utf-8")) or {}
    try:
        return SeedsSchemaConfig.model_validate(raw)
    except ValidationError as exc:
        raise ValueError(f"Failed to parse seeds/schema.yml: {exc}") from exc

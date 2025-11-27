# fastflowtransform/config/model.py
from __future__ import annotations

from collections.abc import Mapping, Sequence
from typing import Any, Literal

from pydantic import BaseModel, ConfigDict, Field, field_validator, model_validator

# ---------------------------------------------------------------------------
# Per-model storage configuration (project.yml → models.storage, or config(storage=...))
# ---------------------------------------------------------------------------


class StorageConfig(BaseModel):
    """
    Per-model storage override, for example:

        {{ config(
            storage={
                "path": ".local/spark/users",
                "format": "parquet",
                "options": {"compression": "snappy"},
            }
        ) }}

    This shape is also compatible with project.yml → models.storage.
    """

    model_config = ConfigDict(extra="forbid")

    path: str
    format: str | None = None
    options: dict[str, Any] = Field(default_factory=dict)


# ---------------------------------------------------------------------------
# Incremental / delta configuration (structured form)
# ---------------------------------------------------------------------------


class InlineDeltaConfig(BaseModel):
    """
    Inline SQL delta definition, for example:

        {{ config(
            incremental=True,
            delta={
                "sql": "select ... from {{ ref('events_base') }} where updated_at > (...)"
            },
        ) }}
    """

    model_config = ConfigDict(extra="forbid")

    sql: str


class IncrementalConfig(BaseModel):
    """
    High-level incremental configuration used in structured form, for example:

        {{ config(
            incremental={
                "enabled": true,
                "unique_key": ["id"],
                "updated_at_column": "updated_at",
                "delta_sql": "select ... where updated_at > (...)",
                "on_schema_change": "append_new_columns",
            }
        ) }}

    This complements the simple shorthand `incremental: true`.
    """

    model_config = ConfigDict(extra="forbid")

    # Master switch (default: enabled)
    enabled: bool = True

    # Canonical business key(s)
    unique_key: list[str] | None = None

    # Updated-at column (single)
    updated_at_column: str | None = None

    # Optional alternative notations
    updated_at_columns: list[str] | None = None
    timestamp_columns: list[str] | None = None

    # Delta definitions:
    # - delta_sql: inline SQL (short form)
    # - delta_python: Python callable for custom merge logic
    delta_sql: str | None = None
    delta_python: str | None = None

    # Schema evolution behaviour; directly mapped to meta["on_schema_change"]
    # and consumed by incremental._get_on_schema_change(...)
    on_schema_change: Literal["ignore", "append_new_columns", "sync_all_columns"] | None = None

    @field_validator("unique_key", "updated_at_columns", "timestamp_columns", mode="before")
    @classmethod
    def _normalize_str_or_seq(cls, v: Any) -> list[str] | None:
        if v is None:
            return None
        if isinstance(v, str):
            return [v]
        if isinstance(v, Sequence) and not isinstance(v, (str, bytes)):
            return [str(x) for x in v]
        raise TypeError("must be a string or a sequence of strings")


class SnapshotConfig(BaseModel):
    """
    Snapshot configuration block, for example:

        {{ config(
            materialized='snapshot',
            snapshot={
                "strategy": "timestamp",   # or "check"
                "updated_at": "updated_at",
                "check_cols": ["col1", "col2"],  # required for strategy='check'
            },
            unique_key=["id"],
        ) }}
    """

    model_config = ConfigDict(extra="forbid")

    strategy: Literal["timestamp", "check"]
    updated_at: str | None = None
    updated_at_column: str | None = None
    check_cols: list[str] | None = None

    @field_validator("check_cols", mode="before")
    @classmethod
    def _normalize_check_cols(cls, v: Any) -> list[str] | None:
        if v is None:
            return None
        if isinstance(v, str):
            return [v]
        if isinstance(v, Sequence) and not isinstance(v, (str, bytes)):
            return [str(x) for x in v]
        raise TypeError("check_cols must be a string or a sequence of strings")


# ---------------------------------------------------------------------------
# ModelConfig - canonical form of config(...) / decorator meta
# ---------------------------------------------------------------------------


class ModelConfig(BaseModel):
    """
    Canonical, *flattened* model configuration for SQL and Python models.

    This represents the keys that ultimately end up in Node.meta after:

      - SQL: {{ config(...) }} in the model header
      - Python: @model(..., meta={...})
      - project.yml overlays (models.incremental / models.storage)

    The schema is intentionally strict (extra="forbid") so that:
      - only documented keys are allowed
      - typos and unknown fields fail fast
    """

    model_config = ConfigDict(extra="forbid")

    # --- Core materialization & classification -----------------------------

    materialized: Literal["table", "view", "incremental", "ephemeral", "snapshot"] | None = None

    # Optional logical kind; useful for selectors (kind:python / kind:sql / etc.)
    kind: str | None = None

    # Tags for selection (tag:...); both SQL & Python models contribute here
    tags: list[str] = Field(default_factory=list)

    # Engine restriction, e.g. engines=["duckdb", "postgres"]
    engines: list[str] = Field(default_factory=list)

    # --- Per-model hooks (pre/post) ----------------------------------------

    pre_hook: list[str] = Field(default_factory=list)
    post_hook: list[str] = Field(default_factory=list)

    # --- Storage override (per model) --------------------------------------

    storage: StorageConfig | None = None

    # --- Incremental flags & shortcuts -------------------------------------

    # Shortcut:
    #   - True → incremental enabled
    #   - False / None → not incremental (unless executors override)
    #
    # Structured:
    #   - { ... IncrementalConfig fields ... }
    incremental: IncrementalConfig | None = None

    # --- Snapshot configuration (structured) ---------------------------------
    snapshot: SnapshotConfig | None = None

    # Top-level shortcuts (backwards-compatible)
    # These are used by existing executor logic.
    unique_key: list[str] | None = None
    primary_key: list[str] | None = None  # alias

    # Updated-at / timestamp information
    updated_at: str | None = None
    updated_at_column: str | None = None
    updated_at_columns: list[str] | None = None
    timestamp_columns: list[str] | None = None

    # Columns used to determine delta recency (used by Python incremental logic)
    delta_columns: list[str] | None = None

    # Delta definitions - shorthand, equivalent to fields on IncrementalConfig
    delta: InlineDeltaConfig | None = None
    delta_sql: str | None = None
    delta_python: str | None = None

    # Schema evolution behaviour; consumed by incremental._get_on_schema_change(...)
    on_schema_change: Literal["ignore", "append_new_columns", "sync_all_columns"] | None = None

    # --- HTTP/API extension points (optional) ------------------------------
    # These are intentionally loose to allow API models to stash config blocks
    # under known keys without having to allow arbitrary extras everywhere.
    http: dict[str, Any] | None = None
    api: dict[str, Any] | None = None

    # ----------------------------------------------------------------------
    # Normalisation helpers
    # ----------------------------------------------------------------------

    @field_validator("pre_hook", "post_hook", mode="before")
    @classmethod
    def _normalize_hooks(cls, v: Any) -> list[str]:
        """
        Allow:
          - string: "delete from {{ this }}" → ["delete from {{ this }}"]
          - sequence: ["stmt1", "stmt2"]
          - null: []
        """
        if v is None:
            return []
        if isinstance(v, str):
            text = v.strip()
            return [text] if text else []
        if isinstance(v, Sequence) and not isinstance(v, (str, bytes)):
            return [str(x).strip() for x in v if str(x).strip()]
        raise TypeError("pre_hook/post_hook must be a string or a sequence of strings")

    @field_validator("tags", "engines", mode="before")
    @classmethod
    def _normalize_tags_engines(cls, v: Any) -> list[str]:
        """
        Allow:
          - string: "duckdb" → ["duckdb"]
          - sequence: ["duckdb", "postgres"]
        """
        if v is None:
            return []
        if isinstance(v, str):
            return [v]
        if isinstance(v, Sequence) and not isinstance(v, (str, bytes)):
            return [str(x) for x in v]
        raise TypeError("must be a string or a sequence of strings")

    @field_validator(
        "unique_key",
        "primary_key",
        "updated_at_columns",
        "timestamp_columns",
        "delta_columns",
        mode="before",
    )
    @classmethod
    def _normalize_key_lists(cls, v: Any) -> list[str] | None:
        """
        Allow single string or list/tuple of strings.
        """
        if v is None:
            return None
        if isinstance(v, str):
            return [v]
        if isinstance(v, Sequence) and not isinstance(v, (str, bytes)):
            return [str(x) for x in v]
        raise TypeError("must be a string or a sequence of strings")

    @model_validator(mode="after")
    def _merge_incremental_overlays(self) -> ModelConfig:
        """
        Backwards- and executor-compatible merge:

        - If `incremental` is an IncrementalConfig instance, mirror the
          central fields onto the top-level shortcuts (unique_key, updated_at_column, delta_*).

        - If `incremental == True` but no IncrementalConfig was provided,
          we simply rely on top-level fields (unique_key, updated_at, …).
        """
        inc = self.incremental

        if isinstance(inc, IncrementalConfig):
            # unique_key
            if self.unique_key is None and inc.unique_key is not None:
                self.unique_key = list(inc.unique_key)

            # updated-at / updated_at_column
            if self.updated_at_column is None and inc.updated_at_column is not None:
                self.updated_at_column = inc.updated_at_column

            if self.updated_at is None and inc.updated_at_column is not None:
                # For older code that only checks `updated_at`
                self.updated_at = inc.updated_at_column

            # timestamp / updated_at columns
            if self.updated_at_columns is None and inc.updated_at_columns is not None:
                self.updated_at_columns = list(inc.updated_at_columns)

            if self.timestamp_columns is None and inc.timestamp_columns is not None:
                self.timestamp_columns = list(inc.timestamp_columns)

            # delta hints
            if self.delta_sql is None and inc.delta_sql is not None:
                self.delta_sql = inc.delta_sql
            if self.delta_python is None and inc.delta_python is not None:
                self.delta_python = inc.delta_python

            # schema evolution
            if self.on_schema_change is None and inc.on_schema_change is not None:
                self.on_schema_change = inc.on_schema_change

        # If InlineDeltaConfig is used, prefer its SQL for delta_sql
        if self.delta and not self.delta_sql:
            self.delta_sql = self.delta.sql

        # Mirror snapshot hints onto top-level shortcuts for backwards compatibility.
        snap = self.snapshot
        if snap:
            if self.updated_at is None and snap.updated_at is not None:
                self.updated_at = snap.updated_at
            if self.updated_at_column is None and snap.updated_at_column is not None:
                self.updated_at_column = snap.updated_at_column

        return self

    # ----------------------------------------------------------------------
    # Convenience helpers for executor code
    # ----------------------------------------------------------------------

    def is_incremental_enabled(self) -> bool:
        """
        Return True if incremental mode is effectively enabled for this model.
        """
        if self.incremental is None:
            return False
        return bool(self.incremental.enabled)

    # ----------------------------------------------------------------------
    # Cross-field guardrails (fail fast with clear messages)
    # ----------------------------------------------------------------------
    @model_validator(mode="after")
    def _validate_model_requirements(self) -> ModelConfig:
        """
        Enforce combinations that must hold for incremental and snapshot models.

        Incremental rules:
          1) If materialized == 'incremental', incremental must be effectively enabled.
          2) If incremental is enabled, at least one freshness/delta hint must exist:
             - updated_at / updated_at_column / updated_at_columns / timestamp_columns
               OR delta_sql OR delta_python.
          3) If both updated_at and updated_at_column are provided, they must match.
          4) Require unique_key when incremental is enabled.

        Snapshot rules:
          1) If materialized == 'snapshot', a snapshot config must be provided.
          2) Snapshot models require unique_key (or primary_key).
          3) strategy must be 'timestamp' or 'check'.
          4) For 'timestamp', require updated_at / updated_at_column.
          5) For 'check', require check_cols.
        """
        # --- Incremental ---------------------------------------------------
        is_mat_inc = self.materialized == "incremental"
        is_inc_enabled = self.is_incremental_enabled()

        # 1) Require incremental block when materialized='incremental'
        if is_mat_inc and not is_inc_enabled:
            raise ValueError(
                "materialized='incremental' requires an enabled incremental configuration. "
                "Either set `incremental: true` or provide a "
                "structured `incremental: { enabled: true, ... }`."
            )

        # 2) If incremental is enabled, ensure at least one delta/freshness hint
        if is_inc_enabled:
            has_time_hints = any(
                [
                    bool(self.updated_at),
                    bool(self.updated_at_column),
                    bool(self.updated_at_columns),
                    bool(self.timestamp_columns),
                ]
            )
            has_delta_hints = any([bool(self.delta_sql), bool(self.delta_python)])
            if not (has_time_hints or has_delta_hints):
                raise ValueError(
                    "incremental.enabled=True but no delta/freshness hints were provided. "
                    "Please set one of: updated_at / updated_at_column / updated_at_columns / "
                    "timestamp_columns, or provide delta_sql / delta_python."
                )

        # 3) If both notations are present, they must agree
        if self.updated_at and self.updated_at_column and self.updated_at != self.updated_at_column:
            raise ValueError(
                f"updated_at ('{self.updated_at}') and "
                f"updated_at_column ('{self.updated_at_column}') "
                "refer to different columns. Use one or make them identical."
            )

        # 4) (Opinionated) Require unique_key when incremental is enabled
        if is_inc_enabled and not (self.unique_key or self.primary_key):
            raise ValueError(
                "incremental.enabled=True requires a unique_key (or primary_key) to be set "
                "for safe merges. Example: unique_key: ['id']"
            )

        # --- Snapshot-specific rules --------------------------------------
        if self.materialized == "snapshot":
            snap = self.snapshot
            if snap is None:
                raise ValueError(
                    "materialized='snapshot' requires a snapshot config block. "
                    "Example:\n"
                    "  snapshot: { strategy: 'timestamp' }"
                )

            # business key
            if not (self.unique_key or self.primary_key):
                raise ValueError(
                    "materialized='snapshot' requires a unique_key (or primary_key). "
                    "Example: unique_key: ['id']"
                )

            # strategy is validated by SnapshotConfig (Literal), but we keep a guardrail here
            if snap.strategy not in ("timestamp", "check"):
                raise ValueError(
                    "Snapshot models require strategy='timestamp' or 'check'. "
                    "Example: snapshot: { strategy: 'timestamp' }"
                )

            # timestamp strategy: needs updated_at
            snap_updated = snap.updated_at or snap.updated_at_column
            if snap.strategy == "timestamp" and not snap_updated:
                raise ValueError(
                    "strategy='timestamp' snapshots require snapshot.updated_at or "
                    "snapshot.updated_at_column."
                )

            # check strategy: needs check_cols
            if snap.strategy == "check" and not snap.check_cols:
                raise ValueError(
                    "strategy='check' snapshots require snapshot.check_cols "
                    "(string or list of column names)."
                )

        return self


# ---------------------------------------------------------------------------
# Helper: validate & normalize raw meta dict
# ---------------------------------------------------------------------------


def validate_model_meta(meta: Mapping[str, Any] | None) -> ModelConfig:
    """
    Validate a raw meta mapping coming from SQL config(...) or Python decorators
    and return a strongly-typed ModelConfig instance.

    This function also normalizes shorthand forms like:
      - incremental: true/false
      - incremental: { ... } (without explicit enabled flag)
    """
    data: dict[str, Any] = dict(meta or {})

    incr = data.get("incremental")

    if isinstance(incr, bool):
        # incremental: true/false → normalize to nested config
        data["incremental"] = {"enabled": incr}
    elif isinstance(incr, Mapping):
        # ensure we can mutate it
        incr_dict = dict(incr)
        # default enabled=True if omitted
        incr_dict.setdefault("enabled", True)
        data["incremental"] = incr_dict
    elif incr is not None:
        raise TypeError("meta.incremental must be a bool, a mapping or null")

    return ModelConfig.model_validate(data)


def validate_model_meta_strict(
    meta: Mapping[str, Any] | None,
    *,
    model_name: str | None = None,
    file_path: str | None = None,
) -> ModelConfig:
    """
    Like validate_model_meta(), but wraps exceptions with model/file context for clearer errors.
    Callers in the loader should prefer this, so a bad config never silently disables a model.
    """
    try:
        return validate_model_meta(meta)
    except Exception as e:
        ctx = []
        if model_name:
            ctx.append(f"model '{model_name}'")
        if file_path:
            ctx.append(f"{file_path}")
        prefix = f"Invalid model config ({', '.join(ctx)})" if ctx else "Invalid model config"
        raise TypeError(f"{prefix}: {e}") from e

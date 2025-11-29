# fastflowtransform/executors/_sql_identifier.py
from __future__ import annotations

from typing import Any

from fastflowtransform.core import relation_for


class SqlIdentifierMixin:
    """
    Thin helper mixin for engines that qualify SQL identifiers with optional
    catalog/database and schema.

    Subclasses must implement `_quote_identifier` and may override the
    *_default_* / *_should_include_catalog methods to match engine quirks.
    """

    def _normalize_identifier(self, ident: str) -> str:
        """
        Normalize fastflowtransform's logical identifiers:
        - Strip `.ff` suffixes via relation_for
        - Leave other strings untouched
        """
        if not isinstance(ident, str):
            return ident
        return relation_for(ident) if ident.endswith(".ff") else ident

    def _clean_part(self, part: Any) -> str | None:
        if not isinstance(part, str):
            return None
        stripped = part.strip()
        return stripped or None

    def _quote_identifier(self, ident: str) -> str:  # pragma: no cover - abstract
        """Engine-specific quoting (e.g., \"name\" or `name`)."""
        raise NotImplementedError

    def _default_schema(self) -> str | None:
        return self._clean_part(getattr(self, "schema", None))

    def _default_catalog(self) -> str | None:
        return self._clean_part(getattr(self, "catalog", None))

    def _default_catalog_for_source(self, schema: str | None) -> str | None:
        """Hook to adjust catalog fallback for sources (override per engine)."""
        return self._default_catalog()

    def _should_include_catalog(
        self, catalog: str | None, schema: str | None, *, explicit: bool
    ) -> bool:
        """
        Decide whether to emit the catalog in a qualified identifier.

        explicit=True when the caller passed a catalog argument (as opposed to
        using defaults), so engines can honour explicit catalogs even if they
        normally omit them.
        """
        return bool(catalog)

    def _qualify_identifier(
        self,
        ident: str,
        *,
        schema: str | None = None,
        catalog: str | None = None,
        quote: bool = True,
    ) -> str:
        """
        Assemble a qualified identifier (catalog.schema.ident) with engine
        defaults and quoting.
        """
        normalized = self._normalize_identifier(ident)
        explicit_catalog = catalog is not None
        sch = self._clean_part(schema) or self._default_schema()
        cat = self._clean_part(catalog) if explicit_catalog else self._default_catalog()

        parts: list[str] = []
        if self._should_include_catalog(cat, sch, explicit=explicit_catalog) and cat:
            parts.append(cat)
        if sch:
            parts.append(sch)
        parts.append(normalized)

        if not quote:
            return ".".join(parts)
        return ".".join(self._quote_identifier(p) for p in parts)

    # ---- Shared formatting hooks -----------------------------------------
    def _format_relation_for_ref(self, name: str) -> str:
        return self._qualify_identifier(relation_for(name))

    def _pick_schema(self, cfg: dict[str, Any]) -> str | None:
        for key in ("schema", "dataset"):
            candidate = self._clean_part(cfg.get(key))
            if candidate:
                return candidate
        return self._default_schema()

    def _pick_catalog(self, cfg: dict[str, Any], schema: str | None) -> str | None:
        for key in ("catalog", "database", "project"):
            candidate = self._clean_part(cfg.get(key))
            if candidate:
                return candidate
        return self._default_catalog_for_source(schema)

    def _format_source_reference(
        self, cfg: dict[str, Any], source_name: str, table_name: str
    ) -> str:
        if cfg.get("location"):
            raise NotImplementedError(
                f"{getattr(self, 'engine_name', 'unknown')} executor "
                "does not support path-based sources."
            )

        ident = cfg.get("identifier")
        if not ident:
            raise KeyError(f"Source {source_name}.{table_name} missing identifier")

        schema = self._pick_schema(cfg)
        catalog = self._pick_catalog(cfg, schema)
        return self._qualify_identifier(ident, schema=schema, catalog=catalog)

# fastflowtransform/storage.py
from __future__ import annotations

import shutil
from collections.abc import Mapping
from dataclasses import dataclass, field
from pathlib import Path
from typing import Any
from uuid import uuid4


@dataclass
class _StorageRegistry:
    model: dict[str, dict[str, Any]] = field(default_factory=dict)
    seed: dict[str, dict[str, Any]] = field(default_factory=dict)


_STORAGE = _StorageRegistry()


def _sanitize_key(name: str) -> str:
    return name.replace("`", "").replace('"', "").strip()


def normalize_storage_map(
    raw: Mapping[str, Any] | None, *, project_dir: Path
) -> dict[str, dict[str, Any]]:
    if not raw or not isinstance(raw, Mapping):
        return {}

    normalized: dict[str, dict[str, Any]] = {}
    for key, cfg in raw.items():
        if not isinstance(cfg, Mapping):
            continue

        entry: dict[str, Any] = {}

        if "path" in cfg and cfg["path"] is not None:
            p = Path(str(cfg["path"]))
            if not p.is_absolute():
                p = (project_dir / p).resolve()
            entry["path"] = str(p)

        fmt = cfg.get("format")
        if fmt:
            entry["format"] = str(fmt)

        options = cfg.get("options")
        if isinstance(options, Mapping):
            entry["options"] = {str(k): v for k, v in options.items()}

        if entry:
            normalized[_sanitize_key(str(key))] = entry
    return normalized


def set_model_storage(mapping: Mapping[str, dict[str, Any]] | None) -> None:
    _STORAGE.model = dict(mapping or {})


def set_seed_storage(mapping: Mapping[str, dict[str, Any]] | None) -> None:
    _STORAGE.seed = dict(mapping or {})


def _lookup(storage_map: Mapping[str, dict[str, Any]], candidates: list[str]) -> dict[str, Any]:
    for cand in candidates:
        key = _sanitize_key(cand)
        meta = storage_map.get(key)
        if meta:
            return dict(meta)
    return {}


def get_model_storage(name: str) -> dict[str, Any]:
    candidates = [name]
    clean = _sanitize_key(name)
    if clean.endswith(".ff"):
        candidates.append(clean[:-3])
    else:
        candidates.append(f"{clean}.ff")
    parts = [p for p in clean.split(".") if p]
    if parts:
        candidates.append(parts[-1])
    return _lookup(_STORAGE.model, candidates)


def get_seed_storage(name: str) -> dict[str, Any]:
    clean = _sanitize_key(name)
    parts = [p for p in clean.split(".") if p]
    candidates = [clean]
    if parts:
        candidates.append(parts[-1])
    return _lookup(_STORAGE.seed, candidates)


def spark_write_to_path(
    spark: Any,
    identifier: str,
    df: Any,
    *,
    storage: Mapping[str, Any],
    default_format: str | None = None,
    default_options: Mapping[str, Any] | None = None,
) -> None:
    """
    Persist a Spark DataFrame to an explicit filesystem location and register it as a table.
    """
    path = storage.get("path")
    if not path:
        raise ValueError("storage path override requires 'path'")

    fmt = storage.get("format") or default_format
    options = dict(default_options or {})
    extra_opts = storage.get("options") or {}
    if isinstance(extra_opts, Mapping):
        options.update({str(k): v for k, v in extra_opts.items()})

    parts = [_sanitize_key(part) for part in identifier.split(".") if part]
    if not parts:
        raise ValueError(f"Invalid Spark identifier: {identifier}")

    def _quote(part: str) -> str:
        return "`" + part.replace("`", "``") + "`"

    target_sql = ".".join(_quote(p) for p in parts)

    writer = df.write.mode("overwrite")
    if fmt:
        writer = writer.format(fmt)
    if options:
        writer = writer.options(**options)

    path_str = str(path)
    is_local_path = "://" not in path_str

    if is_local_path:
        target_path = Path(path_str)
        target_path.parent.mkdir(parents=True, exist_ok=True)
        tmp_path = target_path.parent / f".ff_tmp_{target_path.name}_{uuid4().hex}"
        if tmp_path.exists():
            shutil.rmtree(tmp_path, ignore_errors=True)

        try:
            writer.save(str(tmp_path))
        except Exception:
            shutil.rmtree(tmp_path, ignore_errors=True)
            raise

        spark.sql(f"DROP TABLE IF EXISTS {target_sql}")
        if target_path.exists():
            shutil.rmtree(target_path, ignore_errors=True)
        try:
            tmp_path.rename(target_path)
        except Exception:
            shutil.rmtree(tmp_path, ignore_errors=True)
            raise
    else:
        writer.save(path_str)
        spark.sql(f"DROP TABLE IF EXISTS {target_sql}")

    using_clause = f"USING {fmt}" if fmt else ""
    escaped_path = path_str.replace("'", "''")
    spark.sql(f"CREATE TABLE {target_sql} {using_clause} LOCATION '{escaped_path}'")

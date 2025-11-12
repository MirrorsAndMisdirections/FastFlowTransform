from __future__ import annotations

import pytest
from pydantic import ValidationError

from fastflowtransform.config.models import IncrementalConfig, ModelConfig, validate_model_meta


@pytest.mark.unit
def test_validate_model_meta_basic_storage_and_tags():
    meta = {
        "materialized": "table",
        "tags": ["example", "demo"],
        "storage": {
            "path": "/tmp/users",
            "format": "parquet",
            "options": {"compression": "snappy"},
        },
    }

    cfg = validate_model_meta(meta)
    assert isinstance(cfg, ModelConfig)
    assert cfg.materialized == "table"
    assert cfg.tags == ["example", "demo"]
    assert cfg.storage is not None
    assert cfg.storage.path == "/tmp/users"
    assert cfg.storage.format == "parquet"
    assert cfg.storage.options == {"compression": "snappy"}


@pytest.mark.unit
def test_validate_model_meta_rejects_unknown_keys():
    # extra keys should be rejected if ModelConfig uses extra="forbid"
    with pytest.raises(ValidationError):
        validate_model_meta({"materialized": "table", "unknown_field": 1})


@pytest.mark.unit
def test_validate_model_meta_incremental_bool_and_dict_variants():
    # incremental: true
    cfg_bool = validate_model_meta({"incremental": True})
    assert isinstance(cfg_bool, ModelConfig)
    assert cfg_bool.is_incremental_enabled() is True
    assert isinstance(cfg_bool.incremental, IncrementalConfig)
    assert cfg_bool.incremental.enabled is True

    # incremental: {enabled: False, ...}
    cfg_dict = validate_model_meta(
        {
            "incremental": {
                "enabled": False,
                "unique_key": ["id"],
            }
        }
    )
    assert cfg_dict.is_incremental_enabled() is False
    # unique_key should be accepted and mirrored to the top-level shortcut
    assert cfg_dict.unique_key == ["id"]


@pytest.mark.unit
def test_validate_model_meta_incremental_invalid_strategy_raises():
    with pytest.raises(ValidationError):
        validate_model_meta(
            {
                "incremental": {
                    "enabled": True,
                    "strategy": "not_a_valid_strategy",
                }
            }
        )

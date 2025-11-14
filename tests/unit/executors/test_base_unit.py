from __future__ import annotations

import pytest

from fastflowtransform.executors.base import BaseExecutor


@pytest.mark.unit
def test_meta_is_incremental_simple_bool():
    assert BaseExecutor._meta_is_incremental({"incremental": True}) is True
    assert BaseExecutor._meta_is_incremental({"incremental": False}) is False


@pytest.mark.unit
def test_meta_is_incremental_dict_enabled_flag():
    assert BaseExecutor._meta_is_incremental({"incremental": {"enabled": True}}) is True
    assert BaseExecutor._meta_is_incremental({"incremental": {"enabled": False}}) is False


@pytest.mark.unit
def test_meta_is_incremental_dict_without_enabled_defaults_to_true():
    assert BaseExecutor._meta_is_incremental({"incremental": {"strategy": "merge"}}) is True


@pytest.mark.unit
def test_meta_is_incremental_respects_materialized_incremental():
    assert BaseExecutor._meta_is_incremental({"materialized": "incremental"}) is True
    # Even if incremental is explicitly False, materialized wins
    assert (
        BaseExecutor._meta_is_incremental({"materialized": "incremental", "incremental": False})
        is True
    )


@pytest.mark.unit
def test_meta_is_incremental_handles_empty_and_none():
    assert BaseExecutor._meta_is_incremental({}) is False
    assert BaseExecutor._meta_is_incremental(None) is False

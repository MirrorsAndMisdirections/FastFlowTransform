# tests/unit/api/test_rate_limit_unit.py
from __future__ import annotations

from typing import Any

import pytest

import fastflowtransform.api.rate_limit as rl_mod


@pytest.fixture(autouse=True)
def _reset_rate_limiter():
    """Make sure every test starts with a clean module-level state."""
    rl_mod.reset()
    yield
    rl_mod.reset()


@pytest.mark.unit
def test_tokenbucket_try_consume_enough_tokens(monkeypatch):
    """try_consume should return True and deduct tokens when bucket has enough."""
    tb = rl_mod.TokenBucket(capacity=5.0, refill_per_sec=1.0)
    # prefill
    tb._tokens = 3.0

    # stable time
    monkeypatch.setattr(rl_mod, "monotonic", lambda: 100.0)

    ok = tb.try_consume(2.0)
    assert ok is True
    assert tb._tokens == pytest.approx(1.0)


@pytest.mark.unit
def test_tokenbucket_try_consume_not_enough_tokens(monkeypatch):
    """try_consume should return False when bucket has not enough tokens."""
    tb = rl_mod.TokenBucket(capacity=5.0, refill_per_sec=1.0)
    tb._tokens = 0.5
    monkeypatch.setattr(rl_mod, "monotonic", lambda: 50.0)

    ok = tb.try_consume(1.0)
    assert ok is False
    # token count should be unchanged in this branch
    assert tb._tokens == pytest.approx(0.5)


@pytest.mark.unit
def test_tokenbucket_refills_on_try_consume(monkeypatch):
    """try_consume should trigger a refill before checking tokens."""
    tb = rl_mod.TokenBucket(capacity=5.0, refill_per_sec=2.0)
    tb._tokens = 0.0
    # first call: last_refill = 10.0
    tb._last_refill = 10.0
    # now is 11.0 → + 2 tokens
    monkeypatch.setattr(rl_mod, "monotonic", lambda: 11.0)

    ok = tb.try_consume(1.0)
    assert ok is True
    # we had 2.0, consumed 1.0 → 1.0 left
    assert tb._tokens == pytest.approx(1.0)


@pytest.mark.unit
def test_tokenbucket_wait_does_not_block_when_enough(monkeypatch):
    """wait() should return immediately if enough tokens are present."""
    tb = rl_mod.TokenBucket(capacity=5.0, refill_per_sec=1.0)
    tb._tokens = 4.0
    monkeypatch.setattr(rl_mod, "monotonic", lambda: 200.0)

    # patch sleep to raise if called
    called = {"sleep": False}

    def fake_sleep(x):
        called["sleep"] = True

    monkeypatch.setattr(rl_mod.time, "sleep", fake_sleep)

    tb.wait(2.0)
    assert called["sleep"] is False
    assert tb._tokens == pytest.approx(2.0)


@pytest.mark.unit
def test_tokenbucket_wait_blocks_once_and_consumes(monkeypatch):
    """wait() should sleep exactly once when tokens are not yet available."""
    tb = rl_mod.TokenBucket(capacity=5.0, refill_per_sec=1.0)
    tb._tokens = 0.0
    tb._last_refill = 10.0

    # 1. call inside loop -> 10.0
    # 2. call after sleep -> 11.0 (=> 1 Sekunde refill)
    times = [10.0, 11.0, 11.0]

    def fake_monotonic():
        return times.pop(0)

    slept_for: list[float] = []

    def fake_sleep(dur: float) -> None:
        slept_for.append(dur)

    # wait() ruft direkt rl_mod.monotonic() auf (import aus dem Modul)
    monkeypatch.setattr(rl_mod, "monotonic", fake_monotonic)
    # sleep kommt über rl_mod.time.sleep
    monkeypatch.setattr(rl_mod.time, "sleep", fake_sleep)

    tb.wait(1.0)

    assert len(slept_for) == 1
    # sollte ziemlich genau 1 Sekunde sein
    assert slept_for[0] == pytest.approx(1.0)
    # und danach sollten die Tokens verbraucht sein
    assert tb._tokens == pytest.approx(0.0)


@pytest.mark.unit
def test_tokenbucket_wait_disabled_does_nothing(monkeypatch):
    """If capacity/rps <= 0, wait() should be a no-op (used to disable the limiter)."""
    tb = rl_mod.TokenBucket(capacity=0.0, refill_per_sec=1.0)
    called = {"sleep": False}
    monkeypatch.setattr(rl_mod.time, "sleep", lambda *_: called.__setitem__("sleep", True))

    tb.wait(10.0)
    assert called["sleep"] is False


# ---------------- module-level helpers ----------------


@pytest.mark.unit
def test_init_rate_limiter_creates_bucket():
    """init_rate_limiter should build a TokenBucket when params are positive."""
    rl_mod.init_rate_limiter(5, 2)
    assert isinstance(rl_mod._STATE.rl, rl_mod.TokenBucket)
    expected_capacity = 5
    expected_refill_per_sec = 2
    assert rl_mod._STATE.rl.capacity == expected_capacity
    assert rl_mod._STATE.rl.refill_per_sec == expected_refill_per_sec


@pytest.mark.unit
def test_init_rate_limiter_disables_on_zero():
    """init_rate_limiter should disable when params are non-positive."""
    rl_mod.init_rate_limiter(0, 10)
    assert rl_mod._STATE.rl is None

    rl_mod.init_rate_limiter(10, 0)
    assert rl_mod._STATE.rl is None


@pytest.mark.unit
def test_set_params_on_uninitialized_creates_when_both_given():
    """set_params should create a bucket if none exists and both positive values are passed."""
    assert rl_mod._STATE.rl is None
    rl_mod.set_params(capacity=3, rps=1)
    assert isinstance(rl_mod._STATE.rl, rl_mod.TokenBucket)
    expected_capacity = 3
    expected_refill_per_sec = 1
    assert rl_mod._STATE.rl.capacity == expected_capacity
    assert rl_mod._STATE.rl.refill_per_sec == expected_refill_per_sec


@pytest.mark.unit
def test_set_params_updates_existing():
    """set_params should rebuild the bucket based on existing values when some params are None."""
    rl_mod.init_rate_limiter(5, 2)  # existing
    rl_mod.set_params(rps=10)  # keep capacity=5
    assert isinstance(rl_mod._STATE.rl, rl_mod.TokenBucket)
    expected_capacity = 5
    expected_refill_per_sec = 10
    assert rl_mod._STATE.rl.capacity == expected_capacity
    assert rl_mod._STATE.rl.refill_per_sec == expected_refill_per_sec


@pytest.mark.unit
def test_set_params_can_disable():
    """set_params should disable limiter when resulting params are non-positive."""
    rl_mod.init_rate_limiter(5, 2)
    rl_mod.set_params(capacity=0)
    assert rl_mod._STATE.rl is None


@pytest.mark.unit
def test_rate_limit_delegates_when_initialized(monkeypatch):
    """rate_limit() should call wait() on the current bucket."""
    rl_mod.init_rate_limiter(5, 1)
    bucket = rl_mod._STATE.rl
    assert bucket is not None

    called: dict[str, Any] = {"wait": False}

    def fake_wait(cost: float = 1.0) -> None:
        called["wait"] = cost

    bucket.wait = fake_wait  # type: ignore[assignment]

    rl_mod.rate_limit(3.5)
    expected_wait = 3.5
    assert called["wait"] == expected_wait


@pytest.mark.unit
def test_rate_limit_noop_when_uninitialized():
    """rate_limit() should just return when limiter is not initialized."""
    assert rl_mod._STATE.rl is None
    # should not crash
    rl_mod.rate_limit(10.0)
    assert rl_mod._STATE.rl is None


@pytest.mark.unit
def test_try_consume_noop_when_uninitialized():
    """try_consume() should return True when limiter is not initialized."""
    assert rl_mod._STATE.rl is None
    assert rl_mod.try_consume(999.0) is True


@pytest.mark.unit
def test_try_consume_delegates_when_initialized(monkeypatch):
    """try_consume() should delegate to bucket.try_consume()."""
    rl_mod.init_rate_limiter(5, 1)
    bucket = rl_mod._STATE.rl
    assert bucket is not None

    monkeypatch.setattr(bucket, "try_consume", lambda cost=1.0: cost == 1.0)

    assert rl_mod.try_consume(1.0) is True
    assert rl_mod.try_consume(2.0) is False


@pytest.mark.unit
def test_reset_clears_state():
    """reset() should clear the module-level bucket."""
    rl_mod.init_rate_limiter(5, 1)
    assert rl_mod._STATE.rl is not None
    rl_mod.reset()
    assert rl_mod._STATE.rl is None

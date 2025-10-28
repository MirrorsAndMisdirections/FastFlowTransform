# api/rate_limit.py
from __future__ import annotations

import time
from dataclasses import dataclass, field
from threading import Lock
from time import monotonic

__all__ = ["TokenBucket", "init_rate_limiter", "rate_limit", "reset", "set_params", "try_consume"]


@dataclass
class TokenBucket:
    """Thread-safe token bucket with blocking wait."""

    capacity: float
    refill_per_sec: float
    _tokens: float = 0.0
    _last_refill: float = field(default_factory=monotonic)
    _lock: Lock = field(default_factory=Lock, repr=False)

    def _refill(self, now: float) -> None:
        dt = now - self._last_refill
        if dt <= 0:
            return
        self._tokens = min(self.capacity, self._tokens + dt * self.refill_per_sec)
        self._last_refill = now

    def wait(self, cost: float = 1.0) -> None:
        """Block until at least `cost` tokens are available, then consume them."""
        if self.refill_per_sec <= 0 or self.capacity <= 0:
            return  # disabled
        while True:
            with self._lock:
                now = monotonic()
                self._refill(now)
                if self._tokens >= cost:
                    self._tokens -= cost
                    return
                missing = cost - self._tokens
                to_sleep = missing / max(self.refill_per_sec, 1e-9)
                self._tokens = 0.0
            time.sleep(max(to_sleep, 0.0))

    def try_consume(self, cost: float = 1.0) -> bool:
        """Attempt to consume `cost` tokens without blocking."""
        if self.refill_per_sec <= 0 or self.capacity <= 0:
            return True  # disabled → always allow
        with self._lock:
            now = monotonic()
            self._refill(now)
            if self._tokens >= cost:
                self._tokens -= cost
                return True
            return False


# ---- Module state holder (avoids `global` by mutating attributes) ----


@dataclass
class _State:
    rl: TokenBucket | None = None


_STATE = _State()


def init_rate_limiter(capacity: float, rps: float) -> None:
    """
    Initialize the module-level token bucket. If capacity<=0 or rps<=0, disables the limiter.
    """
    _STATE.rl = (
        None if capacity <= 0 or rps <= 0 else TokenBucket(capacity=capacity, refill_per_sec=rps)
    )


def set_params(capacity: float | None = None, rps: float | None = None) -> None:
    """
    Update parameters. Rebuilds the bucket to apply new settings (keeps behavior simple).
    If not initialized and both values are provided (and >0), initializes it.
    """
    rl = _STATE.rl
    if rl is None:
        if capacity and rps and capacity > 0 and rps > 0:
            _STATE.rl = TokenBucket(capacity=capacity, refill_per_sec=rps)
        return
    cap = capacity if capacity is not None else rl.capacity
    rate = rps if rps is not None else rl.refill_per_sec
    _STATE.rl = None if cap <= 0 or rate <= 0 else TokenBucket(capacity=cap, refill_per_sec=rate)


def rate_limit(cost: float = 1.0) -> None:
    """
    Block until `cost` tokens are available. No-op if limiter is disabled/uninitialized.
    """
    rl = _STATE.rl
    if rl is not None:
        rl.wait(cost)


def try_consume(cost: float = 1.0) -> bool:
    """
    Non-blocking: returns True if tokens were consumed; True as well if disabled/uninitialized.
    """
    rl = _STATE.rl
    return True if rl is None else rl.try_consume(cost)


def reset() -> None:
    """Test helper: clear the limiter state (no globals)."""
    _STATE.rl = None

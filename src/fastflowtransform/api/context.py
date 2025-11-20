from __future__ import annotations

import contextvars
from dataclasses import dataclass, field
from typing import Any

__all__ = [
    "_HttpStats",
    "_clear_http_ctx",
    "_get_http_ctx",
    "record",
    "reset_for_node",
    "snapshot",
]


@dataclass
class _HttpStats:
    """
    Per-context HTTP telemetry.

    Attributes
    ----------
    node : Optional[str]
        Current model/node name the HTTP calls belong to.
    requests : int
        Number of HTTP requests performed in this context.
    cache_hits : int
        Number of requests served from cache.
    bytes : int
        Total response bytes seen.
    hashes : List[str]
        Content hashes (deduplicated by consecutive repeats).
    keys : List[str]
        Stable IO keys (deduplicated by consecutive repeats).
    used_offline : bool
        True if at least one response was served from offline cache.
    """

    node: str | None = None
    requests: int = 0
    cache_hits: int = 0
    bytes: int = 0
    hashes: list[str] = field(default_factory=list)
    keys: list[str] = field(default_factory=list)  # stable IO keys for debugging/FP
    used_offline: bool = False

    def reset_for_node(self, node_name: str) -> None:
        """Reset statistics and set the current node name."""
        self.node = node_name
        self.requests = 0
        self.cache_hits = 0
        self.bytes = 0
        self.hashes.clear()
        self.keys.clear()
        self.used_offline = False

    def snapshot(self) -> dict[str, Any]:
        """Return a plain dict snapshot of the current counters."""
        return {
            "node": self.node,
            "requests": self.requests,
            "cache_hits": self.cache_hits,
            "bytes": self.bytes,
            "used_offline": self.used_offline,
            "content_hashes": list(self.hashes),
            "keys": list(self.keys),
        }


_ctx: contextvars.ContextVar[_HttpStats | None] = contextvars.ContextVar(
    "_ff_http_ctx", default=None
)


def _get_http_ctx() -> _HttpStats:
    """
    Return the per-context stats object, creating and storing it on first use.
    Safe to call from any thread/task; each context gets its own instance.
    """
    ctx = _ctx.get()
    if ctx is None:
        ctx = _HttpStats()
        _ctx.set(ctx)
    return ctx


def _clear_http_ctx() -> None:
    """Clear the per-context stats instance (useful in tests)."""
    _ctx.set(None)


# ----- Module-level convenience wrappers (callable without touching the instance) -----


def reset_for_node(node_name: str) -> None:
    """
    Reset the current context stats and set the node name.
    Equivalent to: _get_http_ctx().reset_for_node(node_name)
    """
    _get_http_ctx().reset_for_node(node_name)


def record(
    io_key: str,
    content_hash: str,
    cache_hit: bool,
    byte_len: int,
    used_offline: bool,
) -> None:
    """
    Record a single HTTP IO event into the current context.

    Parameters
    ----------
    io_key : str
        Stable IO cache key or identifier.
    content_hash : str
        Hash of the content (used for diagnostics).
    cache_hit : bool
        Whether the response came from cache.
    byte_len : int
        Number of response bytes (>= 0).
    used_offline : bool
        Whether the response was served in offline mode.
    """
    stats = _get_http_ctx()
    stats.requests += 1
    if cache_hit:
        stats.cache_hits += 1
    stats.bytes += int(byte_len or 0)

    # Deduplicate consecutive repeats while preserving order.
    if io_key and (not stats.keys or stats.keys[-1] != io_key):
        stats.keys.append(io_key)
    if content_hash and (not stats.hashes or stats.hashes[-1] != content_hash):
        stats.hashes.append(content_hash)

    stats.used_offline = bool(stats.used_offline or used_offline)


def snapshot() -> dict[str, Any]:
    """
    Return a snapshot dict of the current context. If the context is not yet
    initialized, return an empty/default snapshot instead of raising.
    """
    s = _ctx.get()
    if s is None:
        return {
            "node": None,
            "requests": 0,
            "cache_hits": 0,
            "bytes": 0,
            "used_offline": False,
            "content_hashes": [],
            "keys": [],
        }
    return s.snapshot()

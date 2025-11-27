# fastflowtransform/logging.py
from __future__ import annotations

import contextvars
import json

# IMPORTANT: avoid clashing with this module name
import logging as _logging
import os
import sys
from collections.abc import Generator, Iterable
from contextlib import contextmanager
from logging import LogRecord
from typing import Any

import typer

# -----------------------------------------------------------------------------
# Prefix configuration
# -----------------------------------------------------------------------------
LOG_PREFIX = os.getenv("FFT_LOG_PREFIX", "[FFT]").strip()


def _prefix_enabled() -> bool:
    return bool(LOG_PREFIX)


def _prefix_text_line(line: str) -> str:
    if not _prefix_enabled():
        return line
    return f"{LOG_PREFIX} {line}"


def _prefix_text_block(text: str) -> str:
    if not _prefix_enabled() or not text:
        return text
    lines = text.splitlines(keepends=True)
    if not lines:
        return text
    prefixed: list[str] = []
    for line in lines:
        if line.strip():
            prefixed.append(_prefix_text_line(line))
        else:
            prefixed.append(line)
    return "".join(prefixed)


def _prefix_format(fmt: str | None) -> str | None:
    if not fmt:
        return fmt
    if not _prefix_enabled():
        return fmt
    return f"{LOG_PREFIX} {fmt}"


def _apply_prefix(message: Any) -> Any:
    if not _prefix_enabled() or message is None:
        return message
    if isinstance(message, str):
        return _prefix_text_block(message)
    return _prefix_text_line(str(message))


# -----------------------------------------------------------------------------
# Context (enriched into log records) and runtime flags
# -----------------------------------------------------------------------------
_run_id: contextvars.ContextVar[str | None] = contextvars.ContextVar("ff_run_id", default=None)
_engine: contextvars.ContextVar[str | None] = contextvars.ContextVar("ff_engine", default=None)
_env: contextvars.ContextVar[str | None] = contextvars.ContextVar("ff_env", default=None)
_node: contextvars.ContextVar[str | None] = contextvars.ContextVar("ff_node", default=None)

_dbg: contextvars.ContextVar[bool] = contextvars.ContextVar("ff_dbg", default=False)
_sqldbg: contextvars.ContextVar[bool] = contextvars.ContextVar("ff_sqldbg", default=False)


def bind_context(
    *,
    run_id: str | None = None,
    engine: str | None = None,
    env: str | None = None,
    node: str | None = None,
    invocation_id: str | None = None,
) -> None:
    """Bind fields that get injected into every record."""
    if invocation_id is not None:
        run_id = invocation_id
    if run_id is not None:
        _run_id.set(run_id)
    if engine is not None:
        _engine.set(engine)
    if env is not None:
        _env.set(env)
    if node is not None:
        _node.set(node)


def clear_context() -> None:
    """Clear all bound fields."""
    _run_id.set(None)
    _engine.set(None)
    _env.set(None)
    _node.set(None)


@contextmanager
def bound_context(
    *,
    run_id: str | None = None,
    engine: str | None = None,
    env: str | None = None,
    node: str | None = None,
    invocation_id: str | None = None,
) -> Generator[None, None, None]:
    """
    Temporarily bind (or override) selected fields.
    Only 'node' is auto-cleared on exit; other fields persist unless you pass new values.
    """
    prev = (_run_id.get(), _engine.get(), _env.get(), _node.get())
    try:
        bind_context(
            run_id=run_id,
            engine=engine,
            env=env,
            node=node,
            invocation_id=invocation_id,
        )
        yield
    finally:
        # restore previous values; keep run_id/engine/env stable if you want by not overriding
        _run_id.set(prev[0])
        _engine.set(prev[1])
        _env.set(prev[2])
        _node.set(prev[3])


def set_flags(*, debug: bool | None = None, sql_debug: bool | None = None) -> None:
    """Set runtime flags (thread/task-local)."""
    if debug is not None:
        _dbg.set(bool(debug))
    if sql_debug is not None:
        _sqldbg.set(bool(sql_debug))


def is_debug_enabled() -> bool:
    return _dbg.get() or get_logger().isEnabledFor(_logging.DEBUG)


def is_sql_debug_enabled() -> bool:
    if _sqldbg.get():
        return True
    if os.getenv("FFT_SQL_DEBUG", "").lower() in ("1", "true", "yes", "on"):
        return True
    return get_logger("sql").isEnabledFor(_logging.DEBUG)


# -----------------------------------------------------------------------------
# Formatters and filter
# -----------------------------------------------------------------------------
class _CtxFilter(_logging.Filter):
    def filter(self, record: LogRecord) -> bool:  # inject context
        record.ff_run_id = _run_id.get()
        record.ff_engine = _engine.get()
        record.ff_env = _env.get()
        record.ff_node = _node.get()
        return True


class _ConsoleFormatter(_logging.Formatter):
    # Example:
    # 2025-10-28 10:15:12 INFO [DUCK dev users.ff] fastflowtransform.run: message
    _fmt = "%(asctime)s %(levelname)s [%(ff_engine)s %(ff_env)s %(ff_node)s] %(name)s: %(message)s"
    _date = "%Y-%m-%d %H:%M:%S"

    def __init__(self) -> None:
        super().__init__(_prefix_format(self._fmt), self._date)


class _JsonFormatter(_logging.Formatter):
    def format(self, record: LogRecord) -> str:
        payload: dict[str, Any] = {
            "ts": self.formatTime(record, "%Y-%m-%dT%H:%M:%S"),
            "level": record.levelname,
            "logger": record.name,
            "msg": record.getMessage(),
            "ff": {
                "run_id": getattr(record, "ff_run_id", None),
                "engine": getattr(record, "ff_engine", None),
                "env": getattr(record, "ff_env", None),
                "node": getattr(record, "ff_node", None),
            },
        }
        if record.exc_info:
            payload["exc"] = self.formatException(record.exc_info)
        return json.dumps(payload, ensure_ascii=False)


# -----------------------------------------------------------------------------
# Setup (one place to configure everything)
# -----------------------------------------------------------------------------
def setup(
    *,
    level: int = _logging.INFO,
    json: bool = False,
    to_stderr: bool = False,
    propagate_sql: bool = False,
) -> None:
    """
    Configure the 'fastflowtransform' logging tree.
    Idempotent: previous console handlers installed by this function are removed.
    """
    root = get_logger()  # 'fastflowtransform'
    root.setLevel(level)

    # Remove previous console handlers we installed
    for h in list(root.handlers):
        try:
            if h.get_name() == "ff_console":
                root.removeHandler(h)
        except Exception:
            if getattr(h, "name", None) == "ff_console":
                root.removeHandler(h)

    handler = _logging.StreamHandler(sys.stderr if to_stderr else sys.stdout)
    handler.set_name("ff_console")
    handler.setFormatter(_JsonFormatter() if json else _ConsoleFormatter())
    handler.addFilter(_CtxFilter())
    root.addHandler(handler)

    # SQL channel inherits formatter/sink via propagation
    sql = get_logger("sql")
    sql.setLevel(_logging.DEBUG if propagate_sql else level)
    sql.propagate = True

    # Also ensure a NullHandler at import-time on the root package
    _logging.getLogger("fastflowtransform").addHandler(_logging.NullHandler())


def setup_from_cli_flags(
    *,
    verbose: int = 0,
    quiet: int = 0,
    json: bool = False,
    sql_debug: bool = False,
    to_stderr: bool = False,
) -> None:
    """
    Convenience for CLI:
      quiet >=1 → ERROR
      verbose=0 → WARNING
      verbose=1 → INFO
      verbose>=2 → DEBUG
    """
    verbose_debug_level = 2
    level = _logging.WARNING
    if quiet >= 1:
        level = _logging.ERROR
    elif verbose == 1:
        level = _logging.INFO
    elif verbose >= verbose_debug_level:
        level = _logging.DEBUG

    env_sql = os.getenv("FFT_SQL_DEBUG", "").lower() in ("1", "true", "yes", "on")
    setup(
        level=level,
        json=json,
        to_stderr=to_stderr,
        propagate_sql=(sql_debug or env_sql or verbose >= verbose_debug_level),
    )
    set_flags(
        debug=(verbose >= verbose_debug_level),
        sql_debug=(sql_debug or env_sql or verbose >= verbose_debug_level),
    )


# -----------------------------------------------------------------------------
# Public helpers (single import point)
# -----------------------------------------------------------------------------
def get_logger(name: str | None = None) -> _logging.Logger:
    """
    Return a namespaced logger under 'fastflowtransform'.
    get_logger()          → 'fastflowtransform'
    get_logger("sql")     → 'fastflowtransform.sql'
    get_logger("cli.run") → 'fastflowtransform.cli.run'
    """
    base = "fastflowtransform"
    return _logging.getLogger(base if not name else f"{base}.{name}")


def echo(message: Any = "", *, prefix: bool = True, **kwargs: Any) -> None:
    """
    Thin wrapper around typer.echo(...) that prepends the global log prefix.

    Usage:
        echo("hello")
        echo("to stderr", err=True)
        echo("no newline", nl=False)
        echo("colored", color=True)
        echo("raw message", prefix=False)  # skip prefix if needed
    """
    msg = _apply_prefix(message) if prefix else message
    typer.echo(msg, **kwargs)


def echo_debug(message: Any = "", *, prefix: bool = True, **kwargs: Any) -> None:
    """
    Like echo(...), but only emits when `fastflowtransform` logger is in DEBUG.

    Usage:
        echo_debug("SQL preview:", sql_text)
        echo_debug("to stderr only in debug", err=True)
    """
    logger = get_logger()
    if logger.isEnabledFor(_logging.DEBUG):
        echo(message, prefix=prefix, **kwargs)


def info(msg: str, *args: Any, **kwargs: Any) -> None:
    get_logger("app").info(msg, *args, **kwargs)


def warn(msg: str, *args: Any, **kwargs: Any) -> None:
    get_logger("app").warning(msg, *args, **kwargs)


def error(msg: str, *args: Any, **kwargs: Any) -> None:
    get_logger("app").error(msg, *args, **kwargs)


def debug(msg: str, *args: Any, **kwargs: Any) -> None:
    """General debug; gated by runtime flag OR logger level."""
    if is_debug_enabled():
        get_logger("app").debug(msg, *args, **kwargs)


def dprint(*parts: Any) -> None:
    """
    Lightweight stdout debugging; useful for quick probes during dev.
    Only prints when general debug is enabled.
    """
    if is_debug_enabled():
        body = " ".join(str(p) for p in parts) if parts else ""
        msg = "[DBG]" if not body else f"[DBG] {body}"
        print(_prefix_text_line(msg), file=sys.stdout)


def sql_debug(msg: str, *args: Any, **kwargs: Any) -> None:
    """Single-line SQL debug; appears only when SQL debug is enabled."""
    if is_sql_debug_enabled():
        get_logger("sql").debug(msg, *args, **kwargs)


def sql_block(title: str, lines: Iterable[str] | str) -> None:
    """Pretty multi-line SQL preview gated by SQL debug."""
    if not is_sql_debug_enabled():
        return
    body = lines.rstrip() if isinstance(lines, str) else "\n".join(str(x) for x in lines).rstrip()
    if title:
        get_logger("sql").debug("%s\n%s", title, body)
    else:
        get_logger("sql").debug("%s", body)

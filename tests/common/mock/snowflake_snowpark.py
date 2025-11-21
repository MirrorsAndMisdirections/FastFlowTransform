# tests/common/mock/snowflake_snowpark.py
from __future__ import annotations

from dataclasses import dataclass, field
from typing import Any


@dataclass
class FakeSFRow:
    """Simple Row-like object with asDict() support."""

    data: dict[str, Any]

    def asDict(self) -> dict[str, Any]:
        return dict(self.data)


class FakeSFResult:
    """
    Minimal Snowpark Result mimic: only .collect() is used by the executor.
    """

    def __init__(self, rows: list[FakeSFRow] | None = None):
        self._rows = rows or []

    def collect(self) -> list[FakeSFRow]:
        return self._rows


@dataclass
class FakeSnowflakeSession:
    """
    Fake Snowflake Snowpark Session that:
      - records every SQL statement in `sql_calls`
      - returns empty results for all statements
    This is enough to test that our executor *emits* the right SQL.
    """

    sql_calls: list[str] = field(default_factory=list)

    # --- API used by SnowflakeSnowparkExecutor ---

    def sql(self, query: str) -> FakeSFResult:
        self.sql_calls.append(query)
        # For our tests we don't need real rows, just a non-crashing collect().
        return FakeSFResult([])

    # In these SQL-only snapshot tests we never actually call `table(...)`,
    # but we provide a stub so accesses fail loudly if that changes.
    def table(self, name: str) -> Any:
        raise NotImplementedError(
            "FakeSnowflakeSession.table is not implemented for SQL-only tests."
        )

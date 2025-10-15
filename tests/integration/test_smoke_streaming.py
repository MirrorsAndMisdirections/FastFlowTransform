from datetime import UTC, datetime, timedelta

import duckdb
import pandas as pd
import pytest

from flowforge import testing
from flowforge.streaming import StreamSessionizer


@pytest.mark.streaming
@pytest.mark.slow
def test_stream_sessionizer_produces_sessions():
    con = duckdb.connect(":memory:")
    sess = StreamSessionizer(con)

    # Fixe "now" Referenz & echte Timestamps (UTC, pandas dtype)
    now = datetime.now(UTC)
    events = pd.DataFrame(
        [
            {
                "user_id": "u1",
                "session_id": "s1",
                "source": "ads",
                "event_type": "page_view",
                "event_timestamp": now - timedelta(minutes=2),
                "amount": None,
            },
            {
                "user_id": "u1",
                "session_id": "s1",
                "source": "ads",
                "event_type": "purchase",
                "event_timestamp": now - timedelta(minutes=1, seconds=30),
                "amount": 19.9,
            },
            {
                "user_id": "u2",
                "session_id": "s2",
                "source": "organic",
                "event_type": "page_view",
                "event_timestamp": now - timedelta(minutes=1),
                "amount": None,
            },
        ]
    )
    events["event_timestamp"] = pd.to_datetime(events["event_timestamp"], utc=True)

    # Prozessieren
    sess.process_batch(events)

    # Tabelle existiert & hat Zeilen
    row = con.execute("select count(*) from fct_sessions_streaming").fetchone()
    assert row is not None, "Query lieferte keine Zeile"
    rows = int(row[0])
    assert rows >= 2

    # Basis-Checks (funktionieren in DuckDB & PG)
    testing.greater_equal(con, "fct_sessions_streaming", "revenue", 0)
    testing.non_negative_sum(con, "fct_sessions_streaming", "revenue")

    # Freshness: DuckDB-sichere Variante direkt im Test (um SQL-Dialekte zu umgehen)
    # DuckDB: date_diff('minute', max(ts), now())
    delay_min = con.execute(
        "select date_diff('minute', max(session_end), current_timestamp) from fct_sessions_streaming"
    ).fetchone()
    delay_min = int(delay_min[0]) if delay_min is not None else None
    assert delay_min is not None and delay_min <= 15, f"freshness too old: {delay_min} min"

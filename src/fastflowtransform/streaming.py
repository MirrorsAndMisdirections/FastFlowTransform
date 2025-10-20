from __future__ import annotations

import json
import time
from collections.abc import Callable
from pathlib import Path

import duckdb
import pandas as pd


class FileTailSource:
    def __init__(self, path: str):
        self.path = Path(path)
        self._stop = False

    def stop(self):
        self._stop = True

    def consume(
        self, on_batch: Callable[[pd.DataFrame], None], batch_size: int = 100, poll_sec: float = 1.0
    ) -> None:
        self.path.parent.mkdir(parents=True, exist_ok=True)
        self.path.touch(exist_ok=True)
        with self.path.open("r", encoding="utf-8") as f:
            f.seek(0, 2)  # ans Ende
            buf: list[dict] = []
            while not self._stop:
                line = f.readline()
                if not line:
                    time.sleep(poll_sec)
                    continue
                try:
                    buf.append(json.loads(line))
                except Exception:
                    continue
                if len(buf) >= batch_size:
                    on_batch(pd.DataFrame(buf))
                    buf.clear()
            if buf:
                on_batch(pd.DataFrame(buf))


class StreamSessionizer:
    def __init__(self, con: duckdb.DuckDBPyConnection, gap_minutes: int = 30):
        self.con = con
        self.gap_minutes = gap_minutes
        self.con.execute(
            """
            create table if not exists fct_sessions_streaming as
            select * from (
                values (cast(NULL as bigint))
            ) where 1=0;
            """
        )

    def process_batch(self, df: pd.DataFrame) -> None:
        if df.empty:
            return
        self.con.register("_events_batch", df)
        self.con.execute(
            """
            -- 1) Normalisieren
            create or replace table _events_norm as
            select
            cast(user_id as varchar)        as user_id,
            cast(session_id as varchar)     as session_id,
            cast(source as varchar)         as source,
            cast(event_type as varchar)     as event_type,
            cast(event_timestamp as timestamptz) as event_ts,
            try_cast(amount as double)      as amount
            from _events_batch;

            -- 2) Sessions aggregieren
            create or replace table _sessions_batch as
            select
            session_id,
            any_value(user_id)              as user_id,
            any_value(source)               as source,
            min(event_ts)                   as session_start,
            max(event_ts)                   as session_end,
            sum(case when event_type='page_view' then 1 else 0 end) as pageviews,
            sum(coalesce(amount,0)) filter (where event_type='purchase') as revenue
            from _events_norm
            group by session_id;

            -- 3) Zieltabelle sicherstellen (Schema festnageln)
            drop table if exists fct_sessions_streaming;
            create table if not exists fct_sessions_streaming (
            session_id    varchar,
            user_id       varchar,
            source        varchar,
            session_start timestamptz,
            session_end   timestamptz,
            pageviews     bigint,
            revenue       double
            );

            -- 4) Append mit expliziter Spaltenliste (keine '*'!)
            insert into fct_sessions_streaming
            (session_id, user_id, source, session_start, session_end, pageviews, revenue)
            select
            session_id, user_id, source, session_start, session_end, pageviews, revenue
            from _sessions_batch;
            """
        )

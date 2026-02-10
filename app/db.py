import os
from contextlib import contextmanager
from pathlib import Path
from typing import Any

import psycopg
from psycopg.rows import dict_row
from psycopg.types.json import Jsonb

if __package__:
    from .schemas import ImpressionEvent, WatchEvent
else:  # pragma: no cover - fallback for direct script execution
    from schemas import ImpressionEvent, WatchEvent

try:
    from psycopg_pool import ConnectionPool
except ImportError:
    ConnectionPool = None

DATABASE_URL = os.getenv("DATABASE_URL", "postgresql://postgres:postgres@localhost:5432/recsys")
SCHEMA_PATH = Path(__file__).resolve().parents[1] / "sql" / "schema.sql"

_pool = None


def _get_pool():
    global _pool
    if ConnectionPool is None:
        return None
    if _pool is None:
        min_size = int(os.getenv("DB_POOL_MIN_SIZE", "1"))
        max_size = int(os.getenv("DB_POOL_MAX_SIZE", "10"))
        _pool = ConnectionPool(
            conninfo=DATABASE_URL,
            min_size=min_size,
            max_size=max_size,
            kwargs={"row_factory": dict_row},
            open=True,
        )
    return _pool


@contextmanager
def get_conn():
    pool = _get_pool()
    if pool is not None:
        with pool.connection() as conn:
            yield conn
        return

    conn = psycopg.connect(DATABASE_URL, row_factory=dict_row)
    try:
        yield conn
    finally:
        conn.close()


@contextmanager
def transaction():
    with get_conn() as conn:
        try:
            yield conn
            conn.commit()
        except Exception:
            conn.rollback()
            raise


def init_db(schema_path: Path = SCHEMA_PATH):
    ddl = schema_path.read_text(encoding="utf-8")
    with transaction() as conn:
        with conn.cursor() as cur:
            cur.execute(ddl)


def close_pool():
    global _pool
    if _pool is not None:
        _pool.close()
        _pool = None


def _context_to_jsonb(context: Any):
    if context is None:
        return None
    return Jsonb(context.model_dump(exclude_none=True))


def persist_impression_event(event: ImpressionEvent) -> int:
    context_jsonb = _context_to_jsonb(event.context)
    rows = [
        (
            event.event_id,
            event.user_id,
            event.session_id,
            event.request_id,
            event.ts_ms,
            getattr(event, "server_received_ts_ms", None),
            item.item_id,
            item.position,
            item.feed_id,
            item.slot,
            context_jsonb,
        )
        for item in event.items
    ]

    inserted = 0
    with transaction() as conn:
        with conn.cursor() as cur:
            for row in rows:
                cur.execute(
                    """
                    INSERT INTO impressions (
                        event_id,
                        user_id,
                        session_id,
                        request_id,
                        ts_ms,
                        server_received_ts_ms,
                        item_id,
                        position,
                        feed_id,
                        slot,
                        context
                    )
                    VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
                    ON CONFLICT (event_id, position) DO NOTHING
                    RETURNING 1
                    """,
                    row,
                )
                if cur.fetchone() is not None:
                    inserted += 1
    return inserted


def persist_watch_event(event: WatchEvent) -> bool:
    context_jsonb = _context_to_jsonb(event.context)
    with transaction() as conn:
        with conn.cursor() as cur:
            cur.execute(
                """
                INSERT INTO watches (
                    event_id,
                    user_id,
                    session_id,
                    request_id,
                    item_id,
                    ts_ms,
                    server_received_ts_ms,
                    watch_time_sec,
                    percent_watched,
                    ended,
                    playback_speed,
                    rebuffer_count,
                    context
                )
                VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
                ON CONFLICT (event_id) DO NOTHING
                RETURNING 1
                """,
                (
                    event.event_id,
                    event.user_id,
                    event.session_id,
                    event.request_id,
                    event.item_id,
                    event.ts_ms,
                    getattr(event, "server_received_ts_ms", None),
                    event.watch_time_sec,
                    event.percent_watched,
                    event.ended,
                    event.playback_speed,
                    event.rebuffer_count,
                    context_jsonb,
                ),
            )
            return cur.fetchone() is not None

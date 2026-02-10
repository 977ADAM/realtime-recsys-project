import os
from contextlib import contextmanager
from pathlib import Path
from typing import Any, Optional, Tuple

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


def _positive_int_env(name: str, default: int) -> int:
    raw = os.getenv(name)
    if raw is None:
        return default
    try:
        value = int(raw)
    except ValueError:
        return default
    return value if value > 0 else default


STREAM_FEATURE_HISTORY_SIZE = _positive_int_env("STREAM_FEATURE_HISTORY_SIZE", 20)


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


def _watch_signal_to_event(event: WatchEvent) -> Tuple[str, int]:
    # Map watch strength to the existing event taxonomy for consistent feature updates.
    if (event.percent_watched is not None and event.percent_watched >= 80) or event.watch_time_sec >= 120:
        return "purchase", 10
    if (event.percent_watched is not None and event.percent_watched >= 40) or event.watch_time_sec >= 30:
        return "click", 3
    return "view", 1


def _normalize_unix_ts_seconds(ts: Optional[int]) -> Optional[float]:
    if ts is None:
        return None
    return ts / 1000.0 if ts >= 10**12 else float(ts)


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


def persist_watch_event_and_update_features(
    event: WatchEvent,
    history_size: int = STREAM_FEATURE_HISTORY_SIZE,
) -> dict:
    history_size = max(int(history_size), 1)
    context_jsonb = _context_to_jsonb(event.context)
    event_type, weight = _watch_signal_to_event(event)
    ts_seconds = _normalize_unix_ts_seconds(event.ts_ms)
    stream_event_id = f"watch-{event.event_id}"

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
            inserted_watch = cur.fetchone() is not None
            if not inserted_watch:
                return {
                    "inserted_watch": False,
                    "feature_updated": False,
                    "feature_event_type": event_type,
                    "feature_weight": weight,
                }

            cur.execute(
                """
                INSERT INTO events (event_id, user_id, item_id, event_type, ts)
                VALUES (%s, %s, %s, %s, COALESCE(to_timestamp(%s), now()))
                ON CONFLICT (event_id) DO NOTHING
                """,
                (
                    stream_event_id,
                    event.user_id,
                    event.item_id,
                    event_type,
                    ts_seconds,
                ),
            )

            # Serialize writes per user to keep history position monotonic.
            cur.execute("SELECT pg_advisory_xact_lock(hashtext(%s))", (event.user_id,))

            cur.execute(
                """
                INSERT INTO item_popularity (item_id, score)
                VALUES (%s, %s)
                ON CONFLICT (item_id)
                DO UPDATE SET
                    score = item_popularity.score + EXCLUDED.score,
                    updated_at = now()
                """,
                (event.item_id, weight),
            )

            cur.execute(
                """
                SELECT item_id
                FROM user_history
                WHERE user_id = %s
                ORDER BY pos DESC
                LIMIT 1
                """,
                (event.user_id,),
            )
            prev = cur.fetchone()

            if prev and prev["item_id"] != event.item_id:
                cur.execute(
                    """
                    INSERT INTO co_visitation (prev_item_id, next_item_id, cnt)
                    VALUES (%s, %s, 1)
                    ON CONFLICT (prev_item_id, next_item_id)
                    DO UPDATE SET cnt = co_visitation.cnt + 1
                    """,
                    (prev["item_id"], event.item_id),
                )

            cur.execute(
                """
                WITH next_pos AS (
                    SELECT COALESCE(MAX(pos), 0) + 1 AS pos
                    FROM user_history
                    WHERE user_id = %s
                )
                INSERT INTO user_history (user_id, pos, item_id, ts)
                SELECT %s, pos, %s, COALESCE(to_timestamp(%s), now())
                FROM next_pos
                """,
                (
                    event.user_id,
                    event.user_id,
                    event.item_id,
                    ts_seconds,
                ),
            )

            cur.execute(
                """
                DELETE FROM user_history
                WHERE user_id = %s
                  AND pos <= (
                    SELECT COALESCE(MAX(pos), 0) - %s
                    FROM user_history
                    WHERE user_id = %s
                  )
                """,
                (event.user_id, history_size, event.user_id),
            )

            return {
                "inserted_watch": True,
                "feature_updated": True,
                "feature_event_type": event_type,
                "feature_weight": weight,
            }


def get_baseline_kpi_snapshot(days: int = 7) -> dict:
    with get_conn() as conn:
        with conn.cursor() as cur:
            cur.execute(
                """
                WITH bounds AS (
                    SELECT now() - (%s * interval '1 day') AS since_ts
                ),
                recent_impressions AS (
                    SELECT i.*
                    FROM impressions i
                    CROSS JOIN bounds b
                    WHERE to_timestamp(i.ts_ms / 1000.0) >= b.since_ts
                ),
                recent_watches AS (
                    SELECT w.*
                    FROM watches w
                    CROSS JOIN bounds b
                    WHERE to_timestamp(w.ts_ms / 1000.0) >= b.since_ts
                ),
                watched_impressions AS (
                    SELECT DISTINCT i.event_id, i.position
                    FROM recent_impressions i
                    INNER JOIN recent_watches w
                        ON w.user_id = i.user_id
                       AND w.request_id = i.request_id
                       AND w.item_id = i.item_id
                       AND w.ts_ms >= i.ts_ms
                )
                SELECT
                    (SELECT COUNT(*) FROM recent_impressions) AS impressions,
                    (SELECT COUNT(DISTINCT request_id) FROM recent_impressions) AS requests,
                    (SELECT COUNT(*) FROM recent_watches) AS watches,
                    (SELECT COUNT(*) FROM watched_impressions) AS watched_impressions,
                    (SELECT COALESCE(AVG(watch_time_sec), 0) FROM recent_watches) AS avg_watch_time_sec,
                    (
                        SELECT COUNT(DISTINCT item_id)
                        FROM recent_impressions
                        WHERE position < 20
                    ) AS recommended_catalog_top20,
                    (SELECT COUNT(DISTINCT item_id) FROM item_popularity) AS active_catalog_size
                """,
                (days,),
            )
            row = cur.fetchone() or {}

    impressions = int(row.get("impressions") or 0)
    watched_impressions = int(row.get("watched_impressions") or 0)
    active_catalog_size = int(row.get("active_catalog_size") or 0)
    recommended_catalog_top20 = int(row.get("recommended_catalog_top20") or 0)

    ctr_at_20 = (watched_impressions / impressions * 100.0) if impressions else 0.0
    coverage_at_20 = (
        recommended_catalog_top20 / active_catalog_size * 100.0
        if active_catalog_size
        else 0.0
    )

    return {
        "window_days": int(days),
        "requests": int(row.get("requests") or 0),
        "impressions": impressions,
        "watches": int(row.get("watches") or 0),
        "watched_impressions": watched_impressions,
        "ctr_at_20_pct": round(ctr_at_20, 3),
        "avg_watch_time_sec": round(float(row.get("avg_watch_time_sec") or 0.0), 3),
        "catalog_coverage_at_20_pct": round(coverage_at_20, 3),
    }

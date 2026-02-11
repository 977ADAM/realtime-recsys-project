import os
from contextlib import contextmanager
from pathlib import Path
from typing import Any, Tuple

import psycopg
from psycopg.rows import dict_row
from psycopg.types.json import Jsonb

if __package__:
    from .repositories import EventLogRepository, FeatureRepository
    from .runtime_utils import normalize_unix_ts_seconds, positive_int_env
    from .schemas import ImpressionEvent, WatchEvent
else:  # pragma: no cover - fallback for direct script execution
    from repositories import EventLogRepository, FeatureRepository
    from runtime_utils import normalize_unix_ts_seconds, positive_int_env
    from schemas import ImpressionEvent, WatchEvent

try:
    from psycopg_pool import ConnectionPool
except ImportError:
    ConnectionPool = None

DATABASE_URL = os.getenv("DATABASE_URL", "postgresql://adam:977977977@localhost:5432/recsys")
SCHEMA_PATH = Path(__file__).resolve().parents[1] / "sql" / "schema.sql"

_pool = None


STREAM_FEATURE_HISTORY_SIZE = positive_int_env("STREAM_FEATURE_HISTORY_SIZE", 20)
feature_repository = FeatureRepository()
event_log_repository = EventLogRepository()


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


def persist_impression_event(event: ImpressionEvent) -> int:
    context_jsonb = _context_to_jsonb(event.context)
    with transaction() as conn:
        with conn.cursor() as cur:
            return event_log_repository.insert_impression_event(cur, event, context_jsonb)


def persist_watch_event(event: WatchEvent) -> bool:
    context_jsonb = _context_to_jsonb(event.context)
    with transaction() as conn:
        with conn.cursor() as cur:
            return event_log_repository.insert_watch_event(cur, event, context_jsonb)


def persist_watch_event_and_update_features(
    event: WatchEvent,
    history_size: int = STREAM_FEATURE_HISTORY_SIZE,
) -> dict:
    history_size = max(int(history_size), 1)
    context_jsonb = _context_to_jsonb(event.context)
    event_type, weight = _watch_signal_to_event(event)
    ts_seconds = normalize_unix_ts_seconds(event.ts_ms)
    stream_event_id = f"watch-{event.event_id}"

    with transaction() as conn:
        with conn.cursor() as cur:
            inserted_watch = event_log_repository.insert_watch_event(cur, event, context_jsonb)
            if not inserted_watch:
                return {
                    "inserted_watch": False,
                    "feature_updated": False,
                    "feature_event_type": event_type,
                    "feature_weight": weight,
                }

            feature_repository.apply_feedback_event(
                cur,
                event_id=stream_event_id,
                user_id=event.user_id,
                item_id=event.item_id,
                event_type=event_type,
                ts_seconds=ts_seconds,
                weight=weight,
                history_size=history_size,
                require_inserted_event=False,
                skip_same_item_transition=True,
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
            row = event_log_repository.fetch_baseline_kpi_row(cur, days)

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

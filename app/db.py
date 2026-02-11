import os
import hashlib
import json
import logging
from contextlib import contextmanager
from pathlib import Path
from typing import Any, Optional, Tuple
from dotenv import load_dotenv

import psycopg
from psycopg.rows import dict_row
from psycopg.types.json import Jsonb

if __package__:
    from .repositories import EventLogRepository, FeatureRepository
    from .runtime_utils import normalize_unix_ts_seconds, positive_float_env, positive_int_env
    from .schemas import ImpressionEvent, WatchEvent
else:  # pragma: no cover - fallback for direct script execution
    from repositories import EventLogRepository, FeatureRepository
    from runtime_utils import normalize_unix_ts_seconds, positive_float_env, positive_int_env
    from schemas import ImpressionEvent, WatchEvent

try:
    from psycopg_pool import ConnectionPool
except ImportError:
    ConnectionPool = None

SCHEMA_PATH = Path(__file__).resolve().parents[1] / "sql" / "schema.sql"
MIGRATIONS_DIR = Path(__file__).resolve().parents[1] / "sql" / "migrations"
MIGRATIONS_TABLE = "schema_migrations"

_pool = None
logger = logging.getLogger(__name__)


STREAM_FEATURE_HISTORY_SIZE = positive_int_env("STREAM_FEATURE_HISTORY_SIZE", 20)
feature_repository = FeatureRepository()
event_log_repository = EventLogRepository()

DB_CONNECT_TIMEOUT_SEC = positive_float_env("DB_CONNECT_TIMEOUT_SEC", 2.0)
DB_POOL_OPEN_TIMEOUT_SEC = positive_float_env("DB_POOL_OPEN_TIMEOUT_SEC", 5.0)
DB_STATEMENT_TIMEOUT_MS = positive_int_env("DB_STATEMENT_TIMEOUT_MS", 5000)
DB_LOCK_TIMEOUT_MS = positive_int_env("DB_LOCK_TIMEOUT_MS", 1000)

OUTBOX_MAX_ATTEMPTS = positive_int_env("OUTBOX_MAX_ATTEMPTS", 25)
OUTBOX_BASE_RETRY_SEC = positive_int_env("OUTBOX_RELAY_BASE_RETRY_SEC", 1)
OUTBOX_MAX_RETRY_SEC = positive_int_env("OUTBOX_RELAY_MAX_RETRY_SEC", 60)

_INSERT_OUTBOX_SQL = """
    INSERT INTO event_outbox (
        event_id,
        event_type,
        kafka_topic,
        kafka_key,
        payload,
        max_attempts
    )
    VALUES (%s, %s, %s, %s, %s, %s)
    ON CONFLICT (event_id, event_type) DO NOTHING
    RETURNING id
"""

_INSERT_CONSUMED_FEATURE_EVENT_SQL = """
    INSERT INTO feature_event_consumed (
        stream_event_id,
        source_topic,
        source_partition,
        source_offset
    )
    VALUES (%s, %s, %s, %s)
    ON CONFLICT (stream_event_id) DO NOTHING
    RETURNING 1
"""

_CLAIM_OUTBOX_EVENTS_SQL = """
    WITH candidates AS (
        SELECT id
        FROM event_outbox
        WHERE published_at IS NULL
          AND status IN ('pending', 'publishing')
          AND next_attempt_at <= now()
          AND (status = 'pending' OR lease_expires_at IS NULL OR lease_expires_at <= now())
        ORDER BY id
        FOR UPDATE SKIP LOCKED
        LIMIT %s
    )
    UPDATE event_outbox AS o
    SET
        status = 'publishing',
        lease_expires_at = now() + make_interval(secs => %s),
        attempt_count = o.attempt_count + 1,
        updated_at = now(),
        last_error = NULL
    FROM candidates
    WHERE o.id = candidates.id
    RETURNING
        o.id,
        o.event_id,
        o.event_type,
        o.kafka_topic,
        o.kafka_key,
        o.payload,
        CAST(EXTRACT(EPOCH FROM o.created_at) * 1000 AS BIGINT) AS created_at_ms,
        o.attempt_count,
        o.max_attempts
"""


def _database_url() -> str:
    value = os.getenv("DATABASE_URL", "").strip()
    if value:
        return value

    project_env = Path(__file__).resolve().parents[1] / ".env"
    load_dotenv(dotenv_path=project_env, override=False)
    load_dotenv(override=False)
    value = os.getenv("DATABASE_URL", "").strip()
    if value:
        return value
    raise RuntimeError("DATABASE_URL is not configured")


def _get_pool():
    global _pool
    if ConnectionPool is None:
        return None
    if _pool is None:
        min_size = positive_int_env("DB_POOL_MIN_SIZE", 1)
        max_size = max(positive_int_env("DB_POOL_MAX_SIZE", 10), min_size)
        _pool = ConnectionPool(
            conninfo=_database_url(),
            min_size=min_size,
            max_size=max_size,
            kwargs=_connection_kwargs(),
            timeout=DB_POOL_OPEN_TIMEOUT_SEC,
            open=True,
        )
    return _pool


def _connection_kwargs() -> dict[str, Any]:
    kwargs: dict[str, Any] = {
        "row_factory": dict_row,
        "connect_timeout": max(int(DB_CONNECT_TIMEOUT_SEC), 1),
    }

    options: list[str] = []
    if DB_STATEMENT_TIMEOUT_MS > 0:
        options.append(f"-c statement_timeout={int(DB_STATEMENT_TIMEOUT_MS)}")
    if DB_LOCK_TIMEOUT_MS > 0:
        options.append(f"-c lock_timeout={int(DB_LOCK_TIMEOUT_MS)}")
    if options:
        kwargs["options"] = " ".join(options)
    return kwargs


@contextmanager
def get_conn():
    pool = _get_pool()
    if pool is not None:
        try:
            with pool.connection() as conn:
                yield conn
        except Exception:
            logger.exception("Database pool connection acquisition failed")
            raise
        return

    conn = psycopg.connect(_database_url(), **_connection_kwargs())
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


def _migration_files(migrations_dir: Path) -> list[Path]:
    if not migrations_dir.exists():
        return []
    return sorted(path for path in migrations_dir.iterdir() if path.is_file() and path.suffix == ".sql")


def _migration_checksum(ddl: str) -> str:
    return hashlib.sha256(ddl.encode("utf-8")).hexdigest()


def _ensure_migrations_table(cur) -> None:
    cur.execute(
        f"""
        CREATE TABLE IF NOT EXISTS {MIGRATIONS_TABLE} (
            version TEXT PRIMARY KEY,
            checksum TEXT NOT NULL,
            applied_at TIMESTAMPTZ NOT NULL DEFAULT now()
        )
        """
    )


def _fetch_applied_migrations(cur) -> dict[str, str]:
    cur.execute(f"SELECT version, checksum FROM {MIGRATIONS_TABLE}")
    rows = cur.fetchall()
    return {str(row["version"]): str(row["checksum"]) for row in rows}


def _record_migration(cur, version: str, checksum: str) -> None:
    cur.execute(
        f"""
        INSERT INTO {MIGRATIONS_TABLE} (version, checksum)
        VALUES (%s, %s)
        """,
        (version, checksum),
    )


def _apply_migration(cur, version: str, ddl: str, applied: dict[str, str]) -> bool:
    checksum = _migration_checksum(ddl)
    existing = applied.get(version)
    if existing is not None:
        if existing != checksum:
            raise RuntimeError(
                f"Migration checksum mismatch for {version}: stored={existing}, local={checksum}"
            )
        return False

    cur.execute(ddl)
    _record_migration(cur, version=version, checksum=checksum)
    applied[version] = checksum
    return True


def init_db(migrations_dir: Path = MIGRATIONS_DIR, schema_path: Path = SCHEMA_PATH):
    migration_paths = _migration_files(migrations_dir)

    with transaction() as conn:
        with conn.cursor() as cur:
            _ensure_migrations_table(cur)
            applied = _fetch_applied_migrations(cur)

            if migration_paths:
                for migration_path in migration_paths:
                    version = migration_path.name
                    ddl = migration_path.read_text(encoding="utf-8")
                    _apply_migration(cur, version=version, ddl=ddl, applied=applied)
                return

            # Legacy fallback for environments that still ship only sql/schema.sql.
            legacy_version = "legacy_schema.sql"
            ddl = schema_path.read_text(encoding="utf-8")
            _apply_migration(cur, version=legacy_version, ddl=ddl, applied=applied)


def close_pool():
    global _pool
    if _pool is not None:
        try:
            _pool.close(timeout=1.0)
        except TypeError:
            _pool.close()
        except Exception:
            logger.exception("Failed to close database pool cleanly")
        _pool = None


def ping_db() -> bool:
    try:
        with psycopg.connect(_database_url(), **_connection_kwargs()) as conn:
            with conn.cursor() as cur:
                cur.execute("SELECT 1 AS ok")
                row = cur.fetchone()
    except Exception:
        return False
    return bool(row and int(row.get("ok", 0)) == 1)


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


def _outbox_topic(event_type: str) -> str:
    if event_type == "impression":
        return os.getenv("KAFKA_TOPIC_IMPRESSIONS", "recsys.impressions.v1")
    if event_type == "watch":
        return os.getenv("KAFKA_TOPIC_WATCHES", "recsys.watches.v1")
    raise ValueError(f"Unsupported outbox event_type: {event_type}")


def _event_payload(event: ImpressionEvent | WatchEvent) -> dict:
    return event.model_dump(mode="json", exclude_none=True)


def _enqueue_outbox_event(
    cur,
    *,
    event_id: str,
    event_type: str,
    kafka_key: str,
    payload: dict,
) -> bool:
    cur.execute(
        _INSERT_OUTBOX_SQL,
        (
            event_id,
            event_type,
            _outbox_topic(event_type),
            kafka_key,
            Jsonb(payload),
            OUTBOX_MAX_ATTEMPTS,
        ),
    )
    return cur.fetchone() is not None


def compute_outbox_retry_delay_sec(
    attempt_count: int,
    base_retry_sec: int = OUTBOX_BASE_RETRY_SEC,
    max_retry_sec: int = OUTBOX_MAX_RETRY_SEC,
) -> int:
    base_retry_sec = max(int(base_retry_sec), 1)
    max_retry_sec = max(int(max_retry_sec), base_retry_sec)
    exponent = max(int(attempt_count) - 1, 0)
    delay = base_retry_sec * (2**exponent)
    return int(min(delay, max_retry_sec))


def persist_impression_event(event: ImpressionEvent) -> int:
    context_jsonb = _context_to_jsonb(event.context)
    with transaction() as conn:
        with conn.cursor() as cur:
            inserted_rows = event_log_repository.insert_impression_event(cur, event, context_jsonb)
            if inserted_rows > 0:
                _enqueue_outbox_event(
                    cur,
                    event_id=event.event_id,
                    event_type="impression",
                    kafka_key=event.user_id,
                    payload=_event_payload(event),
                )
            return inserted_rows


def persist_watch_event(event: WatchEvent) -> bool:
    context_jsonb = _context_to_jsonb(event.context)
    with transaction() as conn:
        with conn.cursor() as cur:
            inserted_watch = event_log_repository.insert_watch_event(cur, event, context_jsonb)
            if inserted_watch:
                _enqueue_outbox_event(
                    cur,
                    event_id=event.event_id,
                    event_type="watch",
                    kafka_key=event.user_id,
                    payload=_event_payload(event),
                )
            return inserted_watch


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

            _enqueue_outbox_event(
                cur,
                event_id=event.event_id,
                event_type="watch",
                kafka_key=event.user_id,
                payload=_event_payload(event),
            )

            feature_applied = feature_repository.apply_feedback_event(
                cur,
                event_id=stream_event_id,
                user_id=event.user_id,
                item_id=event.item_id,
                event_type=event_type,
                ts_seconds=ts_seconds,
                weight=weight,
                history_size=history_size,
                require_inserted_event=True,
                skip_same_item_transition=True,
            )

            return {
                "inserted_watch": True,
                "feature_updated": bool(feature_applied),
                "feature_event_type": event_type,
                "feature_weight": weight,
            }


def process_watch_event_from_stream(
    event: WatchEvent,
    *,
    source_topic: str,
    source_partition: int,
    source_offset: int,
    history_size: int = STREAM_FEATURE_HISTORY_SIZE,
) -> dict:
    history_size = max(int(history_size), 1)
    event_type, weight = _watch_signal_to_event(event)
    ts_seconds = normalize_unix_ts_seconds(event.ts_ms)
    stream_event_id = f"watch-{event.event_id}"

    with transaction() as conn:
        with conn.cursor() as cur:
            cur.execute(
                _INSERT_CONSUMED_FEATURE_EVENT_SQL,
                (
                    stream_event_id,
                    source_topic,
                    int(source_partition),
                    int(source_offset),
                ),
            )
            consumed_marker = cur.fetchone() is not None
            if not consumed_marker:
                return {
                    "status": "duplicate",
                    "stream_event_id": stream_event_id,
                    "feature_event_type": event_type,
                    "feature_weight": weight,
                }

            feature_applied = feature_repository.apply_feedback_event(
                cur,
                event_id=stream_event_id,
                user_id=event.user_id,
                item_id=event.item_id,
                event_type=event_type,
                ts_seconds=ts_seconds,
                weight=weight,
                history_size=history_size,
                require_inserted_event=True,
                skip_same_item_transition=True,
            )

            return {
                "status": "processed" if feature_applied else "duplicate",
                "stream_event_id": stream_event_id,
                "feature_event_type": event_type,
                "feature_weight": weight,
            }


def claim_outbox_events(limit: int = 100, lease_sec: int = 30) -> list[dict]:
    limit = max(int(limit), 1)
    lease_sec = max(int(lease_sec), 1)
    with transaction() as conn:
        with conn.cursor() as cur:
            cur.execute(_CLAIM_OUTBOX_EVENTS_SQL, (limit, lease_sec))
            rows = cur.fetchall()

    claimed: list[dict] = []
    for row in rows:
        payload = row.get("payload")
        if isinstance(payload, str):
            payload = json.loads(payload)
        row_dict = dict(row)
        row_dict["payload"] = payload
        claimed.append(row_dict)
    return claimed


def mark_outbox_published(outbox_id: int) -> None:
    with transaction() as conn:
        with conn.cursor() as cur:
            cur.execute(
                """
                UPDATE event_outbox
                SET
                    status = 'published',
                    published_at = now(),
                    lease_expires_at = NULL,
                    updated_at = now()
                WHERE id = %s
                """,
                (int(outbox_id),),
            )


def mark_outbox_failed(
    outbox_id: int,
    *,
    error_text: str,
    attempt_count: int,
    max_attempts: int,
    base_retry_sec: int = OUTBOX_BASE_RETRY_SEC,
    max_retry_sec: int = OUTBOX_MAX_RETRY_SEC,
) -> dict:
    outbox_id = int(outbox_id)
    attempt_count = max(int(attempt_count), 1)
    max_attempts = max(int(max_attempts), 1)
    is_dead = attempt_count >= max_attempts
    next_retry_sec = 0 if is_dead else compute_outbox_retry_delay_sec(
        attempt_count,
        base_retry_sec=base_retry_sec,
        max_retry_sec=max_retry_sec,
    )

    with transaction() as conn:
        with conn.cursor() as cur:
            cur.execute(
                """
                UPDATE event_outbox
                SET
                    status = CASE WHEN %s THEN 'dead' ELSE 'pending' END,
                    lease_expires_at = NULL,
                    next_attempt_at = CASE
                        WHEN %s THEN now()
                        ELSE now() + make_interval(secs => %s)
                    END,
                    last_error = %s,
                    updated_at = now()
                WHERE id = %s
                """,
                (is_dead, is_dead, int(next_retry_sec), error_text[:4000], outbox_id),
            )

    return {
        "status": "dead" if is_dead else "pending",
        "next_retry_sec": int(next_retry_sec),
        "attempt_count": attempt_count,
        "max_attempts": max_attempts,
    }


def get_outbox_snapshot() -> dict:
    with get_conn() as conn:
        with conn.cursor() as cur:
            cur.execute(
                """
                SELECT
                    COUNT(*) FILTER (WHERE status = 'pending' AND published_at IS NULL) AS pending,
                    COUNT(*) FILTER (WHERE status = 'publishing' AND published_at IS NULL) AS publishing,
                    COUNT(*) FILTER (WHERE status = 'published') AS published,
                    COUNT(*) FILTER (WHERE status = 'dead') AS dead
                FROM event_outbox
                """
            )
            counters = cur.fetchone() or {}
            cur.execute(
                """
                SELECT CAST(EXTRACT(EPOCH FROM (now() - MIN(created_at))) * 1000 AS BIGINT) AS oldest_lag_ms
                FROM event_outbox
                WHERE published_at IS NULL
                  AND status IN ('pending', 'publishing')
                """
            )
            lag_row = cur.fetchone() or {}

    return {
        "pending": int(counters.get("pending") or 0),
        "publishing": int(counters.get("publishing") or 0),
        "published": int(counters.get("published") or 0),
        "dead": int(counters.get("dead") or 0),
        "oldest_lag_ms": int(lag_row.get("oldest_lag_ms") or 0),
    }


def requeue_outbox_events(topic: Optional[str] = None, limit: int = 1000) -> int:
    limit = max(int(limit), 1)
    with transaction() as conn:
        with conn.cursor() as cur:
            if topic:
                cur.execute(
                    """
                    WITH picked AS (
                        SELECT id
                        FROM event_outbox
                        WHERE kafka_topic = %s
                          AND status IN ('published', 'dead')
                        ORDER BY id DESC
                        LIMIT %s
                    )
                    UPDATE event_outbox AS o
                    SET
                        status = 'pending',
                        published_at = NULL,
                        next_attempt_at = now(),
                        lease_expires_at = NULL,
                        attempt_count = 0,
                        last_error = NULL,
                        updated_at = now()
                    FROM picked
                    WHERE o.id = picked.id
                    RETURNING 1
                    """,
                    (topic, limit),
                )
            else:
                cur.execute(
                    """
                    WITH picked AS (
                        SELECT id
                        FROM event_outbox
                        WHERE status IN ('published', 'dead')
                        ORDER BY id DESC
                        LIMIT %s
                    )
                    UPDATE event_outbox AS o
                    SET
                        status = 'pending',
                        published_at = NULL,
                        next_attempt_at = now(),
                        lease_expires_at = NULL,
                        attempt_count = 0,
                        last_error = NULL,
                        updated_at = now()
                    FROM picked
                    WHERE o.id = picked.id
                    RETURNING 1
                    """,
                    (limit,),
                )
            rows = cur.fetchall()
    return len(rows)


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

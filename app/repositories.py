from typing import Any, Iterable, Optional


class FeatureRepository:
    _INSERT_EVENT_SQL = """
        INSERT INTO events (event_id, user_id, item_id, event_type, ts)
        VALUES (%s, %s, %s, %s, COALESCE(to_timestamp(%s), now()))
        ON CONFLICT (event_id) DO NOTHING
    """
    _INSERT_EVENT_RETURNING_SQL = _INSERT_EVENT_SQL + "\nRETURNING id"

    _LOCK_USER_SQL = "SELECT pg_advisory_xact_lock(hashtext(%s))"

    _UPSERT_ITEM_POPULARITY_SQL = """
        INSERT INTO item_popularity (item_id, score)
        VALUES (%s, %s)
        ON CONFLICT (item_id)
        DO UPDATE SET
            score = item_popularity.score + EXCLUDED.score,
            updated_at = now()
    """

    _SELECT_LAST_HISTORY_ITEM_SQL = """
        SELECT item_id
        FROM user_history
        WHERE user_id = %s
        ORDER BY pos DESC
        LIMIT 1
    """

    _UPSERT_CO_VISITATION_SQL = """
        INSERT INTO co_visitation (prev_item_id, next_item_id, cnt)
        VALUES (%s, %s, 1)
        ON CONFLICT (prev_item_id, next_item_id)
        DO UPDATE SET cnt = co_visitation.cnt + 1
    """

    _INSERT_USER_HISTORY_SQL = """
        WITH next_pos AS (
            SELECT COALESCE(MAX(pos), 0) + 1 AS pos
            FROM user_history
            WHERE user_id = %s
        )
        INSERT INTO user_history (user_id, pos, item_id, ts)
        SELECT %s, pos, %s, COALESCE(to_timestamp(%s), now())
        FROM next_pos
    """

    _TRIM_USER_HISTORY_SQL = """
        DELETE FROM user_history
        WHERE user_id = %s
          AND pos <= (
            SELECT COALESCE(MAX(pos), 0) - %s
            FROM user_history
            WHERE user_id = %s
          )
    """

    _SELECT_USER_HISTORY_SQL = """
        SELECT item_id
        FROM user_history
        WHERE user_id = %s
        ORDER BY pos ASC
    """

    _SELECT_RELATED_ITEMS_SQL = """
        SELECT next_item_id, cnt
        FROM co_visitation
        WHERE prev_item_id = %s
        ORDER BY cnt DESC
    """

    _SELECT_RELATED_ITEMS_WITH_LIMIT_SQL = _SELECT_RELATED_ITEMS_SQL + "\nLIMIT %s"

    _SELECT_RELATED_ITEMS_FOR_TARGETS_SQL = """
        SELECT next_item_id, cnt
        FROM co_visitation
        WHERE prev_item_id = %s
          AND next_item_id = ANY(%s)
    """

    _SELECT_RELATED_ITEMS_FOR_SOURCES_SQL = """
        WITH ranked AS (
            SELECT
                prev_item_id,
                next_item_id,
                cnt,
                ROW_NUMBER() OVER (PARTITION BY prev_item_id ORDER BY cnt DESC) AS rn
            FROM co_visitation
            WHERE prev_item_id = ANY(%s)
        )
        SELECT prev_item_id, next_item_id, cnt
        FROM ranked
        WHERE rn <= %s
    """

    _SELECT_POPULARITY_SCORES_SQL = "SELECT item_id, score FROM item_popularity ORDER BY score DESC"
    _SELECT_POPULARITY_SCORES_WITH_LIMIT_SQL = _SELECT_POPULARITY_SCORES_SQL + "\nLIMIT %s"

    _SELECT_POPULARITY_SCORES_FOR_ITEMS_SQL = """
        SELECT item_id, score
        FROM item_popularity
        WHERE item_id = ANY(%s)
    """

    def insert_event(
        self,
        cur,
        *,
        event_id: str,
        user_id: str,
        item_id: str,
        event_type: str,
        ts_seconds: Optional[float],
        require_inserted: bool,
    ) -> bool:
        sql = self._INSERT_EVENT_RETURNING_SQL if require_inserted else self._INSERT_EVENT_SQL
        cur.execute(sql, (event_id, user_id, item_id, event_type, ts_seconds))
        if not require_inserted:
            return True
        return cur.fetchone() is not None

    def apply_feedback_event(
        self,
        cur,
        *,
        event_id: str,
        user_id: str,
        item_id: str,
        event_type: str,
        ts_seconds: Optional[float],
        weight: int,
        history_size: int,
        require_inserted_event: bool,
        skip_same_item_transition: bool,
    ) -> bool:
        inserted_event = self.insert_event(
            cur,
            event_id=event_id,
            user_id=user_id,
            item_id=item_id,
            event_type=event_type,
            ts_seconds=ts_seconds,
            require_inserted=require_inserted_event,
        )
        if not inserted_event:
            return False

        cur.execute(self._LOCK_USER_SQL, (user_id,))
        cur.execute(self._UPSERT_ITEM_POPULARITY_SQL, (item_id, weight))

        cur.execute(self._SELECT_LAST_HISTORY_ITEM_SQL, (user_id,))
        prev = cur.fetchone()
        prev_item_id = prev["item_id"] if prev is not None else None
        if prev_item_id and (not skip_same_item_transition or prev_item_id != item_id):
            cur.execute(self._UPSERT_CO_VISITATION_SQL, (prev_item_id, item_id))

        cur.execute(self._INSERT_USER_HISTORY_SQL, (user_id, user_id, item_id, ts_seconds))
        cur.execute(self._TRIM_USER_HISTORY_SQL, (user_id, max(int(history_size), 1), user_id))
        return True

    def get_user_history(self, cur, user_id: str) -> list[str]:
        cur.execute(self._SELECT_USER_HISTORY_SQL, (user_id,))
        rows = cur.fetchall()
        return [row["item_id"] for row in rows]

    def get_related_items(self, cur, item_id: str, limit: Optional[int]) -> dict:
        if limit is None:
            cur.execute(self._SELECT_RELATED_ITEMS_SQL, (item_id,))
        else:
            cur.execute(self._SELECT_RELATED_ITEMS_WITH_LIMIT_SQL, (item_id, limit))
        rows = cur.fetchall()
        return {row["next_item_id"]: row["cnt"] for row in rows}

    def get_related_items_for_targets(self, cur, item_id: str, target_item_ids: Iterable[str]) -> dict:
        target_ids = list(target_item_ids)
        if not target_ids:
            return {}
        cur.execute(self._SELECT_RELATED_ITEMS_FOR_TARGETS_SQL, (item_id, target_ids))
        rows = cur.fetchall()
        return {row["next_item_id"]: row["cnt"] for row in rows}

    def get_related_items_for_sources(
        self,
        cur,
        source_item_ids: Iterable[str],
        limit_per_source: int,
    ) -> dict:
        source_ids = list(dict.fromkeys(source_item_ids))
        if not source_ids:
            return {}

        cur.execute(
            self._SELECT_RELATED_ITEMS_FOR_SOURCES_SQL,
            (source_ids, max(int(limit_per_source), 1)),
        )
        rows = cur.fetchall()
        result = {source_id: {} for source_id in source_ids}
        for row in rows:
            result.setdefault(row["prev_item_id"], {})[row["next_item_id"]] = row["cnt"]
        return result

    def get_popularity_scores(self, cur, limit: Optional[int]) -> dict:
        if limit is None:
            cur.execute(self._SELECT_POPULARITY_SCORES_SQL)
        else:
            cur.execute(self._SELECT_POPULARITY_SCORES_WITH_LIMIT_SQL, (limit,))
        rows = cur.fetchall()
        return {row["item_id"]: row["score"] for row in rows}

    def get_popularity_scores_for_items(self, cur, item_ids: Iterable[str]) -> dict:
        ids = list(item_ids)
        if not ids:
            return {}
        cur.execute(self._SELECT_POPULARITY_SCORES_FOR_ITEMS_SQL, (ids,))
        rows = cur.fetchall()
        return {row["item_id"]: row["score"] for row in rows}


class EventLogRepository:
    _INSERT_IMPRESSION_SQL = """
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
    """

    _INSERT_WATCH_SQL = """
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
    """

    _BASELINE_KPI_SQL = """
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
    """

    def insert_impression_event(self, cur, event, context_jsonb: Any) -> int:
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
        for row in rows:
            cur.execute(self._INSERT_IMPRESSION_SQL, row)
            if cur.fetchone() is not None:
                inserted += 1
        return inserted

    def insert_watch_event(self, cur, event, context_jsonb: Any) -> bool:
        cur.execute(
            self._INSERT_WATCH_SQL,
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

    def fetch_baseline_kpi_row(self, cur, days: int) -> dict:
        cur.execute(self._BASELINE_KPI_SQL, (days,))
        return cur.fetchone() or {}

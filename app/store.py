from typing import Optional

if __package__:
    from .db import get_conn, transaction
else:  # pragma: no cover - fallback for direct script execution
    from db import get_conn, transaction


WEIGHTS = {
    "view": 1,
    "click": 3,
    "purchase": 10,
}


def _normalize_unix_ts_seconds(ts: Optional[int]) -> Optional[float]:
    if ts is None:
        return None
    # Accept both seconds and milliseconds Unix timestamps.
    return ts / 1000.0 if ts >= 10**12 else float(ts)


class FeatureStore:
    def __init__(self, history_size: int = 20):
        self.history_size = history_size

    def add_event(self, user_id: str, item_id: str, event_type: str, ts: Optional[int] = None):
        weight = WEIGHTS[event_type]
        ts_seconds = _normalize_unix_ts_seconds(ts)

        with transaction() as conn:
            with conn.cursor() as cur:
                # Serialize writes per user to keep history position monotonic.
                cur.execute("SELECT pg_advisory_xact_lock(hashtext(%s))", (user_id,))

                cur.execute(
                    """
                    INSERT INTO events (user_id, item_id, event_type, ts)
                    VALUES (%s, %s, %s, COALESCE(to_timestamp(%s), now()))
                    """,
                    (user_id, item_id, event_type, ts_seconds),
                )

                cur.execute(
                    """
                    INSERT INTO item_popularity (item_id, score)
                    VALUES (%s, %s)
                    ON CONFLICT (item_id)
                    DO UPDATE SET
                        score = item_popularity.score + EXCLUDED.score,
                        updated_at = now()
                    """,
                    (item_id, weight),
                )

                cur.execute(
                    """
                    SELECT item_id
                    FROM user_history
                    WHERE user_id = %s
                    ORDER BY pos DESC
                    LIMIT 1
                    """,
                    (user_id,),
                )
                prev = cur.fetchone()

                if prev:
                    cur.execute(
                        """
                        INSERT INTO co_visitation (prev_item_id, next_item_id, cnt)
                        VALUES (%s, %s, 1)
                        ON CONFLICT (prev_item_id, next_item_id)
                        DO UPDATE SET cnt = co_visitation.cnt + 1
                        """,
                        (prev["item_id"], item_id),
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
                    (user_id, user_id, item_id, ts_seconds),
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
                    (user_id, self.history_size, user_id),
                )

    def get_user_history(self, user_id: str):
        with get_conn() as conn:
            with conn.cursor() as cur:
                cur.execute(
                    """
                    SELECT item_id
                    FROM user_history
                    WHERE user_id = %s
                    ORDER BY pos ASC
                    """,
                    (user_id,),
                )
                rows = cur.fetchall()
        return [row["item_id"] for row in rows]

    def get_related_items(self, item_id: str, limit: Optional[int] = None):
        with get_conn() as conn:
            with conn.cursor() as cur:
                if limit is None:
                    cur.execute(
                        """
                        SELECT next_item_id, cnt
                        FROM co_visitation
                        WHERE prev_item_id = %s
                        ORDER BY cnt DESC
                        """,
                        (item_id,),
                    )
                else:
                    cur.execute(
                        """
                        SELECT next_item_id, cnt
                        FROM co_visitation
                        WHERE prev_item_id = %s
                        ORDER BY cnt DESC
                        LIMIT %s
                        """,
                        (item_id, limit),
                    )
                rows = cur.fetchall()
        return {row["next_item_id"]: row["cnt"] for row in rows}

    def get_related_items_for_targets(self, item_id: str, target_item_ids):
        if not target_item_ids:
            return {}
        with get_conn() as conn:
            with conn.cursor() as cur:
                cur.execute(
                    """
                    SELECT next_item_id, cnt
                    FROM co_visitation
                    WHERE prev_item_id = %s
                      AND next_item_id = ANY(%s)
                    """,
                    (item_id, list(target_item_ids)),
                )
                rows = cur.fetchall()
        return {row["next_item_id"]: row["cnt"] for row in rows}

    def get_popularity_scores(self, limit: Optional[int] = None):
        with get_conn() as conn:
            with conn.cursor() as cur:
                if limit is None:
                    cur.execute("SELECT item_id, score FROM item_popularity ORDER BY score DESC")
                else:
                    cur.execute(
                        """
                        SELECT item_id, score
                        FROM item_popularity
                        ORDER BY score DESC
                        LIMIT %s
                        """,
                        (limit,),
                    )
                rows = cur.fetchall()
        return {row["item_id"]: row["score"] for row in rows}

    def get_popularity_scores_for_items(self, item_ids):
        if not item_ids:
            return {}
        with get_conn() as conn:
            with conn.cursor() as cur:
                cur.execute(
                    """
                    SELECT item_id, score
                    FROM item_popularity
                    WHERE item_id = ANY(%s)
                    """,
                    (list(item_ids),),
                )
                rows = cur.fetchall()
        return {row["item_id"]: row["score"] for row in rows}

    def retrieve_candidates(self, user_id: str, limit: int = 200):
        history = self.get_user_history(user_id)
        seen = set(history)
        scores = {}

        # Use a few recent interactions with decay to stabilize near-real-time retrieval.
        for depth, src_item in enumerate(reversed(history[-5:])):
            decay = 1.0 / (depth + 1)
            related = self.get_related_items(src_item, limit=200)
            for item_id, cnt in related.items():
                if item_id in seen:
                    continue
                scores[item_id] = scores.get(item_id, 0.0) + (cnt * decay)

        # Popularity fallback keeps recall robust for cold users / sparse history.
        popularity = self.get_popularity_scores(limit=max(limit * 5, 500))
        for item_id, score in popularity.items():
            if item_id in seen:
                continue
            scores[item_id] = scores.get(item_id, 0.0) + (0.1 * score)

        ranked = sorted(scores.items(), key=lambda pair: pair[1], reverse=True)
        return [item_id for item_id, _ in ranked[:limit]]

    def get_features_for_ranking(self, user_id: str, item_ids):
        history = self.get_user_history(user_id)
        history_set = set(history)
        last_item = history[-1] if history else None

        popularity = self.get_popularity_scores_for_items(item_ids)
        co_vis_last = (
            self.get_related_items_for_targets(last_item, item_ids)
            if last_item
            else {}
        )

        features = {}
        for item_id in item_ids:
            features[item_id] = {
                "seen_recent": 1.0 if item_id in history_set else 0.0,
                "popularity_score": float(popularity.get(item_id, 0.0)),
                "co_vis_last": float(co_vis_last.get(item_id, 0.0)),
            }
        return features

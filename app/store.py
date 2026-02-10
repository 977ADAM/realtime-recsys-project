from typing import Optional

from db import get_conn, transaction


WEIGHTS = {
    "view": 1,
    "click": 3,
    "purchase": 10,
}


class FeatureStore:
    def __init__(self, history_size: int = 20):
        self.history_size = history_size

    def add_event(self, user_id: str, item_id: str, event_type: str, ts: Optional[int] = None):
        weight = WEIGHTS[event_type]

        with transaction() as conn:
            with conn.cursor() as cur:
                # Serialize writes per user to keep history position monotonic.
                cur.execute("SELECT pg_advisory_xact_lock(hashtext(%s))", (user_id,))

                cur.execute(
                    """
                    INSERT INTO events (user_id, item_id, event_type, ts)
                    VALUES (%s, %s, %s, COALESCE(to_timestamp(%s), now()))
                    """,
                    (user_id, item_id, event_type, ts),
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
                    (user_id, user_id, item_id, ts),
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

    def get_related_items(self, item_id: str):
        with get_conn() as conn:
            with conn.cursor() as cur:
                cur.execute(
                    """
                    SELECT next_item_id, cnt
                    FROM co_visitation
                    WHERE prev_item_id = %s
                    """,
                    (item_id,),
                )
                rows = cur.fetchall()
        return {row["next_item_id"]: row["cnt"] for row in rows}

    def get_popularity_scores(self):
        with get_conn() as conn:
            with conn.cursor() as cur:
                cur.execute("SELECT item_id, score FROM item_popularity")
                rows = cur.fetchall()
        return {row["item_id"]: row["score"] for row in rows}

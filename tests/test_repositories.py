import unittest
from types import SimpleNamespace

from app.repositories import EventLogRepository, FeatureRepository


class _FakeCursor:
    def __init__(self, fetchone_results=None, fetchall_results=None):
        self._fetchone_results = list(fetchone_results or [])
        self._fetchall_results = list(fetchall_results or [])
        self.executed = []

    def execute(self, sql, params=None):
        self.executed.append((sql, params))

    def fetchone(self):
        if self._fetchone_results:
            return self._fetchone_results.pop(0)
        return None

    def fetchall(self):
        if self._fetchall_results:
            return self._fetchall_results.pop(0)
        return []


class RepositoryTests(unittest.TestCase):
    def test_feature_repo_duplicate_event_short_circuits_projection(self):
        repo = FeatureRepository()
        cur = _FakeCursor(fetchone_results=[None])

        applied = repo.apply_feedback_event(
            cur,
            event_id="event-1",
            user_id="user-1",
            item_id="item-1",
            event_type="view",
            ts_seconds=1700000000.0,
            weight=1,
            history_size=20,
            require_inserted_event=True,
            skip_same_item_transition=False,
        )

        self.assertFalse(applied)
        self.assertEqual(len(cur.executed), 1)
        self.assertIn("INSERT INTO events", cur.executed[0][0])

    def test_feature_repo_skips_self_transition_when_flag_enabled(self):
        repo = FeatureRepository()
        cur = _FakeCursor(fetchone_results=[{"item_id": "item-1"}])

        applied = repo.apply_feedback_event(
            cur,
            event_id="stream-1",
            user_id="user-1",
            item_id="item-1",
            event_type="click",
            ts_seconds=1700000000.0,
            weight=3,
            history_size=20,
            require_inserted_event=False,
            skip_same_item_transition=True,
        )

        self.assertTrue(applied)
        self.assertFalse(any("INSERT INTO co_visitation" in sql for sql, _ in cur.executed))
        self.assertTrue(any("INSERT INTO user_history" in sql for sql, _ in cur.executed))

    def test_feature_repo_related_items_uses_limit_query(self):
        repo = FeatureRepository()
        cur = _FakeCursor(fetchall_results=[[{"next_item_id": "item-2", "cnt": 7}]])

        related = repo.get_related_items(cur, item_id="item-1", limit=5)

        self.assertEqual(related, {"item-2": 7})
        self.assertIn("LIMIT %s", cur.executed[0][0])

    def test_feature_repo_related_items_for_sources_is_grouped(self):
        repo = FeatureRepository()
        cur = _FakeCursor(
            fetchall_results=[[
                {"prev_item_id": "item-1", "next_item_id": "item-2", "cnt": 7},
                {"prev_item_id": "item-1", "next_item_id": "item-3", "cnt": 3},
                {"prev_item_id": "item-9", "next_item_id": "item-5", "cnt": 11},
            ]]
        )

        related = repo.get_related_items_for_sources(
            cur,
            source_item_ids=["item-1", "item-9"],
            limit_per_source=5,
        )

        self.assertEqual(
            related,
            {
                "item-1": {"item-2": 7, "item-3": 3},
                "item-9": {"item-5": 11},
            },
        )
        self.assertIn("ROW_NUMBER()", cur.executed[0][0])

    def test_event_log_repo_counts_inserted_impression_rows(self):
        repo = EventLogRepository()
        cur = _FakeCursor(fetchone_results=[{"ok": 1}, None])
        event = SimpleNamespace(
            event_id="imp-1",
            user_id="user-1",
            session_id="session-1",
            request_id="req-1",
            ts_ms=1700000000000,
            server_received_ts_ms=1700000000500,
            items=[
                SimpleNamespace(item_id="item-1", position=0, feed_id=None, slot=None),
                SimpleNamespace(item_id="item-2", position=1, feed_id=None, slot=None),
            ],
        )

        inserted = repo.insert_impression_event(cur, event, context_jsonb={"device": "ios"})

        self.assertEqual(inserted, 1)
        self.assertEqual(len(cur.executed), 2)
        self.assertTrue(all("INSERT INTO impressions" in sql for sql, _ in cur.executed))


if __name__ == "__main__":
    unittest.main()

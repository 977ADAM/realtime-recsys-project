import unittest
from contextlib import contextmanager

from app.repositories import FeatureRepository
from app.store import FeatureStore


class _FakeCursor:
    def __init__(self, fetchone_results=None):
        self._fetchone_results = list(fetchone_results or [])
        self.executed_sql = []

    def __enter__(self):
        return self

    def __exit__(self, exc_type, exc, tb):
        return False

    def execute(self, sql, params=None):
        _ = params
        self.executed_sql.append(sql)

    def fetchone(self):
        if self._fetchone_results:
            return self._fetchone_results.pop(0)
        return None

    def fetchall(self):
        return []


class _FakeConnection:
    def __init__(self, cursor: _FakeCursor):
        self._cursor = cursor

    def cursor(self):
        return self._cursor


def _fake_ctx(conn: _FakeConnection):
    @contextmanager
    def _ctx():
        yield conn

    return _ctx


class FeatureStoreIdempotencyTests(unittest.TestCase):
    def test_duplicate_event_short_circuits_feature_updates(self):
        cursor = _FakeCursor(fetchone_results=[None])
        conn = _FakeConnection(cursor)
        store = FeatureStore(
            history_size=20,
            repository=FeatureRepository(),
            get_conn_factory=_fake_ctx(conn),
            transaction_factory=_fake_ctx(conn),
        )

        applied = store.add_event(
            user_id="user-1",
            item_id="item-1",
            event_type="view",
            ts=1700000000000,
            event_id="event-dup-1",
        )

        self.assertFalse(applied)
        self.assertEqual(len(cursor.executed_sql), 1)
        self.assertIn("INSERT INTO events", cursor.executed_sql[0])

    def test_new_event_applies_feature_updates(self):
        cursor = _FakeCursor(fetchone_results=[{"id": 1}, None])
        conn = _FakeConnection(cursor)
        store = FeatureStore(
            history_size=20,
            repository=FeatureRepository(),
            get_conn_factory=_fake_ctx(conn),
            transaction_factory=_fake_ctx(conn),
        )

        applied = store.add_event(
            user_id="user-1",
            item_id="item-1",
            event_type="click",
            ts=1700000000000,
            event_id="event-new-1",
        )

        self.assertTrue(applied)
        self.assertTrue(any("INSERT INTO item_popularity" in sql for sql in cursor.executed_sql))
        self.assertTrue(any("INSERT INTO user_history" in sql for sql in cursor.executed_sql))


if __name__ == "__main__":
    unittest.main()

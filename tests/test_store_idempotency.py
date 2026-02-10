import unittest
from contextlib import contextmanager
from unittest.mock import patch

from app.store import FeatureStore


class _FakeCursor:
    def __init__(self, duplicate: bool):
        self.duplicate = duplicate
        self.last_sql = ""
        self.executed_sql = []

    def __enter__(self):
        return self

    def __exit__(self, exc_type, exc, tb):
        return False

    def execute(self, sql, params=None):
        _ = params
        self.last_sql = sql
        self.executed_sql.append(sql)

    def fetchone(self):
        if "RETURNING id" in self.last_sql:
            return None if self.duplicate else {"id": 1}
        if "SELECT item_id" in self.last_sql:
            return None
        return None


class _FakeConnection:
    def __init__(self, cursor: _FakeCursor):
        self._cursor = cursor

    def cursor(self):
        return self._cursor


def _fake_transaction(conn: _FakeConnection):
    @contextmanager
    def _tx():
        yield conn

    return _tx


class FeatureStoreIdempotencyTests(unittest.TestCase):
    def test_duplicate_event_short_circuits_feature_updates(self):
        cursor = _FakeCursor(duplicate=True)
        conn = _FakeConnection(cursor)

        with patch("app.store.transaction", _fake_transaction(conn)):
            applied = FeatureStore(history_size=20).add_event(
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
        cursor = _FakeCursor(duplicate=False)
        conn = _FakeConnection(cursor)

        with patch("app.store.transaction", _fake_transaction(conn)):
            applied = FeatureStore(history_size=20).add_event(
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

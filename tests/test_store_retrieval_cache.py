import os
import unittest
from contextlib import contextmanager
from unittest.mock import patch

from app.online_cache import OnlineFeatureCache
from app.store import FeatureStore


class _FakeCursor:
    def __enter__(self):
        return self

    def __exit__(self, exc_type, exc, tb):
        return False


class _FakeConnection:
    def cursor(self):
        return _FakeCursor()


def _fake_ctx(conn):
    @contextmanager
    def _ctx():
        yield conn

    return _ctx


class _FakeFeatureRepository:
    def __init__(self):
        self.calls = {
            "user_history": 0,
            "related_sources": 0,
            "related_items": 0,
            "popularity": 0,
            "popularity_for_items": 0,
        }

    def get_user_history(self, cur, user_id):
        _ = cur
        _ = user_id
        self.calls["user_history"] += 1
        return ["item-1", "item-3", "item-4"]

    def get_related_items_for_sources(self, cur, source_item_ids, limit_per_source):
        _ = cur
        _ = limit_per_source
        self.calls["related_sources"] += 1
        base = {
            "item-4": {"item-2": 5, "item-8": 1},
            "item-3": {"item-2": 2, "item-6": 4},
            "item-1": {"item-6": 1, "item-7": 2},
        }
        return {source_id: dict(base.get(source_id, {})) for source_id in source_item_ids}

    def get_related_items(self, cur, item_id, limit):
        _ = cur
        _ = item_id
        _ = limit
        self.calls["related_items"] += 1
        return {"item-2": 8, "item-6": 4}

    def get_popularity_scores(self, cur, limit):
        _ = cur
        _ = limit
        self.calls["popularity"] += 1
        return {"item-9": 100, "item-2": 10, "item-6": 5}

    def get_popularity_scores_for_items(self, cur, item_ids):
        _ = cur
        self.calls["popularity_for_items"] += 1
        return {item_id: idx + 1 for idx, item_id in enumerate(item_ids)}


class FeatureStoreRetrievalCacheTests(unittest.TestCase):
    @staticmethod
    def _env_patch():
        return patch.dict(
            os.environ,
            {
                "ONLINE_CACHE_ENABLED": "true",
                "ONLINE_CACHE_BACKEND": "memory",
                "ONLINE_CACHE_HISTORY_TTL_SEC": "60",
                "ONLINE_CACHE_RELATED_TTL_SEC": "60",
                "ONLINE_CACHE_POPULARITY_TTL_SEC": "60",
            },
            clear=False,
        )

    def _build_store(self):
        conn = _FakeConnection()
        repo = _FakeFeatureRepository()
        cache = OnlineFeatureCache()
        store = FeatureStore(
            repository=repo,
            cache=cache,
            get_conn_factory=_fake_ctx(conn),
            transaction_factory=_fake_ctx(conn),
        )
        store.start_cache()
        return store, repo

    def test_retrieve_candidates_uses_cache_after_first_call(self):
        with self._env_patch():
            store, repo = self._build_store()

            first = store.retrieve_candidates(user_id="user-1", limit=3)
            second = store.retrieve_candidates(user_id="user-1", limit=3)

        self.assertEqual(first, ["item-9", "item-2", "item-6"])
        self.assertEqual(first, second)
        self.assertEqual(repo.calls["user_history"], 1)
        self.assertEqual(repo.calls["related_sources"], 1)
        self.assertEqual(repo.calls["popularity"], 1)

    def test_get_features_for_ranking_reuses_cached_related_items(self):
        with self._env_patch():
            store, repo = self._build_store()

            store.get_features_for_ranking(user_id="user-1", item_ids=["item-2", "item-6"])
            store.get_features_for_ranking(user_id="user-1", item_ids=["item-2", "item-6"])

        self.assertEqual(repo.calls["user_history"], 1)
        self.assertEqual(repo.calls["related_items"], 1)
        self.assertEqual(repo.calls["popularity_for_items"], 2)

    def test_invalidate_user_history_cache_forces_refetch(self):
        with self._env_patch():
            store, repo = self._build_store()

            store.retrieve_candidates(user_id="user-1", limit=3)
            store.invalidate_user_history_cache("user-1")
            store.retrieve_candidates(user_id="user-1", limit=3)

        self.assertEqual(repo.calls["user_history"], 2)

    def test_retrieve_candidates_and_history_returns_consistent_pair(self):
        with self._env_patch():
            store, repo = self._build_store()
            candidates, history = store.retrieve_candidates_and_history(user_id="user-1", limit=3)

        self.assertEqual(history, ["item-1", "item-3", "item-4"])
        self.assertEqual(candidates, ["item-9", "item-2", "item-6"])
        self.assertEqual(repo.calls["user_history"], 1)

    def test_get_features_uses_preloaded_history_without_extra_fetch(self):
        with self._env_patch():
            store, repo = self._build_store()
            history = ["item-1", "item-3", "item-4"]
            features = store.get_features_for_ranking(
                user_id="user-1",
                item_ids=["item-2", "item-6"],
                user_history=history,
            )

        self.assertEqual(repo.calls["user_history"], 0)
        self.assertEqual(set(features.keys()), {"item-2", "item-6"})


if __name__ == "__main__":
    unittest.main()

import os
import unittest
from unittest.mock import patch

from app.online_cache import OnlineFeatureCache


class OnlineFeatureCacheTests(unittest.TestCase):
    def test_memory_backend_set_get(self):
        with patch.dict(
            os.environ,
            {
                "ONLINE_CACHE_ENABLED": "true",
                "ONLINE_CACHE_BACKEND": "memory",
            },
            clear=False,
        ):
            cache = OnlineFeatureCache()
            self.assertTrue(cache.start())
            self.assertTrue(cache.set("k1", {"value": 7}, ttl_sec=5))
            self.assertEqual(cache.get("k1"), {"value": 7})

    def test_redis_fallback_to_memory_when_non_strict(self):
        with patch.dict(
            os.environ,
            {
                "ONLINE_CACHE_ENABLED": "true",
                "ONLINE_CACHE_BACKEND": "redis",
                "ONLINE_CACHE_STRICT_BACKEND": "false",
            },
            clear=False,
        ):
            with patch("app.online_cache.REDIS_CLIENT_AVAILABLE", False):
                cache = OnlineFeatureCache()
                started = cache.start()

        self.assertTrue(started)
        self.assertTrue(cache.is_ready)
        self.assertEqual(cache.active_backend, "memory_fallback")

    def test_redis_strict_mode_marks_cache_not_ready(self):
        with patch.dict(
            os.environ,
            {
                "ONLINE_CACHE_ENABLED": "true",
                "ONLINE_CACHE_BACKEND": "redis",
                "ONLINE_CACHE_STRICT_BACKEND": "true",
            },
            clear=False,
        ):
            with patch("app.online_cache.REDIS_CLIENT_AVAILABLE", False):
                cache = OnlineFeatureCache()
                started = cache.start()

        self.assertFalse(started)
        self.assertFalse(cache.is_ready)
        self.assertEqual(cache.active_backend, "redis_unavailable")


if __name__ == "__main__":
    unittest.main()

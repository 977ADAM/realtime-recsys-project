import json
import logging
import os
import time
from threading import RLock
from typing import Any, Optional

if __package__:
    from .runtime_utils import env_bool, positive_int_env
else:  # pragma: no cover - fallback for direct script execution
    from runtime_utils import env_bool, positive_int_env

logger = logging.getLogger(__name__)

try:
    from redis import Redis

    REDIS_CLIENT_AVAILABLE = True
except ImportError:  # pragma: no cover - runtime fallback when dependency is optional
    Redis = None
    REDIS_CLIENT_AVAILABLE = False


class OnlineFeatureCache:
    def __init__(self):
        self.enabled = env_bool("ONLINE_CACHE_ENABLED", True)
        self.backend = os.getenv("ONLINE_CACHE_BACKEND", "memory").strip().lower() or "memory"
        self.strict_backend = env_bool("ONLINE_CACHE_STRICT_BACKEND", False)
        self.redis_url = os.getenv("ONLINE_CACHE_REDIS_URL", "redis://localhost:6379/0")
        self.redis_socket_timeout_sec = positive_int_env("ONLINE_CACHE_REDIS_TIMEOUT_SEC", 1)
        self.memory_max_keys = positive_int_env("ONLINE_CACHE_MEMORY_MAX_KEYS", 100000)

        self._started = False
        self._ready = not self.enabled
        self._active_backend = "disabled" if not self.enabled else "memory"
        self._last_error: Optional[str] = None

        self._hits = 0
        self._misses = 0
        self._errors = 0

        self._redis_client: Optional[Any] = None
        self._memory: dict[str, tuple[float, Any]] = {}
        self._lock = RLock()

    @property
    def is_ready(self) -> bool:
        return self._ready

    @property
    def active_backend(self) -> str:
        return self._active_backend

    def snapshot(self) -> dict[str, Any]:
        return {
            "enabled": self.enabled,
            "configured_backend": self.backend,
            "active_backend": self._active_backend,
            "ready": self._ready,
            "strict_backend": self.strict_backend,
            "redis_client_available": REDIS_CLIENT_AVAILABLE,
            "hits": self._hits,
            "misses": self._misses,
            "errors": self._errors,
            "size": len(self._memory),
            "last_error": self._last_error,
        }

    def start(self) -> bool:
        with self._lock:
            if self._started:
                return self._ready

            self._started = True
            if not self.enabled:
                self._active_backend = "disabled"
                self._ready = True
                return True

            if self.backend == "redis":
                if not REDIS_CLIENT_AVAILABLE:
                    return self._handle_redis_unavailable("redis dependency is not installed")

                try:
                    client = Redis.from_url(
                        self.redis_url,
                        socket_connect_timeout=self.redis_socket_timeout_sec,
                        socket_timeout=self.redis_socket_timeout_sec,
                        decode_responses=True,
                    )
                    client.ping()
                    self._redis_client = client
                    self._active_backend = "redis"
                    self._ready = True
                    self._last_error = None
                    return True
                except Exception as exc:
                    return self._handle_redis_unavailable(
                        f"{exc.__class__.__name__}: {exc}",
                    )

            self._active_backend = "memory"
            self._ready = True
            self._last_error = None
            return True

    def close(self) -> None:
        with self._lock:
            client = self._redis_client
            self._redis_client = None
            self._started = False
            self._ready = not self.enabled
            self._active_backend = "disabled" if not self.enabled else "memory"

        if client is None:
            return

        try:
            client.close()
        except Exception:
            logger.exception("Failed to close online cache redis client cleanly")

    def get(self, key: str) -> Optional[Any]:
        if not self.enabled:
            return None

        if not self._ensure_started():
            self._misses += 1
            return None

        if self._active_backend == "redis":
            value = self._get_from_redis(key)
        else:
            value = self._get_from_memory(key)

        if value is None:
            self._misses += 1
        else:
            self._hits += 1
        return value

    def set(self, key: str, value: Any, ttl_sec: int) -> bool:
        if not self.enabled:
            return False

        if not self._ensure_started():
            return False

        ttl_sec = max(int(ttl_sec), 1)

        if self._active_backend == "redis":
            return self._set_to_redis(key, value, ttl_sec)

        self._set_to_memory(key, value, ttl_sec)
        return True

    def delete(self, key: str) -> None:
        if not self.enabled:
            return

        if not self._ensure_started():
            return

        if self._active_backend == "redis":
            client = self._redis_client
            if client is None:
                return
            try:
                client.delete(key)
            except Exception:
                self._errors += 1
                logger.exception("Failed to delete redis key %s", key)
            return

        with self._lock:
            self._memory.pop(key, None)

    def delete_prefix(self, prefix: str) -> None:
        if not self.enabled:
            return

        if not self._ensure_started():
            return

        if self._active_backend == "redis":
            client = self._redis_client
            if client is None:
                return
            try:
                keys = list(client.scan_iter(match=f"{prefix}*", count=500))
                if keys:
                    client.delete(*keys)
            except Exception:
                self._errors += 1
                logger.exception("Failed to delete redis keys by prefix %s", prefix)
            return

        with self._lock:
            keys_to_delete = [key for key in self._memory.keys() if key.startswith(prefix)]
            for key in keys_to_delete:
                self._memory.pop(key, None)

    def _ensure_started(self) -> bool:
        with self._lock:
            if self._started:
                return self._ready
        return self.start()

    def _handle_redis_unavailable(self, error_text: str) -> bool:
        self._last_error = error_text
        self._errors += 1

        if self.strict_backend:
            self._active_backend = "redis_unavailable"
            self._ready = False
            logger.warning("Online cache strict redis backend is unavailable: %s", error_text)
            return False

        self._active_backend = "memory_fallback"
        self._ready = True
        logger.warning("Online cache falls back to memory backend: %s", error_text)
        return True

    def _get_from_redis(self, key: str) -> Optional[Any]:
        client = self._redis_client
        if client is None:
            return None

        try:
            raw = client.get(key)
        except Exception:
            self._errors += 1
            logger.exception("Redis get failed for key %s", key)
            return None

        if raw is None:
            return None

        try:
            return json.loads(raw)
        except Exception:
            self._errors += 1
            logger.exception("Redis value deserialization failed for key %s", key)
            return None

    def _set_to_redis(self, key: str, value: Any, ttl_sec: int) -> bool:
        client = self._redis_client
        if client is None:
            return False

        try:
            payload = json.dumps(value, ensure_ascii=True, separators=(",", ":"))
            client.setex(key, ttl_sec, payload)
            return True
        except Exception:
            self._errors += 1
            logger.exception("Redis set failed for key %s", key)
            return False

    def _get_from_memory(self, key: str) -> Optional[Any]:
        now = time.time()
        with self._lock:
            entry = self._memory.get(key)
            if entry is None:
                return None

            expires_at, value = entry
            if expires_at <= now:
                self._memory.pop(key, None)
                return None

            return value

    def _set_to_memory(self, key: str, value: Any, ttl_sec: int) -> None:
        expires_at = time.time() + ttl_sec
        with self._lock:
            self._memory[key] = (expires_at, value)
            self._evict_if_needed()

    def _evict_if_needed(self) -> None:
        if len(self._memory) <= self.memory_max_keys:
            return

        now = time.time()
        expired_keys = [key for key, (expires_at, _) in self._memory.items() if expires_at <= now]
        for key in expired_keys:
            self._memory.pop(key, None)

        while len(self._memory) > self.memory_max_keys:
            oldest_key = next(iter(self._memory))
            self._memory.pop(oldest_key, None)

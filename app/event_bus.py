import json
import logging
import os
from typing import Any, Optional

if __package__:
    from .runtime_utils import env_bool, positive_int_env
    from .schemas import ImpressionEvent, WatchEvent
else:  # pragma: no cover - fallback for direct script execution
    from runtime_utils import env_bool, positive_int_env
    from schemas import ImpressionEvent, WatchEvent

logger = logging.getLogger(__name__)

try:
    from aiokafka import AIOKafkaProducer  # type: ignore

    KAFKA_CLIENT_AVAILABLE = True
except ImportError:  # pragma: no cover - runtime fallback when dependency is optional
    AIOKafkaProducer = None
    KAFKA_CLIENT_AVAILABLE = False


def _serialize_payload(value: dict[str, Any]) -> bytes:
    return json.dumps(value, ensure_ascii=True, separators=(",", ":")).encode("utf-8")


class KafkaEventBus:
    def __init__(self):
        self.enabled = env_bool("KAFKA_DUAL_WRITE_ENABLED", False)
        self.bootstrap_servers = os.getenv("KAFKA_BOOTSTRAP_SERVERS", "localhost:9092")
        self.topic_impressions = os.getenv("KAFKA_TOPIC_IMPRESSIONS", "recsys.impressions.v1")
        self.topic_watches = os.getenv("KAFKA_TOPIC_WATCHES", "recsys.watches.v1")
        self.client_id = os.getenv("KAFKA_CLIENT_ID", "recsys-api")
        self.request_timeout_ms = positive_int_env("KAFKA_REQUEST_TIMEOUT_MS", 15000)
        self._producer: Optional[Any] = None
        self._last_error: Optional[str] = None

    @property
    def is_ready(self) -> bool:
        return self.enabled and self._producer is not None

    @property
    def mode(self) -> str:
        return "dual_write" if self.enabled else "direct_db"

    def snapshot(self) -> dict[str, Any]:
        return {
            "enabled": self.enabled,
            "ready": self.is_ready,
            "mode": self.mode,
            "bootstrap_servers": self.bootstrap_servers,
            "topic_impressions": self.topic_impressions,
            "topic_watches": self.topic_watches,
            "kafka_client_available": KAFKA_CLIENT_AVAILABLE,
            "last_error": self._last_error,
        }

    async def start(self) -> bool:
        if not self.enabled:
            return False

        if self._producer is not None:
            return True

        if not KAFKA_CLIENT_AVAILABLE:
            self._last_error = "aiokafka dependency is not installed"
            logger.warning("Kafka dual-write is enabled but aiokafka is not installed")
            return False

        try:
            self._producer = AIOKafkaProducer(
                bootstrap_servers=self.bootstrap_servers,
                client_id=self.client_id,
                request_timeout_ms=self.request_timeout_ms,
                value_serializer=_serialize_payload,
            )
            await self._producer.start()
            self._last_error = None
            logger.info(
                "Kafka event bus started (bootstrap=%s, impressions_topic=%s, watches_topic=%s)",
                self.bootstrap_servers,
                self.topic_impressions,
                self.topic_watches,
            )
            return True
        except Exception as exc:
            self._last_error = f"{exc.__class__.__name__}: {exc}"
            logger.exception("Failed to start Kafka event bus")
            self._producer = None
            return False

    async def stop(self) -> None:
        producer = self._producer
        self._producer = None
        if producer is None:
            return
        try:
            await producer.stop()
        except Exception:
            logger.exception("Failed to stop Kafka event bus producer cleanly")

    async def publish_impression(self, event: ImpressionEvent) -> bool:
        payload = event.model_dump(mode="json", exclude_none=True)
        key = event.user_id.encode("utf-8")
        return await self._publish(topic=self.topic_impressions, payload=payload, key=key)

    async def publish_watch(self, event: WatchEvent) -> bool:
        payload = event.model_dump(mode="json", exclude_none=True)
        key = event.user_id.encode("utf-8")
        return await self._publish(topic=self.topic_watches, payload=payload, key=key)

    async def _publish(self, topic: str, payload: dict[str, Any], key: bytes) -> bool:
        producer = self._producer
        if producer is None and self.enabled:
            started = await self.start()
            producer = self._producer if started else None
        if producer is None:
            return False

        try:
            await producer.send_and_wait(topic=topic, value=payload, key=key)
            return True
        except Exception as exc:
            self._last_error = f"{exc.__class__.__name__}: {exc}"
            logger.exception("Failed to publish event into Kafka topic %s", topic)
            return False

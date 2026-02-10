import json
import os
import time
from typing import Iterable, Optional, Union

import anyio
from aiokafka import AIOKafkaProducer

if __package__:
    from .schemas import ImpressionEvent, WatchEvent
else:  # pragma: no cover - fallback for direct script execution
    from schemas import ImpressionEvent, WatchEvent


def now_ms() -> int:
    return int(time.time() * 1000)


class KafkaPublisher:
    def __init__(
        self,
        bootstrap_servers: Optional[str] = None,
        topic_impression: Optional[str] = None,
        topic_watch: Optional[str] = None,
        batch_max_concurrency: Optional[int] = None,
    ):
        self.bootstrap_servers = bootstrap_servers or os.getenv("KAFKA_BOOTSTRAP", "localhost:9092")
        self.topic_impression = topic_impression or os.getenv("TOPIC_IMPRESSION", "reco_impression")
        self.topic_watch = topic_watch or os.getenv("TOPIC_WATCH", "reco_watch")
        self.batch_max_concurrency = batch_max_concurrency or self._env_int(
            "KAFKA_BATCH_MAX_CONCURRENCY",
            100,
        )
        self._producer: Optional[AIOKafkaProducer] = None

    @staticmethod
    def _env_int(name: str, default: int) -> int:
        raw = os.getenv(name)
        if raw is None:
            return default
        try:
            value = int(raw)
        except ValueError:
            return default
        return value if value > 0 else default

    @property
    def is_ready(self) -> bool:
        return self._producer is not None

    async def start(self) -> None:
        if self._producer is not None:
            return
        producer = AIOKafkaProducer(
            bootstrap_servers=self.bootstrap_servers,
            acks="all",
            linger_ms=5,
            retries=5,
            compression_type="lz4",
            request_timeout_ms=30000,
            enable_idempotence=True,
            max_in_flight_requests_per_connection=5,
        )
        await producer.start()
        self._producer = producer

    async def stop(self) -> None:
        if self._producer is None:
            return
        await self._producer.stop()
        self._producer = None

    async def publish(self, topic: str, key: str, payload: dict) -> None:
        if self._producer is None:
            raise RuntimeError("Kafka producer is not initialized")
        data = json.dumps(payload, ensure_ascii=False, separators=(",", ":")).encode("utf-8")
        await self._producer.send_and_wait(topic, key=key.encode("utf-8"), value=data)

    async def publish_impression(self, event: ImpressionEvent) -> None:
        payload = event.model_dump()
        payload["server_received_ts_ms"] = now_ms()
        await self.publish(self.topic_impression, key=event.user_id, payload=payload)

    async def publish_watch(self, event: WatchEvent) -> None:
        payload = event.model_dump()
        payload["server_received_ts_ms"] = now_ms()
        await self.publish(self.topic_watch, key=event.user_id, payload=payload)

    async def publish_batch(self, events: Iterable[Union[ImpressionEvent, WatchEvent]]) -> None:
        if self._producer is None:
            raise RuntimeError("Kafka producer is not initialized")

        limiter = anyio.Semaphore(self.batch_max_concurrency)

        async def _publish_event(event: Union[ImpressionEvent, WatchEvent]) -> None:
            async with limiter:
                if event.event_type == "impression":
                    await self.publish_impression(event)
                else:
                    await self.publish_watch(event)

        async with anyio.create_task_group() as tg:
            for event in events:
                tg.start_soon(_publish_event, event)


class KafkaConsumerWorker:
    """
    Placeholder for the P3 streaming worker.
    Interface is defined now so API/service wiring does not change later.
    """

    async def start(self) -> None:
        return

    async def stop(self) -> None:
        return

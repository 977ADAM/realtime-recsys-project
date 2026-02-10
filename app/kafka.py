import json
import os
import time
from typing import Iterable, Optional, Union

import anyio
from aiokafka import AIOKafkaProducer

from schemas import ImpressionEvent, WatchEvent


def now_ms() -> int:
    return int(time.time() * 1000)


class KafkaPublisher:
    def __init__(
        self,
        bootstrap_servers: Optional[str] = None,
        topic_impression: Optional[str] = None,
        topic_watch: Optional[str] = None,
    ):
        self.bootstrap_servers = bootstrap_servers or os.getenv("KAFKA_BOOTSTRAP", "localhost:9092")
        self.topic_impression = topic_impression or os.getenv("TOPIC_IMPRESSION", "reco_impression")
        self.topic_watch = topic_watch or os.getenv("TOPIC_WATCH", "reco_watch")
        self._producer: Optional[AIOKafkaProducer] = None

    async def start(self) -> None:
        if self._producer is not None:
            return
        self._producer = AIOKafkaProducer(
            bootstrap_servers=self.bootstrap_servers,
            acks="all",
            linger_ms=5,
            retries=5,
            compression_type="lz4",
            request_timeout_ms=30000,
            enable_idempotence=True,
            max_in_flight_requests_per_connection=5,
        )
        await self._producer.start()

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
        async with anyio.create_task_group() as tg:
            for event in events:
                if event.event_type == "impression":
                    tg.start_soon(self.publish_impression, event)
                else:
                    tg.start_soon(self.publish_watch, event)


class KafkaConsumerWorker:
    """
    Placeholder for the P3 streaming worker.
    Interface is defined now so API/service wiring does not change later.
    """

    async def start(self) -> None:
        return

    async def stop(self) -> None:
        return

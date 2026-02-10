import asyncio
import json
import logging
import os
import time
from typing import Iterable, Optional, Union

import anyio
from aiokafka import AIOKafkaConsumer, AIOKafkaProducer

if __package__:
    from .db import (
        persist_impression_event,
        persist_watch_event_and_update_features,
    )
    from .schemas import ImpressionEvent, WatchEvent
else:  # pragma: no cover - fallback for direct script execution
    from db import (
        persist_impression_event,
        persist_watch_event_and_update_features,
    )
    from schemas import ImpressionEvent, WatchEvent


logger = logging.getLogger(__name__)


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
    def __init__(
        self,
        bootstrap_servers: Optional[str] = None,
        topic_impression: Optional[str] = None,
        topic_watch: Optional[str] = None,
        topic_dlq: Optional[str] = None,
        group_id: Optional[str] = None,
        enabled: Optional[bool] = None,
        auto_offset_reset: Optional[str] = None,
        poll_timeout_ms: Optional[int] = None,
        max_poll_records: Optional[int] = None,
    ):
        self.bootstrap_servers = bootstrap_servers or os.getenv("KAFKA_BOOTSTRAP", "localhost:9092")
        self.topic_impression = topic_impression or os.getenv("TOPIC_IMPRESSION", "reco_impression")
        self.topic_watch = topic_watch or os.getenv("TOPIC_WATCH", "reco_watch")
        self.topic_dlq = topic_dlq or os.getenv("TOPIC_DLQ", "reco_events_dlq")
        self.group_id = group_id or os.getenv("KAFKA_WORKER_GROUP_ID", "reco-stream-worker-v1")
        self.enabled = enabled if enabled is not None else self._env_bool("KAFKA_WORKER_ENABLED", False)
        self.auto_offset_reset = auto_offset_reset or os.getenv("KAFKA_WORKER_AUTO_OFFSET_RESET", "latest")
        self.poll_timeout_ms = poll_timeout_ms or self._env_int("KAFKA_WORKER_POLL_TIMEOUT_MS", 1000)
        self.max_poll_records = max_poll_records or self._env_int("KAFKA_WORKER_MAX_POLL_RECORDS", 200)
        self.feature_history_size = self._env_int("STREAM_FEATURE_HISTORY_SIZE", 20)

        if self.auto_offset_reset not in {"earliest", "latest"}:
            self.auto_offset_reset = "latest"

        self._consumer: Optional[AIOKafkaConsumer] = None
        self._dlq_producer: Optional[AIOKafkaProducer] = None
        self._task: Optional[asyncio.Task] = None
        self.processed_count = 0
        self.dlq_count = 0
        self.watch_feature_updates = 0
        self.watch_duplicates = 0

    @staticmethod
    def _env_bool(name: str, default: bool) -> bool:
        raw = os.getenv(name)
        if raw is None:
            return default
        return raw.strip().lower() in {"1", "true", "yes", "on"}

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
    def is_running(self) -> bool:
        return self._task is not None and not self._task.done()

    def snapshot(self) -> dict:
        return {
            "enabled": self.enabled,
            "is_running": self.is_running,
            "group_id": self.group_id,
            "topics": {
                "impression": self.topic_impression,
                "watch": self.topic_watch,
                "dlq": self.topic_dlq,
            },
            "auto_offset_reset": self.auto_offset_reset,
            "processed_count": self.processed_count,
            "dlq_count": self.dlq_count,
            "feature_loop": {
                "watch_feature_updates": self.watch_feature_updates,
                "watch_duplicates": self.watch_duplicates,
                "history_size": self.feature_history_size,
            },
        }

    async def start(self) -> None:
        if not self.enabled:
            logger.info("KafkaConsumerWorker is disabled by config")
            return
        if self._task is not None:
            return

        consumer = AIOKafkaConsumer(
            self.topic_impression,
            self.topic_watch,
            bootstrap_servers=self.bootstrap_servers,
            group_id=self.group_id,
            enable_auto_commit=False,
            auto_offset_reset=self.auto_offset_reset,
            request_timeout_ms=30000,
        )
        dlq_producer = AIOKafkaProducer(
            bootstrap_servers=self.bootstrap_servers,
            acks="all",
            retries=5,
            linger_ms=5,
            request_timeout_ms=30000,
            enable_idempotence=True,
        )

        try:
            await consumer.start()
            await dlq_producer.start()
        except Exception:
            try:
                await consumer.stop()
            except Exception:
                logger.exception("Failed to stop consumer after startup error")
            try:
                await dlq_producer.stop()
            except Exception:
                logger.exception("Failed to stop DLQ producer after startup error")
            raise

        self._consumer = consumer
        self._dlq_producer = dlq_producer
        self._task = asyncio.create_task(self._run_loop(), name="kafka-consumer-worker")
        logger.info(
            "KafkaConsumerWorker started (group_id=%s, auto_offset_reset=%s)",
            self.group_id,
            self.auto_offset_reset,
        )

    async def stop(self) -> None:
        task = self._task
        self._task = None
        if task is not None:
            task.cancel()
            try:
                await task
            except asyncio.CancelledError:
                pass
            except Exception:
                logger.exception("KafkaConsumerWorker loop failed during shutdown")

        if self._consumer is not None:
            await self._consumer.stop()
            self._consumer = None
        if self._dlq_producer is not None:
            await self._dlq_producer.stop()
            self._dlq_producer = None

    async def _run_loop(self) -> None:
        if self._consumer is None:
            return

        while True:
            try:
                batches = await self._consumer.getmany(
                    timeout_ms=self.poll_timeout_ms,
                    max_records=self.max_poll_records,
                )
                if not batches:
                    continue

                commits = {}
                for topic_partition, messages in batches.items():
                    for message in messages:
                        await self._process_message(message)
                        self.processed_count += 1

                    if messages:
                        commits[topic_partition] = messages[-1].offset + 1

                if commits:
                    await self._consumer.commit(commits)
            except asyncio.CancelledError:
                raise
            except Exception:
                logger.exception("Kafka consumer loop iteration failed")
                await anyio.sleep(1)

    async def _process_message(self, message) -> None:
        raw_payload = self._message_to_text(message.value)
        try:
            payload = json.loads(raw_payload)
            if message.topic == self.topic_impression:
                event = ImpressionEvent.model_validate(payload)
                await anyio.to_thread.run_sync(persist_impression_event, event)
                return

            if message.topic == self.topic_watch:
                event = WatchEvent.model_validate(payload)
                result = await anyio.to_thread.run_sync(
                    persist_watch_event_and_update_features,
                    event,
                    self.feature_history_size,
                )
                if result["inserted_watch"]:
                    self.watch_feature_updates += 1
                else:
                    self.watch_duplicates += 1
                return

            raise ValueError(f"Unsupported topic {message.topic}")
        except Exception as exc:
            await self._publish_dlq(message=message, payload_text=raw_payload, error=exc)
            logger.exception(
                "Moved message to DLQ (topic=%s partition=%s offset=%s)",
                message.topic,
                message.partition,
                message.offset,
            )

    async def _publish_dlq(self, message, payload_text: str, error: Exception) -> None:
        if self._dlq_producer is None:
            raise RuntimeError("DLQ producer is not initialized")

        envelope = {
            "source_topic": message.topic,
            "source_partition": message.partition,
            "source_offset": message.offset,
            "source_timestamp_ms": message.timestamp,
            "source_key": self._message_to_text(message.key),
            "error_type": type(error).__name__,
            "error_message": str(error),
            "payload": payload_text,
            "failed_at_ms": now_ms(),
        }
        data = json.dumps(envelope, ensure_ascii=False, separators=(",", ":")).encode("utf-8")
        await self._dlq_producer.send_and_wait(
            self.topic_dlq,
            key=message.key,
            value=data,
        )
        self.dlq_count += 1

    @staticmethod
    def _message_to_text(value) -> str:
        if value is None:
            return ""
        if isinstance(value, bytes):
            return value.decode("utf-8", errors="replace")
        return str(value)

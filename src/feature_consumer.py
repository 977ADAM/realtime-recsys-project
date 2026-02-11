#!/usr/bin/env python3
import asyncio
import json
import logging
import os
import signal
import sys
from pathlib import Path

PROJECT_ROOT = Path(__file__).resolve().parents[1]
if str(PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(PROJECT_ROOT))

from app.db import (
    get_feature_consumer_dlq_snapshot,
    process_watch_event_from_stream,
    record_feature_consumer_dlq_event,
)
from app.observability import configure_logging, log_context
from app.prom_metrics import (
    observe_feature_consumer_error,
    observe_feature_consumer_dlq,
    observe_consumer_event_lag,
    observe_consumer_processed,
    observe_watch_event_to_feature_latency,
    set_feature_consumer_dlq_backlog,
    set_consumer_partition_lag,
    start_metrics_http_server,
)
from app.runtime_utils import non_negative_int_env, now_ms, positive_float_env, positive_int_env
from app.security import enforce_runtime_security
from app.schemas import WatchEvent
from app.store import FeatureStore


try:
    from aiokafka import AIOKafkaConsumer  # type: ignore
except ImportError as exc:  # pragma: no cover - runtime guard for optional dependency
    raise RuntimeError("aiokafka dependency is required for feature consumer") from exc


configure_logging(component="feature-consumer")
logger = logging.getLogger("feature-consumer")


def _deserialize_payload(raw: bytes):
    return json.loads(raw.decode("utf-8"))


class FeatureConsumer:
    def __init__(self):
        self.bootstrap_servers = os.getenv("KAFKA_BOOTSTRAP_SERVERS", "localhost:9092")
        self.watch_topic = os.getenv("KAFKA_TOPIC_WATCHES", "recsys.watches.v1")
        self.impression_topic = os.getenv("KAFKA_TOPIC_IMPRESSIONS", "recsys.impressions.v1")
        self.group_id = os.getenv("FEATURE_CONSUMER_GROUP_ID", "recsys-feature-consumer-v1")
        self.client_id = os.getenv("FEATURE_CONSUMER_CLIENT_ID", "recsys-feature-consumer")
        self.poll_timeout_ms = positive_int_env("FEATURE_CONSUMER_POLL_TIMEOUT_MS", 1000)
        self.max_poll_records = positive_int_env("FEATURE_CONSUMER_MAX_POLL_RECORDS", 500)
        self.max_retries_per_message = positive_int_env("FEATURE_CONSUMER_MAX_RETRIES_PER_MESSAGE", 5)
        self.shutdown_timeout_sec = positive_float_env("FEATURE_CONSUMER_SHUTDOWN_TIMEOUT_SEC", 20.0)
        self.startup_max_retries = non_negative_int_env("FEATURE_CONSUMER_STARTUP_MAX_RETRIES", 0)
        self.startup_backoff_sec = positive_float_env("FEATURE_CONSUMER_STARTUP_BACKOFF_SEC", 2.0)

        self._consumer: AIOKafkaConsumer | None = None
        self._store = FeatureStore()
        self._stop_event = asyncio.Event()
        self._message_retries: dict[tuple[str, int, int], int] = {}

    async def start(self) -> None:
        attempts = 0
        while True:
            if self._stop_event.is_set():
                raise RuntimeError("stop requested during feature consumer startup")
            try:
                self._consumer = AIOKafkaConsumer(
                    self.watch_topic,
                    self.impression_topic,
                    bootstrap_servers=self.bootstrap_servers,
                    group_id=self.group_id,
                    client_id=self.client_id,
                    enable_auto_commit=False,
                    auto_offset_reset="earliest",
                    value_deserializer=_deserialize_payload,
                )
                await self._consumer.start()
                break
            except Exception as exc:
                attempts += 1
                observe_feature_consumer_error("startup")
                logger.warning(
                    "Feature consumer startup failed (attempt=%s max_retries=%s error=%s)",
                    attempts,
                    self.startup_max_retries,
                    f"{exc.__class__.__name__}: {exc}",
                )
                if self.startup_max_retries > 0 and attempts >= self.startup_max_retries:
                    raise
                await self._sleep_or_stop(min(self.startup_backoff_sec * attempts, 30.0))

        self._store.start_cache()
        logger.info(
            "Feature consumer started (bootstrap=%s, group_id=%s, watch_topic=%s)",
            self.bootstrap_servers,
            self.group_id,
            self.watch_topic,
        )
        await self._publish_dlq_metrics()

    async def stop(self) -> None:
        consumer = self._consumer
        self._consumer = None
        self._store.stop_cache()
        if consumer is not None:
            try:
                await asyncio.wait_for(consumer.stop(), timeout=self.shutdown_timeout_sec)
            except asyncio.TimeoutError:
                observe_feature_consumer_error("stop_timeout")
                logger.warning("Timed out while stopping feature consumer")
            except Exception:
                observe_feature_consumer_error("stop")
                logger.exception("Failed to stop feature consumer cleanly")
        logger.info("Feature consumer stopped")

    def request_stop(self) -> None:
        self._stop_event.set()

    async def _update_kafka_lag_metrics(self) -> None:
        consumer = self._consumer
        if consumer is None:
            return
        try:
            for tp in consumer.assignment():
                highwater = consumer.highwater(tp)
                if highwater is None:
                    continue
                try:
                    position = await consumer.position(tp)
                except Exception:
                    continue
                set_consumer_partition_lag(tp.topic, tp.partition, max(highwater - position, 0))
        except Exception:
            observe_feature_consumer_error("lag_metrics")
            logger.exception("Failed to update consumer lag metrics")

    async def _publish_dlq_metrics(self) -> None:
        try:
            snapshot = await asyncio.to_thread(get_feature_consumer_dlq_snapshot)
            set_feature_consumer_dlq_backlog(snapshot)
        except Exception:
            observe_feature_consumer_error("dlq_metrics")
            logger.exception("Failed to update feature consumer DLQ metrics")

    @staticmethod
    def _decode_message_key(raw_key) -> str:
        if raw_key is None:
            return ""
        if isinstance(raw_key, (bytes, bytearray)):
            return raw_key.decode("utf-8", errors="replace")
        return str(raw_key)

    async def _store_poison_message(self, message, *, retries: int, error_text: str) -> bool:
        safe_payload = message.value if isinstance(message.value, dict) else {"raw_payload": message.value}
        try:
            await asyncio.to_thread(
                record_feature_consumer_dlq_event,
                source_topic=message.topic,
                source_partition=int(message.partition),
                source_offset=int(message.offset),
                kafka_key=self._decode_message_key(getattr(message, "key", None)),
                payload=safe_payload,
                error_text=error_text,
                retry_count=max(int(retries), 1),
            )
            observe_feature_consumer_dlq("stored")
            return True
        except Exception:
            observe_feature_consumer_error("dlq_store")
            logger.exception(
                "Failed to persist poison message to DLQ (topic=%s partition=%s offset=%s)",
                message.topic,
                message.partition,
                message.offset,
            )
            return False

    async def _handle_message(self, message) -> tuple[bool, str | None]:
        topic = message.topic
        event_key = f"{topic}:{message.partition}:{message.offset}"
        if message.timestamp is not None and int(message.timestamp) > 0:
            observe_consumer_event_lag(topic, max(now_ms() - int(message.timestamp), 0))

        payload = message.value
        if not isinstance(payload, dict):
            observe_consumer_processed(topic, "invalid_payload")
            logger.warning(
                "Skip message with invalid payload type (topic=%s partition=%s offset=%s type=%s)",
                topic,
                message.partition,
                message.offset,
                type(payload).__name__,
            )
            return True, None

        if topic != self.watch_topic:
            observe_consumer_processed(topic, "ignored")
            return True, None

        with log_context(
            request_id=event_key,
            correlation_id=event_key,
            component="feature-consumer",
        ):
            try:
                event = WatchEvent.model_validate(payload)
            except Exception as exc:
                observe_consumer_processed(topic, "invalid_event")
                logger.warning(
                    "Skip invalid watch event (partition=%s offset=%s error=%s)",
                    message.partition,
                    message.offset,
                    exc,
                )
                return True, None

            try:
                result = await asyncio.to_thread(
                    process_watch_event_from_stream,
                    event,
                    source_topic=topic,
                    source_partition=int(message.partition),
                    source_offset=int(message.offset),
                )
            except Exception as exc:
                observe_feature_consumer_error("process")
                observe_consumer_processed(topic, "error")
                logger.exception(
                    "Watch event processing failed (partition=%s offset=%s event_id=%s)",
                    message.partition,
                    message.offset,
                    event.event_id,
                )
                return False, f"{exc.__class__.__name__}: {exc}"

            status = str(result.get("status", "processed"))
            observe_consumer_processed(topic, status)
            if status == "processed":
                observe_watch_event_to_feature_latency(max(now_ms() - int(event.ts_ms), 0.0))
                self._store.invalidate_user_history_cache(event.user_id)
                self._store.invalidate_popularity_cache()
            return True, None

    async def _sleep_or_stop(self, duration_sec: float) -> None:
        if duration_sec <= 0:
            return
        try:
            await asyncio.wait_for(self._stop_event.wait(), timeout=duration_sec)
        except asyncio.TimeoutError:
            return

    async def run(self) -> int:
        await self.start()
        try:
            while not self._stop_event.is_set():
                consumer = self._consumer
                if consumer is None:
                    break

                try:
                    batches = await consumer.getmany(
                        timeout_ms=self.poll_timeout_ms,
                        max_records=self.max_poll_records,
                    )
                except Exception:
                    observe_feature_consumer_error("poll")
                    logger.exception("Feature consumer poll failed")
                    await self._sleep_or_stop(max(self.poll_timeout_ms / 1000.0, 0.05))
                    continue
                had_messages = False

                for tp, messages in batches.items():
                    if not messages:
                        continue
                    had_messages = True
                    for message in messages:
                        message_key = (tp.topic, int(message.partition), int(message.offset))
                        success, error_text = await self._handle_message(message)
                        if success:
                            self._message_retries.pop(message_key, None)
                            continue

                        retries = self._message_retries.get(message_key, 0) + 1
                        self._message_retries[message_key] = retries
                        if retries >= self.max_retries_per_message:
                            safe_error = error_text or "processing_error_without_details"
                            stored = await self._store_poison_message(
                                message,
                                retries=retries,
                                error_text=safe_error,
                            )
                            if stored:
                                observe_feature_consumer_error("poison_message")
                                observe_consumer_processed(tp.topic, "dlq")
                                logger.error(
                                    "Moved poison message to DLQ after retries "
                                    "(topic=%s partition=%s offset=%s retries=%s)",
                                    tp.topic,
                                    message.partition,
                                    message.offset,
                                    retries,
                                )
                                self._message_retries.pop(message_key, None)
                                continue

                        consumer.seek(tp, message.offset)
                        break

                if had_messages:
                    try:
                        await consumer.commit()
                    except Exception:
                        observe_feature_consumer_error("commit")
                        logger.exception("Feature consumer commit failed")
                        await self._sleep_or_stop(0.2)

                await self._update_kafka_lag_metrics()
                await self._publish_dlq_metrics()
        finally:
            await self.stop()
        return 0


def _install_signal_handlers(consumer: FeatureConsumer) -> None:
    loop = asyncio.get_running_loop()

    def _stop() -> None:
        logger.info("Feature consumer stop requested")
        consumer.request_stop()

    for sig in (signal.SIGINT, signal.SIGTERM):
        try:
            loop.add_signal_handler(sig, _stop)
        except NotImplementedError:  # pragma: no cover - windows fallback
            signal.signal(sig, lambda *_args: _stop())


async def _main_async() -> int:
    security_issue = enforce_runtime_security(component="feature-consumer")
    if security_issue:
        raise RuntimeError(security_issue)
    start_metrics_http_server()
    consumer = FeatureConsumer()
    _install_signal_handlers(consumer)
    return await consumer.run()


def main() -> int:
    return asyncio.run(_main_async())


if __name__ == "__main__":
    raise SystemExit(main())

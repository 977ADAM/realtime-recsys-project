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

from app.db import process_watch_event_from_stream
from app.observability import configure_logging, log_context
from app.prom_metrics import (
    observe_feature_consumer_error,
    observe_consumer_event_lag,
    observe_consumer_processed,
    observe_watch_event_to_feature_latency,
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
        self.shutdown_timeout_sec = positive_float_env("FEATURE_CONSUMER_SHUTDOWN_TIMEOUT_SEC", 20.0)
        self.startup_max_retries = non_negative_int_env("FEATURE_CONSUMER_STARTUP_MAX_RETRIES", 0)
        self.startup_backoff_sec = positive_float_env("FEATURE_CONSUMER_STARTUP_BACKOFF_SEC", 2.0)

        self._consumer: AIOKafkaConsumer | None = None
        self._store = FeatureStore()
        self._stop_event = asyncio.Event()

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

    async def _handle_message(self, message) -> bool:
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
            return True

        if topic != self.watch_topic:
            observe_consumer_processed(topic, "ignored")
            return True

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
                return True

            try:
                result = process_watch_event_from_stream(
                    event,
                    source_topic=topic,
                    source_partition=int(message.partition),
                    source_offset=int(message.offset),
                )
            except Exception:
                observe_feature_consumer_error("process")
                observe_consumer_processed(topic, "error")
                logger.exception(
                    "Watch event processing failed (partition=%s offset=%s event_id=%s)",
                    message.partition,
                    message.offset,
                    event.event_id,
                )
                return False

            status = str(result.get("status", "processed"))
            observe_consumer_processed(topic, status)
            if status == "processed":
                observe_watch_event_to_feature_latency(max(now_ms() - int(event.ts_ms), 0.0))
                self._store.invalidate_user_history_cache(event.user_id)
                self._store.invalidate_popularity_cache()
            return True

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
                        success = await self._handle_message(message)
                        if success:
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

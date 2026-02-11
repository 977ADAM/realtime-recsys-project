#!/usr/bin/env python3
import asyncio
import json
import logging
import os
import signal
import sys
from pathlib import Path
from typing import Any

PROJECT_ROOT = Path(__file__).resolve().parents[1]
if str(PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(PROJECT_ROOT))

from app.db import (
    OUTBOX_MAX_ATTEMPTS,
    claim_outbox_events,
    get_outbox_snapshot,
    mark_outbox_failed,
    mark_outbox_published,
)
from app.observability import configure_logging, log_context
from app.prom_metrics import (
    observe_outbox_relay_error,
    observe_outbox_relay_event_lag,
    observe_outbox_relay_failed,
    observe_outbox_relay_published,
    set_outbox_backlog_metrics,
    start_metrics_http_server,
)
from app.runtime_utils import now_ms, non_negative_int_env, positive_float_env, positive_int_env
from app.security import enforce_runtime_security


try:
    from aiokafka import AIOKafkaProducer  # type: ignore
except ImportError as exc:  # pragma: no cover - runtime guard for optional dependency
    raise RuntimeError("aiokafka dependency is required for outbox relay") from exc


configure_logging(component="outbox-relay")
logger = logging.getLogger("outbox-relay")


def _serialize_payload(payload: dict[str, Any]) -> bytes:
    return json.dumps(payload, ensure_ascii=True, separators=(",", ":")).encode("utf-8")


class OutboxRelay:
    def __init__(self):
        self.bootstrap_servers = os.getenv("KAFKA_BOOTSTRAP_SERVERS", "localhost:9092")
        self.client_id = os.getenv("KAFKA_CLIENT_ID", "recsys-outbox-relay")
        self.request_timeout_ms = positive_int_env("KAFKA_REQUEST_TIMEOUT_MS", 15000)

        self.poll_interval_ms = positive_int_env("OUTBOX_RELAY_POLL_INTERVAL_MS", 500)
        self.batch_size = positive_int_env("OUTBOX_RELAY_BATCH_SIZE", 200)
        self.lease_sec = positive_int_env("OUTBOX_RELAY_LEASE_SEC", 30)
        self.publish_timeout_sec = positive_float_env("OUTBOX_RELAY_PUBLISH_TIMEOUT_SEC", 10.0)
        self.shutdown_timeout_sec = positive_float_env("OUTBOX_RELAY_SHUTDOWN_TIMEOUT_SEC", 15.0)
        self.startup_max_retries = non_negative_int_env("OUTBOX_RELAY_STARTUP_MAX_RETRIES", 0)
        self.startup_backoff_sec = positive_float_env("OUTBOX_RELAY_STARTUP_BACKOFF_SEC", 2.0)

        self._producer: AIOKafkaProducer | None = None
        self._stop_event = asyncio.Event()

    async def start(self) -> None:
        attempts = 0
        while True:
            if self._stop_event.is_set():
                raise RuntimeError("stop requested during outbox relay startup")
            try:
                self._producer = AIOKafkaProducer(
                    bootstrap_servers=self.bootstrap_servers,
                    client_id=self.client_id,
                    request_timeout_ms=self.request_timeout_ms,
                    value_serializer=_serialize_payload,
                )
                await self._producer.start()
                logger.info(
                    "Outbox relay started (bootstrap=%s, poll_ms=%s, batch_size=%s, lease_sec=%s)",
                    self.bootstrap_servers,
                    self.poll_interval_ms,
                    self.batch_size,
                    self.lease_sec,
                )
                return
            except Exception as exc:
                attempts += 1
                observe_outbox_relay_error("startup")
                logger.warning(
                    "Outbox relay startup failed (attempt=%s max_retries=%s error=%s)",
                    attempts,
                    self.startup_max_retries,
                    f"{exc.__class__.__name__}: {exc}",
                )
                if self.startup_max_retries > 0 and attempts >= self.startup_max_retries:
                    raise
                await self._sleep_or_stop(min(self.startup_backoff_sec * attempts, 30.0))

    async def stop(self) -> None:
        producer = self._producer
        self._producer = None
        if producer is None:
            return
        try:
            await asyncio.wait_for(producer.stop(), timeout=self.shutdown_timeout_sec)
        except asyncio.TimeoutError:
            observe_outbox_relay_error("stop_timeout")
            logger.warning("Timed out while stopping outbox relay producer")
        except Exception:
            observe_outbox_relay_error("stop")
            logger.exception("Failed to stop outbox relay producer cleanly")
        logger.info("Outbox relay stopped")

    def request_stop(self) -> None:
        self._stop_event.set()

    async def _publish_backlog_metrics(self) -> None:
        try:
            snapshot = get_outbox_snapshot()
            set_outbox_backlog_metrics(snapshot)
        except Exception:
            observe_outbox_relay_error("metrics")
            logger.exception("Failed to publish outbox backlog metrics")

    async def _process_event(self, row: dict[str, Any]) -> None:
        producer = self._producer
        if producer is None:
            raise RuntimeError("Outbox relay producer is not initialized")

        topic = str(row.get("kafka_topic", ""))
        key = str(row.get("kafka_key", "")).encode("utf-8")
        payload = row.get("payload")
        if not isinstance(payload, dict):
            raise ValueError(f"Outbox payload must be a JSON object, got {type(payload).__name__}")

        created_at_ms = int(row.get("created_at_ms") or now_ms())
        observe_outbox_relay_event_lag(max(now_ms() - created_at_ms, 0.0))

        try:
            await asyncio.wait_for(
                producer.send_and_wait(topic=topic, key=key, value=payload),
                timeout=self.publish_timeout_sec,
            )
        except Exception as exc:
            observe_outbox_relay_failed(topic=topic)
            observe_outbox_relay_error("publish")
            try:
                result = mark_outbox_failed(
                    int(row["id"]),
                    error_text=f"{exc.__class__.__name__}: {exc}",
                    attempt_count=int(row.get("attempt_count") or 1),
                    max_attempts=int(row.get("max_attempts") or OUTBOX_MAX_ATTEMPTS),
                )
            except Exception:
                observe_outbox_relay_error("mark_failed")
                logger.exception("Failed to mark outbox event as failed (id=%s)", row.get("id"))
                return
            logger.warning(
                "Outbox publish failed (id=%s topic=%s attempt=%s/%s status=%s next_retry_sec=%s)",
                row.get("id"),
                topic,
                row.get("attempt_count"),
                row.get("max_attempts"),
                result.get("status"),
                result.get("next_retry_sec"),
            )
            return

        mark_outbox_published(int(row["id"]))
        observe_outbox_relay_published(topic=topic)

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
                try:
                    claimed = claim_outbox_events(
                        self.batch_size,
                        self.lease_sec,
                    )
                except Exception:
                    observe_outbox_relay_error("claim")
                    logger.exception("Failed to claim outbox events")
                    await self._sleep_or_stop(max(self.poll_interval_ms / 1000.0, 0.05))
                    continue

                if not claimed:
                    await self._publish_backlog_metrics()
                    await self._sleep_or_stop(max(self.poll_interval_ms / 1000.0, 0.05))
                    continue

                for row in claimed:
                    if self._stop_event.is_set():
                        break
                    with log_context(
                        request_id=f"outbox-{row.get('event_id', row.get('id', 'unknown'))}",
                        correlation_id=f"outbox-{row.get('event_id', row.get('id', 'unknown'))}",
                        component="outbox-relay",
                    ):
                        try:
                            await self._process_event(row)
                        except Exception:
                            observe_outbox_relay_error("process")
                            logger.exception("Unexpected outbox processing error for row id=%s", row.get("id"))

                await self._publish_backlog_metrics()
        finally:
            await self.stop()
        return 0


def _install_signal_handlers(relay: OutboxRelay) -> None:
    loop = asyncio.get_running_loop()

    def _stop() -> None:
        logger.info("Outbox relay stop requested")
        relay.request_stop()

    for sig in (signal.SIGINT, signal.SIGTERM):
        try:
            loop.add_signal_handler(sig, _stop)
        except NotImplementedError:  # pragma: no cover - windows fallback
            signal.signal(sig, lambda *_args: _stop())


async def _main_async() -> int:
    security_issue = enforce_runtime_security(component="outbox-relay")
    if security_issue:
        raise RuntimeError(security_issue)
    start_metrics_http_server()
    relay = OutboxRelay()
    _install_signal_handlers(relay)
    return await relay.run()


def main() -> int:
    return asyncio.run(_main_async())


if __name__ == "__main__":
    raise SystemExit(main())

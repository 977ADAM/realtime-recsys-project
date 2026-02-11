import asyncio
import os
import time
import unittest
from unittest.mock import AsyncMock, patch

from app.event_bus import KafkaEventBus
from app.schemas import ImpressionEvent, ImpressionItem, WatchEvent


class KafkaEventBusTests(unittest.TestCase):
    def test_mode_is_direct_db_when_disabled(self):
        with patch.dict(os.environ, {"KAFKA_DUAL_WRITE_ENABLED": "false"}):
            bus = KafkaEventBus()
            snapshot = bus.snapshot()

        self.assertEqual(bus.mode, "direct_db")
        self.assertFalse(snapshot["enabled"])
        self.assertFalse(snapshot["ready"])

    def test_start_reports_missing_client(self):
        with patch.dict(os.environ, {"KAFKA_DUAL_WRITE_ENABLED": "true"}):
            bus = KafkaEventBus()

        with patch("app.event_bus.KAFKA_CLIENT_AVAILABLE", False):
            started = asyncio.run(bus.start())

        self.assertFalse(started)
        self.assertIn("aiokafka", bus.snapshot()["last_error"])

    def test_publish_returns_false_when_producer_not_started(self):
        now_ms = int(time.time() * 1000)
        with patch.dict(os.environ, {"KAFKA_DUAL_WRITE_ENABLED": "true"}):
            bus = KafkaEventBus()

        impression = ImpressionEvent(
            user_id="user-1",
            session_id="session-1",
            request_id="request-1",
            ts_ms=now_ms,
            items=[ImpressionItem(item_id="item-1", position=0)],
        )
        watch = WatchEvent(
            user_id="user-1",
            session_id="session-1",
            request_id="request-1",
            ts_ms=now_ms,
            item_id="item-1",
            watch_time_sec=12.0,
        )

        with patch.object(bus, "start", new=AsyncMock(return_value=False)):
            published_imp = asyncio.run(bus.publish_impression(impression))
            published_watch = asyncio.run(bus.publish_watch(watch))

        self.assertFalse(published_imp)
        self.assertFalse(published_watch)


if __name__ == "__main__":
    unittest.main()

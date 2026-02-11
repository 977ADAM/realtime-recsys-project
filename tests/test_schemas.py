import time
import unittest

from pydantic import ValidationError

from app.config import CONTRACT
from app.schemas import Event, EventBatch, ImpressionEvent, ImpressionItem, WatchEvent


class SchemaValidationTests(unittest.TestCase):
    def test_event_rejects_too_future_timestamp(self):
        future_ts_ms = int(time.time() * 1000) + CONTRACT.max_event_future_ms + 5000
        with self.assertRaises(ValidationError):
            Event(
                user_id="user-1",
                item_id="item-1",
                event_type="view",
                ts=future_ts_ms,
            )

    def test_impression_rejects_duplicate_positions(self):
        now_ms = int(time.time() * 1000)
        with self.assertRaises(ValidationError):
            ImpressionEvent(
                user_id="user-1",
                session_id="session-1",
                request_id="request-1",
                ts_ms=now_ms,
                items=[
                    ImpressionItem(item_id="item-1", position=0),
                    ImpressionItem(item_id="item-2", position=0),
                ],
            )

    def test_batch_rejects_duplicate_event_id(self):
        now_ms = int(time.time() * 1000)
        duplicate_id = "duplicate1"
        watch_1 = WatchEvent(
            event_id=duplicate_id,
            user_id="user-1",
            session_id="session-1",
            request_id="request-1",
            ts_ms=now_ms,
            item_id="item-1",
            watch_time_sec=11.0,
        )
        watch_2 = WatchEvent(
            event_id=duplicate_id,
            user_id="user-1",
            session_id="session-1",
            request_id="request-1",
            ts_ms=now_ms,
            item_id="item-2",
            watch_time_sec=12.0,
        )

        with self.assertRaises(ValidationError):
            EventBatch(events=[watch_1, watch_2])


if __name__ == "__main__":
    unittest.main()

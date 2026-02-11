import pytest
from pydantic import ValidationError

from app.runtime_utils import now_ms
from app.schemas import EventBatch, ImpressionEvent, ImpressionItem, WatchEvent


def test_watch_event_seconds_timestamp_is_normalized_to_ms():
    seconds_ts = max(int(now_ms() / 1000) - 60, 1)

    event = WatchEvent(
        user_id="u1",
        session_id="s1",
        request_id="r1",
        item_id="item-1",
        ts_ms=seconds_ts,
        watch_time_sec=12.5,
    )

    assert event.ts_ms == seconds_ts * 1000


def test_impression_event_rejects_duplicate_positions():
    with pytest.raises(ValidationError):
        ImpressionEvent(
            user_id="u1",
            session_id="s1",
            request_id="r1",
            ts_ms=now_ms(),
            items=[
                ImpressionItem(item_id="item-a", position=0),
                ImpressionItem(item_id="item-b", position=0),
            ],
        )


def test_event_batch_rejects_duplicate_event_ids():
    shared_event_id = "event-id-0001"
    impression = ImpressionEvent(
        event_id=shared_event_id,
        user_id="u1",
        session_id="s1",
        request_id="r1",
        ts_ms=now_ms(),
        items=[ImpressionItem(item_id="item-a", position=0)],
    )
    watch = WatchEvent(
        event_id=shared_event_id,
        user_id="u1",
        session_id="s1",
        request_id="r1",
        item_id="item-a",
        ts_ms=now_ms(),
        watch_time_sec=33.0,
    )

    with pytest.raises(ValidationError):
        EventBatch(events=[impression, watch])

import asyncio
from dataclasses import dataclass
from types import SimpleNamespace

import pytest

pytest.importorskip("aiokafka")

from src.feature_consumer import FeatureConsumer


@dataclass(frozen=True)
class _TopicPartition:
    topic: str
    partition: int


class _FakeConsumer:
    def __init__(self, owner: FeatureConsumer):
        self.owner = owner
        self.commit_calls = 0
        self.seek_calls: list[tuple[str, int, int]] = []
        self.getmany_calls = 0

    async def getmany(self, timeout_ms: int, max_records: int):
        _ = timeout_ms
        _ = max_records
        self.getmany_calls += 1
        if self.getmany_calls == 1:
            tp = _TopicPartition(topic=self.owner.watch_topic, partition=0)
            message = SimpleNamespace(
                topic=self.owner.watch_topic,
                partition=0,
                offset=42,
                timestamp=None,
                value={},
            )
            return {tp: [message]}

        self.owner.request_stop()
        return {}

    def seek(self, tp: _TopicPartition, offset: int) -> None:
        self.seek_calls.append((tp.topic, tp.partition, int(offset)))

    async def commit(self) -> None:
        self.commit_calls += 1


def test_consumer_drops_poison_message_after_retry_limit(monkeypatch):
    monkeypatch.setenv("FEATURE_CONSUMER_MAX_RETRIES_PER_MESSAGE", "1")
    consumer = FeatureConsumer()
    fake_consumer = _FakeConsumer(owner=consumer)
    consumer._consumer = fake_consumer

    async def _fake_start() -> None:
        return None

    async def _fake_stop() -> None:
        return None

    async def _always_fail(_message) -> bool:
        return False, "RuntimeError: boom"

    async def _noop_lag_update() -> None:
        return None

    async def _store_poison(_message, *, retries: int, error_text: str) -> bool:
        _ = retries
        _ = error_text
        return True

    async def _noop_dlq_metrics() -> None:
        return None

    monkeypatch.setattr(consumer, "start", _fake_start)
    monkeypatch.setattr(consumer, "stop", _fake_stop)
    monkeypatch.setattr(consumer, "_handle_message", _always_fail)
    monkeypatch.setattr(consumer, "_store_poison_message", _store_poison)
    monkeypatch.setattr(consumer, "_update_kafka_lag_metrics", _noop_lag_update)
    monkeypatch.setattr(consumer, "_publish_dlq_metrics", _noop_dlq_metrics)

    exit_code = asyncio.run(consumer.run())

    assert exit_code == 0
    assert fake_consumer.commit_calls == 1
    assert fake_consumer.seek_calls == []
    assert consumer._message_retries == {}

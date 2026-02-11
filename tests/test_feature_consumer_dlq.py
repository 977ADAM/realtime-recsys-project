import contextlib

import pytest

from app import db as db_module


def test_list_feature_consumer_dlq_events_rejects_unknown_status():
    with pytest.raises(ValueError, match="Unsupported DLQ statuses"):
        db_module.list_feature_consumer_dlq_events(statuses=["unknown"])


def test_record_feature_consumer_dlq_event_clamps_retry_count(monkeypatch):
    captured: dict = {}

    class _FakeCursor:
        def execute(self, _sql, params):
            captured["params"] = params

        def fetchone(self):
            return {"id": 77}

    class _FakeConn:
        @contextlib.contextmanager
        def cursor(self):
            yield _FakeCursor()

    @contextlib.contextmanager
    def _fake_transaction():
        yield _FakeConn()

    monkeypatch.setattr(db_module, "transaction", _fake_transaction)

    row_id = db_module.record_feature_consumer_dlq_event(
        source_topic="recsys.watches.v1",
        source_partition=0,
        source_offset=123,
        kafka_key="user-1",
        payload={"event_id": "watch-1"},
        error_text="boom",
        retry_count=0,
    )

    assert row_id == 77
    assert captured["params"][0] == "recsys.watches.v1"
    assert captured["params"][1] == 0
    assert captured["params"][2] == 123
    assert captured["params"][3] == "user-1"
    assert captured["params"][5] == "boom"
    assert captured["params"][6] == 1


def test_normalize_json_payload_handles_string_and_non_json():
    assert db_module._normalize_json_payload('{"k":1}') == {"k": 1}
    assert db_module._normalize_json_payload("not-json") == {"raw_payload": "not-json"}

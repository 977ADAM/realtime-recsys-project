import contextlib

import pytest

from app import db as db_module
from app.runtime_utils import now_ms
from app.schemas import ImpressionEvent, ImpressionItem, WatchEvent


def _sample_batch_events() -> tuple[ImpressionEvent, WatchEvent]:
    ts_ms = now_ms()
    impression = ImpressionEvent(
        event_id="batch-imp-1",
        user_id="user-1",
        session_id="session-1",
        request_id="request-1",
        ts_ms=ts_ms,
        items=[ImpressionItem(item_id="item-1", position=0)],
    )
    watch = WatchEvent(
        event_id="batch-watch-1",
        user_id="user-1",
        session_id="session-1",
        request_id="request-1",
        item_id="item-1",
        ts_ms=ts_ms,
        watch_time_sec=42.0,
    )
    return impression, watch


def _fake_transaction(state: dict[str, int]):
    class _Conn:
        @contextlib.contextmanager
        def cursor(self):
            yield object()

    @contextlib.contextmanager
    def _transaction():
        conn = _Conn()
        try:
            yield conn
            state["commit"] += 1
        except Exception:
            state["rollback"] += 1
            raise

    return _transaction


def test_persist_event_batch_commits_once_when_all_events_are_valid(monkeypatch):
    impression, watch = _sample_batch_events()
    tx_state = {"commit": 0, "rollback": 0}

    monkeypatch.setattr(db_module, "transaction", _fake_transaction(tx_state))
    monkeypatch.setattr(db_module, "_persist_impression_event_with_cursor", lambda _cur, _event: 1)
    monkeypatch.setattr(db_module, "_persist_watch_event_with_cursor", lambda _cur, _event: True)

    inserted_events = db_module.persist_event_batch([impression, watch])

    assert inserted_events == 2
    assert tx_state["commit"] == 1
    assert tx_state["rollback"] == 0


def test_persist_event_batch_rolls_back_when_any_event_fails(monkeypatch):
    impression, watch = _sample_batch_events()
    tx_state = {"commit": 0, "rollback": 0}

    monkeypatch.setattr(db_module, "transaction", _fake_transaction(tx_state))
    monkeypatch.setattr(db_module, "_persist_impression_event_with_cursor", lambda _cur, _event: 1)

    def _raise_watch_error(_cur, _event):
        raise RuntimeError("watch insert failed")

    monkeypatch.setattr(db_module, "_persist_watch_event_with_cursor", _raise_watch_error)

    with pytest.raises(RuntimeError, match="watch insert failed"):
        db_module.persist_event_batch([impression, watch])

    assert tx_state["commit"] == 0
    assert tx_state["rollback"] == 1

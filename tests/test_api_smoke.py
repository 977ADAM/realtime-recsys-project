import importlib

import pytest
from fastapi.testclient import TestClient


class FakeStore:
    def start_cache(self) -> bool:
        return True

    def stop_cache(self) -> None:
        return None

    def cache_snapshot(self) -> dict:
        return {
            "enabled": False,
            "configured_backend": "memory",
            "active_backend": "disabled",
            "ready": True,
        }

    def cache_ready(self) -> bool:
        return True

    def retrieve_candidates_and_history(self, user_id: str, limit: int) -> tuple[list[str], list[str]]:
        _ = user_id
        _ = limit
        return ["item-a", "item-b", "item-c"], ["history-item"]

    def get_features_for_ranking(self, user_id: str, item_ids, user_history=None) -> dict:
        _ = user_id
        _ = item_ids
        _ = user_history
        return {
            "item-a": {"co_vis_last": 1.0, "popularity_score": 10.0, "seen_recent": 0.0},
            "item-b": {"co_vis_last": 5.0, "popularity_score": 1.0, "seen_recent": 0.0},
            "item-c": {"co_vis_last": 1.0, "popularity_score": 1.0, "seen_recent": 1.0},
        }

    def invalidate_user_history_cache(self, user_id: str) -> None:
        _ = user_id
        return None

    def invalidate_popularity_cache(self) -> None:
        return None


@pytest.fixture
def client(monkeypatch):
    monkeypatch.setenv("DB_INIT_ON_STARTUP", "false")
    monkeypatch.setenv("KAFKA_DUAL_WRITE_ENABLED", "false")
    monkeypatch.setenv("PROMETHEUS_METRICS_ENABLED", "false")
    monkeypatch.setenv("ONLINE_CACHE_ENABLED", "false")

    import app.main as main

    main = importlib.reload(main)
    main.store = FakeStore()

    with TestClient(main.app) as test_client:
        yield test_client


def test_contract_endpoint_smoke(client: TestClient):
    response = client.get("/contract")

    assert response.status_code == 200
    payload = response.json()
    assert "kpi_targets" in payload
    assert "sla_targets" in payload
    assert "event_contract" in payload


def test_health_endpoint_smoke(client: TestClient):
    response = client.get("/healthz")

    assert response.status_code == 200
    payload = response.json()
    assert payload["status"] == "ok"
    assert "event_backbone" in payload
    assert payload["event_backbone"]["mode"] == "transactional_outbox"
    assert "online_cache" in payload


def test_reco_endpoint_smoke(client: TestClient):
    response = client.get(
        "/reco",
        params={
            "user_id": "user-1",
            "session_id": "session-1",
            "k": 2,
            "x_autolog_impressions": "false",
        },
    )

    assert response.status_code == 200
    payload = response.json()
    assert payload["items"] == ["item-b", "item-a"]
    assert payload["model_version"] == "rules-v1"
    assert payload["strategy"] == "co_vis_popularity_hybrid"
    assert isinstance(payload["request_id"], str)
    assert len(payload["request_id"]) > 0

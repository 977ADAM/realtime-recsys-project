import logging
import os
import time
import uuid
from typing import Optional

import anyio
from fastapi import FastAPI, Header, HTTPException, Query, Request, Response

if __package__:
    from .config import contract_snapshot
    from .db import (
        close_pool,
        get_baseline_kpi_snapshot,
        init_db,
        persist_impression_event,
        persist_watch_event_and_update_features,
    )
    from .observability import RecoMetricsWindow
    from .prom_metrics import (
        observe_reco_request,
        prometheus_content_type,
        prometheus_payload,
    )
    from .ranking import rank_items_by_features
    from .recommend import recommend
    from .schemas import (
        Context,
        Event,
        EventBatch,
        ImpressionEvent,
        ImpressionItem,
        RecoResponse,
        RecommendResponse,
        WatchEvent,
    )
    from .store import FeatureStore
else:  # pragma: no cover - fallback for direct script execution
    from config import contract_snapshot
    from db import (
        close_pool,
        get_baseline_kpi_snapshot,
        init_db,
        persist_impression_event,
        persist_watch_event_and_update_features,
    )
    from observability import RecoMetricsWindow
    from prom_metrics import (
        observe_reco_request,
        prometheus_content_type,
        prometheus_payload,
    )
    from ranking import rank_items_by_features
    from recommend import recommend
    from schemas import (
        Context,
        Event,
        EventBatch,
        ImpressionEvent,
        ImpressionItem,
        RecoResponse,
        RecommendResponse,
        WatchEvent,
    )
    from store import FeatureStore


APP_NAME = "Real-time Recommender MVP"

app = FastAPI(title=APP_NAME)
logger = logging.getLogger(__name__)


def _env_bool(name: str, default: bool) -> bool:
    raw = os.getenv(name)
    if raw is None:
        return default
    return raw.strip().lower() in {"1", "true", "yes", "on"}


def now_ms() -> int:
    return int(time.time() * 1000)


DB_INIT_ON_STARTUP = _env_bool("DB_INIT_ON_STARTUP", True)

store = FeatureStore()
reco_metrics = RecoMetricsWindow()


@app.on_event("startup")
async def startup():
    if not DB_INIT_ON_STARTUP:
        logger.info("DB init on startup is disabled; set DB_INIT_ON_STARTUP=true to auto-apply schema")
        return

    try:
        await anyio.to_thread.run_sync(init_db)
    except Exception as exc:
        logger.warning(
            "Postgres is unavailable at startup; DB-backed endpoints may fail until DB is restored (%s: %s)",
            exc.__class__.__name__,
            exc,
        )


@app.on_event("shutdown")
async def shutdown():
    close_pool()


@app.middleware("http")
async def collect_reco_metrics(request: Request, call_next):
    if request.url.path != "/reco":
        return await call_next(request)

    started_at = time.perf_counter()
    status_code = 500
    try:
        response = await call_next(request)
        status_code = response.status_code
        return response
    except Exception:
        status_code = 500
        raise
    finally:
        latency_ms = (time.perf_counter() - started_at) * 1000.0
        reco_metrics.record(latency_ms=latency_ms, status_code=status_code)
        observe_reco_request(latency_ms=latency_ms, status_code=status_code)


@app.post("/event")
def add_event(event: Event):
    applied = store.add_event(
        user_id=event.user_id,
        item_id=event.item_id,
        event_type=event.event_type,
        ts=event.ts,
        event_id=event.event_id,
    )
    return {"status": "ok" if applied else "duplicate", "event_id": event.event_id}


@app.get("/contract")
def get_contract():
    return contract_snapshot()


@app.get("/metrics/reco")
def reco_metrics_snapshot():
    return reco_metrics.snapshot()


@app.get("/metrics/prometheus")
def prometheus_metrics():
    payload = prometheus_payload()
    if payload is None:
        raise HTTPException(status_code=501, detail="prometheus_client is not installed")
    return Response(content=payload, media_type=prometheus_content_type())


@app.get("/analytics/baseline")
def baseline_kpi_snapshot(days: int = Query(default=7, ge=1, le=30)):
    return {
        "kpi_targets": contract_snapshot()["kpi_targets"],
        "metrics": get_baseline_kpi_snapshot(days=days),
    }


@app.get("/recommend", response_model=RecommendResponse)
def get_recommend(user_id: str, k: int = Query(default=10, ge=1, le=200)):
    items = recommend(user_id, store, k)
    return RecommendResponse(
        user_id=user_id,
        items=items,
        reason={"type": "hybrid", "sources": ["co_visitation", "trending"]},
    )


def new_request_id() -> str:
    return uuid.uuid4().hex


def retrieve_candidates(user_id: str, k_retrieval: int = 200) -> list[str]:
    return store.retrieve_candidates(user_id=user_id, limit=k_retrieval)


def rank_candidates(user_id: str, candidates: list[str], k: int) -> list[str]:
    if not candidates:
        return []

    features = store.get_features_for_ranking(user_id=user_id, item_ids=candidates)
    return rank_items_by_features(candidates=candidates, features=features, k=k)


async def _persist_impression(evt: ImpressionEvent) -> int:
    return await anyio.to_thread.run_sync(persist_impression_event, evt)


async def _persist_watch(evt: WatchEvent) -> dict:
    return await anyio.to_thread.run_sync(persist_watch_event_and_update_features, evt)


@app.post("/log/impression")
async def log_impression(evt: ImpressionEvent):
    payload = evt.model_copy(update={"server_received_ts_ms": now_ms()})
    try:
        inserted_rows = await _persist_impression(payload)
    except Exception as exc:
        raise HTTPException(status_code=503, detail="failed to persist impression event") from exc

    return {
        "status": "ok" if inserted_rows > 0 else "duplicate",
        "inserted_rows": inserted_rows,
        "mode": "direct_db",
    }


@app.post("/log/watch")
async def log_watch(evt: WatchEvent):
    payload = evt.model_copy(update={"server_received_ts_ms": now_ms()})
    try:
        persist_result = await _persist_watch(payload)
    except Exception as exc:
        raise HTTPException(status_code=503, detail="failed to persist watch event") from exc

    return {
        "status": "ok" if persist_result.get("inserted_watch") else "duplicate",
        "feature_updated": bool(persist_result.get("feature_updated")),
        "mode": "direct_db",
    }


@app.get("/reco", response_model=RecoResponse)
async def get_reco(
    user_id: str,
    session_id: str,
    k: int = Query(default=20, ge=1, le=200),
    x_autolog_impressions: bool = True,
    user_agent: Optional[str] = Header(default=None, alias="User-Agent"),
):
    req_id = new_request_id()
    candidates = await anyio.to_thread.run_sync(
        retrieve_candidates,
        user_id,
        max(k * 20, 200),
    )
    items = await anyio.to_thread.run_sync(rank_candidates, user_id, candidates, k)

    if x_autolog_impressions and items:
        imp_evt = ImpressionEvent(
            user_id=user_id,
            session_id=session_id,
            request_id=req_id,
            ts_ms=now_ms(),
            items=[ImpressionItem(item_id=item, position=pos) for pos, item in enumerate(items)],
            context=Context(user_agent=user_agent),
        )
        payload = imp_evt.model_copy(update={"server_received_ts_ms": now_ms()})
        try:
            await _persist_impression(payload)
        except Exception:
            logger.exception(
                "Failed to auto-log impressions for request_id=%s user_id=%s",
                req_id,
                user_id,
            )
    elif x_autolog_impressions:
        logger.debug(
            "Skipping auto-log impressions for request_id=%s because recommendation list is empty",
            req_id,
        )

    return RecoResponse(
        request_id=req_id,
        items=items,
        model_version="rules-v1",
        strategy="co_vis_popularity_hybrid",
    )


@app.post("/log/batch")
async def log_batch(batch: EventBatch):
    inserted_events = 0
    try:
        for event in batch.events:
            payload = event.model_copy(update={"server_received_ts_ms": now_ms()})
            if event.event_type == "impression":
                inserted_events += int(await _persist_impression(payload) > 0)
            else:
                watch_result = await _persist_watch(payload)
                inserted_events += int(bool(watch_result.get("inserted_watch")))
    except Exception as exc:
        raise HTTPException(status_code=503, detail="failed to persist batch events") from exc

    return {
        "status": "ok",
        "count": len(batch.events),
        "inserted_events": inserted_events,
        "mode": "direct_db",
    }

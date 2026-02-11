import asyncio
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
        get_outbox_snapshot,
        init_db,
        ping_db,
        persist_impression_event,
        persist_watch_event,
    )
    from .observability import RecoMetricsWindow
    from .prom_metrics import (
        observe_reco_request,
        observe_reco_stage_latency,
        prometheus_content_type,
        prometheus_payload,
        start_metrics_http_server,
    )
    from .runtime_utils import env_bool, now_ms, positive_int_env
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
        get_outbox_snapshot,
        init_db,
        ping_db,
        persist_impression_event,
        persist_watch_event,
    )
    from observability import RecoMetricsWindow
    from prom_metrics import (
        observe_reco_request,
        observe_reco_stage_latency,
        prometheus_content_type,
        prometheus_payload,
        start_metrics_http_server,
    )
    from runtime_utils import env_bool, now_ms, positive_int_env
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

DB_INIT_ON_STARTUP = env_bool("DB_INIT_ON_STARTUP", True)
RECO_ASYNC_IMPRESSION_LOGGING = env_bool("RECO_ASYNC_IMPRESSION_LOGGING", True)
RECO_BACKGROUND_SHUTDOWN_TIMEOUT_SEC = positive_int_env(
    "RECO_BACKGROUND_SHUTDOWN_TIMEOUT_SEC",
    5,
)
WATCH_FEATURE_UPDATE_MODE = os.getenv("WATCH_FEATURE_UPDATE_MODE", "async_consumer").strip().lower() or "async_consumer"
EVENT_BACKBONE_MODE = "transactional_outbox"

store = FeatureStore()
reco_metrics = RecoMetricsWindow()
_background_tasks: set[asyncio.Task] = set()


@app.on_event("startup")
async def startup():
    start_metrics_http_server()

    if not DB_INIT_ON_STARTUP:
        logger.info("DB init on startup is disabled; set DB_INIT_ON_STARTUP=true to auto-apply schema")
    else:
        try:
            await anyio.to_thread.run_sync(init_db)
        except Exception as exc:
            logger.warning(
                "Postgres is unavailable at startup; DB-backed endpoints may fail until DB is restored (%s: %s)",
                exc.__class__.__name__,
                exc,
            )

    await anyio.to_thread.run_sync(store.start_cache)


@app.on_event("shutdown")
async def shutdown():
    await _drain_background_tasks()
    await anyio.to_thread.run_sync(store.stop_cache)
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


@app.get("/healthz")
def healthcheck():
    outbox_snapshot = _safe_outbox_snapshot() if ping_db() else None
    return {
        "status": "ok",
        "event_backbone": {"mode": EVENT_BACKBONE_MODE, "outbox": outbox_snapshot},
        "online_cache": store.cache_snapshot(),
    }


@app.get("/readyz")
async def readiness_check():
    db_ready = ping_db()
    if not store.cache_ready():
        await anyio.to_thread.run_sync(store.start_cache)
    cache_ready = store.cache_ready()
    ready = db_ready and cache_ready
    outbox_snapshot = _safe_outbox_snapshot() if db_ready else None

    payload = {
        "ready": ready,
        "checks": {
            "db": db_ready,
            "online_cache": cache_ready,
        },
        "event_backbone": {"mode": EVENT_BACKBONE_MODE, "outbox": outbox_snapshot},
        "online_cache": store.cache_snapshot(),
    }

    if not ready:
        raise HTTPException(status_code=503, detail=payload)
    return payload


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


def retrieve_candidates_and_history(user_id: str, k_retrieval: int = 200) -> tuple[list[str], list[str]]:
    return store.retrieve_candidates_and_history(user_id=user_id, limit=k_retrieval)


def rank_candidates(
    user_id: str,
    candidates: list[str],
    k: int,
    user_history: Optional[list[str]] = None,
) -> list[str]:
    if not candidates:
        return []

    features = store.get_features_for_ranking(
        user_id=user_id,
        item_ids=candidates,
        user_history=user_history,
    )
    return rank_items_by_features(candidates=candidates, features=features, k=k)


async def _persist_impression(evt: ImpressionEvent) -> int:
    return await anyio.to_thread.run_sync(persist_impression_event, evt)


async def _persist_watch(evt: WatchEvent) -> bool:
    return await anyio.to_thread.run_sync(persist_watch_event, evt)


def _with_server_received_ts(event: ImpressionEvent | WatchEvent) -> ImpressionEvent | WatchEvent:
    return event.model_copy(update={"server_received_ts_ms": now_ms()})


def _safe_outbox_snapshot():
    try:
        return get_outbox_snapshot()
    except Exception as exc:
        logger.warning("Failed to read outbox snapshot (%s: %s)", exc.__class__.__name__, exc)
        return {"error": f"{exc.__class__.__name__}: {exc}"}


def _track_background_task(task: asyncio.Task) -> None:
    _background_tasks.add(task)

    def _cleanup(done_task: asyncio.Task) -> None:
        _background_tasks.discard(done_task)
        if done_task.cancelled():
            return
        exc = done_task.exception()
        if exc is not None:
            logger.exception("Background task failed: %s", exc)

    task.add_done_callback(_cleanup)


def _schedule_background(coro) -> None:
    task = asyncio.create_task(coro)
    _track_background_task(task)


async def _drain_background_tasks() -> None:
    if not _background_tasks:
        return

    pending = list(_background_tasks)
    done, still_pending = await asyncio.wait(
        pending,
        timeout=RECO_BACKGROUND_SHUTDOWN_TIMEOUT_SEC,
    )
    _ = done
    for task in still_pending:
        task.cancel()
    if still_pending:
        logger.warning(
            "Cancelled %s outstanding background tasks during shutdown",
            len(still_pending),
        )


async def _persist_impression_to_outbox(payload: ImpressionEvent, req_id: str, user_id: str) -> bool:
    started_at = time.perf_counter()
    try:
        inserted_rows = await _persist_impression(payload)
    except Exception:
        logger.exception(
            "Failed to auto-log impressions for request_id=%s user_id=%s",
            req_id,
            user_id,
        )
        return False
    finally:
        observe_reco_stage_latency("impression_log", (time.perf_counter() - started_at) * 1000.0)

    return inserted_rows > 0


@app.post("/log/impression")
async def log_impression(evt: ImpressionEvent):
    payload = _with_server_received_ts(evt)
    try:
        inserted_rows = await _persist_impression(payload)
    except Exception as exc:
        raise HTTPException(status_code=503, detail="failed to persist impression event") from exc

    return {
        "status": "ok" if inserted_rows > 0 else "duplicate",
        "inserted_rows": inserted_rows,
        "event_backbone_mode": EVENT_BACKBONE_MODE,
        "outbox_enqueued": bool(inserted_rows > 0),
    }


@app.post("/log/watch")
async def log_watch(evt: WatchEvent):
    payload = _with_server_received_ts(evt)
    try:
        inserted_watch = await _persist_watch(payload)
    except Exception as exc:
        raise HTTPException(status_code=503, detail="failed to persist watch event") from exc

    return {
        "status": "ok" if inserted_watch else "duplicate",
        "feature_update_mode": WATCH_FEATURE_UPDATE_MODE,
        "feature_updated": False,
        "event_backbone_mode": EVENT_BACKBONE_MODE,
        "outbox_enqueued": bool(inserted_watch),
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

    retrieval_started = time.perf_counter()
    candidates, user_history = await anyio.to_thread.run_sync(
        retrieve_candidates_and_history,
        user_id,
        max(k * 20, 200),
    )
    observe_reco_stage_latency("retrieval", (time.perf_counter() - retrieval_started) * 1000.0)

    ranking_started = time.perf_counter()
    items = await anyio.to_thread.run_sync(rank_candidates, user_id, candidates, k, user_history)
    observe_reco_stage_latency("ranking", (time.perf_counter() - ranking_started) * 1000.0)

    if x_autolog_impressions and items:
        imp_evt = ImpressionEvent(
            user_id=user_id,
            session_id=session_id,
            request_id=req_id,
            ts_ms=now_ms(),
            items=[ImpressionItem(item_id=item, position=pos) for pos, item in enumerate(items)],
            context=Context(user_agent=user_agent),
        )
        payload = _with_server_received_ts(imp_evt)
        if RECO_ASYNC_IMPRESSION_LOGGING:
            _schedule_background(_persist_impression_to_outbox(payload, req_id, user_id))
            observe_reco_stage_latency("impression_schedule", 0.0)
        else:
            await _persist_impression_to_outbox(payload, req_id, user_id)
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
            payload = _with_server_received_ts(event)
            if event.event_type == "impression":
                inserted_impression = await _persist_impression(payload)
                inserted_events += int(inserted_impression > 0)
            else:
                inserted_watch = await _persist_watch(payload)
                inserted_events += int(inserted_watch)
    except Exception as exc:
        raise HTTPException(status_code=503, detail="failed to persist batch events") from exc

    return {
        "status": "ok",
        "count": len(batch.events),
        "inserted_events": inserted_events,
        "event_backbone_mode": EVENT_BACKBONE_MODE,
        "outbox_enqueued": inserted_events,
        "feature_update_mode": WATCH_FEATURE_UPDATE_MODE,
    }

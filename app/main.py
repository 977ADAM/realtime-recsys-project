import asyncio
import logging
import os
import re
import time
import uuid
from contextlib import asynccontextmanager
from typing import Optional

from fastapi import FastAPI, Header, HTTPException, Query, Request, Response

if __package__:
    from .config import contract_snapshot
    from .db import (
        close_pool,
        get_baseline_kpi_snapshot,
        get_outbox_snapshot,
        init_db,
        ping_db,
        persist_event_batch,
        persist_impression_event,
        persist_watch_event,
    )
    from .observability import RecoMetricsWindow, configure_logging, log_context
    from .prom_metrics import (
        observe_reco_request,
        observe_reco_stage_latency,
        set_api_check_status,
        set_outbox_backlog_metrics,
        prometheus_content_type,
        prometheus_payload,
        start_metrics_http_server,
    )
    from .runtime_utils import env_bool, now_ms, positive_int_env, sanitize_identifier
    from .security import enforce_runtime_security
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
        persist_event_batch,
        persist_impression_event,
        persist_watch_event,
    )
    from observability import RecoMetricsWindow, configure_logging, log_context
    from prom_metrics import (
        observe_reco_request,
        observe_reco_stage_latency,
        set_api_check_status,
        set_outbox_backlog_metrics,
        prometheus_content_type,
        prometheus_payload,
        start_metrics_http_server,
    )
    from runtime_utils import env_bool, now_ms, positive_int_env, sanitize_identifier
    from security import enforce_runtime_security
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

configure_logging(component="api")
logger = logging.getLogger(__name__)


@asynccontextmanager
async def lifespan(_app: FastAPI):
    await startup()
    try:
        yield
    finally:
        await shutdown()


app = FastAPI(title=APP_NAME, lifespan=lifespan)

DB_INIT_ON_STARTUP = env_bool("DB_INIT_ON_STARTUP", True)
RECO_ASYNC_IMPRESSION_LOGGING = env_bool("RECO_ASYNC_IMPRESSION_LOGGING", True)
RECO_BACKGROUND_SHUTDOWN_TIMEOUT_SEC = positive_int_env(
    "RECO_BACKGROUND_SHUTDOWN_TIMEOUT_SEC",
    5,
)
RECO_MAX_BACKGROUND_TASKS = positive_int_env("RECO_MAX_BACKGROUND_TASKS", 1000)
RECO_BACKGROUND_BACKPRESSURE_MODE = (
    os.getenv("RECO_BACKGROUND_BACKPRESSURE_MODE", "sync").strip().lower() or "sync"
)
if RECO_BACKGROUND_BACKPRESSURE_MODE not in {"sync", "drop"}:
    RECO_BACKGROUND_BACKPRESSURE_MODE = "sync"
WATCH_FEATURE_UPDATE_MODE = os.getenv("WATCH_FEATURE_UPDATE_MODE", "async_consumer").strip().lower() or "async_consumer"
EVENT_BACKBONE_MODE = "transactional_outbox"
_SAFE_HEADER_ID_PATTERN = re.compile(r"^[A-Za-z0-9._:-]{8,128}$")

store = FeatureStore()
reco_metrics = RecoMetricsWindow()
_background_tasks: set[asyncio.Task] = set()


def _normalized_header_id(raw_value: Optional[str]) -> Optional[str]:
    if raw_value is None:
        return None
    value = raw_value.strip()[:128]
    if not value:
        return None
    if _SAFE_HEADER_ID_PATTERN.match(value) is None:
        return None
    return value


async def startup():
    security_issue = enforce_runtime_security(component="api")
    if security_issue:
        raise RuntimeError(security_issue)

    start_metrics_http_server()

    if not DB_INIT_ON_STARTUP:
        logger.info("DB init on startup is disabled; set DB_INIT_ON_STARTUP=true to auto-apply schema")
    else:
        try:
            init_db()
        except Exception as exc:
            logger.warning(
                "Postgres is unavailable at startup; DB-backed endpoints may fail until DB is restored (%s: %s)",
                exc.__class__.__name__,
                exc,
            )

    cache_started = store.start_cache()
    if not cache_started:
        logger.warning("Online cache startup is degraded; readiness endpoint may report not ready")


async def shutdown():
    await _drain_background_tasks()
    store.stop_cache()
    close_pool()


@app.middleware("http")
async def bind_request_context(request: Request, call_next):
    incoming_request_id = _normalized_header_id(request.headers.get("x-request-id"))
    incoming_correlation_id = _normalized_header_id(request.headers.get("x-correlation-id"))
    request_id = incoming_request_id or uuid.uuid4().hex
    correlation_id = incoming_correlation_id or request_id
    request.state.request_id = request_id
    request.state.correlation_id = correlation_id

    started_at = time.perf_counter()
    with log_context(request_id=request_id, correlation_id=correlation_id, component="api"):
        try:
            response = await call_next(request)
        except Exception:
            logger.exception(
                "Request failed method=%s path=%s",
                request.method,
                request.url.path,
            )
            raise

    latency_ms = (time.perf_counter() - started_at) * 1000.0
    response.headers["X-Request-ID"] = request_id
    response.headers["X-Correlation-ID"] = correlation_id
    logger.info(
        "http_request method=%s path=%s status_code=%s latency_ms=%.3f",
        request.method,
        request.url.path,
        response.status_code,
        latency_ms,
    )
    return response


@app.middleware("http")
async def collect_reco_metrics(request: Request, call_next):
    if request.url.path != "/reco":
        return await call_next(request)

    request_id = sanitize_identifier(
        getattr(request.state, "request_id", None),
        fallback=uuid.uuid4().hex,
    )
    correlation_id = sanitize_identifier(
        getattr(request.state, "correlation_id", None),
        fallback=request_id,
    )

    started_at = time.perf_counter()
    status_code = 500
    with log_context(request_id=request_id, correlation_id=correlation_id, component="api"):
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
    db_ok = ping_db()
    cache_ok = store.cache_ready()
    set_api_check_status(scope="healthz", check="db", ok=db_ok)
    set_api_check_status(scope="healthz", check="online_cache", ok=cache_ok)
    outbox_snapshot = _safe_outbox_snapshot() if db_ok else None
    if outbox_snapshot and isinstance(outbox_snapshot, dict) and "pending" in outbox_snapshot:
        set_outbox_backlog_metrics(outbox_snapshot)
    return {
        "status": "ok",
        "checks": {
            "db": db_ok,
            "online_cache": cache_ok,
        },
        "event_backbone": {"mode": EVENT_BACKBONE_MODE, "outbox": outbox_snapshot},
        "online_cache": store.cache_snapshot(),
    }


@app.get("/readyz")
async def readiness_check():
    db_ready = await asyncio.to_thread(ping_db)
    if not store.cache_ready():
        store.start_cache()
    cache_ready = store.cache_ready()
    set_api_check_status(scope="readyz", check="db", ok=db_ready)
    set_api_check_status(scope="readyz", check="online_cache", ok=cache_ready)
    ready = db_ready and cache_ready
    outbox_snapshot = await asyncio.to_thread(_safe_outbox_snapshot) if db_ready else None
    if outbox_snapshot and isinstance(outbox_snapshot, dict) and "pending" in outbox_snapshot:
        set_outbox_backlog_metrics(outbox_snapshot)

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
    return await asyncio.to_thread(persist_impression_event, evt)


async def _persist_watch(evt: WatchEvent) -> bool:
    return await asyncio.to_thread(persist_watch_event, evt)


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


def _schedule_background(coro) -> bool:
    if len(_background_tasks) >= RECO_MAX_BACKGROUND_TASKS:
        return False
    task = asyncio.create_task(coro)
    _track_background_task(task)
    return True


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
    request: Request,
    user_id: str,
    session_id: str,
    k: int = Query(default=20, ge=1, le=200),
    x_autolog_impressions: bool = True,
    user_agent: Optional[str] = Header(default=None, alias="User-Agent"),
):
    req_id = sanitize_identifier(
        getattr(request.state, "request_id", None),
        fallback=new_request_id(),
    )
    safe_user_agent = user_agent[:512] if user_agent else None

    retrieval_started = time.perf_counter()
    candidates, user_history = await asyncio.to_thread(
        retrieve_candidates_and_history,
        user_id,
        max(k * 20, 200),
    )
    observe_reco_stage_latency("retrieval", (time.perf_counter() - retrieval_started) * 1000.0)

    ranking_started = time.perf_counter()
    items = await asyncio.to_thread(rank_candidates, user_id, candidates, k, user_history)
    observe_reco_stage_latency("ranking", (time.perf_counter() - ranking_started) * 1000.0)

    if x_autolog_impressions and items:
        imp_evt = ImpressionEvent(
            user_id=user_id,
            session_id=session_id,
            request_id=req_id,
            ts_ms=now_ms(),
            items=[ImpressionItem(item_id=item, position=pos) for pos, item in enumerate(items)],
            context=Context(user_agent=safe_user_agent),
        )
        payload = _with_server_received_ts(imp_evt)
        if RECO_ASYNC_IMPRESSION_LOGGING:
            scheduled = _schedule_background(_persist_impression_to_outbox(payload, req_id, user_id))
            if scheduled:
                observe_reco_stage_latency("impression_schedule", 0.0)
            elif RECO_BACKGROUND_BACKPRESSURE_MODE == "drop":
                observe_reco_stage_latency("impression_drop", 0.0)
                logger.warning(
                    "Dropping async impression logging due to background queue saturation request_id=%s",
                    req_id,
                )
            else:
                observe_reco_stage_latency("impression_backpressure_sync", 0.0)
                await _persist_impression_to_outbox(payload, req_id, user_id)
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
    payload_events = [_with_server_received_ts(event) for event in batch.events]
    try:
        inserted_events = await asyncio.to_thread(persist_event_batch, payload_events)
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

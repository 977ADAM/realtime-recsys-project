import uuid
import logging
import anyio
from typing import Optional
from fastapi import FastAPI, Header, HTTPException, Query

if __package__:
    from .config import contract_snapshot
    from .schemas import (
        Event,
        RecommendResponse,
        ImpressionEvent,
        ImpressionItem,
        WatchEvent,
        RecoResponse,
        Context,
        EventBatch,
    )
    from .store import FeatureStore
    from .recommend import recommend
    from .db import close_pool, init_db
    from .kafka import KafkaConsumerWorker, KafkaPublisher, now_ms
else:  # pragma: no cover - fallback for direct script execution
    from config import contract_snapshot
    from schemas import (
        Event,
        RecommendResponse,
        ImpressionEvent,
        ImpressionItem,
        WatchEvent,
        RecoResponse,
        Context,
        EventBatch,
    )
    from store import FeatureStore
    from recommend import recommend
    from db import close_pool, init_db
    from kafka import KafkaConsumerWorker, KafkaPublisher, now_ms

APP_NAME = "Real-time Recommender MVP + reco-logger"

app = FastAPI(title=APP_NAME)
logger = logging.getLogger(__name__)

store = FeatureStore()
publisher = KafkaPublisher()
worker = KafkaConsumerWorker()


@app.on_event("startup")
async def startup():
    # Initialize DB schema (sync) without blocking the event loop
    await anyio.to_thread.run_sync(init_db)
    try:
        await publisher.start()
    except Exception:
        logger.exception("Kafka is unavailable at startup; recommendation endpoints will stay online")
    try:
        await worker.start()
    except Exception:
        logger.exception("Kafka consumer worker failed at startup; API will keep serving")

@app.on_event("shutdown")
async def shutdown():
    await worker.stop()
    await publisher.stop()
    close_pool()


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


@app.get("/stream/worker")
def stream_worker_status():
    return worker.snapshot()


@app.get("/recommend", response_model=RecommendResponse)
def get_recommend(user_id: str, k: int = Query(default=10, ge=1, le=200)):
    items = recommend(user_id, store, k)
    return RecommendResponse(
        user_id=user_id,
        items=items,
        reason={"type": "hybrid", "sources": ["co_visitation", "trending"]},
    )


def new_request_id() -> str:
    # uuid4 is fine; you can switch to ULID if you prefer sortable ids
    return uuid.uuid4().hex


def retrieve_candidates(user_id: str, k_retrieval: int = 200) -> list[str]:
    return store.retrieve_candidates(user_id=user_id, limit=k_retrieval)


def rank_candidates(user_id: str, candidates: list[str], k: int) -> list[str]:
    if not candidates:
        return []

    features = store.get_features_for_ranking(user_id=user_id, item_ids=candidates)
    ranked = sorted(
        candidates,
        key=lambda item: (
            2.5 * features[item]["co_vis_last"]
            + 1.0 * features[item]["popularity_score"]
            - 5.0 * features[item]["seen_recent"]
        ),
        reverse=True,
    )
    return ranked[:k]

@app.post("/log/impression")
async def log_impression(evt: ImpressionEvent):
    if len(evt.items) == 0:
        raise HTTPException(status_code=400, detail="items must be non-empty")
    if not publisher.is_ready:
        raise HTTPException(status_code=503, detail="kafka is unavailable")
    try:
        await publisher.publish_impression(evt)
    except Exception as exc:
        raise HTTPException(status_code=503, detail="failed to publish impression event") from exc
    return {"status": "ok"}


@app.post("/log/watch")
async def log_watch(evt: WatchEvent):
    if not publisher.is_ready:
        raise HTTPException(status_code=503, detail="kafka is unavailable")
    try:
        await publisher.publish_watch(evt)
    except Exception as exc:
        raise HTTPException(status_code=503, detail="failed to publish watch event") from exc
    return {"status": "ok"}


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

    if x_autolog_impressions:
        imp_evt = ImpressionEvent(
            user_id=user_id,
            session_id=session_id,
            request_id=req_id,
            ts_ms=now_ms(),
            items=[ImpressionItem(item_id=item, position=pos) for pos, item in enumerate(items)],
            context=Context(user_agent=user_agent),
        )
        if publisher.is_ready:
            try:
                await publisher.publish_impression(imp_evt)
            except Exception:
                logger.exception(
                    "Failed to auto-log impressions for request_id=%s user_id=%s",
                    req_id,
                    user_id,
                )
        else:
            logger.warning(
                "Skipping auto-log impressions for request_id=%s because kafka is unavailable",
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
    if not publisher.is_ready:
        raise HTTPException(status_code=503, detail="kafka is unavailable")
    try:
        await publisher.publish_batch(batch.events)
    except Exception as exc:
        raise HTTPException(status_code=503, detail="failed to publish batch events") from exc
    return {"status": "ok", "count": len(batch.events)}

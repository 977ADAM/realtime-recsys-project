import uuid
import anyio
from typing import Optional
from fastapi import FastAPI, Header, HTTPException, Query

if __package__:
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
    from .kafka import KafkaPublisher, now_ms
else:  # pragma: no cover - fallback for direct script execution
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
    from kafka import KafkaPublisher, now_ms

APP_NAME = "Real-time Recommender MVP + reco-logger"

app = FastAPI(title=APP_NAME)

store = FeatureStore()
publisher = KafkaPublisher()


@app.on_event("startup")
async def startup():
    # Initialize DB schema (sync) without blocking the event loop
    await anyio.to_thread.run_sync(init_db)
    await publisher.start()

@app.on_event("shutdown")
async def shutdown():
    close_pool()
    await publisher.stop()


@app.post("/event")
def add_event(event: Event):
    store.add_event(event.user_id, event.item_id, event.event_type, event.ts)
    return {"status": "ok"}


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
    await publisher.publish_impression(evt)
    return {"status": "ok"}


@app.post("/log/watch")
async def log_watch(evt: WatchEvent):
    await publisher.publish_watch(evt)
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
        await publisher.publish_impression(imp_evt)

    return RecoResponse(
        request_id=req_id,
        items=items,
        model_version="rules-v1",
        strategy="co_vis_popularity_hybrid",
    )

@app.post("/log/batch")
async def log_batch(batch: EventBatch):
    await publisher.publish_batch(batch.events)
    return {"status": "ok", "count": len(batch.events)}

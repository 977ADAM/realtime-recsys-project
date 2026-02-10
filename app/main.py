import os
import json
import time
import uuid
import anyio
from typing import Optional
from fastapi import FastAPI, Header, Request, HTTPException
from schemas import (
    Event,
    RecommendResponse,
    ImpressionEvent,
    ImpressionItem,
    WatchEvent,
    RecoResponse,
    Context,
    EventBatch
    )
from aiokafka import AIOKafkaProducer
from store import FeatureStore
from recommend import recommend
from db import close_pool, init_db

APP_NAME = "Real-time Recommender MVP + reco-logger"

KAFKA_BOOTSTRAP = os.getenv("KAFKA_BOOTSTRAP", "localhost:9092")
TOPIC_IMPRESSION = os.getenv("TOPIC_IMPRESSION", "reco_impression")
TOPIC_WATCH = os.getenv("TOPIC_WATCH", "reco_watch")

app = FastAPI(title=APP_NAME)

store = FeatureStore()

producer: Optional[AIOKafkaProducer] = None


@app.on_event("startup")
async def startup():
    # Initialize DB schema (sync) without blocking the event loop
    await anyio.to_thread.run_sync(init_db)

    global producer
    producer = AIOKafkaProducer(
        bootstrap_servers=KAFKA_BOOTSTRAP,
        acks="all",
        linger_ms=5,
        retries=5,
        compression_type="lz4",
        request_timeout_ms=30000,
        enable_idempotence=True,
        max_in_flight_requests_per_connection=5,
    )
    await producer.start()

@app.on_event("shutdown")
async def shutdown():
    close_pool()
    global producer
    if producer is not None:
        await producer.stop()
        producer = None


@app.post("/event")
def add_event(event: Event):
    store.add_event(event.user_id, event.item_id, event.event_type, event.ts)
    return {"status": "ok"}


@app.get("/recommend", response_model=RecommendResponse)
def get_recommend(user_id: str, k: int = 10):
    items = recommend(user_id, store, k)
    return RecommendResponse(
        user_id=user_id,
        items=items,
        reason={"type": "hybrid", "sources": ["co_visitation", "trending"]},
    )


def now_ms() -> int:
    return int(time.time() * 1000)


def new_request_id() -> str:
    # uuid4 is fine; you can switch to ULID if you prefer sortable ids
    return uuid.uuid4().hex


async def kafka_send(topic: str, key: str, payload: dict) -> None:
    global producer
    if producer is None:
        raise RuntimeError("Kafka producer is not initialized")

    # Serialize to UTF-8 JSON bytes for Kafka
    data = json.dumps(payload, ensure_ascii=False, separators=(",", ":")).encode("utf-8")
    await producer.send_and_wait(topic, key=key.encode("utf-8"), value=data)

@app.post("/log/impression")
async def log_impression(evt: ImpressionEvent):
    # Minimal validation
    if len(evt.items) == 0:
        raise HTTPException(status_code=400, detail="items must be non-empty")

    payload = evt.model_dump()
    # server receive timestamp to help debugging pipelines
    payload["server_received_ts_ms"] = now_ms()

    await kafka_send(TOPIC_IMPRESSION, key=evt.user_id, payload=payload)
    return {"status": "ok"}


@app.post("/log/watch")
async def log_watch(evt: WatchEvent):
    payload = evt.model_dump()
    payload["server_received_ts_ms"] = now_ms()

    await kafka_send(TOPIC_WATCH, key=evt.user_id, payload=payload)
    return {"status": "ok"}


@app.get("/reco", response_model=RecoResponse)
async def get_reco(
    request: Request,
    user_id: str,
    session_id: str,
    k: int = 20,
    x_autolog_impressions: bool = True,
    user_agent: Optional[str] = Header(default=None, alias="User-Agent"),
):
    req_id = new_request_id()

    # Dummy results
    items = [f"item_{i}" for i in range(k)]

    if x_autolog_impressions:
        imp_evt = ImpressionEvent(
            user_id=user_id,
            session_id=session_id,
            request_id=req_id,
            ts_ms=now_ms(),
            items=[ImpressionItem(item_id=item, position=pos) for pos, item in enumerate(items)],
            context=Context(user_agent=user_agent),
        )
        payload = imp_evt.model_dump()
        payload["server_received_ts_ms"] = now_ms()
        await kafka_send(TOPIC_IMPRESSION, key=user_id, payload=payload)

    return RecoResponse(request_id=req_id, items=items)





@app.post("/log/batch")
async def log_batch(batch: EventBatch):
    async with anyio.create_task_group() as tg:
        for evt in batch.events:
            payload = evt.model_dump()
            payload["server_received_ts_ms"] = now_ms()

            topic = TOPIC_IMPRESSION if evt.event_type == "impression" else TOPIC_WATCH
            tg.start_soon(kafka_send, topic, evt.user_id, payload)

    return {"status": "ok", "count": len(batch.events)}

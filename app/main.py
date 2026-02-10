from fastapi import FastAPI
from schemas import Event, RecommendResponse
from store import FeatureStore
from recommend import recommend
from db import close_pool, init_db

app = FastAPI(title="Real-time Recommender MVP")

store = FeatureStore()


@app.on_event("startup")
def on_startup():
    init_db()


@app.on_event("shutdown")
def on_shutdown():
    close_pool()


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

from fastapi import FastAPI
from schemas import Event, RecommendResponse
from store import FeatureStore
from recommend import recommend

app = FastAPI(title="Real-time Recommender MVP")

store = FeatureStore()


@app.post("/event")
def add_event(event: Event):
    store.add_event(event.user_id, event.item_id, event.event_type)
    return {"status": "ok"}


@app.get("/recommend", response_model=RecommendResponse)
def get_recommend(user_id: str, k: int = 10):
    items = recommend(user_id, store, k)
    return RecommendResponse(
        user_id=user_id,
        items=items,
        reason={"type": "hybrid", "sources": ["co_visitation", "trending"]}
    )

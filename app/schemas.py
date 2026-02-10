from pydantic import BaseModel
from typing import Literal, List, Dict


EventType = Literal["view", "click", "purchase"]


class Event(BaseModel):
    user_id: str
    item_id: str
    event_type: EventType
    ts: int


class RecommendResponse(BaseModel):
    user_id: str
    items: List[str]
    reason: Dict

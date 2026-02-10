import uuid
from pydantic import BaseModel, Field
from typing import Literal, List, Dict, Optional, Union


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


class Context(BaseModel):
    device: Optional[str] = None
    platform: Optional[str] = None          # ios/android/web
    app_version: Optional[str] = None
    locale: Optional[str] = None
    country: Optional[str] = None
    timezone: Optional[str] = None
    referrer: Optional[str] = None          # push/search/direct/...
    user_agent: Optional[str] = None


class ImpressionItem(BaseModel):
    item_id: str
    position: int = Field(ge=0)
    feed_id: Optional[str] = None
    slot: Optional[str] = None              # e.g. "home_feed", "following_feed"


class BaseLogEvent(BaseModel):
    event_id: str = Field(default_factory=lambda: uuid.uuid4().hex)
    user_id: str
    session_id: str
    request_id: str
    ts_ms: int
    context: Optional[Context] = None


class ImpressionEvent(BaseLogEvent):
    event_type: Literal["impression"] = "impression"
    items: List[ImpressionItem]


class WatchEvent(BaseLogEvent):
    event_type: Literal["watch"] = "watch"
    item_id: str
    watch_time_sec: float = Field(ge=0)
    percent_watched: Optional[float] = Field(default=None, ge=0, le=100)
    ended: Optional[bool] = None
    playback_speed: Optional[float] = Field(default=None, ge=0.25, le=4.0)
    rebuffer_count: Optional[int] = Field(default=None, ge=0)


class RecoResponse(BaseModel):
    request_id: str
    items: List[str]
    model_version: str = "rules-v1"
    strategy: str = "co_vis_popularity_hybrid"


class RecoRequest(BaseModel):
    user_id: str
    session_id: str
    k: int = Field(default=20, ge=1, le=200)


class EventBatch(BaseModel):
    events: List[Union[ImpressionEvent, WatchEvent]] = Field(min_length=1, max_length=1000)

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


class ImpressionEvent(BaseModel):
    event_type: Literal["impression"] = "impression"
    user_id: str
    session_id: str
    request_id: str
    ts_ms: int                               # client or server timestamp in ms
    items: List[ImpressionItem]
    context: Optional[Context] = None


class WatchEvent(BaseModel):
    event_type: Literal["watch"] = "watch"
    user_id: str
    session_id: str
    request_id: str
    item_id: str
    ts_ms: int
    watch_time_sec: float = Field(ge=0)
    percent_watched: Optional[float] = Field(default=None, ge=0, le=100)
    ended: Optional[bool] = None
    playback_speed: Optional[float] = Field(default=None, ge=0.25, le=4.0)
    rebuffer_count: Optional[int] = Field(default=None, ge=0)
    context: Optional[Context] = None


class RecoResponse(BaseModel):
    request_id: str
    items: List[str]

class EventBatch(BaseModel):
    events: List[Union[ImpressionEvent, WatchEvent]]

import uuid
from typing import Dict, List, Literal, Optional, Union

from pydantic import BaseModel, ConfigDict, Field, field_validator, model_validator

if __package__:
    from .config import CONTRACT
    from .runtime_utils import normalize_unix_ts_ms, now_ms
else:  # pragma: no cover - fallback for direct script execution
    from config import CONTRACT
    from runtime_utils import normalize_unix_ts_ms, now_ms


EventType = Literal["view", "click", "purchase"]


def _validate_event_time(ts: int, field_name: str) -> int:
    normalized = normalize_unix_ts_ms(ts)
    current_ms = now_ms()
    if normalized > current_ms + CONTRACT.max_event_future_ms:
        raise ValueError(
            f"{field_name} is too far in the future; max_future_ms={CONTRACT.max_event_future_ms}"
        )
    if normalized < current_ms - CONTRACT.max_event_age_ms:
        raise ValueError(
            f"{field_name} is too old; max_event_age_ms={CONTRACT.max_event_age_ms}"
        )
    return normalized


class Event(BaseModel):
    model_config = ConfigDict(extra="forbid")

    event_id: str = Field(default_factory=lambda: uuid.uuid4().hex, min_length=8, max_length=64)
    user_id: str = Field(min_length=1, max_length=128)
    item_id: str = Field(min_length=1, max_length=128)
    event_type: EventType
    ts: int = Field(gt=0)

    @field_validator("ts")
    @classmethod
    def validate_ts(cls, value: int) -> int:
        _validate_event_time(value, "ts")
        return value


class RecommendResponse(BaseModel):
    model_config = ConfigDict(extra="forbid")

    user_id: str
    items: List[str]
    reason: Dict


class Context(BaseModel):
    model_config = ConfigDict(extra="forbid")

    device: Optional[str] = None
    platform: Optional[str] = None  # ios/android/web
    app_version: Optional[str] = None
    locale: Optional[str] = None
    country: Optional[str] = None
    timezone: Optional[str] = None
    referrer: Optional[str] = None  # push/search/direct/...
    user_agent: Optional[str] = None


class ImpressionItem(BaseModel):
    model_config = ConfigDict(extra="forbid")

    item_id: str = Field(min_length=1, max_length=128)
    position: int = Field(ge=0)
    feed_id: Optional[str] = None
    slot: Optional[str] = None  # e.g. "home_feed", "following_feed"


class BaseLogEvent(BaseModel):
    model_config = ConfigDict(extra="forbid")

    event_id: str = Field(default_factory=lambda: uuid.uuid4().hex, min_length=8, max_length=64)
    user_id: str = Field(min_length=1, max_length=128)
    session_id: str = Field(min_length=1, max_length=128)
    request_id: str = Field(min_length=1, max_length=128)
    ts_ms: int = Field(gt=0)
    server_received_ts_ms: Optional[int] = Field(default=None, gt=0)
    context: Optional[Context] = None

    @field_validator("ts_ms")
    @classmethod
    def validate_ts_ms(cls, value: int) -> int:
        return _validate_event_time(value, "ts_ms")


class ImpressionEvent(BaseLogEvent):
    event_type: Literal["impression"] = "impression"
    items: List[ImpressionItem] = Field(min_length=1, max_length=500)

    @model_validator(mode="after")
    def validate_items(self):
        positions = [item.position for item in self.items]
        if len(set(positions)) != len(positions):
            raise ValueError("impression items must have unique positions")

        item_ids = [item.item_id for item in self.items]
        if len(set(item_ids)) != len(item_ids):
            raise ValueError("impression items must not contain duplicate item_id")
        return self


class WatchEvent(BaseLogEvent):
    event_type: Literal["watch"] = "watch"
    item_id: str = Field(min_length=1, max_length=128)
    watch_time_sec: float = Field(ge=0)
    percent_watched: Optional[float] = Field(default=None, ge=0, le=100)
    ended: Optional[bool] = None
    playback_speed: Optional[float] = Field(default=None, ge=0.25, le=4.0)
    rebuffer_count: Optional[int] = Field(default=None, ge=0)


class RecoResponse(BaseModel):
    model_config = ConfigDict(extra="forbid")

    request_id: str
    items: List[str]
    model_version: str = "rules-v1"
    strategy: str = "co_vis_popularity_hybrid"


class RecoRequest(BaseModel):
    model_config = ConfigDict(extra="forbid")

    user_id: str = Field(min_length=1, max_length=128)
    session_id: str = Field(min_length=1, max_length=128)
    k: int = Field(default=20, ge=1, le=200)


class EventBatch(BaseModel):
    model_config = ConfigDict(extra="forbid")

    events: List[Union[ImpressionEvent, WatchEvent]] = Field(
        min_length=1,
        max_length=CONTRACT.max_batch_size,
    )

    @model_validator(mode="after")
    def validate_event_ids(self):
        event_ids = [event.event_id for event in self.events]
        if len(set(event_ids)) != len(event_ids):
            raise ValueError("batch events must have unique event_id")
        return self

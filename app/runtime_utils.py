import os
import re
import time
from typing import Optional


_TRUTHY_ENV_VALUES = frozenset({"1", "true", "yes", "on"})
_SAFE_ID_PATTERN = re.compile(r"^[a-zA-Z0-9._:-]{1,128}$")


def env_bool(name: str, default: bool) -> bool:
    raw = os.getenv(name)
    if raw is None:
        return default
    return raw.strip().lower() in _TRUTHY_ENV_VALUES


def positive_int_env(name: str, default: int) -> int:
    raw = os.getenv(name)
    if raw is None:
        return default
    try:
        value = int(raw)
    except ValueError:
        return default
    return value if value > 0 else default


def non_negative_int_env(name: str, default: int) -> int:
    raw = os.getenv(name)
    if raw is None:
        return default
    try:
        value = int(raw)
    except ValueError:
        return default
    return value if value >= 0 else default


def positive_float_env(name: str, default: float) -> float:
    raw = os.getenv(name)
    if raw is None:
        return default
    try:
        value = float(raw)
    except ValueError:
        return default
    return value if value > 0 else default


def bounded_int(value: int, *, lower: int, upper: int) -> int:
    return max(lower, min(int(value), upper))


def now_ms() -> int:
    return int(time.time() * 1000)


def normalize_unix_ts_seconds(ts: Optional[int]) -> Optional[float]:
    if ts is None:
        return None
    return ts / 1000.0 if ts >= 10**12 else float(ts)


def normalize_unix_ts_ms(ts: int) -> int:
    return ts if ts >= 10**12 else ts * 1000


def sanitize_identifier(raw: Optional[str], *, fallback: str, max_len: int = 128) -> str:
    if raw is None:
        return fallback
    value = raw.strip()[:max_len]
    if not value:
        return fallback
    if _SAFE_ID_PATTERN.match(value) is None:
        return fallback
    return value

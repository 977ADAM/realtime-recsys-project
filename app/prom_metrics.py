import logging
import os
from typing import Optional

if __package__:
    from .runtime_utils import env_bool, positive_int_env
else:  # pragma: no cover - fallback for direct script execution
    from runtime_utils import env_bool, positive_int_env

logger = logging.getLogger(__name__)


try:
    from prometheus_client import (  # type: ignore
        CONTENT_TYPE_LATEST,
        Counter,
        Gauge,
        Histogram,
        generate_latest,
        start_http_server,
    )

    PROMETHEUS_AVAILABLE = True
except ImportError:  # pragma: no cover - runtime fallback when dependency is optional
    CONTENT_TYPE_LATEST = "text/plain; version=0.0.4; charset=utf-8"
    PROMETHEUS_AVAILABLE = False

    class _NoopMetric:
        def __init__(self, *args, **kwargs):
            _ = args
            _ = kwargs

        def labels(self, **_kwargs):
            return self

        def inc(self, _value: float = 1.0):
            return None

        def observe(self, _value: float):
            return None

        def set(self, _value: float):
            return None

    def generate_latest():
        return b""

    def start_http_server(port: int, addr: str = "0.0.0.0"):
        _ = port
        _ = addr
        return None

    Counter = Gauge = Histogram = _NoopMetric


_LAG_BUCKETS_MS = (
    10,
    25,
    50,
    100,
    250,
    500,
    1000,
    2500,
    5000,
    10000,
    30000,
    60000,
    120000,
    300000,
)


RECO_REQUEST_TOTAL = Counter(
    "reco_api_requests_total",
    "Total /reco requests grouped by response status family.",
    labelnames=("status_family",),
)

RECO_REQUEST_LATENCY_MS = Histogram(
    "reco_api_latency_ms",
    "Latency distribution for /reco API requests.",
    buckets=_LAG_BUCKETS_MS,
)

WATCH_EVENT_TO_FEATURE_LATENCY_MS = Histogram(
    "reco_watch_event_to_feature_latency_ms",
    "Latency from client event timestamp to successful online feature update.",
    buckets=_LAG_BUCKETS_MS,
)


def observe_reco_request(latency_ms: float, status_code: int) -> None:
    status_family = f"{max(status_code, 0) // 100}xx"
    RECO_REQUEST_TOTAL.labels(status_family=status_family).inc()
    RECO_REQUEST_LATENCY_MS.observe(max(float(latency_ms), 0.0))


def observe_watch_event_to_feature_latency(latency_ms: float) -> None:
    WATCH_EVENT_TO_FEATURE_LATENCY_MS.observe(max(float(latency_ms), 0.0))


def prometheus_payload() -> Optional[bytes]:
    if not PROMETHEUS_AVAILABLE:
        return None
    return generate_latest()


def prometheus_content_type() -> str:
    return CONTENT_TYPE_LATEST


def start_metrics_http_server() -> bool:
    if not env_bool("PROMETHEUS_METRICS_ENABLED", True):
        logger.info("Prometheus metrics server is disabled by config")
        return False

    if not PROMETHEUS_AVAILABLE:
        logger.warning("prometheus_client is not installed; metrics server is disabled")
        return False

    host = os.getenv("METRICS_HOST", "0.0.0.0")
    port = positive_int_env("METRICS_PORT", 9108)
    try:
        start_http_server(port=port, addr=host)
    except Exception as exc:
        logger.warning("Failed to start metrics server (%s: %s)", exc.__class__.__name__, exc)
        return False
    logger.info("Metrics server started at http://%s:%s/metrics", host, port)
    return True

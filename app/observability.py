import math
from collections import deque
from threading import Lock

if __package__:
    from .config import SLA_TARGETS
    from .runtime_utils import now_ms, positive_int_env
else:  # pragma: no cover - fallback for direct script execution
    from config import SLA_TARGETS
    from runtime_utils import now_ms, positive_int_env


def _percentile(values, q: float):
    if not values:
        return None
    pos = (len(values) - 1) * q
    low = int(math.floor(pos))
    high = int(math.ceil(pos))
    if low == high:
        return values[low]
    left = values[low]
    right = values[high]
    return left + (right - left) * (pos - low)


class RecoMetricsWindow:
    def __init__(self):
        self.window_sec = positive_int_env("RECO_METRICS_WINDOW_SEC", 3600)
        self.max_samples = positive_int_env("RECO_METRICS_MAX_SAMPLES", 50000)
        self._window_ms = self.window_sec * 1000
        self._samples = deque()
        self._lock = Lock()

    def _prune(self, now_ms: int) -> None:
        cutoff = now_ms - self._window_ms
        while self._samples and self._samples[0][0] < cutoff:
            self._samples.popleft()
        while len(self._samples) > self.max_samples:
            self._samples.popleft()

    def record(self, latency_ms: float, status_code: int) -> None:
        current_ms = now_ms()
        sample = (current_ms, float(latency_ms), int(status_code))
        with self._lock:
            self._samples.append(sample)
            self._prune(current_ms)

    def snapshot(self) -> dict:
        current_ms = now_ms()
        with self._lock:
            self._prune(current_ms)
            samples = list(self._samples)

        latencies = sorted(sample[1] for sample in samples)
        total = len(samples)
        status_2xx = sum(1 for _, _, code in samples if 200 <= code < 300)
        status_4xx = sum(1 for _, _, code in samples if 400 <= code < 500)
        status_5xx = sum(1 for _, _, code in samples if code >= 500)
        errors = status_4xx + status_5xx

        p50 = _percentile(latencies, 0.50)
        p95 = _percentile(latencies, 0.95)
        p99 = _percentile(latencies, 0.99)

        p95_target = SLA_TARGETS["reco_p95_ms"]
        p99_target = SLA_TARGETS["reco_p99_ms"]

        return {
            "window_sec": self.window_sec,
            "sample_size": total,
            "status_counts": {
                "2xx": status_2xx,
                "4xx": status_4xx,
                "5xx": status_5xx,
            },
            "error_rate_pct": round((errors / total) * 100.0, 3) if total else 0.0,
            "latency_ms": {
                "p50": round(p50, 3) if p50 is not None else None,
                "p95": round(p95, 3) if p95 is not None else None,
                "p99": round(p99, 3) if p99 is not None else None,
            },
            "sla": {
                "target_p95_ms": p95_target,
                "target_p99_ms": p99_target,
                "p95_met": (p95 is None) or (p95 <= p95_target),
                "p99_met": (p99 is None) or (p99 <= p99_target),
            },
        }

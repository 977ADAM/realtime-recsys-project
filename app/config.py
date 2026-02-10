import os
from dataclasses import dataclass


def _positive_int_env(name: str, default: int) -> int:
    raw = os.getenv(name)
    if raw is None:
        return default
    try:
        value = int(raw)
    except ValueError:
        return default
    return value if value > 0 else default


@dataclass(frozen=True)
class ContractConfig:
    max_event_future_ms: int
    max_event_age_ms: int
    max_batch_size: int


CONTRACT = ContractConfig(
    max_event_future_ms=_positive_int_env("CONTRACT_MAX_EVENT_FUTURE_MS", 5 * 60 * 1000),
    max_event_age_ms=_positive_int_env("CONTRACT_MAX_EVENT_AGE_MS", 30 * 24 * 60 * 60 * 1000),
    max_batch_size=_positive_int_env("CONTRACT_MAX_BATCH_SIZE", 1000),
)


KPI_TARGETS = {
    "ctr_at_20": ">= 8%",
    "avg_watch_time_sec": ">= 35",
    "catalog_coverage_at_20": ">= 60%",
}


SLA_TARGETS = {
    "reco_p95_ms": 120,
    "reco_p99_ms": 200,
    "api_error_rate_pct": 1.0,
}


def contract_snapshot() -> dict:
    return {
        "kpi_targets": KPI_TARGETS,
        "sla_targets": SLA_TARGETS,
        "event_contract": {
            "max_event_future_ms": CONTRACT.max_event_future_ms,
            "max_event_age_ms": CONTRACT.max_event_age_ms,
            "max_batch_size": CONTRACT.max_batch_size,
        },
    }

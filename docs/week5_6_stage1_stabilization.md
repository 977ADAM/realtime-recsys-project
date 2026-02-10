# Week 5-6 Stage 1: Foundation Stabilization

## Scope
- Move Kafka consumer worker to a dedicated process by default.
- Add Prometheus metrics for serving and stream processing.
- Add baseline regression tests for contracts, idempotency, and ranking behavior.

## Architecture Changes
- API no longer starts the Kafka worker by default.
- New worker entrypoint: `python3 -m app.worker_main`
- Optional embedded mode for local debug:
  - `API_EMBEDDED_WORKER_ENABLED=true`

## Prometheus Metrics
- API metrics endpoint: `GET /metrics/prometheus`
- Worker metrics HTTP server:
  - `WORKER_METRICS_HOST` (default `0.0.0.0`)
  - `WORKER_METRICS_PORT` (default `9108`)
  - `PROMETHEUS_METRICS_ENABLED` (default `true`)

Key metrics:
- `reco_api_requests_total`
- `reco_api_latency_ms`
- `reco_worker_processed_total`
- `reco_worker_consumer_lag_ms`
- `reco_worker_dlq_total`
- `reco_watch_event_to_feature_latency_ms`
- `reco_worker_running`

## Worker Runtime Snapshot
`GET /stream/worker` now includes:
- `mode`: `embedded` or `external`
- runtime fields:
  - `runtime.last_consumer_lag_ms`
  - `runtime.last_event_to_feature_latency_ms`
  - `runtime.last_processed_at_ms`

## Test Baseline
Added tests:
- `tests/test_schemas.py`
- `tests/test_store_idempotency.py`
- `tests/test_ranking.py`

Command:
- `./venv/bin/python -m unittest discover -s tests -v`

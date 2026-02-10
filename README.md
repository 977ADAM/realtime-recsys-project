# Real-time Recommender (MVP -> Production)

Backend service for online recommendation retrieval/ranking and event logging.

## Current Stage
- MVP serving API with rule-based ranking (`co-visitation + popularity`).
- Kafka publisher for impression/watch events.
- Postgres-backed feature tables.
- Week `1-2` foundation delivered: data contract, KPI/SLA contract, idempotent event ingestion.
- Week `1-2` observability baseline delivered: `/reco` latency/error metrics + baseline KPI snapshot endpoint.
- Week `3-4` foundation delivered: streaming consumer worker, Postgres sinks, DLQ routing.
- Week `3-4` freshness loop delivered: watch stream updates online feature tables (`item_popularity`, `co_visitation`, `user_history`) idempotently.
- Week `5-6` stage 1 stabilization delivered: external worker mode by default, Prometheus metrics, baseline tests.

## Week 1-2 Artifacts
- Contract document: `docs/week1_2_foundation.md`
- Runtime contract endpoint: `GET /contract`

## Week 3-4 Artifacts
- Streaming foundation doc: `docs/week3_4_streaming_foundation.md`
- Worker status endpoint: `GET /stream/worker`

## Week 5-6 Artifacts
- Stabilization doc: `docs/week5_6_stage1_stabilization.md`
- Prometheus metrics endpoint: `GET /metrics/prometheus`

## Core Endpoints
- `POST /event`: ingest interaction event (`view/click/purchase`) with idempotent `event_id`
- `GET /recommend`: basic recommendation endpoint
- `GET /reco`: retrieval + ranking endpoint with optional auto-impression logging
- `GET /metrics/reco`: rolling `/reco` runtime latency + error-rate metrics
- `GET /metrics/prometheus`: Prometheus metrics for API and worker counters/histograms
- `GET /analytics/baseline`: baseline KPI snapshot from `impressions`/`watches`
- `POST /log/impression`
- `POST /log/watch`
- `POST /log/batch`
- `GET /stream/worker`: streaming worker state and counters

## Streaming Worker Enablement
- Start dedicated worker process:
  - `python3 -m app.worker_main`
- Optional embedded worker mode inside API process:
  - `API_EMBEDDED_WORKER_ENABLED=true`
- Optional feature loop tuning:
  - `STREAM_FEATURE_HISTORY_SIZE=20`
- Optional replay mode:
  - `KAFKA_WORKER_AUTO_OFFSET_RESET=earliest`
  - dedicated `KAFKA_WORKER_GROUP_ID=<your-replay-group>`
- Worker Prometheus server:
  - `WORKER_METRICS_HOST=0.0.0.0`
  - `WORKER_METRICS_PORT=9108`
  - `PROMETHEUS_METRICS_ENABLED=true`

## Test Baseline
- `./venv/bin/python -m unittest discover -s tests -v`

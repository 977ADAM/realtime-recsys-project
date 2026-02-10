# Real-time Recommender (MVP -> Production)

Backend service for online recommendation retrieval/ranking and event logging.

## Current Stage
- MVP serving API with rule-based ranking (`co-visitation + popularity`).
- Kafka publisher for impression/watch events.
- Postgres-backed feature tables.
- Week `1-2` foundation delivered: data contract, KPI/SLA contract, idempotent event ingestion.
- Week `3-4` foundation delivered: streaming consumer worker, Postgres sinks, DLQ routing.

## Week 1-2 Artifacts
- Contract document: `docs/week1_2_foundation.md`
- Runtime contract endpoint: `GET /contract`

## Week 3-4 Artifacts
- Streaming foundation doc: `docs/week3_4_streaming_foundation.md`
- Worker status endpoint: `GET /stream/worker`

## Core Endpoints
- `POST /event`: ingest interaction event (`view/click/purchase`) with idempotent `event_id`
- `GET /recommend`: basic recommendation endpoint
- `GET /reco`: retrieval + ranking endpoint with optional auto-impression logging
- `POST /log/impression`
- `POST /log/watch`
- `POST /log/batch`
- `GET /stream/worker`: streaming worker state and counters

## Streaming Worker Enablement
- Export `KAFKA_WORKER_ENABLED=true`
- Optional replay mode:
  - `KAFKA_WORKER_AUTO_OFFSET_RESET=earliest`
  - dedicated `KAFKA_WORKER_GROUP_ID=<your-replay-group>`

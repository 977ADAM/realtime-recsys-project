# Week 1-2 Foundation: Product + Data Contract

## Scope
- Align product KPI and serving SLA for the first production-grade milestone.
- Define strict ingestion contract for all online events.
- Add idempotency guarantees for interaction ingestion.

## KPI Targets (initial)
- `ctr_at_20 >= 8%`
- `avg_watch_time_sec >= 35`
- `catalog_coverage_at_20 >= 60%`

## Serving SLA Targets (initial)
- `p95 /reco latency <= 120 ms`
- `p99 /reco latency <= 200 ms`
- `API error rate <= 1.0%`

## Event Contract
- Every event must include `event_id` (idempotency key).
- All IDs (`user_id`, `item_id`, `session_id`, `request_id`) are non-empty bounded strings.
- Event timestamps are accepted in seconds/ms and validated against allowed time window:
  - `max_event_future_ms` (default 5 min)
  - `max_event_age_ms` (default 30 days)
- Batch ingest rejects duplicate `event_id` inside a single request.
- Impression payload rejects duplicate positions and duplicate item IDs.

## Data Quality Gates
- Invalid/late/future events are rejected at API validation layer.
- Duplicate events are accepted as request but ignored in feature updates (`status=duplicate`).
- Contract settings are published via `GET /contract`.

## Definition of Done (Week 1-2)
- Contract is documented and versioned in repo.
- API validates payload shape and time windows.
- Ingestion is idempotent on `event_id` for core interaction events.
- Team can inspect active KPI/SLA and event limits via API.

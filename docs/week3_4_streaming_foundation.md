# Week 3-4 Foundation: Streaming Worker + DLQ

## Scope
- Build streaming pipeline from Kafka topics to Postgres sinks.
- Add DLQ routing for malformed/failed events.
- Support replay/backfill mode through consumer configuration.

## Implemented Components
- `KafkaConsumerWorker` in `app/kafka.py`:
  - Consumes `TOPIC_IMPRESSION` and `TOPIC_WATCH`
  - Validates payloads via Pydantic schemas
  - Persists valid events to Postgres
  - Publishes failed events to `TOPIC_DLQ`
  - Commits offsets after processing
- Worker lifecycle integrated into API startup/shutdown.
- Worker status endpoint: `GET /stream/worker`.

## Data Sink Behavior
- Impressions:
  - Stored as one row per shown item
  - Idempotent key: `(event_id, position)`
- Watches:
  - Stored as one row per event
  - Idempotent key: `event_id`
- `server_received_ts_ms` is persisted for both event types.

## DLQ Envelope
DLQ message contains:
- `source_topic`, `source_partition`, `source_offset`
- `source_timestamp_ms`, `source_key`
- `error_type`, `error_message`
- `payload`, `failed_at_ms`

## Replay / Backfill
- Set `KAFKA_WORKER_AUTO_OFFSET_RESET=earliest` to consume from earliest available offsets.
- Use a dedicated `KAFKA_WORKER_GROUP_ID` for controlled replay runs.
- Keep production and replay groups separate.

## Runtime Configuration
- `KAFKA_WORKER_ENABLED` (default `false`)
- `KAFKA_WORKER_GROUP_ID` (default `reco-stream-worker-v1`)
- `KAFKA_WORKER_AUTO_OFFSET_RESET` (`latest` or `earliest`)
- `KAFKA_WORKER_POLL_TIMEOUT_MS` (default `1000`)
- `KAFKA_WORKER_MAX_POLL_RECORDS` (default `200`)
- `TOPIC_DLQ` (default `reco_events_dlq`)

## Definition of Done (Week 3-4)
- Worker processes impression/watch topics continuously.
- Invalid events do not break pipeline and are routed to DLQ.
- Event persistence is idempotent for at-least-once delivery.
- Team can run backfill/replay with isolated consumer group.

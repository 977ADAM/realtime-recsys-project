# Incident and Rollback Plan

## Severity Levels
- `SEV-1`: Full outage, data loss risk, or sustained elevated 5xx.
- `SEV-2`: Degraded performance or partial unavailability.
- `SEV-3`: Non-critical regressions with available workaround.

## Incident Response Flow
1. **Detect**: Alert fires (readiness, outbox dead rows, consumer lag, 5xx rate).
2. **Triage**: Identify failing component (`api`, `outbox-relay`, `feature-consumer`, `postgres`, `kafka`, `redis`).
3. **Mitigate**: Apply fastest safe mitigation (scale/restart/fallback toggle).
4. **Recover**: Restore normal traffic and validate KPIs/SLAs.
5. **Review**: Capture timeline, root cause, and preventive actions.

## Quick Triage Commands
```bash
docker compose ps
docker compose logs --tail 200 api
docker compose logs --tail 200 outbox-relay
docker compose logs --tail 200 feature-consumer
curl -fsS http://localhost:8080/healthz | jq
curl -fsS http://localhost:8080/readyz | jq
```

## Common Failure Modes
### 1. API ready=false
- Check DB connectivity (`DATABASE_URL`, Postgres health).
- Check cache readiness (`ONLINE_CACHE_STRICT_BACKEND`, Redis health).
- Mitigation: temporary `ONLINE_CACHE_BACKEND=memory` if Redis degraded.

### 2. Outbox backlog/dead rows growing
- Verify Kafka availability and relay logs.
- Check `reco_outbox_relay_errors_total` and `reco_outbox_relay_failed_total`.
- Mitigation:
  - restart relay service
  - requeue dead rows after root cause fix:
    ```bash
    python src/replay_outbox.py --limit 1000
    ```

### 3. Consumer lag growth
- Check Kafka consumer group and `reco_feature_consumer_partition_lag`.
- Check `reco_feature_consumer_processed_total{status="dlq"}` and `reco_feature_consumer_dlq_backlog{status="pending"}`.
- Verify DB latency and lock timeout errors.
- Mitigation: restart `feature-consumer`, temporarily increase poll batch/timeout, tune `FEATURE_CONSUMER_MAX_RETRIES_PER_MESSAGE`.
- After root cause fix, replay DLQ:
  ```bash
  python src/replay_feature_consumer_dlq.py --status pending --limit 100
  ```

## Rollback Strategy
### Application rollback
1. Identify last known good image/tag.
2. Update deploy manifest/compose image tag.
3. Redeploy previous version:
```bash
docker compose up -d --no-deps api outbox-relay feature-consumer
```
4. Validate `/healthz` and `/readyz`.

### Schema rollback policy
- Migrations are **forward-only**.
- If a migration introduces issues:
  - deploy app code compatible with current schema, or
  - apply a new forward "fix migration".
- Never edit already-applied migration files.

## Data Safety Notes
- Event delivery uses transactional outbox + retry/dead states.
- Feature consumer is idempotent (`feature_event_consumed` primary key dedupe).
- Watch/impression ingestion is idempotent by unique event identifiers.

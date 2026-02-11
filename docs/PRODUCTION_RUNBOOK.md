# Production Runbook

## 1. Preconditions
- Docker/Compose runtime available on target host.
- Secrets provisioned via secret manager or runtime environment:
  - `POSTGRES_PASSWORD`
  - `DATABASE_URL`
- Network access between `api`, `postgres`, `kafka`, `redis`.
- `APP_ENV=production` enabled in production to enforce security checks.

## 2. Deployment
1. Prepare env:
```bash
cp .env.example .env
# edit .env with production values
```
2. Render and validate config:
```bash
docker compose config > /tmp/recsys.compose.rendered.yml
```
3. Deploy:
```bash
docker compose up -d --build
```
4. Verify running services:
```bash
docker compose ps
```

## 3. Post-Deploy Checks
```bash
curl -fsS http://localhost:8080/healthz | jq
curl -fsS http://localhost:8080/readyz | jq
python src/smoke_stack.py --base-url http://localhost:8080 --output-json smoke-report.json
python src/load_reco.py --base-url http://localhost:8080 --warmup 50 --requests 400 --concurrency 20 --autolog-impressions --output-json load-report.json
```

Guardrails:
```bash
curl -fsS "http://localhost:8080/analytics/baseline?days=1" -o baseline-kpi.json
curl -fsS "http://localhost:8080/contract" -o contract.json
curl -fsS "http://localhost:8080/metrics/reco" -o reco-metrics.json
python src/check_guardrails.py --baseline-json baseline-kpi.json --contract-json contract.json --reco-metrics-json reco-metrics.json --min-sla-samples 1 --output-json guardrail-report.json
```

Migration drift/checksum validation:
```bash
python src/check_migrations.py --database-url "${DATABASE_URL}" --output-json migration-report.json
```

## 4. Operational Dashboards
Minimum panels:
- API latency/error: `reco_api_latency_ms`, `reco_api_requests_total`, `/metrics/reco` snapshot.
- Readiness/health status: `reco_api_check_status`.
- Outbox reliability: `reco_outbox_backlog_events`, `reco_outbox_backlog_oldest_lag_ms`, `reco_outbox_relay_failed_total`, `reco_outbox_relay_errors_total`.
- Consumer progress: `reco_feature_consumer_processed_total`, `reco_feature_consumer_partition_lag`, `reco_feature_consumer_errors_total`.
- Consumer poison-message handling: `reco_feature_consumer_processed_total{status="dlq"}`, `reco_feature_consumer_dlq_events_total`, `reco_feature_consumer_dlq_backlog`, `reco_feature_consumer_dlq_oldest_pending_ms`.

## 5. Suggested Alerts
- `readyz` down > 2m.
- `reco_outbox_backlog_events{status="dead"} > 0` for 5m.
- `reco_outbox_backlog_oldest_lag_ms > 120000` for 5m.
- `reco_feature_consumer_partition_lag > 5000` for 10m.
- `increase(reco_feature_consumer_processed_total{status="dlq"}[10m]) > 0`.
- `reco_feature_consumer_dlq_oldest_pending_ms > 300000` for 10m.
- `rate(reco_api_requests_total{status_family="5xx"}[5m]) / rate(reco_api_requests_total[5m]) > 0.01`.

## 6. Routine Operations
### Requeue outbox rows (replay)
```bash
python src/replay_outbox.py --limit 1000
python src/replay_outbox.py --topic recsys.watches.v1 --limit 1000
```

### Replay feature-consumer DLQ
```bash
python src/replay_feature_consumer_dlq.py --limit 100
python src/replay_feature_consumer_dlq.py --topic recsys.watches.v1 --status pending --limit 100
```

### Schema checks
```bash
python src/check_migrations.py --skip-db-check
python src/check_migrations.py --database-url "${DATABASE_URL}"
```

## 7. Graceful Shutdown
```bash
docker compose stop
```
The API drains outstanding background tasks up to `RECO_BACKGROUND_SHUTDOWN_TIMEOUT_SEC`.
Relay/consumer use configurable shutdown timeouts and stop processing cleanly.
Feature consumer retries the same message up to `FEATURE_CONSUMER_MAX_RETRIES_PER_MESSAGE`; after that it stores the message in `feature_consumer_dlq` and continues partition progress.

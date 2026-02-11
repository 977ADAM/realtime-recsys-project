# realtime-recsys

Production-hardened real-time recommendation system built on `FastAPI + Postgres + Kafka + Redis` with:
- transactional outbox delivery (`event_outbox`)
- Kafka relay/consumer workers
- online serving cache
- Prometheus-compatible metrics
- guardrails and load/smoke tooling

## Services
- `api` (`app/main.py`): online recommendation and event ingestion APIs.
- `outbox-relay` (`src/outbox_relay.py`): publishes outbox rows to Kafka with retries and dead-letter state.
- `feature-consumer` (`src/feature_consumer.py`): consumes stream events and updates online features idempotently.
- `postgres`, `kafka`, `redis`: storage, stream backbone, and cache.

## Quick Start (Docker Compose)
1. Create environment file and set real secrets:
```bash
cp .env.example .env
# Edit .env: at minimum set POSTGRES_PASSWORD and DATABASE_URL
```
2. Start stack:
```bash
docker compose up -d --build
```
3. Verify liveness/readiness:
```bash
curl -fsS http://localhost:8080/healthz | jq
curl -fsS http://localhost:8080/readyz | jq
```

## Verification Commands
### Unit tests
```bash
pytest -q
```

### Migration checks (naming/order/checksum)
```bash
python src/check_migrations.py --skip-db-check
python src/check_migrations.py --database-url postgresql://... --output-json migration-report.json
```

### End-to-end smoke (API + outbox + consumer)
```bash
python src/smoke_stack.py --base-url http://localhost:8080 --output-json smoke-report.json
```

### Load test `/reco`
```bash
python src/load_reco.py \
  --base-url http://localhost:8080 \
  --warmup 50 \
  --requests 400 \
  --concurrency 20 \
  --autolog-impressions \
  --output-json load-report.json
```

### Guardrails evaluation
```bash
curl -fsS "http://localhost:8080/analytics/baseline?days=1" -o baseline-kpi.json
curl -fsS "http://localhost:8080/contract" -o contract.json
curl -fsS "http://localhost:8080/metrics/reco" -o reco-metrics.json
python src/check_guardrails.py \
  --baseline-json baseline-kpi.json \
  --contract-json contract.json \
  --reco-metrics-json reco-metrics.json \
  --min-sla-samples 1 \
  --output-json guardrail-report.json
```

## API Highlights
- `GET /healthz`: liveness + subsystem snapshots.
- `GET /readyz`: readiness gate (DB + cache).
- `GET /reco`: recommendation response (supports auto-impression logging).
- `POST /log/impression`, `POST /log/watch`, `POST /log/batch`.
- `GET /metrics/reco`: in-process SLA/KPI window snapshot.
- `GET /metrics/prometheus`: Prometheus scrape endpoint.
- `GET /contract`: KPI/SLA contract snapshot.

All HTTP responses include `X-Request-ID` and `X-Correlation-ID` headers.

## CI/CD
`/.github/workflows/ci.yml` runs:
- `ruff` lint
- `pytest`
- migration validation (`src/check_migrations.py`)
- Docker build and compose smoke run
- load + guardrail checks
- artifact upload (smoke/load/guardrails/migration + release notes draft)

## Operations Docs
- `docs/PRODUCTION_RUNBOOK.md`
- `docs/INCIDENT_ROLLBACK_PLAN.md`
- `docs/ENV_MATRIX.md`
- `PROD_READINESS_REPORT.md`

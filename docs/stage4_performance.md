# Stage 4: Reco Load Baseline and Latency Guardrails

## What changed

- `/reco` internal stage timings are now exported as Prometheus histogram `reco_stage_latency_ms{stage=...}`.
- Impression auto-logging in `/reco` can run in background to avoid adding DB/Kafka write latency to online response path.
- Retrieval uses batched co-visitation query and online cache to reduce DB round-trips.

## Runtime flags

- `RECO_ASYNC_IMPRESSION_LOGGING=true|false`
- `RECO_BACKGROUND_SHUTDOWN_TIMEOUT_SEC=5`
- `ONLINE_CACHE_*` variables from `.env.example`

## Local benchmark flow

1. Start stack:

```bash
docker compose up -d --build
```

2. Run load test:

```bash
python scripts/load_reco.py \
  --base-url http://localhost:8000 \
  --warmup 300 \
  --requests 5000 \
  --concurrency 100 \
  --k 20 \
  --output-json /tmp/reco-load-report.json
```

3. Optional run including impression auto-log path:

```bash
python scripts/load_reco.py \
  --base-url http://localhost:8000 \
  --warmup 300 \
  --requests 3000 \
  --concurrency 80 \
  --k 20 \
  --autolog-impressions \
  --output-json /tmp/reco-load-with-autolog.json
```

## Interpreting output

The report includes:

- `summary.latency_ms.p95` and `summary.latency_ms.p99`
- `summary.error_rate_pct`
- `sla.targets` and `sla.met` pulled from `/contract`
- `/metrics/reco` snapshot for cross-checking in-service window metrics

Use these as baseline artifacts for regression checks before each release.

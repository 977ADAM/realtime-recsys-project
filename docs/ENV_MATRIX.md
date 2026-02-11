# Environment Matrix

## Required Secrets
| Variable | Required | Scope | Notes |
|---|---|---|---|
| `POSTGRES_PASSWORD` | Yes | compose/postgres | Never commit real value. Use secret manager in production. |
| `DATABASE_URL` | Yes | api, relay, consumer | Must point to production Postgres in deployed environment. |

## Core Runtime
| Variable | Default | Scope | Purpose |
|---|---|---|---|
| `APP_ENV` | `dev` | all | Set `production` to enable strict secret checks. |
| `LOG_FORMAT` | `json` | all | Structured logs (`json` or `text`). |
| `LOG_LEVEL` | `INFO` | all | Logging level. |
| `PROMETHEUS_METRICS_ENABLED` | `true` | all | Enable metrics HTTP server. |

## Database Safety/Timeouts
| Variable | Default | Scope | Purpose |
|---|---|---|---|
| `DB_INIT_ON_STARTUP` | `true` | api | Auto-apply forward migrations on startup. |
| `DB_CONNECT_TIMEOUT_SEC` | `2` | all DB clients | Connection timeout to avoid hangs. |
| `DB_POOL_OPEN_TIMEOUT_SEC` | `5` | all DB clients | Pool acquisition timeout. |
| `DB_STATEMENT_TIMEOUT_MS` | `5000` | all DB sessions | Query timeout at DB session level. |
| `DB_LOCK_TIMEOUT_MS` | `1000` | all DB sessions | Lock wait timeout. |
| `DB_POOL_MIN_SIZE` | `1` | all DB clients | Connection pool min size. |
| `DB_POOL_MAX_SIZE` | `10` | all DB clients | Connection pool max size. |

## API Reliability Controls
| Variable | Default | Purpose |
|---|---|---|
| `RECO_ASYNC_IMPRESSION_LOGGING` | `true` | Async outbox enqueue for auto impressions. |
| `RECO_MAX_BACKGROUND_TASKS` | `1000` | Backpressure threshold for background tasks. |
| `RECO_BACKGROUND_BACKPRESSURE_MODE` | `sync` | `sync` fallback or `drop` under queue saturation. |
| `RECO_BACKGROUND_SHUTDOWN_TIMEOUT_SEC` | `5` | Graceful shutdown drain timeout. |

## Outbox Relay Controls
| Variable | Default | Purpose |
|---|---|---|
| `OUTBOX_MAX_ATTEMPTS` | `25` | Max delivery attempts per outbox row. |
| `OUTBOX_RELAY_POLL_INTERVAL_MS` | `500` | Polling interval for claim cycle. |
| `OUTBOX_RELAY_BATCH_SIZE` | `200` | Claim batch size. |
| `OUTBOX_RELAY_LEASE_SEC` | `30` | Lease timeout for claimed rows. |
| `OUTBOX_RELAY_BASE_RETRY_SEC` | `1` | Base retry backoff. |
| `OUTBOX_RELAY_MAX_RETRY_SEC` | `60` | Max retry backoff cap. |
| `OUTBOX_RELAY_PUBLISH_TIMEOUT_SEC` | `10` | Kafka publish timeout. |
| `OUTBOX_RELAY_STARTUP_MAX_RETRIES` | `0` | `0` means retry indefinitely. |
| `OUTBOX_RELAY_STARTUP_BACKOFF_SEC` | `2` | Startup retry backoff base. |
| `OUTBOX_RELAY_SHUTDOWN_TIMEOUT_SEC` | `15` | Shutdown timeout for producer stop. |

## Feature Consumer Controls
| Variable | Default | Purpose |
|---|---|---|
| `FEATURE_CONSUMER_POLL_TIMEOUT_MS` | `1000` | Kafka poll timeout. |
| `FEATURE_CONSUMER_MAX_POLL_RECORDS` | `500` | Poll batch max. |
| `FEATURE_CONSUMER_STARTUP_MAX_RETRIES` | `0` | `0` means retry indefinitely. |
| `FEATURE_CONSUMER_STARTUP_BACKOFF_SEC` | `2` | Startup retry backoff base. |
| `FEATURE_CONSUMER_SHUTDOWN_TIMEOUT_SEC` | `20` | Shutdown timeout for consumer stop. |

## Cache Controls
| Variable | Default | Purpose |
|---|---|---|
| `ONLINE_CACHE_ENABLED` | `true` | Enable online feature cache. |
| `ONLINE_CACHE_BACKEND` | `memory`/`redis` | Cache backend mode. |
| `ONLINE_CACHE_STRICT_BACKEND` | `false` | If `true`, fail readiness when Redis is unavailable. |
| `ONLINE_CACHE_REDIS_URL` | `redis://localhost:6379/0` | Redis URL for cache backend. |
| `ONLINE_CACHE_REDIS_TIMEOUT_SEC` | `1` | Redis socket timeout. |
| `ONLINE_CACHE_MEMORY_MAX_KEYS` | `100000` | Memory backend key cap. |
| `ONLINE_CACHE_HISTORY_TTL_SEC` | `20` | History cache TTL. |
| `ONLINE_CACHE_RELATED_TTL_SEC` | `30` | Related-items cache TTL. |
| `ONLINE_CACHE_POPULARITY_TTL_SEC` | `5` | Popularity cache TTL. |

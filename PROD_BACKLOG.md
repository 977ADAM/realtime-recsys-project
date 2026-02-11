# Production Readiness Backlog (Gap Analysis)

## P0 (Critical)
| Item | Gap Found | Action | Status |
|---|---|---|---|
| Runtime hangs / timeout safety | DB checks and async thread bridges could block progress | Added DB connect/statement/lock timeouts; removed unstable thread-bridge usage; hardened error handling | Done |
| Graceful degradation + shutdown | Background impression tasks could grow without bounds | Added queue backpressure (`RECO_MAX_BACKGROUND_TASKS`, sync/drop mode), bounded shutdown drain | Done |
| Outbox relay reliability | Missing startup retry policy and loop-level error metrics | Added startup retries/backoff, publish timeout, stop timeout, granular error counters | Done |
| Consumer reliability | Poll/commit/startup failures not fully instrumented | Added retry/backoff, commit/poll error handling, lag metric updates with protected loop | Done |
| Idempotency guarantees | Need explicit duplicate-path validation | Added smoke scenario check for duplicate watch event behavior | Done |
| Observability baseline | No structured logs or request correlation context | Added JSON logs, contextvars-based request/correlation IDs, response headers | Done |

## P1 (High)
| Item | Gap Found | Action | Status |
|---|---|---|---|
| Secret hardening | Insecure defaults could leak into production | Added `APP_ENV=production` runtime validation to block placeholder credentials | Done |
| Container hardening | Root containers and weak runtime security defaults | Non-root Docker image, no-new-privileges, dropped capabilities, read-only root FS for app workers | Done |
| Health/readiness visibility | Missing metrics for check status | Added `reco_api_check_status` gauge + outbox snapshot propagation | Done |
| Migration safety automation | Manual-only migration checks | Added `src/check_migrations.py` (naming/order/checksum/drift) + tests + CI integration | Done |
| CI/CD coverage | No lint, no compose smoke, weak artifacts | CI now runs lint/tests/migration checks, Docker compose smoke, load/guardrails, artifact uploads | Done |
| Reproducible runbook | Missing deployment/incident docs | Added runbook, incident+rollback plan, env matrix, readiness report | Done |

## P2 (Medium)
| Item | Gap Found | Action | Status |
|---|---|---|---|
| Advanced alerting stack | No packaged Grafana/Alertmanager manifests in repo | Documented minimum metrics/alerts in runbook; infra templates can be added later | Deferred |
| Blue/green/canary rollout automation | No deployment orchestrator manifests | Documented rollback strategy; can extend to Helm/Terraform pipeline | Deferred |
| Chaos/fault-injection tests | No dedicated chaos suite | Added smoke + load + guardrails automation; chaos plan remains future work | Deferred |

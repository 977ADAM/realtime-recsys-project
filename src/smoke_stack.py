#!/usr/bin/env python3
import argparse
import json
import time
import urllib.error
import urllib.parse
import urllib.request
import uuid
from typing import Any


def _request_json(
    url: str,
    *,
    method: str,
    timeout_sec: float,
    payload: dict[str, Any] | None = None,
) -> tuple[int, dict[str, Any]]:
    data = None
    headers = {"Accept": "application/json"}
    if payload is not None:
        data = json.dumps(payload, ensure_ascii=True).encode("utf-8")
        headers["Content-Type"] = "application/json"

    request = urllib.request.Request(url, method=method, data=data, headers=headers)
    try:
        with urllib.request.urlopen(request, timeout=timeout_sec) as response:
            status = int(response.getcode())
            raw = response.read().decode("utf-8")
            return status, json.loads(raw) if raw else {}
    except urllib.error.HTTPError as exc:
        raw = exc.read().decode("utf-8")
        try:
            body = json.loads(raw) if raw else {}
        except Exception:
            body = {"raw": raw}
        return int(exc.code), body


def _wait_ready(base_url: str, timeout_sec: float, poll_sec: float) -> dict[str, Any]:
    deadline = time.time() + timeout_sec
    last_payload: dict[str, Any] = {}
    while time.time() < deadline:
        status, payload = _request_json(
            f"{base_url}/readyz",
            method="GET",
            timeout_sec=min(timeout_sec, 5.0),
        )
        last_payload = payload
        if status == 200 and bool(payload.get("ready")):
            return payload
        time.sleep(max(poll_sec, 0.1))
    raise RuntimeError(f"readyz did not become ready before timeout; last_payload={last_payload}")


def _seed_events(base_url: str, timeout_sec: float, users: int, items: int, rounds: int) -> int:
    inserted = 0
    item_ids = [f"seed-item-{idx}" for idx in range(items)]
    for round_idx in range(rounds):
        for user_idx in range(users):
            user_id = f"seed-user-{user_idx}"
            for item_id in item_ids:
                payload = {
                    "event_id": uuid.uuid4().hex,
                    "user_id": user_id,
                    "item_id": item_id,
                    "event_type": "view" if round_idx == 0 else "click",
                    "ts": int(time.time() * 1000),
                }
                status, body = _request_json(
                    f"{base_url}/event",
                    method="POST",
                    payload=payload,
                    timeout_sec=timeout_sec,
                )
                if status == 200 and body.get("status") in {"ok", "duplicate"}:
                    inserted += 1
    return inserted


def _generate_reco_and_watches(base_url: str, timeout_sec: float, requests: int) -> dict[str, Any]:
    reco_ok = 0
    watch_ok = 0
    empty_reco = 0

    for idx in range(requests):
        user_id = f"smoke-user-{idx % 5}"
        session_id = f"smoke-session-{idx}"
        query = urllib.parse.urlencode(
            {
                "user_id": user_id,
                "session_id": session_id,
                "k": "5",
                "x_autolog_impressions": "true",
            }
        )
        status, reco = _request_json(
            f"{base_url}/reco?{query}",
            method="GET",
            timeout_sec=timeout_sec,
        )
        if status != 200:
            continue
        reco_ok += 1
        items = reco.get("items", [])
        if not items:
            empty_reco += 1
            continue

        watch_payload = {
            "event_id": uuid.uuid4().hex,
            "user_id": user_id,
            "session_id": session_id,
            "request_id": reco.get("request_id", uuid.uuid4().hex),
            "item_id": str(items[0]),
            "ts_ms": int(time.time() * 1000),
            "watch_time_sec": 60.0,
            "percent_watched": 95.0,
            "ended": True,
        }
        watch_status, watch_body = _request_json(
            f"{base_url}/log/watch",
            method="POST",
            payload=watch_payload,
            timeout_sec=timeout_sec,
        )
        if watch_status == 200 and watch_body.get("status") in {"ok", "duplicate"}:
            watch_ok += 1

    return {
        "reco_ok": reco_ok,
        "watch_ok": watch_ok,
        "empty_reco": empty_reco,
    }


def _validate_duplicate_watch(base_url: str, timeout_sec: float) -> bool:
    event_id = uuid.uuid4().hex
    payload = {
        "event_id": event_id,
        "user_id": "duplicate-check-user",
        "session_id": "duplicate-check-session",
        "request_id": "duplicate-check-request",
        "item_id": "seed-item-0",
        "ts_ms": int(time.time() * 1000),
        "watch_time_sec": 45.0,
        "percent_watched": 90.0,
        "ended": True,
    }
    status_a, body_a = _request_json(
        f"{base_url}/log/watch",
        method="POST",
        payload=payload,
        timeout_sec=timeout_sec,
    )
    status_b, body_b = _request_json(
        f"{base_url}/log/watch",
        method="POST",
        payload=payload,
        timeout_sec=timeout_sec,
    )
    return (
        status_a == 200
        and status_b == 200
        and body_a.get("status") in {"ok", "duplicate"}
        and body_b.get("status") == "duplicate"
    )


def _wait_outbox_settled(
    base_url: str,
    timeout_sec: float,
    poll_sec: float,
    max_pending: int,
) -> dict[str, Any]:
    deadline = time.time() + timeout_sec
    last_outbox = {}
    while time.time() < deadline:
        status, health = _request_json(
            f"{base_url}/healthz",
            method="GET",
            timeout_sec=min(timeout_sec, 5.0),
        )
        if status != 200:
            time.sleep(max(poll_sec, 0.1))
            continue
        outbox = (
            health.get("event_backbone", {})
            .get("outbox", {})
        )
        if not isinstance(outbox, dict):
            time.sleep(max(poll_sec, 0.1))
            continue
        last_outbox = outbox
        pending = int(outbox.get("pending", 0) or 0)
        publishing = int(outbox.get("publishing", 0) or 0)
        dead = int(outbox.get("dead", 0) or 0)
        if dead == 0 and (pending + publishing) <= max_pending:
            return outbox
        time.sleep(max(poll_sec, 0.1))
    raise RuntimeError(f"outbox did not settle before timeout; last_outbox={last_outbox}")


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Run end-to-end smoke checks against running realtime-recsys stack.")
    parser.add_argument("--base-url", default="http://localhost:8080", help="API base URL")
    parser.add_argument("--timeout-sec", type=float, default=3.0, help="HTTP timeout per request")
    parser.add_argument("--ready-timeout-sec", type=float, default=90.0, help="Readiness wait timeout")
    parser.add_argument("--poll-sec", type=float, default=2.0, help="Polling interval")
    parser.add_argument("--seed-users", type=int, default=5, help="Users to seed via /event")
    parser.add_argument("--seed-items", type=int, default=5, help="Items to seed via /event")
    parser.add_argument("--seed-rounds", type=int, default=2, help="How many rounds of /event seeding to run")
    parser.add_argument("--reco-requests", type=int, default=30, help="How many /reco requests to issue")
    parser.add_argument("--outbox-timeout-sec", type=float, default=120.0, help="Timeout for outbox settle wait")
    parser.add_argument("--max-outbox-pending", type=int, default=0, help="Allowed pending+publishing rows")
    parser.add_argument("--output-json", default="", help="Optional report path")
    return parser.parse_args()


def main() -> int:
    args = parse_args()
    base_url = args.base_url.rstrip("/")

    checks = {}
    _wait_ready(base_url, timeout_sec=args.ready_timeout_sec, poll_sec=args.poll_sec)
    checks["readyz"] = "pass"

    seeded = _seed_events(
        base_url,
        timeout_sec=max(args.timeout_sec, 0.1),
        users=max(args.seed_users, 1),
        items=max(args.seed_items, 1),
        rounds=max(args.seed_rounds, 1),
    )
    checks["seed_events"] = "pass" if seeded > 0 else "fail"

    traffic = _generate_reco_and_watches(
        base_url,
        timeout_sec=max(args.timeout_sec, 0.1),
        requests=max(args.reco_requests, 1),
    )
    checks["reco_traffic"] = "pass" if traffic["reco_ok"] > 0 else "fail"
    checks["watch_traffic"] = "pass" if traffic["watch_ok"] > 0 else "fail"

    duplicate_ok = _validate_duplicate_watch(base_url, timeout_sec=max(args.timeout_sec, 0.1))
    checks["watch_idempotency"] = "pass" if duplicate_ok else "fail"

    outbox_snapshot = _wait_outbox_settled(
        base_url,
        timeout_sec=max(args.outbox_timeout_sec, 1.0),
        poll_sec=max(args.poll_sec, 0.1),
        max_pending=max(args.max_outbox_pending, 0),
    )
    checks["outbox_settled"] = "pass"

    _, reco_metrics = _request_json(
        f"{base_url}/metrics/reco",
        method="GET",
        timeout_sec=max(args.timeout_sec, 0.1),
    )
    sample_size = int(reco_metrics.get("sample_size") or 0)
    checks["reco_metrics_samples"] = "pass" if sample_size > 0 else "fail"

    report = {
        "timestamp_ms": int(time.time() * 1000),
        "checks": checks,
        "traffic": traffic,
        "seeded_events": seeded,
        "outbox_snapshot": outbox_snapshot,
        "reco_metrics_sample_size": sample_size,
    }

    payload = json.dumps(report, ensure_ascii=True, indent=2)
    print(payload)
    if args.output_json:
        with open(args.output_json, "w", encoding="utf-8") as f:
            f.write(payload + "\n")

    failed = [name for name, status in checks.items() if status != "pass"]
    return 1 if failed else 0


if __name__ == "__main__":
    raise SystemExit(main())

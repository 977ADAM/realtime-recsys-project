#!/usr/bin/env python3
import argparse
import concurrent.futures
import json
import random
import statistics
import time
import urllib.error
import urllib.parse
import urllib.request
from dataclasses import dataclass


@dataclass
class RequestResult:
    latency_ms: float
    status_code: int
    ok: bool
    error: str = ""


def percentile(sorted_values: list[float], q: float) -> float:
    if not sorted_values:
        return 0.0
    if len(sorted_values) == 1:
        return sorted_values[0]

    position = (len(sorted_values) - 1) * q
    low = int(position)
    high = min(low + 1, len(sorted_values) - 1)
    if low == high:
        return sorted_values[low]

    left = sorted_values[low]
    right = sorted_values[high]
    return left + (right - left) * (position - low)


def read_json(url: str, timeout_sec: float) -> dict:
    req = urllib.request.Request(url, method="GET")
    with urllib.request.urlopen(req, timeout=timeout_sec) as response:
        return json.loads(response.read().decode("utf-8"))


def run_single_request(base_url: str, k: int, timeout_sec: float, autolog: bool) -> RequestResult:
    user_id = f"load-user-{random.randint(1, 5000)}"
    session_id = f"session-{random.randint(1, 500000)}"
    query = urllib.parse.urlencode(
        {
            "user_id": user_id,
            "session_id": session_id,
            "k": str(k),
            "x_autolog_impressions": "true" if autolog else "false",
        }
    )
    url = f"{base_url.rstrip('/')}/reco?{query}"

    started = time.perf_counter()
    try:
        request = urllib.request.Request(url, method="GET")
        with urllib.request.urlopen(request, timeout=timeout_sec) as response:
            _ = response.read()
            status_code = int(response.getcode())
        latency_ms = (time.perf_counter() - started) * 1000.0
        return RequestResult(latency_ms=latency_ms, status_code=status_code, ok=(200 <= status_code < 300))
    except urllib.error.HTTPError as exc:
        latency_ms = (time.perf_counter() - started) * 1000.0
        return RequestResult(
            latency_ms=latency_ms,
            status_code=int(exc.code),
            ok=False,
            error=f"HTTPError: {exc.code}",
        )
    except Exception as exc:
        latency_ms = (time.perf_counter() - started) * 1000.0
        return RequestResult(
            latency_ms=latency_ms,
            status_code=0,
            ok=False,
            error=f"{exc.__class__.__name__}: {exc}",
        )


def summarize(results: list[RequestResult], elapsed_sec: float) -> dict:
    if not results:
        return {
            "requests_total": 0,
            "requests_ok": 0,
            "requests_failed": 0,
            "error_rate_pct": 100.0,
            "rps": 0.0,
            "latency_ms": {"p50": 0.0, "p95": 0.0, "p99": 0.0, "mean": 0.0, "max": 0.0},
            "status_codes": {},
            "errors": {},
        }

    latencies = sorted(result.latency_ms for result in results)
    ok_count = sum(1 for result in results if result.ok)
    failures = len(results) - ok_count

    status_counts: dict[str, int] = {}
    error_counts: dict[str, int] = {}
    for result in results:
        status_key = str(result.status_code)
        status_counts[status_key] = status_counts.get(status_key, 0) + 1
        if result.error:
            error_counts[result.error] = error_counts.get(result.error, 0) + 1

    return {
        "requests_total": len(results),
        "requests_ok": ok_count,
        "requests_failed": failures,
        "error_rate_pct": round((failures / len(results)) * 100.0, 3),
        "rps": round((len(results) / elapsed_sec), 3) if elapsed_sec > 0 else 0.0,
        "latency_ms": {
            "p50": round(percentile(latencies, 0.50), 3),
            "p95": round(percentile(latencies, 0.95), 3),
            "p99": round(percentile(latencies, 0.99), 3),
            "mean": round(statistics.fmean(latencies), 3),
            "max": round(max(latencies), 3),
        },
        "status_codes": status_counts,
        "errors": error_counts,
    }


def run_load(base_url: str, total_requests: int, concurrency: int, k: int, timeout_sec: float, autolog: bool) -> tuple[list[RequestResult], float]:
    started = time.perf_counter()
    with concurrent.futures.ThreadPoolExecutor(max_workers=concurrency) as pool:
        futures = [pool.submit(run_single_request, base_url, k, timeout_sec, autolog) for _ in range(total_requests)]
        results = [future.result() for future in futures]
    elapsed_sec = time.perf_counter() - started
    return results, elapsed_sec


def evaluate_sla(summary: dict, contract: dict) -> dict:
    sla_targets = contract.get("sla_targets", {}) if isinstance(contract, dict) else {}
    p95_target = float(sla_targets.get("reco_p95_ms", 0.0) or 0.0)
    p99_target = float(sla_targets.get("reco_p99_ms", 0.0) or 0.0)
    error_target = float(sla_targets.get("api_error_rate_pct", 0.0) or 0.0)

    p95 = float(summary["latency_ms"]["p95"])
    p99 = float(summary["latency_ms"]["p99"])
    error_rate = float(summary["error_rate_pct"])

    return {
        "targets": {
            "reco_p95_ms": p95_target,
            "reco_p99_ms": p99_target,
            "api_error_rate_pct": error_target,
        },
        "observed": {
            "reco_p95_ms": p95,
            "reco_p99_ms": p99,
            "api_error_rate_pct": error_rate,
        },
        "met": {
            "reco_p95_ms": (p95 <= p95_target) if p95_target > 0 else None,
            "reco_p99_ms": (p99 <= p99_target) if p99_target > 0 else None,
            "api_error_rate_pct": (error_rate <= error_target) if error_target > 0 else None,
        },
    }


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Load-test /reco endpoint and report p95/p99/error rate.")
    parser.add_argument("--base-url", default="http://localhost:8000", help="API base URL")
    parser.add_argument("--requests", type=int, default=2000, help="Total requests")
    parser.add_argument("--concurrency", type=int, default=50, help="Concurrent workers")
    parser.add_argument("--k", type=int, default=20, help="Recommendations count parameter")
    parser.add_argument("--timeout-sec", type=float, default=2.0, help="Per-request timeout")
    parser.add_argument("--warmup", type=int, default=200, help="Warmup requests count")
    parser.add_argument(
        "--autolog-impressions",
        action="store_true",
        help="Enable impression auto-logging in /reco calls during test",
    )
    parser.add_argument(
        "--output-json",
        default="",
        help="Optional output path for JSON report",
    )
    return parser.parse_args()


def main() -> int:
    args = parse_args()

    print("[load] warmup started")
    _, warmup_elapsed = run_load(
        base_url=args.base_url,
        total_requests=max(args.warmup, 0),
        concurrency=max(args.concurrency, 1),
        k=max(args.k, 1),
        timeout_sec=max(args.timeout_sec, 0.1),
        autolog=args.autolog_impressions,
    )
    print(f"[load] warmup done in {warmup_elapsed:.3f}s")

    print("[load] benchmark started")
    results, elapsed_sec = run_load(
        base_url=args.base_url,
        total_requests=max(args.requests, 1),
        concurrency=max(args.concurrency, 1),
        k=max(args.k, 1),
        timeout_sec=max(args.timeout_sec, 0.1),
        autolog=args.autolog_impressions,
    )
    summary = summarize(results, elapsed_sec)

    contract = {}
    metrics_snapshot = {}
    try:
        contract = read_json(f"{args.base_url.rstrip('/')}/contract", timeout_sec=args.timeout_sec)
    except Exception as exc:
        contract = {"error": f"{exc.__class__.__name__}: {exc}"}

    try:
        metrics_snapshot = read_json(f"{args.base_url.rstrip('/')}/metrics/reco", timeout_sec=args.timeout_sec)
    except Exception as exc:
        metrics_snapshot = {"error": f"{exc.__class__.__name__}: {exc}"}

    report = {
        "timestamp_ms": int(time.time() * 1000),
        "config": {
            "base_url": args.base_url,
            "requests": args.requests,
            "concurrency": args.concurrency,
            "k": args.k,
            "timeout_sec": args.timeout_sec,
            "warmup": args.warmup,
            "autolog_impressions": bool(args.autolog_impressions),
        },
        "benchmark_elapsed_sec": round(elapsed_sec, 3),
        "summary": summary,
        "sla": evaluate_sla(summary, contract),
        "contract": contract,
        "reco_metrics_snapshot": metrics_snapshot,
    }

    payload = json.dumps(report, ensure_ascii=True, indent=2)
    print(payload)

    if args.output_json:
        with open(args.output_json, "w", encoding="utf-8") as f:
            f.write(payload + "\n")
        print(f"[load] saved report to {args.output_json}")

    failure_rate = summary["error_rate_pct"]
    if failure_rate > 10.0:
        return 2
    return 0


if __name__ == "__main__":
    raise SystemExit(main())

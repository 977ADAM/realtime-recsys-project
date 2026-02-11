#!/usr/bin/env python3
import argparse
import json
import re
from pathlib import Path
from typing import Any, Optional


def _load_json(path: Path) -> dict:
    payload = json.loads(path.read_text(encoding="utf-8"))
    if not isinstance(payload, dict):
        raise ValueError(f"Expected JSON object in {path}, got {type(payload).__name__}")
    return payload


def _to_float(value: Any) -> Optional[float]:
    if isinstance(value, (int, float)):
        return float(value)
    if value is None:
        return None
    match = re.search(r"-?\d+(?:\.\d+)?", str(value))
    if match is None:
        return None
    return float(match.group(0))


def _new_check(
    name: str,
    observed: Optional[float],
    target: Optional[float],
    comparator: str,
    unit: str,
) -> dict:
    if observed is None or target is None:
        return {
            "name": name,
            "status": "unknown",
            "observed": observed,
            "target": target,
            "comparator": comparator,
            "unit": unit,
            "message": "missing observed value or target",
        }

    if comparator == ">=":
        passed = observed >= target
    elif comparator == "<=":
        passed = observed <= target
    else:
        raise ValueError(f"Unsupported comparator: {comparator}")

    return {
        "name": name,
        "status": "pass" if passed else "fail",
        "observed": observed,
        "target": target,
        "comparator": comparator,
        "unit": unit,
    }


def evaluate_guardrails(
    baseline: dict,
    contract: dict,
    reco_metrics: dict,
    min_sla_samples: int,
) -> dict:
    checks = []

    kpi_targets = contract.get("kpi_targets", {})
    sla_targets = contract.get("sla_targets", {})
    metrics = baseline.get("metrics", baseline)
    if not isinstance(metrics, dict):
        metrics = {}

    checks.append(
        _new_check(
            name="ctr_at_20_pct",
            observed=_to_float(metrics.get("ctr_at_20_pct")),
            target=_to_float(kpi_targets.get("ctr_at_20")),
            comparator=">=",
            unit="pct",
        )
    )
    checks.append(
        _new_check(
            name="avg_watch_time_sec",
            observed=_to_float(metrics.get("avg_watch_time_sec")),
            target=_to_float(kpi_targets.get("avg_watch_time_sec")),
            comparator=">=",
            unit="sec",
        )
    )
    checks.append(
        _new_check(
            name="catalog_coverage_at_20_pct",
            observed=_to_float(metrics.get("catalog_coverage_at_20_pct")),
            target=_to_float(kpi_targets.get("catalog_coverage_at_20")),
            comparator=">=",
            unit="pct",
        )
    )

    sample_size = _to_float(reco_metrics.get("sample_size"))
    latency = reco_metrics.get("latency_ms", {})
    if not isinstance(latency, dict):
        latency = {}
    if sample_size is not None and sample_size >= float(max(min_sla_samples, 0)):
        checks.append(
            _new_check(
                name="reco_p95_ms",
                observed=_to_float(latency.get("p95")),
                target=_to_float(sla_targets.get("reco_p95_ms")),
                comparator="<=",
                unit="ms",
            )
        )
        checks.append(
            _new_check(
                name="reco_p99_ms",
                observed=_to_float(latency.get("p99")),
                target=_to_float(sla_targets.get("reco_p99_ms")),
                comparator="<=",
                unit="ms",
            )
        )
    else:
        checks.append(
            {
                "name": "reco_latency_checks",
                "status": "skipped",
                "observed": sample_size,
                "target": float(max(min_sla_samples, 0)),
                "comparator": ">=",
                "unit": "samples",
                "message": "sample size is below threshold for stable SLA latency checks",
            }
        )

    checks.append(
        _new_check(
            name="api_error_rate_pct",
            observed=_to_float(reco_metrics.get("error_rate_pct")),
            target=_to_float(sla_targets.get("api_error_rate_pct")),
            comparator="<=",
            unit="pct",
        )
    )

    counts = {"pass": 0, "fail": 0, "unknown": 0, "skipped": 0}
    for check in checks:
        counts[check["status"]] = counts.get(check["status"], 0) + 1

    return {
        "checks": checks,
        "summary": {
            "passed": counts["pass"],
            "failed": counts["fail"],
            "unknown": counts["unknown"],
            "skipped": counts["skipped"],
            "min_sla_samples": int(max(min_sla_samples, 0)),
        },
    }


def _print_report(report: dict) -> None:
    for check in report["checks"]:
        status = check["status"].upper()
        name = check["name"]
        observed = check.get("observed")
        comparator = check.get("comparator")
        target = check.get("target")
        unit = check.get("unit")
        message = check.get("message")
        details = f"observed={observed} {comparator} target={target} ({unit})"
        if message:
            details = f"{details}; {message}"
        print(f"[{status}] {name}: {details}")

    summary = report["summary"]
    print(
        "[SUMMARY] "
        f"passed={summary['passed']} failed={summary['failed']} "
        f"unknown={summary['unknown']} skipped={summary['skipped']}"
    )


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Evaluate KPI and SLA guardrails from JSON snapshots.")
    parser.add_argument("--baseline-json", default="baseline-kpi.json", help="Path to baseline KPI snapshot.")
    parser.add_argument("--contract-json", default="contract.json", help="Path to contract snapshot.")
    parser.add_argument("--reco-metrics-json", default="reco-metrics.json", help="Path to /metrics/reco snapshot.")
    parser.add_argument("--min-sla-samples", type=int, default=100, help="Minimum /metrics/reco sample_size for latency SLA checks.")
    parser.add_argument("--output-json", default="", help="Optional path to JSON report.")
    return parser.parse_args()


def main() -> int:
    args = parse_args()

    baseline = _load_json(Path(args.baseline_json))
    contract = _load_json(Path(args.contract_json))
    reco_metrics = _load_json(Path(args.reco_metrics_json))

    report = evaluate_guardrails(
        baseline=baseline,
        contract=contract,
        reco_metrics=reco_metrics,
        min_sla_samples=max(args.min_sla_samples, 0),
    )
    _print_report(report)

    if args.output_json:
        Path(args.output_json).write_text(
            json.dumps(report, ensure_ascii=True, indent=2) + "\n",
            encoding="utf-8",
        )

    if report["summary"]["failed"] > 0 or report["summary"]["unknown"] > 0:
        return 1
    return 0


if __name__ == "__main__":
    raise SystemExit(main())

from src.check_guardrails import evaluate_guardrails


def test_evaluate_guardrails_reports_pass_for_healthy_snapshot():
    baseline = {
        "metrics": {
            "ctr_at_20_pct": 10.0,
            "avg_watch_time_sec": 40.0,
            "catalog_coverage_at_20_pct": 70.0,
        }
    }
    contract = {
        "kpi_targets": {
            "ctr_at_20": ">= 8%",
            "avg_watch_time_sec": ">= 35",
            "catalog_coverage_at_20": ">= 60%",
        },
        "sla_targets": {
            "reco_p95_ms": 120,
            "reco_p99_ms": 200,
            "api_error_rate_pct": 1.0,
        },
    }
    reco = {
        "sample_size": 1000,
        "latency_ms": {"p95": 80.0, "p99": 120.0},
        "error_rate_pct": 0.1,
    }

    report = evaluate_guardrails(
        baseline=baseline,
        contract=contract,
        reco_metrics=reco,
        min_sla_samples=100,
    )

    assert report["summary"]["failed"] == 0
    assert report["summary"]["unknown"] == 0


def test_evaluate_guardrails_skips_latency_for_small_sample():
    baseline = {
        "metrics": {
            "ctr_at_20_pct": 8.0,
            "avg_watch_time_sec": 35.0,
            "catalog_coverage_at_20_pct": 60.0,
        }
    }
    contract = {
        "kpi_targets": {
            "ctr_at_20": ">= 8%",
            "avg_watch_time_sec": ">= 35",
            "catalog_coverage_at_20": ">= 60%",
        },
        "sla_targets": {
            "reco_p95_ms": 120,
            "reco_p99_ms": 200,
            "api_error_rate_pct": 1.0,
        },
    }
    reco = {
        "sample_size": 50,
        "latency_ms": {"p95": 999.0, "p99": 999.0},
        "error_rate_pct": 0.1,
    }

    report = evaluate_guardrails(
        baseline=baseline,
        contract=contract,
        reco_metrics=reco,
        min_sla_samples=100,
    )

    statuses = {check["name"]: check["status"] for check in report["checks"]}
    assert statuses["reco_latency_checks"] == "skipped"
    assert report["summary"]["failed"] == 0

from src.check_migrations import compare_local_and_applied, inspect_local_migrations


def test_inspect_local_migrations_reports_invalid_filename(tmp_path):
    (tmp_path / "bad-name.sql").write_text("SELECT 1;", encoding="utf-8")
    report = inspect_local_migrations(tmp_path)
    assert report["issues"]
    assert "invalid migration filename" in report["issues"][0]


def test_compare_local_and_applied_detects_drift_and_pending():
    local = {
        "migrations": [
            {"file": "0001_initial.sql", "checksum": "aaa"},
            {"file": "0002_next.sql", "checksum": "bbb"},
        ]
    }
    applied = {
        "migrations": [
            {"version": "0001_initial.sql", "checksum": "ccc"},
        ]
    }

    comparison = compare_local_and_applied(local, applied)

    assert comparison["pending"] == ["0002_next.sql"]
    assert len(comparison["issues"]) == 1
    assert "checksum drift" in comparison["issues"][0]

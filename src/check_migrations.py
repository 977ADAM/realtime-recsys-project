#!/usr/bin/env python3
import argparse
import json
import os
import re
import sys
from pathlib import Path
from typing import Any

import psycopg
from psycopg.rows import dict_row

PROJECT_ROOT = Path(__file__).resolve().parents[1]
if str(PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(PROJECT_ROOT))

from app.db import MIGRATIONS_DIR, MIGRATIONS_TABLE, _migration_checksum, _migration_files


_MIGRATION_NAME_PATTERN = re.compile(r"^(?P<seq>\d{4})_[a-z0-9_]+\.sql$")


def inspect_local_migrations(migrations_dir: Path) -> dict[str, Any]:
    issues: list[str] = []
    rows: list[dict[str, Any]] = []
    seq_to_file: dict[int, str] = {}

    previous_seq: int | None = None
    for path in _migration_files(migrations_dir):
        match = _MIGRATION_NAME_PATTERN.match(path.name)
        if match is None:
            issues.append(
                f"{path.name}: invalid migration filename. "
                "Expected pattern like 0001_initial_schema.sql"
            )
            continue

        seq = int(match.group("seq"))
        if seq in seq_to_file:
            issues.append(f"{path.name}: duplicate sequence number already used by {seq_to_file[seq]}")
        seq_to_file[seq] = path.name

        if previous_seq is not None and seq <= previous_seq:
            issues.append(
                f"{path.name}: sequence is not strictly increasing (previous={previous_seq:04d}, current={seq:04d})"
            )
        previous_seq = seq

        ddl = path.read_text(encoding="utf-8")
        checksum = _migration_checksum(ddl)
        rows.append(
            {
                "file": path.name,
                "seq": seq,
                "checksum": checksum,
            }
        )

    rows.sort(key=lambda entry: entry["seq"])
    return {
        "dir": str(migrations_dir),
        "count": len(rows),
        "migrations": rows,
        "issues": issues,
    }


def inspect_applied_migrations(database_url: str) -> dict[str, Any]:
    with psycopg.connect(database_url, row_factory=dict_row, connect_timeout=3) as conn:
        with conn.cursor() as cur:
            cur.execute(
                "SELECT to_regclass(%s) AS table_name",
                (MIGRATIONS_TABLE,),
            )
            table_row = cur.fetchone() or {}
            table_name = table_row.get("table_name")
            if table_name is None:
                return {
                    "table": MIGRATIONS_TABLE,
                    "count": 0,
                    "migrations": [],
                    "issues": [f"table {MIGRATIONS_TABLE} is missing"],
                }

            cur.execute(
                f"SELECT version, checksum FROM {MIGRATIONS_TABLE} ORDER BY version ASC"
            )
            rows = cur.fetchall()

    return {
        "table": MIGRATIONS_TABLE,
        "count": len(rows),
        "migrations": [
            {
                "version": str(row["version"]),
                "checksum": str(row["checksum"]),
            }
            for row in rows
        ],
        "issues": [],
    }


def compare_local_and_applied(local: dict[str, Any], applied: dict[str, Any]) -> dict[str, Any]:
    issues: list[str] = []
    local_checksums = {entry["file"]: entry["checksum"] for entry in local.get("migrations", [])}
    applied_checksums = {entry["version"]: entry["checksum"] for entry in applied.get("migrations", [])}

    for version, checksum in applied_checksums.items():
        local_checksum = local_checksums.get(version)
        if local_checksum is None:
            issues.append(f"applied migration {version} is missing locally")
            continue
        if checksum != local_checksum:
            issues.append(
                f"checksum drift for {version}: applied={checksum} local={local_checksum}"
            )

    pending = sorted(version for version in local_checksums if version not in applied_checksums)
    return {
        "pending": pending,
        "issues": issues,
    }


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Validate migration naming/ordering and optionally verify applied checksums."
    )
    parser.add_argument(
        "--migrations-dir",
        default=str(MIGRATIONS_DIR),
        help="Path to local migrations directory.",
    )
    parser.add_argument(
        "--database-url",
        default="",
        help="Optional database URL for checking applied checksums against schema_migrations table.",
    )
    parser.add_argument(
        "--skip-db-check",
        action="store_true",
        help="Skip schema_migrations validation against a live database.",
    )
    parser.add_argument(
        "--output-json",
        default="",
        help="Optional file path for machine-readable report output.",
    )
    return parser.parse_args()


def main() -> int:
    args = parse_args()
    migrations_dir = Path(args.migrations_dir).resolve()
    local_report = inspect_local_migrations(migrations_dir)

    report: dict[str, Any] = {
        "local": local_report,
        "db": {"status": "skipped"},
        "comparison": {"pending": [], "issues": []},
    }

    issues = list(local_report["issues"])

    if args.skip_db_check:
        report["db"] = {"status": "skipped", "reason": "flag --skip-db-check is set"}
    else:
        database_url = args.database_url.strip() or os.getenv("DATABASE_URL", "").strip()
        if not database_url:
            report["db"] = {"status": "skipped", "reason": "DATABASE_URL is not set"}
        else:
            try:
                applied_report = inspect_applied_migrations(database_url)
            except Exception as exc:
                report["db"] = {
                    "status": "failed",
                    "issues": [f"{exc.__class__.__name__}: {exc}"],
                }
                issues.append(f"failed to read applied migrations: {exc.__class__.__name__}: {exc}")
            else:
                report["db"] = {
                    "status": "ok" if not applied_report["issues"] else "failed",
                    **applied_report,
                }
                issues.extend(applied_report["issues"])

                comparison = compare_local_and_applied(local_report, applied_report)
                report["comparison"] = comparison
                issues.extend(comparison["issues"])

    report["summary"] = {
        "local_count": int(local_report["count"]),
        "pending_count": int(len(report["comparison"].get("pending", []))),
        "issues_count": int(len(issues)),
        "status": "ok" if not issues else "failed",
    }

    print(json.dumps(report, ensure_ascii=True, indent=2))
    if args.output_json:
        Path(args.output_json).write_text(
            json.dumps(report, ensure_ascii=True, indent=2) + "\n",
            encoding="utf-8",
        )

    return 1 if issues else 0


if __name__ == "__main__":
    raise SystemExit(main())

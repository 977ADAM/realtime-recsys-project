#!/usr/bin/env python3
import argparse
import json
import sys
from pathlib import Path

PROJECT_ROOT = Path(__file__).resolve().parents[1]
if str(PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(PROJECT_ROOT))

from app.db import (
    get_feature_consumer_dlq_snapshot,
    list_feature_consumer_dlq_events,
    mark_feature_consumer_dlq_replay_failed,
    mark_feature_consumer_dlq_replayed,
    process_watch_event_from_stream,
)
from app.schemas import WatchEvent


def _resolve_statuses(value: str) -> list[str]:
    normalized = (value or "pending").strip().lower()
    if normalized == "all":
        return ["pending", "replayed"]
    if normalized in {"pending", "replayed"}:
        return [normalized]
    raise ValueError(f"Unsupported --status value: {value}")


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Replay feature-consumer DLQ events from Postgres.")
    parser.add_argument("--topic", default="", help="Optional source topic filter.")
    parser.add_argument(
        "--status",
        default="pending",
        choices=("pending", "replayed", "all"),
        help="DLQ row status filter.",
    )
    parser.add_argument("--limit", type=int, default=100, help="Maximum DLQ rows to process.")
    parser.add_argument("--dry-run", action="store_true", help="List candidate rows without replay.")
    parser.add_argument("--output-json", default="", help="Optional path to JSON report.")
    return parser.parse_args()


def main() -> int:
    args = parse_args()
    topic = args.topic.strip() or None
    statuses = _resolve_statuses(args.status)

    rows = list_feature_consumer_dlq_events(
        limit=max(int(args.limit), 1),
        topic=topic,
        statuses=statuses,
    )

    replayed = 0
    replay_duplicates = 0
    replay_failed = 0
    mark_failures = 0
    details: list[dict] = []

    for row in rows:
        row_id = int(row.get("id") or 0)
        row_topic = str(row.get("source_topic") or "")
        row_partition = int(row.get("source_partition") or 0)
        row_offset = int(row.get("source_offset") or 0)
        payload = row.get("payload")

        if args.dry_run:
            details.append(
                {
                    "id": row_id,
                    "topic": row_topic,
                    "partition": row_partition,
                    "offset": row_offset,
                    "status": "candidate",
                }
            )
            continue

        try:
            if not isinstance(payload, dict):
                raise ValueError(f"DLQ payload must be object, got {type(payload).__name__}")
            event = WatchEvent.model_validate(payload)
            result = process_watch_event_from_stream(
                event,
                source_topic=row_topic,
                source_partition=row_partition,
                source_offset=row_offset,
            )
        except Exception as exc:
            replay_failed += 1
            error_text = f"{exc.__class__.__name__}: {exc}"
            try:
                mark_feature_consumer_dlq_replay_failed(row_id, error_text=error_text)
            except Exception as mark_exc:
                mark_failures += 1
                error_text = f"{error_text}; mark_failed_error={mark_exc.__class__.__name__}: {mark_exc}"
            details.append(
                {
                    "id": row_id,
                    "topic": row_topic,
                    "partition": row_partition,
                    "offset": row_offset,
                    "status": "failed",
                    "error": error_text,
                }
            )
            continue

        try:
            mark_feature_consumer_dlq_replayed(row_id)
        except Exception as exc:
            mark_failures += 1
            replay_failed += 1
            details.append(
                {
                    "id": row_id,
                    "topic": row_topic,
                    "partition": row_partition,
                    "offset": row_offset,
                    "status": "failed",
                    "error": f"mark_replayed_failed={exc.__class__.__name__}: {exc}",
                }
            )
            continue

        status = str(result.get("status", "processed"))
        replayed += 1
        if status == "duplicate":
            replay_duplicates += 1
        details.append(
            {
                "id": row_id,
                "topic": row_topic,
                "partition": row_partition,
                "offset": row_offset,
                "status": status,
                "stream_event_id": result.get("stream_event_id"),
            }
        )

    snapshot = get_feature_consumer_dlq_snapshot()
    report = {
        "dry_run": bool(args.dry_run),
        "selected": len(rows),
        "replayed": replayed,
        "replay_duplicates": replay_duplicates,
        "replay_failed": replay_failed,
        "mark_failures": mark_failures,
        "status_filter": statuses,
        "topic_filter": topic,
        "dlq_snapshot": snapshot,
        "details": details,
    }

    payload = json.dumps(report, ensure_ascii=True, indent=2)
    print(payload)
    if args.output_json:
        Path(args.output_json).write_text(payload + "\n", encoding="utf-8")

    if replay_failed > 0 or mark_failures > 0:
        return 1
    return 0


if __name__ == "__main__":
    raise SystemExit(main())

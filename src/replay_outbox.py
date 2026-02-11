#!/usr/bin/env python3
import argparse
import sys
from pathlib import Path

PROJECT_ROOT = Path(__file__).resolve().parents[1]
if str(PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(PROJECT_ROOT))

from app.db import requeue_outbox_events


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Requeue published/dead outbox rows for replay.")
    parser.add_argument("--topic", default="", help="Optional Kafka topic filter.")
    parser.add_argument("--limit", type=int, default=1000, help="Maximum rows to requeue.")
    return parser.parse_args()


def main() -> int:
    args = parse_args()
    topic = args.topic.strip() or None
    requeued = requeue_outbox_events(topic=topic, limit=max(int(args.limit), 1))
    print(f"requeued_outbox_events={requeued}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())

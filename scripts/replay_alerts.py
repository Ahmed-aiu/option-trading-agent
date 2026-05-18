#!/usr/bin/env python3
"""Replay raw notification JSONL through parser and risk guard."""

from __future__ import annotations

import argparse
from pathlib import Path

from parse_alert import parse_records
from pipeline_common import DATA_DIR, read_jsonl
from risk_guard import decide_alerts


def compare_expected(parsed: list[dict], expected_path: Path) -> tuple[int, int]:
    expected = read_jsonl(expected_path)
    comparable = [
        {
            "ticker": row.get("ticker"),
            "side": row.get("side"),
            "entry_price": row.get("entry_price"),
            "stop_price": row.get("stop_price"),
            "target_price": row.get("target_price"),
        }
        for row in parsed
    ]
    expected_comparable = [
        {
            "ticker": row.get("ticker"),
            "side": row.get("side"),
            "entry_price": row.get("entry_price"),
            "stop_price": row.get("stop_price"),
            "target_price": row.get("target_price"),
        }
        for row in expected
    ]
    correct = sum(1 for row in comparable if row in expected_comparable)
    return correct, len(expected_comparable)


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--input", default=str(DATA_DIR / "raw_notifications.jsonl"))
    parser.add_argument("--expect", help="Expected parsed JSONL")
    parser.add_argument("--dry-run", action="store_true", help="Do not append outputs")
    parser.add_argument("--ignore-age", action="store_true", help="Ignore max alert age during replay")
    args = parser.parse_args()

    raw = read_jsonl(Path(args.input))
    parsed, rejected = parse_records(raw, write=not args.dry_run)
    decisions = decide_alerts(
        parsed,
        write=not args.dry_run,
        ignore_age=args.ignore_age or args.dry_run,
        prior_decisions=[] if args.dry_run else None,
    )
    allowed = [item for item in decisions if item.get("allowed")]
    blocked = [item for item in decisions if not item.get("allowed")]
    duplicate_count = sum(1 for item in blocked if item.get("reason") == "duplicate_trade_window")

    print(f"total raw alerts: {len(raw)}")
    print(f"parsed count: {len(parsed)}")
    print(f"rejected count: {len(rejected)}")
    print(f"allowed count: {len(allowed)}")
    print(f"blocked count: {len(blocked)}")
    print(f"duplicate count: {duplicate_count}")
    if args.expect:
        correct, expected = compare_expected(parsed, Path(args.expect))
        accuracy = (correct / expected) if expected else 1.0
        print(f"parse accuracy: {correct}/{expected} ({accuracy:.1%})")
        return 0 if correct == expected else 1
    return 0


if __name__ == "__main__":
    raise SystemExit(main())

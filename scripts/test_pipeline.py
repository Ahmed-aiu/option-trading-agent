#!/usr/bin/env python3
"""End-to-end local test without Discord."""

from __future__ import annotations

import sys
from parse_alert import parse_records
from pipeline_common import TESTS_DIR, atomic_touch_jsonl_files, read_jsonl
from risk_guard import decide_alerts


def main() -> int:
    atomic_touch_jsonl_files()
    sample_path = TESTS_DIR / "sample_alerts.jsonl"
    raw = read_jsonl(sample_path)
    parsed, rejected = parse_records(raw, write=False)
    decisions = decide_alerts(parsed, write=False, ignore_age=True, prior_decisions=[])
    allowed = [item for item in decisions if item.get("allowed")]
    blocked = [item for item in decisions if not item.get("allowed")]

    expected = {
        "total": 7,
        "parsed": 4,
        "rejected": 3,
        "allowed": 3,
        "blocked": 1,
    }
    actual = {
        "total": len(raw),
        "parsed": len(parsed),
        "rejected": len(rejected),
        "allowed": len(allowed),
        "blocked": len(blocked),
    }

    print("Pipeline test summary")
    for key in ("total", "parsed", "rejected", "allowed", "blocked"):
        print(f"{key}: {actual[key]}")
    print()
    print("Rejected reasons:")
    for item in rejected:
        print(f"- {item.get('reason')}: {item.get('raw_text')}")
    print()
    print("Blocked reasons:")
    for item in blocked:
        print(f"- {item.get('reason')}: {item.get('ticker')} {item.get('side')}")
    print()
    print("Paper-only: no broker orders were placed.")

    if actual != expected:
        print(f"Expected {expected}, got {actual}", file=sys.stderr)
        return 1
    return 0


if __name__ == "__main__":
    raise SystemExit(main())

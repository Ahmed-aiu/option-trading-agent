#!/usr/bin/env python3
"""Full pipeline test using temporary JSONL files."""

from __future__ import annotations

import tempfile
import uuid
from pathlib import Path

from pipeline_common import append_jsonl, read_jsonl
from run_pipeline_once import process_raw_notifications


def raw_record(dedupe_key: str, body: str) -> dict:
    return {
        "event_type": "raw_discord_notification",
        "captured_at": "2026-05-09T09:30:00-04:00",
        "notification_timestamp": "",
        "source_app": "Discord",
        "bundle_id": "com.hnc.Discord",
        "title": "OTWSteve (#long-swings-call-outs-2-6-weeks)",
        "subtitle": "alerts",
        "body": body,
        "raw": {},
        "dedupe_key": dedupe_key,
    }


def main() -> int:
    run_id = uuid.uuid4().hex[:10]
    records = [
        raw_record(f"full-stock-{run_id}", "BUY TSLA over 182.50 stop 179.80 target 188"),
        raw_record(f"full-option-{run_id}", "#NVDA May 15 215 call @ 7.15 bought 3 #swing"),
        raw_record(f"full-add-{run_id}", "added 3 @ 3.30 #swing"),
    ]
    with tempfile.TemporaryDirectory() as tmp:
        path = Path(tmp) / "raw.jsonl"
        for record in records:
            append_jsonl(path, record)
        counts = process_raw_notifications(read_jsonl(path), dry_run_orders=True, prior_decisions_override=[])
    print("Full pipeline test summary")
    for key in (
        "raw_seen",
        "raw_new",
        "parsed",
        "rejected",
        "decisions",
        "allowed",
        "blocked",
        "alpaca_dry_runs",
        "option_shadow_positions",
        "option_approval_cards",
        "option_exits",
        "option_validation_errors",
    ):
        print(f"{key}: {counts[key]}")
    expected = {
        "raw_seen": 3,
        "raw_new": 3,
        "parsed": 2,
        "rejected": 1,
        "decisions": 2,
        "allowed": 1,
        "blocked": 1,
        "alpaca_dry_runs": 1,
        "option_shadow_positions": 1,
        "option_approval_cards": 1,
        "option_exits": 0,
        "option_validation_errors": 0,
    }
    if counts != expected:
        print(f"Expected {expected}, got {counts}")
        return 1
    return 0


if __name__ == "__main__":
    raise SystemExit(main())

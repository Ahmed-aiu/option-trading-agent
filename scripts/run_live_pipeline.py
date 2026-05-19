#!/usr/bin/env python3
"""Run the live notification capture and processing loop."""

from __future__ import annotations

import argparse
import json
import signal
import time

from notification_watcher import poll_once
from option_validation import track_open_positions_once
from pipeline_common import CONFIG_DIR, DATA_DIR, LOG_DIR, append_jsonl, atomic_touch_jsonl_files, load_seen_keys, load_simple_yaml, now_iso, read_jsonl, setup_logging
from run_pipeline_once import process_raw_notifications
from steve_trade_bot import poll_once as poll_telegram_approvals


STOP = False
HEARTBEAT_FILE = DATA_DIR / "live_pipeline_heartbeat.json"
HEARTBEAT_HISTORY_FILE = DATA_DIR / "live_pipeline_heartbeats.jsonl"


def request_stop(signum: int, frame: object) -> None:
    global STOP
    STOP = True


def write_heartbeat(record: dict) -> None:
    HEARTBEAT_FILE.parent.mkdir(parents=True, exist_ok=True)
    tmp_path = HEARTBEAT_FILE.with_suffix(".json.tmp")
    tmp_path.write_text(json.dumps(record, sort_keys=True, separators=(",", ":")) + "\n", encoding="utf-8")
    tmp_path.replace(HEARTBEAT_FILE)
    append_jsonl(HEARTBEAT_HISTORY_FILE, record)


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--once", action="store_true")
    parser.add_argument("--no-alpaca-dry-run", action="store_true")
    args = parser.parse_args()
    atomic_touch_jsonl_files()
    config = load_simple_yaml(CONFIG_DIR / "watcher.yaml")
    logger = setup_logging("live_pipeline", LOG_DIR / "live_pipeline.log")
    signal.signal(signal.SIGTERM, request_stop)
    signal.signal(signal.SIGINT, request_stop)
    seen_notifications = load_seen_keys(DATA_DIR / "raw_notifications.jsonl")
    interval = float(config.get("poll_interval_seconds", 1))
    option_track_interval = float(config.get("option_track_interval_seconds", 15))
    heartbeat_interval = float(config.get("heartbeat_interval_seconds", 30))
    last_option_track = 0.0
    last_heartbeat = 0.0
    logger.info("Live pipeline starting")
    while not STOP:
        loop_started = time.monotonic()
        written, duplicates = poll_once(config, seen_notifications, logger)
        counts = process_raw_notifications(
            read_jsonl(DATA_DIR / "raw_notifications.jsonl"),
            dry_run_orders=not args.no_alpaca_dry_run,
        )
        telegram_counts = {"updates": 0, "messages": 0, "actions": 0}
        try:
            telegram_counts = poll_telegram_approvals(require_config=False)
        except Exception as exc:  # noqa: BLE001
            logger.info("telegram_approval_poll_skipped=%s", exc)
        option_counts = {"open_positions": 0, "snapshots": 0}
        now_monotonic = time.monotonic()
        if now_monotonic - last_option_track >= option_track_interval:
            try:
                option_counts = track_open_positions_once()
                last_option_track = now_monotonic
            except Exception as exc:  # noqa: BLE001
                logger.info("option_tracking_skipped=%s", exc)
        now_monotonic = time.monotonic()
        if now_monotonic - last_heartbeat >= heartbeat_interval:
            write_heartbeat(
                {
                    "event_type": "live_pipeline_heartbeat",
                    "recorded_at": now_iso(config.get("timezone", "America/Detroit")),
                    "capture_written": written,
                    "duplicate_notifications": duplicates,
                    "pipeline": counts,
                    "telegram": telegram_counts,
                    "option_tracking": option_counts,
                    "loop_seconds": round(now_monotonic - loop_started, 3),
                }
            )
            last_heartbeat = now_monotonic
        if written or duplicates or counts["raw_new"] or telegram_counts.get("actions") or option_counts.get("snapshots"):
            logger.info(
                "capture_written=%d duplicate_notifications=%d pipeline=%s telegram=%s option_tracking=%s",
                written,
                duplicates,
                counts,
                telegram_counts,
                option_counts,
            )
            print(
                "capture_written={written} duplicates={duplicates} raw_new={raw_new} "
                "parsed={parsed} rejected={rejected} allowed={allowed} blocked={blocked} dry_runs={dry_runs} "
                "option_shadow={option_shadow} option_cards={option_cards} telegram_actions={telegram_actions} "
                "option_auto_buys={option_auto_buys} option_snapshots={option_snapshots} human_exits={human_exits}".format(
                    written=written,
                    duplicates=duplicates,
                    raw_new=counts["raw_new"],
                    parsed=counts["parsed"],
                    rejected=counts["rejected"],
                    allowed=counts["allowed"],
                    blocked=counts["blocked"],
                    dry_runs=counts["alpaca_dry_runs"],
                    option_shadow=counts.get("option_shadow_positions", 0),
                    option_cards=counts.get("option_approval_cards", 0),
                    option_auto_buys=counts.get("option_auto_buys", 0),
                    telegram_actions=telegram_counts.get("actions", 0),
                    option_snapshots=option_counts.get("snapshots", 0),
                    human_exits=option_counts.get("human_exits", 0),
                )
            )
        if args.once:
            break
        time.sleep(interval)
    logger.info("Live pipeline stopped")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())

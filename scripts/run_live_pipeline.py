#!/usr/bin/env python3
"""Run the live notification capture and processing loop."""

from __future__ import annotations

import argparse
import signal
import time

from notification_watcher import poll_once
from option_validation import track_open_positions_once
from pipeline_common import CONFIG_DIR, DATA_DIR, LOG_DIR, atomic_touch_jsonl_files, load_seen_keys, load_simple_yaml, read_jsonl, setup_logging
from run_pipeline_once import process_raw_notifications
from steve_trade_bot import poll_once as poll_telegram_approvals


STOP = False


def request_stop(signum: int, frame: object) -> None:
    global STOP
    STOP = True


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
    last_option_track = 0.0
    logger.info("Live pipeline starting")
    while not STOP:
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
                "option_snapshots={option_snapshots} human_exits={human_exits}".format(
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

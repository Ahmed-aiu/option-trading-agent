#!/usr/bin/env python3
"""Full pipeline test using temporary JSONL files."""

from __future__ import annotations

import datetime as dt
import tempfile
import uuid
from pathlib import Path
from zoneinfo import ZoneInfo

import option_validation
import run_pipeline_once
import steve_trade_bot
import alpaca_paper_adapter
from alpaca_options import option_symbol
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


def fake_snapshot(alert: dict) -> dict:
    contract_symbol = option_symbol(alert["ticker"], alert["expiration_date"], alert["option_type"], alert["strike_price"])
    return {
        "event_type": "option_market_snapshot",
        "snapshot_id": "full-pipeline-test-snap",
        "recorded_at": "2026-05-09T09:30:05-04:00",
        "source_dedupe_key": alert.get("source_dedupe_key"),
        "ticker": alert.get("ticker"),
        "contract_symbol": contract_symbol,
        "option_quote": {
            "symbol": contract_symbol,
            "status": "ok",
            "bid": 7.05,
            "ask": 7.15,
            "mark": 7.1,
            "spread_pct": 1.4,
            "timestamp": dt.datetime.now(ZoneInfo("America/Detroit")).isoformat(timespec="seconds"),
        },
        "underlying_indicators": {"status": "ok"},
    }


def patch_runtime_paths(tmp_path: Path) -> None:
    run_pipeline_once.DATA_DIR = tmp_path
    run_pipeline_once.PROCESSED_FILE = tmp_path / "processed_notifications.jsonl"
    alpaca_paper_adapter.DATA_DIR = tmp_path

    option_validation.SHADOW_POSITIONS_FILE = tmp_path / "shadow_option_positions.jsonl"
    option_validation.QUOTE_SNAPSHOTS_FILE = tmp_path / "option_quote_snapshots.jsonl"
    option_validation.STEVE_EXITS_FILE = tmp_path / "steve_option_exits.jsonl"
    option_validation.HUMAN_POSITIONS_FILE = tmp_path / "human_paper_positions.jsonl"
    option_validation.HUMAN_EXITS_FILE = tmp_path / "human_paper_exits.jsonl"
    option_validation.DAILY_SUMMARIES_FILE = tmp_path / "daily_option_summaries.jsonl"
    option_validation.DAILY_PL_REPORTS_FILE = tmp_path / "daily_pl_reports.jsonl"
    option_validation.STEVE_ALERT_PL_REPORTS_FILE = tmp_path / "steve_alert_pl_reports.jsonl"
    option_validation.enrich_option_alert = fake_snapshot

    steve_trade_bot.APPROVAL_CARDS_FILE = tmp_path / "steve_approval_cards.jsonl"
    steve_trade_bot.APPROVAL_ACTIONS_FILE = tmp_path / "steve_approval_actions.jsonl"
    steve_trade_bot.CLOSE_REPORTS_FILE = tmp_path / "steve_close_reports.jsonl"
    steve_trade_bot.AUTO_BUY_REPORTS_FILE = tmp_path / "steve_auto_buy_reports.jsonl"
    steve_trade_bot.BROKER_ORDER_REPORTS_FILE = tmp_path / "steve_broker_order_reports.jsonl"
    steve_trade_bot.DAILY_PL_REPORTS_FILE = tmp_path / "daily_pl_reports.jsonl"
    steve_trade_bot.HUMAN_POSITIONS_FILE = tmp_path / "human_paper_positions.jsonl"
    steve_trade_bot.BOT_STATE_FILE = tmp_path / "steve_trade_bot_state.json"
    steve_trade_bot.load_bot_config = lambda required=False: None
    steve_trade_bot.submit_option_paper_order = lambda position: {
        "status": "submitted",
        "reason": "",
        "position_id": position.get("position_id"),
        "payload": {},
        "response": {},
    }


def main() -> int:
    run_id = uuid.uuid4().hex[:10]
    records = [
        raw_record(f"full-stock-{run_id}", "BUY TSLA over 182.50 stop 179.80 target 188"),
        raw_record(f"full-option-{run_id}", "#NVDA May 15 215 call @ 7.15 bought 3 #swing"),
        raw_record(f"full-add-{run_id}", "added 3 @ 3.30 #swing"),
    ]
    with tempfile.TemporaryDirectory() as tmp:
        tmp_path = Path(tmp)
        patch_runtime_paths(tmp_path)
        path = tmp_path / "raw.jsonl"
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
        "option_auto_buys",
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
        "option_approval_cards": 0,
        "option_auto_buys": 1,
        "option_exits": 0,
        "option_validation_errors": 0,
    }
    if counts != expected:
        print(f"Expected {expected}, got {counts}")
        return 1
    return 0


if __name__ == "__main__":
    raise SystemExit(main())

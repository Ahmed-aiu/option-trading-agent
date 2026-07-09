"""Shared helpers for Steve options tests.

New focused Steve options test scripts should import this module instead of
adding more reusable setup code to ``test_steve_options_mvp.py``.
"""

from __future__ import annotations

import datetime as dt
from pathlib import Path
from zoneinfo import ZoneInfo

import alpaca_options
import broker_order_monitor
import nightly_review
import option_validation
import steve_trade_bot


def parsed_records(value):
    return value if isinstance(value, list) else [value]


def fake_snapshot(alert: dict) -> dict:
    contract_symbol = alpaca_options.option_symbol(alert["ticker"], alert["expiration_date"], alert["option_type"], alert["strike_price"])
    return {
        "event_type": "option_market_snapshot",
        "snapshot_id": "snap-test",
        "recorded_at": "2026-05-08T13:09:05-04:00",
        "source_dedupe_key": alert.get("source_dedupe_key"),
        "ticker": alert.get("ticker"),
        "contract_symbol": contract_symbol,
        "dte": 0,
        "option_quote": {
            "symbol": contract_symbol,
            "status": "ok",
            "bid": 0.86,
            "ask": 0.9,
            "mark": 0.88,
            "spread_pct": 4.54,
            "timestamp": dt.datetime.now(ZoneInfo("America/Detroit")).isoformat(timespec="seconds"),
        },
        "underlying_indicators": {
            "status": "ok",
            "price_vs_vwap_pct": 0.7,
            "ema_alignment": "bullish",
            "rsi_14": 61.2,
            "relative_volume": 1.8,
        },
        "signal_score": 72,
        "signal_warnings": ["zero_dte"],
    }


def patch_runtime_paths(tmp_path: Path) -> None:
    option_validation.SHADOW_POSITIONS_FILE = tmp_path / "shadow_option_positions.jsonl"
    option_validation.QUOTE_SNAPSHOTS_FILE = tmp_path / "option_quote_snapshots.jsonl"
    option_validation.TRACKING_STATE_FILE = tmp_path / "option_tracking_state.json"
    option_validation.STEVE_EXITS_FILE = tmp_path / "steve_option_exits.jsonl"
    option_validation.HUMAN_POSITIONS_FILE = tmp_path / "human_paper_positions.jsonl"
    option_validation.HUMAN_EXITS_FILE = tmp_path / "human_paper_exits.jsonl"
    option_validation.DAILY_SUMMARIES_FILE = tmp_path / "daily_option_summaries.jsonl"
    option_validation.DAILY_PL_REPORTS_FILE = tmp_path / "daily_pl_reports.jsonl"
    option_validation.STEVE_ALERT_PL_REPORTS_FILE = tmp_path / "steve_alert_pl_reports.jsonl"
    option_validation.submit_option_paper_sell_order = lambda position, contracts, reason, trigger_key: {
        "status": "submitted",
        "reason": "",
        "payload": {"client_order_id": f"test-exit-{trigger_key}", "side": "sell", "qty": str(contracts)},
        "response": {"id": f"order-{trigger_key}", "client_order_id": f"test-exit-{trigger_key}"},
    }
    steve_trade_bot.APPROVAL_CARDS_FILE = tmp_path / "steve_approval_cards.jsonl"
    steve_trade_bot.APPROVAL_ACTIONS_FILE = tmp_path / "steve_approval_actions.jsonl"
    steve_trade_bot.CLOSE_REPORTS_FILE = tmp_path / "steve_close_reports.jsonl"
    steve_trade_bot.AUTO_BUY_REPORTS_FILE = tmp_path / "steve_auto_buy_reports.jsonl"
    steve_trade_bot.BROKER_ORDER_REPORTS_FILE = tmp_path / "steve_broker_order_reports.jsonl"
    steve_trade_bot.BROKER_STATUS_REPORTS_FILE = tmp_path / "broker_order_status_reports.jsonl"
    steve_trade_bot.DAILY_PL_REPORTS_FILE = tmp_path / "daily_pl_reports.jsonl"
    steve_trade_bot.HUMAN_POSITIONS_FILE = tmp_path / "human_paper_positions.jsonl"
    steve_trade_bot.PARSED_ALERTS_FILE = tmp_path / "parsed_alerts.jsonl"
    steve_trade_bot.RAW_NOTIFICATIONS_FILE = tmp_path / "raw_notifications.jsonl"
    steve_trade_bot.BOT_STATE_FILE = tmp_path / "steve_trade_bot_state.json"
    alpaca_options.ORDERS_FILE = tmp_path / "orders_paper.jsonl"
    alpaca_options.ORDER_LOCK_FILE = tmp_path / ".orders_paper.lock"
    broker_order_monitor.ORDERS_FILE = tmp_path / "orders_paper.jsonl"
    broker_order_monitor.HUMAN_POSITIONS_FILE = tmp_path / "human_paper_positions.jsonl"
    broker_order_monitor.ORDER_STATUS_FILE = tmp_path / "broker_order_status_reports.jsonl"
    nightly_review.NIGHTLY_DIR = tmp_path / "nightly_reviews"
    nightly_review.NIGHTLY_SUMMARY_FILE = tmp_path / "nightly_review_reports.jsonl"
    nightly_review.NIGHTLY_TELEGRAM_FILE = tmp_path / "nightly_telegram_reports.jsonl"
    nightly_review.STEVE_ALERT_PL_FILE = tmp_path / "steve_alert_pl_reports.jsonl"
    nightly_review.BROKER_FILL_PL_FILE = tmp_path / "broker_fill_pl_reports.jsonl"
    nightly_review.BROWSER_MESSAGES_FILE = tmp_path / "discord_browser_messages.jsonl"
    nightly_review.RAW_FILE = tmp_path / "raw_notifications.jsonl"
    nightly_review.PARSED_FILE = tmp_path / "parsed_alerts.jsonl"
    nightly_review.REJECTED_FILE = tmp_path / "rejected_alerts.jsonl"
    nightly_review.APPROVAL_CARDS_FILE = tmp_path / "steve_approval_cards.jsonl"
    nightly_review.AUTO_BUY_REPORTS_FILE = tmp_path / "steve_auto_buy_reports.jsonl"
    nightly_review.HUMAN_POSITIONS_FILE = tmp_path / "human_paper_positions.jsonl"
    nightly_review.HUMAN_EXITS_FILE = tmp_path / "human_paper_exits.jsonl"
    nightly_review.ORDERS_FILE = tmp_path / "orders_paper.jsonl"
    nightly_review.BROKER_STATUS_FILE = tmp_path / "broker_order_status_reports.jsonl"
    nightly_review.STEVE_EXITS_FILE = tmp_path / "steve_option_exits.jsonl"
    nightly_review.PIPELINE_HEALTH_FILE = tmp_path / "pipeline_health_checks.jsonl"
    nightly_review.DAILY_PL_FILE = tmp_path / "daily_pl_reports.jsonl"
    nightly_review.BROWSER_HEALTH_LATEST_FILE = tmp_path / "discord_browser_health_latest.json"

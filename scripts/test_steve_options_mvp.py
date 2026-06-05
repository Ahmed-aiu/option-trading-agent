#!/usr/bin/env python3
"""Focused tests for the Steve options validation MVP."""

from __future__ import annotations

import datetime as dt
import json
import tempfile
from pathlib import Path
from zoneinfo import ZoneInfo

import alpaca_options
import backfill_steve_text
import broker_order_monitor
import data_hygiene
import discord_browser_channel_watcher
import discord_chrome_visible_capture
import notification_watcher
import nightly_review
import option_validation
import pipeline_health_monitor
import run_live_pipeline
import run_pipeline_once
import steve_trade_bot
from parse_alert import parse_trade_alert
from pipeline_common import append_jsonl, read_jsonl


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
    steve_trade_bot.DAILY_PL_REPORTS_FILE = tmp_path / "daily_pl_reports.jsonl"
    steve_trade_bot.HUMAN_POSITIONS_FILE = tmp_path / "human_paper_positions.jsonl"
    steve_trade_bot.BOT_STATE_FILE = tmp_path / "steve_trade_bot_state.json"
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


def test_parser() -> None:
    raw = {
        "captured_at": "2026-05-08T13:09:00-04:00",
        "dedupe_key": "screen-001",
        "body": "#CRWV MAY 8 113 call @ .88 Bought 10 #Lotto\n"
        "#CRWV May 8 114 call @ .54 Bought 10 #Lotto\n"
        "#IREN May 15 60 call @ 3.10 Bought 5 #swing",
    }
    parsed = parsed_records(parse_trade_alert(raw))
    assert len(parsed) == 3
    assert parsed[0]["ticker"] == "CRWV"
    assert parsed[0]["entry_price"] == 0.88
    assert parsed[0]["expiration_date"] == "2026-05-08"
    assert parsed[0]["primary_tag"] == "lotto"
    assert parsed[2]["ticker"] == "IREN"
    assert parsed[2]["primary_tag"] == "swing"

    exit_alert = parse_trade_alert({"body": "sold 2 @ 4.11", "dedupe_key": "exit-001"})
    assert exit_alert["instrument_type"] == "option"
    assert exit_alert["side"] == "exit"
    assert exit_alert["contracts"] == 2
    assert exit_alert["exit_price"] == 4.11
    assert exit_alert["ticker"] is None

    quoted_exit = parse_trade_alert(
        {
            "captured_at": "2026-05-18T14:17:00-04:00",
            "dedupe_key": "exit-quoted-xom",
            "body": "@OTWSteve\n#XOM MAY 22 160 call @ 1.62 Bought 10 #swing\nSteveOTWS\nSold 2 @ 3.26",
        }
    )
    assert quoted_exit["side"] == "exit"
    assert quoted_exit["ticker"] == "XOM"
    assert quoted_exit["expiration_date"] == "2026-05-22"
    assert quoted_exit["option_type"] == "call"
    assert quoted_exit["strike_price"] == 160.0
    assert quoted_exit["contracts"] == 2
    assert quoted_exit["exit_price"] == 3.26

    missing_type_exit = parse_trade_alert(
        {
            "captured_at": "2026-05-18T13:38:00-04:00",
            "dedupe_key": "exit-quoted-spy",
            "body": "@OTWSteve\n#SPY MAY 18 744 @ 1.81 Bought 5 #lotto\nSteveOTWS\nClosed @ 7.54",
        }
    )
    assert missing_type_exit["side"] == "exit"
    assert missing_type_exit["ticker"] == "SPY"
    assert missing_type_exit["expiration_date"] == "2026-05-18"
    assert missing_type_exit["strike_price"] == 744.0
    assert missing_type_exit["contracts"] is None
    assert missing_type_exit["exit_price"] == 7.54

    author_only_exit = parse_trade_alert(
        {
            "dedupe_key": "exit-author-only",
            "title": "SteveOTWS (#short-term-call-outs-same-week-or-1-week)",
            "body": "Sold 2 @ 3.26",
        }
    )
    assert author_only_exit["side"] == "exit"
    assert author_only_exit["ticker"] is None



def test_validation_and_approval() -> None:
    with tempfile.TemporaryDirectory() as tmp:
        tmp_path = Path(tmp)
        patch_runtime_paths(tmp_path)
        option_validation.enrich_option_alert = fake_snapshot
        steve_trade_bot.load_bot_config = lambda required=False: None
        steve_trade_bot.submit_option_paper_order = lambda position: {
            "status": "blocked",
            "reason": "paper_order_submission_disabled",
            "position_id": position.get("position_id"),
        }

        alert = parsed_records(
            parse_trade_alert(
                {
                    "captured_at": "2026-05-08T13:09:00-04:00",
                    "dedupe_key": "screen-002",
                    "body": "#CRWV MAY 8 113 call @ .88 Bought 10 #hedge",
                }
            )
        )[0]
        result = option_validation.handle_option_entry(alert, send_approval=True)
        assert result["shadow_position_created"] is True
        assert result["route"] == "approval_required"
        cards = read_jsonl(steve_trade_bot.APPROVAL_CARDS_FILE)
        assert len(cards) == 1
        assert cards[0]["status"] == "telegram_disabled"
        assert cards[0]["message_text"].startswith("Alert: #CRWV MAY 8 113 call @ .88 Bought 10")
        assert "\nbuy\n" in cards[0]["message_text"]
        assert "buy contracts=1 stop=35% take=80%" in cards[0]["message_text"]
        cards[0]["telegram_message_id"] = 100
        steve_trade_bot.APPROVAL_CARDS_FILE.write_text(json.dumps(cards[0], sort_keys=True) + "\n", encoding="utf-8")

        default_buy = steve_trade_bot.parse_approval_command("buy")
        assert default_buy["ok"] is True
        assert default_buy["contracts"] is None
        assert default_buy["stop_percent"] == 35.0
        assert default_buy["take_percent"] == 80.0
        assert default_buy["used_default_contracts"] is True
        assert default_buy["used_default_risk"] is True
        percent_buy = steve_trade_bot.parse_approval_command("buy contracts=1 stop=35% take=80%")
        assert percent_buy["ok"] is True
        assert percent_buy["stop_percent"] == 35.0
        assert percent_buy["take_percent"] == 80.0

        config = steve_trade_bot.BotConfig(
            token="test",
            approval_chat_id="-1001112223334",
            owner_chat_id="123456789",
            owner_user_id="123456789",
        )
        unauthorized = steve_trade_bot.process_approval_message(
            {
                "message_id": 1,
                "chat": {"id": "999"},
                "from": {"id": 42},
                "text": "buy contracts=1 stop=35 take=50",
            },
            config,
        )
        assert unauthorized["action"] == "unauthorized"

        rejected = steve_trade_bot.process_approval_message(
            {
                "message_id": 2,
                "chat": {"id": "-1001112223334"},
                "from": {"id": 123456789},
                "text": "buy contracts=1",
                "reply_to_message": {"message_id": 100},
            },
            config,
        )
        assert rejected["action"] == "rejected_command"
        assert rejected["reason"] == "missing_stop_take"

        invalid_price_risk = steve_trade_bot.process_approval_message(
            {
                "message_id": 20,
                "chat": {"id": "-1001112223334"},
                "from": {"id": 123456789},
                "text": "buy contracts=1 stop_price=3.80 take_price=6.20",
                "reply_to_message": {"message_id": 100},
            },
            config,
        )
        assert invalid_price_risk["action"] == "rejected_command"
        assert invalid_price_risk["reason"].startswith("price_risk_must_bracket_entry")

        approved = steve_trade_bot.process_approval_message(
            {
                "message_id": 3,
                "chat": {"id": "-1001112223334"},
                "from": {"id": 123456789},
                "text": "buy contracts=1 stop=35 take=50",
                "reply_to_message": {"message_id": 100},
            },
            config,
        )
        assert approved["action"] == "approved"
        positions = read_jsonl(steve_trade_bot.HUMAN_POSITIONS_FILE)
        assert len(positions) == 1
        assert positions[0]["contracts"] == 1
        assert positions[0]["stop_percent"] == 35.0
        assert positions[0]["take_percent"] == 50.0
        assert positions[0]["alert_contracts"] == 10
        assert positions[0]["exit_plan"] == [
            {"action": "sell", "contracts": 1, "take_percent": 50.0, "take_price": 1.35}
        ]

        duplicate = steve_trade_bot.process_approval_message(
            {
                "message_id": 4,
                "chat": {"id": "-1001112223334"},
                "from": {"id": 222333444},
                "text": "skip",
                "reply_to_message": {"message_id": 100},
            },
            config,
        )
        assert duplicate["action"] == "duplicate_command"

        owner_dm = steve_trade_bot.process_approval_message(
            {
                "message_id": 5,
                "chat": {"id": "123456789"},
                "from": {"id": 123456789},
                "text": "skip",
                "reply_to_message": {"message_id": 100},
            },
            config,
        )
        assert owner_dm["action"] == "duplicate_command"


def test_non_hedge_auto_paper_buy() -> None:
    with tempfile.TemporaryDirectory() as tmp:
        tmp_path = Path(tmp)
        patch_runtime_paths(tmp_path)
        sent_messages: list[tuple[str, str]] = []
        option_validation.enrich_option_alert = fake_snapshot
        steve_trade_bot.load_bot_config = lambda required=False: steve_trade_bot.BotConfig(
            token="test",
            approval_chat_id="123456789",
            owner_chat_id="123456789",
            owner_user_id="123456789",
            approval_chat_ids=("123456789", "-1001112223334"),
        )
        steve_trade_bot.send_telegram_message = lambda config, text, chat_id=None: (
            sent_messages.append((str(chat_id or config.approval_chat_id), text))
            or {"ok": True, "result": {"message_id": len(sent_messages), "chat": {"id": chat_id or config.approval_chat_id}}}
        )
        steve_trade_bot.submit_option_paper_order = lambda position: {
            "status": "submitted",
            "reason": "",
            "position_id": position.get("position_id"),
        }

        alert = parsed_records(
            parse_trade_alert(
                {
                    "captured_at": "2026-05-08T13:09:00-04:00",
                    "dedupe_key": "screen-auto-001",
                    "body": "#CRWV MAY 8 113 call @ .88 Bought 5 #swing",
                }
            )
        )[0]
        result = option_validation.handle_option_entry(alert, send_approval=True)
        assert result["route"] == "auto_paper_buy"
        assert result["approval_card"] == {}
        assert result["auto_buy"]["created"] is True
        assert read_jsonl(steve_trade_bot.APPROVAL_CARDS_FILE) == []

        positions = read_jsonl(steve_trade_bot.HUMAN_POSITIONS_FILE)
        assert len(positions) == 1
        assert positions[0]["contracts"] == 5
        assert positions[0]["stop_percent"] == 35.0
        assert positions[0]["take_percent"] == 80.0
        assert positions[0]["exit_plan"] == [
            {"action": "sell", "contracts": 2, "take_percent": 80.0, "take_price": 1.62},
            {"action": "sell", "contracts": 1, "take_percent": 120.0, "take_price": 1.98},
            {"action": "sell", "contracts": 2, "take_percent": 200.0, "take_price": 2.7},
        ]

        reports = read_jsonl(steve_trade_bot.AUTO_BUY_REPORTS_FILE)
        assert len(reports) == 1
        assert reports[0]["status"] == "sent"
        assert "AUTO PAPER BUY" in reports[0]["message_text"]
        assert "Bought 5 @ 0.90" in reports[0]["message_text"]
        assert "Takes: 2 @ +80%, 1 @ +120%, 2 @ +200%" in reports[0]["message_text"]
        assert len(sent_messages) == 2

        duplicate = option_validation.handle_option_entry(alert, send_approval=True)
        assert duplicate["auto_buy"]["created"] is False
        assert len(read_jsonl(steve_trade_bot.HUMAN_POSITIONS_FILE)) == 1
        assert len(read_jsonl(steve_trade_bot.AUTO_BUY_REPORTS_FILE)) == 1


def test_non_hedge_bad_entry_requires_approval() -> None:
    with tempfile.TemporaryDirectory() as tmp:
        tmp_path = Path(tmp)
        patch_runtime_paths(tmp_path)
        sent_messages: list[tuple[str, str]] = []

        def moved_snapshot(alert: dict) -> dict:
            snapshot = fake_snapshot(alert)
            snapshot["option_quote"]["bid"] = 1.06
            snapshot["option_quote"]["ask"] = 1.1
            snapshot["option_quote"]["mark"] = 1.08
            return snapshot

        option_validation.enrich_option_alert = moved_snapshot
        steve_trade_bot.load_bot_config = lambda required=False: steve_trade_bot.BotConfig(
            token="test",
            approval_chat_id="123456789",
            owner_chat_id="123456789",
            owner_user_id="123456789",
            approval_chat_ids=("123456789",),
        )
        steve_trade_bot.send_telegram_message = lambda config, text, chat_id=None: (
            sent_messages.append((str(chat_id or config.approval_chat_id), text))
            or {"ok": True, "result": {"message_id": len(sent_messages), "chat": {"id": chat_id or config.approval_chat_id}}}
        )

        def fail_submit(position: dict) -> dict:
            raise AssertionError("bad entry guard should not submit a paper order")

        steve_trade_bot.submit_option_paper_order = fail_submit
        alert = parsed_records(
            parse_trade_alert(
                {
                    "captured_at": "2026-05-08T13:09:00-04:00",
                    "dedupe_key": "screen-slippage-001",
                    "body": "#CRWV MAY 8 113 call @ .88 Bought 5 #swing",
                }
            )
        )[0]
        result = option_validation.handle_option_entry(alert, send_approval=True)
        assert result["route"] == "approval_required"
        assert result["route_reason"] == "auto_entry_guard"
        assert "entry_price_above_alert_threshold" in result["auto_entry_guard"]["reasons"]
        assert result["auto_buy"] == {}
        assert read_jsonl(steve_trade_bot.HUMAN_POSITIONS_FILE) == []
        cards = read_jsonl(steve_trade_bot.APPROVAL_CARDS_FILE)
        assert len(cards) == 1
        assert "Auto buy held" in cards[0]["message_text"]
        assert "price moved beyond threshold" in cards[0]["message_text"]
        assert len(sent_messages) == 1


def test_non_hedge_mixed_buy_exit_requires_approval() -> None:
    with tempfile.TemporaryDirectory() as tmp:
        tmp_path = Path(tmp)
        patch_runtime_paths(tmp_path)
        option_validation.enrich_option_alert = fake_snapshot
        steve_trade_bot.load_bot_config = lambda required=False: None

        alert = parsed_records(
            parse_trade_alert(
                {
                    "captured_at": "2026-05-08T13:09:00-04:00",
                    "dedupe_key": "screen-mixed-001",
                    "body": "#IREN JUN 18 60 call @ 4.65 Bought 5 #swing",
                }
            )
        )[0]
        alert["raw_text"] = "#IREN JUN 18 60 call @ 4.65 Bought 5 #swing\nsold 3 @ 11.60"
        result = option_validation.handle_option_entry(alert, send_approval=True)
        assert result["route"] == "approval_required"
        assert result["route_reason"] == "auto_entry_guard"
        assert "mixed_buy_exit_message" in result["auto_entry_guard"]["reasons"]
        assert read_jsonl(steve_trade_bot.HUMAN_POSITIONS_FILE) == []


def test_fill_price_caps_excessive_slippage() -> None:
    card = {
        "approval_id": "card-slippage-cap",
        "alert": {"entry_price": 4.70},
        "snapshot": {
            "option_quote": {
                "status": "ok",
                "ask": 8.40,
                "mark": 8.20,
                "timestamp": dt.datetime.now(ZoneInfo("America/Detroit")).isoformat(timespec="seconds"),
            }
        },
    }
    price, source = steve_trade_bot.fill_price_from_card(card)
    assert price == 4.94
    assert source == "current_ask_slippage_capped"


def test_exit_plan_contract_allocation() -> None:
    expected = {
        1: [(80.0, 1)],
        2: [(80.0, 1), (120.0, 1)],
        3: [(80.0, 1), (120.0, 1), (200.0, 1)],
        5: [(80.0, 2), (120.0, 1), (200.0, 2)],
        6: [(80.0, 3), (120.0, 1), (200.0, 2)],
        10: [(80.0, 5), (120.0, 2), (200.0, 3)],
    }
    for contracts, tranches in expected.items():
        plan = steve_trade_bot.exit_plan_for_contracts(contracts, entry_price=10)
        assert [(row["take_percent"], row["contracts"]) for row in plan] == tranches
    assert steve_trade_bot.exit_plan_for_contracts(5, entry_price=6.15)[0]["take_price"] == 11.07
    custom = steve_trade_bot.exit_plan_for_contracts(2, entry_price=10, first_take_price=15)
    assert custom[0]["take_price"] == 15
    assert custom[0]["take_percent"] == 50.0


def test_multi_destination_approval_cards() -> None:
    with tempfile.TemporaryDirectory() as tmp:
        tmp_path = Path(tmp)
        patch_runtime_paths(tmp_path)
        assert steve_trade_bot.split_approval_chat_ids("123456789,1001234567890") == [
            "123456789",
            "-1001234567890",
        ]
        config = steve_trade_bot.BotConfig(
            token="test",
            approval_chat_id="123456789",
            owner_chat_id="123456789",
            owner_user_id="123456789",
            approval_chat_ids=("123456789", "-1001234567890"),
        )
        steve_trade_bot.load_bot_config = lambda required=False: config

        sent_chat_ids: list[str] = []

        def fake_send_message(config, text, chat_id=None):
            sent_chat_ids.append(str(chat_id))
            message_id = 10 if str(chat_id) == "123456789" else 20
            return {"ok": True, "result": {"message_id": message_id, "chat": {"id": int(chat_id)}}}

        steve_trade_bot.send_telegram_message = fake_send_message
        alert = parsed_records(
            parse_trade_alert(
                {
                    "captured_at": "2026-05-08T13:09:00-04:00",
                    "dedupe_key": "screen-multi",
                    "body": "#QQQ May 15 710 put @ 5.86 Bought 4 #hedge",
                }
            )
        )[0]
        card = steve_trade_bot.send_approval_card(alert, fake_snapshot(alert), {"position_id": "shadow-multi"})
        assert card["status"] == "sent"
        assert sent_chat_ids == ["123456789", "-1001234567890"]
        assert [(row["chat_id"], row["message_id"]) for row in card["telegram_messages"]] == [
            ("123456789", 10),
            ("-1001234567890", 20),
        ]

        group_skip = steve_trade_bot.process_approval_message(
            {
                "message_id": 21,
                "chat": {"id": "-1001234567890"},
                "from": {"id": 222333444},
                "text": "skip",
                "reply_to_message": {"message_id": 20},
            },
            config,
        )
        assert group_skip["action"] == "skipped"
        assert group_skip["authorization_scope"] == "approval_group"
        assert group_skip["approval_id"] == card["approval_id"]


def test_close_report_message_and_delivery() -> None:
    with tempfile.TemporaryDirectory() as tmp:
        tmp_path = Path(tmp)
        patch_runtime_paths(tmp_path)
        config = steve_trade_bot.BotConfig(
            token="test",
            approval_chat_id="123456789",
            owner_chat_id="123456789",
            owner_user_id="123456789",
            approval_chat_ids=("123456789", "-1001234567890"),
        )
        steve_trade_bot.load_bot_config = lambda required=False: config
        sent: list[tuple[str, str]] = []

        def fake_send_message(config, text, chat_id=None):
            sent.append((str(chat_id), text))
            message_id = 31 if str(chat_id) == "123456789" else 32
            return {"ok": True, "result": {"message_id": message_id, "chat": {"id": int(chat_id)}}}

        steve_trade_bot.send_telegram_message = fake_send_message
        exit_record = {
            "exit_id": "human-exit-test",
            "position_id": "human-test",
            "approval_id": "approval-test",
            "ticker": "MSFT",
            "option_type": "call",
            "expiration_date": "2026-07-17",
            "strike_price": 475.0,
            "contracts": 3,
            "position_contracts": 6,
            "entry_price": 6.15,
            "exit_price": 11.07,
            "pnl_percent": 80.0,
            "pnl_dollars": 1476.0,
            "remaining_after_exit": 3,
            "reason": "take_profit",
            "take_percent": 80.0,
        }
        text = steve_trade_bot.close_report_message(exit_record)
        assert text == "\n".join(
            [
                "CLOSED PARTIAL",
                "MSFT Jul 17 475C",
                "Sold 3/6 @ 11.07 (+80.0%)",
                "P/L: +$1,476",
                "Remain: 3",
                "Reason: 80% target hit",
            ]
        )
        report = steve_trade_bot.send_human_exit_report(exit_record)
        assert report["status"] == "sent"
        assert [row[0] for row in sent] == ["123456789", "-1001234567890"]
        reports = read_jsonl(steve_trade_bot.CLOSE_REPORTS_FILE)
        assert len(reports) == 1
        assert reports[0]["message_text"] == text


def test_human_exit_rules_and_steve_catch_up() -> None:
    with tempfile.TemporaryDirectory() as tmp:
        tmp_path = Path(tmp)
        patch_runtime_paths(tmp_path)
        steve_trade_bot.load_bot_config = lambda required=False: None
        position = {
            "event_type": "human_paper_option_position",
            "position_id": "human-test",
            "approval_id": "approval-test",
            "opened_at": "2026-05-08T13:09:00-04:00",
            "source_dedupe_key": "source-test",
            "ticker": "MSFT",
            "contract_symbol": "MSFT260717C00475000",
            "option_type": "call",
            "expiration_date": "2026-07-17",
            "strike_price": 475.0,
            "contracts": 5,
            "entry_price": 10.0,
            "risk_type": "percent",
            "stop_percent": 35.0,
            "exit_plan": steve_trade_bot.exit_plan_for_contracts(5, entry_price=10),
        }
        shadow = {
            "event_type": "shadow_option_position",
            "position_id": "shadow-test",
            "source_dedupe_key": "source-test",
            "contract_symbol": "MSFT260717C00475000",
            "contracts": 5,
        }
        append_jsonl(option_validation.HUMAN_POSITIONS_FILE, position)
        append_jsonl(option_validation.SHADOW_POSITIONS_FILE, shadow)
        append_jsonl(
            option_validation.QUOTE_SNAPSHOTS_FILE,
            {
                "recorded_at": "2026-05-08T13:10:00-04:00",
                "source_dedupe_key": "source-test",
                "contract_symbol": "MSFT260717C00475000",
                "option_quote": {"mark": 18.0},
            },
        )
        target_exits = option_validation.apply_human_exit_rules_once()
        assert len(target_exits) == 1
        assert target_exits[0]["reason"] == "take_profit"
        assert target_exits[0]["contracts"] == 2

        steve_exit_one = {
            "event_type": "steve_option_exit",
            "exit_id": "exit-steve-1",
            "matched_shadow_position_id": "shadow-test",
            "contracts": 2,
            "exit_price": 19.0,
        }
        append_jsonl(option_validation.STEVE_EXITS_FILE, steve_exit_one)
        assert option_validation.apply_steve_exit_to_human_positions(steve_exit_one, shadow) == []

        steve_exit_two = {
            "event_type": "steve_option_exit",
            "exit_id": "exit-steve-2",
            "matched_shadow_position_id": "shadow-test",
            "contracts": 2,
            "exit_price": 22.0,
        }
        append_jsonl(option_validation.STEVE_EXITS_FILE, steve_exit_two)
        catch_up = option_validation.apply_steve_exit_to_human_positions(steve_exit_two, shadow)
        assert len(catch_up) == 1
        assert catch_up[0]["reason"] == "steve_exit_catch_up"
        assert catch_up[0]["contracts"] == 2
        assert catch_up[0]["remaining_after_exit"] == 1


def test_steve_alert_pl_summary_uses_steve_prices() -> None:
    with tempfile.TemporaryDirectory() as tmp:
        tmp_path = Path(tmp)
        patch_runtime_paths(tmp_path)
        day = "2026-05-28"
        append_jsonl(
            option_validation.SHADOW_POSITIONS_FILE,
            {
                "event_type": "shadow_option_position",
                "position_id": "shadow-steve-pl",
                "opened_at": f"{day}T10:00:00-04:00",
                "source_dedupe_key": "ui-steve-pl",
                "ticker": "AAPL",
                "contract_symbol": "AAPL260618C00310000",
                "option_type": "call",
                "expiration_date": "2026-06-18",
                "strike_price": 310.0,
                "contracts": 5,
                "alert_entry_price": 2.0,
                "bot_entry_price": 3.0,
            },
        )
        append_jsonl(
            option_validation.STEVE_EXITS_FILE,
            {
                "event_type": "steve_option_exit",
                "exit_id": "exit-steve-pl",
                "recorded_at": f"{day}T15:00:00-04:00",
                "matched_shadow_position_id": "shadow-steve-pl",
                "ticker": "AAPL",
                "contracts": 2,
                "exit_price": 4.0,
            },
        )
        append_jsonl(
            option_validation.QUOTE_SNAPSHOTS_FILE,
            {
                "recorded_at": f"{day}T15:59:00-04:00",
                "position_id": "shadow-steve-pl",
                "contract_symbol": "AAPL260618C00310000",
                "option_quote": {"mark": 3.0},
            },
        )
        summary = option_validation.compute_steve_alert_pl_summary(day)
        assert summary["basis"] == "steve_buy_alert_and_steve_sell_alert_prices"
        assert summary["realized_pnl"] == 400.0
        assert summary["open_pnl"] == 300.0
        assert summary["total_pnl"] == 700.0
        assert summary["contracts_closed"] == 2
        assert summary["open_contracts"] == 3


def test_option_exit_reply_matches_shadow_context() -> None:
    with tempfile.TemporaryDirectory() as tmp:
        tmp_path = Path(tmp)
        patch_runtime_paths(tmp_path)
        append_jsonl(
            option_validation.SHADOW_POSITIONS_FILE,
            {
                "event_type": "shadow_option_position",
                "position_id": "shadow-xom",
                "opened_at": "2026-05-15T15:03:00-04:00",
                "source_dedupe_key": "source-xom",
                "ticker": "XOM",
                "contract_symbol": "XOM260522C00160000",
                "option_type": "call",
                "expiration_date": "2026-05-22",
                "strike_price": 160.0,
                "contracts": 10,
            },
        )
        append_jsonl(
            option_validation.SHADOW_POSITIONS_FILE,
            {
                "event_type": "shadow_option_position",
                "position_id": "shadow-qqq-newer",
                "opened_at": "2026-05-18T10:00:00-04:00",
                "source_dedupe_key": "source-qqq",
                "ticker": "QQQ",
                "contract_symbol": "QQQ260519P00710000",
                "option_type": "put",
                "expiration_date": "2026-05-19",
                "strike_price": 710.0,
                "contracts": 3,
            },
        )
        exit_alert = parse_trade_alert(
            {
                "captured_at": "2026-05-18T14:17:00-04:00",
                "dedupe_key": "exit-xom-context",
                "body": "@OTWSteve\n#XOM MAY 22 160 call @ 1.62 Bought 10 #swing\nSteveOTWS\nSold 2 @ 3.26",
            }
        )
        result = option_validation.handle_option_exit(exit_alert)
        assert result["created"] is True
        exits = read_jsonl(option_validation.STEVE_EXITS_FILE)
        assert len(exits) == 1
        assert exits[0]["ticker"] == "XOM"
        assert exits[0]["matched_shadow_position_id"] == "shadow-xom"
        assert exits[0]["match_confidence"] == "high"
        assert exits[0]["contracts"] == 2


def test_pipeline_processes_close_reply_as_option_exit() -> None:
    with tempfile.TemporaryDirectory() as tmp:
        tmp_path = Path(tmp)
        patch_runtime_paths(tmp_path)
        original_data_dir = run_pipeline_once.DATA_DIR
        original_processed_file = run_pipeline_once.PROCESSED_FILE
        original_summary = run_pipeline_once.write_openclaw_summary
        try:
            run_pipeline_once.DATA_DIR = tmp_path
            run_pipeline_once.PROCESSED_FILE = tmp_path / "processed_notifications.jsonl"
            run_pipeline_once.write_openclaw_summary = lambda decision: None
            append_jsonl(
                option_validation.SHADOW_POSITIONS_FILE,
                {
                    "event_type": "shadow_option_position",
                    "position_id": "shadow-xom",
                    "opened_at": "2026-05-15T15:03:00-04:00",
                    "source_dedupe_key": "source-xom",
                    "ticker": "XOM",
                    "contract_symbol": "XOM260522C00160000",
                    "option_type": "call",
                    "expiration_date": "2026-05-22",
                    "strike_price": 160.0,
                    "contracts": 10,
                },
            )
            counts = run_pipeline_once.process_raw_notifications(
                [
                    {
                        "event_type": "raw_discord_notification",
                        "dedupe_key": "raw-close-xom",
                        "captured_at": "2026-05-18T14:17:01-04:00",
                        "notification_timestamp": "2026-05-18T14:17:00-04:00",
                        "source_app": "Discord",
                        "bundle_id": "com.hnc.Discord",
                        "title": "SteveOTWS (#short-term-call-outs-same-week-or-1-week)",
                        "subtitle": "short-term-call-outs-same-week-or-1-week",
                        "body": "@OTWSteve\n#XOM MAY 22 160 call @ 1.62 Bought 10 #swing\nSold 2 @ 3.26",
                    }
                ],
                dry_run_orders=False,
                prior_decisions_override=[],
            )
            assert counts["parsed"] == 1
            assert counts["option_exits"] == 1
            assert counts["option_approval_cards"] == 0
            exits = read_jsonl(option_validation.STEVE_EXITS_FILE)
            assert exits[0]["matched_shadow_position_id"] == "shadow-xom"
        finally:
            run_pipeline_once.DATA_DIR = original_data_dir
            run_pipeline_once.PROCESSED_FILE = original_processed_file
            run_pipeline_once.write_openclaw_summary = original_summary


def test_backfill_text_audit_matches_contextual_exits() -> None:
    with tempfile.TemporaryDirectory() as tmp:
        tmp_path = Path(tmp)
        patch_runtime_paths(tmp_path)
        original_data_dir = backfill_steve_text.DATA_DIR
        original_backfills = backfill_steve_text.BACKFILLS_FILE
        original_enrich = option_validation.enrich_option_alert
        try:
            backfill_steve_text.DATA_DIR = tmp_path
            backfill_steve_text.BACKFILLS_FILE = tmp_path / "discord_text_backfills.jsonl"
            option_validation.enrich_option_alert = fake_snapshot
            text = "\n".join(
                [
                    "OTWSteve #XOM MAY 22 160 call @ 1.62 Bought 10 #swing",
                    "OTWSteve #XOM MAY 22 160 call @ 1.62 Bought 10 #swing",
                    "OTWSteve 10:33 AM",
                    "sold 5 @ 3.35",
                    "sold 5 @ 3.35",
                    "OTWSteve #CVX May 22 192.50 call @ 1.54 bought 5 #swing #Lotto",
                    "OTWSteve 10:34 AM",
                    "sold 4 @ 4.20",
                ]
            )
            records = backfill_steve_text.build_raw_records(text, "test-backfill")
            assert len(records) == 4
            counts = backfill_steve_text.process_audit(records)
            assert counts["entries"] == 2
            assert counts["exits"] == 2
            exits = read_jsonl(option_validation.STEVE_EXITS_FILE)
            assert exits[0]["ticker"] == "XOM"
            assert exits[0]["contracts"] == 5
            assert exits[0]["matched_shadow_position_id"]
            assert exits[1]["ticker"] == "CVX"
            assert exits[1]["contracts"] == 4
            assert exits[1]["matched_shadow_position_id"]
        finally:
            backfill_steve_text.DATA_DIR = original_data_dir
            backfill_steve_text.BACKFILLS_FILE = original_backfills
            option_validation.enrich_option_alert = original_enrich


def test_chrome_visible_capture_filters_history_by_default() -> None:
    today = discord_chrome_visible_capture.today_label()
    snapshot = {
        "messages": [
            {"text": "Friday, May 15, 2026 at 3:37 PM\n#TSLA May 15 425 call @ 4.75 bought 1 #lotto"},
            {"text": f"{today} at 3:19 PM\n#CVX May 22 200 call @ 1.59 Bought 3 #Lotto"},
        ]
    }
    filtered = discord_chrome_visible_capture.filter_visible_messages(snapshot, include_history=False)
    assert len(filtered) == 1
    assert "CVX" in filtered[0]["text"]
    assert len(discord_chrome_visible_capture.filter_visible_messages(snapshot, include_history=True)) == 2

    with tempfile.TemporaryDirectory() as tmp:
        original_state = discord_chrome_visible_capture.STATE_FILE
        try:
            discord_chrome_visible_capture.STATE_FILE = Path(tmp) / "chrome_state.json"
            state = discord_chrome_visible_capture.mark_messages_seen(filtered)
            assert state["seen_count"] == 1
            assert discord_chrome_visible_capture.unseen_messages(filtered) == []
        finally:
            discord_chrome_visible_capture.STATE_FILE = original_state


def test_option_order_payload() -> None:
    position = {
        "position_id": "human-test",
        "source_dedupe_key": "source-test",
        "contract_symbol": "QQQ260515P00710000",
        "contracts": 2,
        "entry_price": 5.86,
    }
    payload = alpaca_options.build_option_order_payload(position)
    assert payload["symbol"] == "QQQ260515P00710000"
    assert payload["qty"] == "2"
    assert payload["type"] == "limit"
    assert "notional" not in payload
    sell_payload = alpaca_options.build_option_sell_order_payload(position, 1, "stop_loss")
    assert sell_payload["symbol"] == "QQQ260515P00710000"
    assert sell_payload["qty"] == "1"
    assert sell_payload["side"] == "sell"
    assert sell_payload["type"] == "market"


def test_broker_order_monitor_reports_terminal_fills() -> None:
    with tempfile.TemporaryDirectory() as tmp:
        tmp_path = Path(tmp)
        patch_runtime_paths(tmp_path)
        sent_messages: list[tuple[str, str]] = []
        steve_trade_bot.load_bot_config = lambda required=False: steve_trade_bot.BotConfig(
            token="test",
            approval_chat_id="123456789",
            owner_chat_id="123456789",
            owner_user_id="123456789",
            approval_chat_ids=("123456789",),
        )
        steve_trade_bot.send_telegram_message = lambda config, text, chat_id=None: (
            sent_messages.append((str(chat_id or config.approval_chat_id), text))
            or {"ok": True, "result": {"message_id": len(sent_messages), "chat": {"id": chat_id or config.approval_chat_id}}}
        )
        broker_order_monitor.load_order_environment = lambda: {"base_url": "paper", "key_id": "key", "secret_key": "secret"}
        broker_order_monitor.fetch_order_status = lambda env, order_id: {
            "id": order_id,
            "client_order_id": "openclaw-opt-test",
            "symbol": "CVX260522C00200000",
            "side": "buy",
            "qty": "3",
            "filled_qty": "3",
            "filled_avg_price": "0.52",
            "status": "filled",
            "submitted_at": "2026-05-20T14:51:37Z",
            "filled_at": "2026-05-20T14:54:05Z",
        }
        append_jsonl(
            broker_order_monitor.HUMAN_POSITIONS_FILE,
            {
                "position_id": "human-test",
                "ticker": "CVX",
                "expiration_date": "2026-05-22",
                "option_type": "call",
                "strike_price": 200,
                "contract_symbol": "CVX260522C00200000",
            },
        )
        append_jsonl(
            broker_order_monitor.ORDERS_FILE,
            {
                "event_type": "alpaca_option_paper_order_audit",
                "recorded_at": dt.datetime.now(ZoneInfo("America/Detroit")).isoformat(timespec="seconds"),
                "status": "submitted",
                "position_id": "human-test",
                "contract_symbol": "CVX260522C00200000",
                "payload": {"client_order_id": "openclaw-opt-test", "symbol": "CVX260522C00200000", "side": "buy", "qty": "3"},
                "response": {"id": "order-test"},
            },
        )
        counts = broker_order_monitor.check_broker_order_statuses_once(max_age_hours=24)
        assert counts["reported"] == 1
        reports = read_jsonl(broker_order_monitor.ORDER_STATUS_FILE)
        assert reports[0]["broker_status"] == "filled"
        assert "BROKER FILLED" in sent_messages[0][1]
        assert "Bought 3 @ 0.52" in sent_messages[0][1]


def test_daily_pl_summary_short_report() -> None:
    with tempfile.TemporaryDirectory() as tmp:
        tmp_path = Path(tmp)
        patch_runtime_paths(tmp_path)
        sent_messages: list[tuple[str, str]] = []
        steve_trade_bot.load_bot_config = lambda required=False: steve_trade_bot.BotConfig(
            token="test",
            approval_chat_id="123456789",
            owner_chat_id="123456789",
            owner_user_id="123456789",
            approval_chat_ids=("123456789",),
        )
        steve_trade_bot.send_telegram_message = lambda config, text, chat_id=None: (
            sent_messages.append((str(chat_id or config.approval_chat_id), text))
            or {"ok": True, "result": {"message_id": len(sent_messages), "chat": {"id": chat_id or config.approval_chat_id}}}
        )
        today = dt.datetime.now(ZoneInfo("America/Detroit")).date().isoformat()
        append_jsonl(
            option_validation.HUMAN_POSITIONS_FILE,
            {
                "position_id": "human-pl",
                "approval_id": "auto-pl",
                "source_dedupe_key": "pl-key",
                "ticker": "CVX",
                "contract_symbol": "CVX260522C00200000",
                "contracts": 2,
                "entry_price": 1.0,
                "opened_at": f"{today}T10:00:00-04:00",
            },
        )
        append_jsonl(
            option_validation.QUOTE_SNAPSHOTS_FILE,
            {
                "position_id": "shadow-pl",
                "source_dedupe_key": "pl-key",
                "contract_symbol": "CVX260522C00200000",
                "recorded_at": f"{today}T15:55:00-04:00",
                "option_quote": {"mark": 1.5},
            },
        )
        append_jsonl(
            option_validation.HUMAN_EXITS_FILE,
            {
                "position_id": "human-other",
                "recorded_at": f"{today}T11:00:00-04:00",
                "contracts": 1,
                "pnl_dollars": -25.0,
            },
        )
        summary = option_validation.compute_daily_pl_summary(today)
        assert summary["realized_pnl"] == -25.0
        assert summary["open_pnl"] == 100.0
        report = steve_trade_bot.send_daily_pl_report(summary)
        assert report["status"] == "sent"
        assert "DAILY PAPER P/L" in sent_messages[0][1]
        assert "Total: +$75" in sent_messages[0][1]


def test_watcher_steve_filters() -> None:
    config = {
        "app_names": ["Discord"],
        "bundle_ids": ["com.hnc.Discord"],
        "alert_author_names": ["OTWSteve", "SteveOTWS"],
        "alert_channel_ids": ["492098253337264138"],
        "require_alert_channel_id_match": False,
        "capture_all_author_notifications": True,
        "body_keywords": ["CALL", "PUT"],
    }
    steve_record = {
        "source_app": "Discord",
        "bundle_id": "com.hnc.Discord",
        "title": "OTWSteve",
        "subtitle": "1503963447065317551",
        "body": "#QQQ May 19 710 put @ 4.25 Bought 3 #swing",
        "raw": {"thread": "1503963447065317551"},
    }
    assert notification_watcher.is_matching_notification(steve_record, config) is True

    non_steve_record = dict(steve_record)
    non_steve_record["title"] = "ahmed_aiu"
    assert notification_watcher.is_matching_notification(non_steve_record, config) is False

    close_record = dict(steve_record)
    close_record["title"] = "OTWSteve (#short-term-call-outs-same-week-or-1-week)"
    close_record["body"] = "Closed @ 7.54"
    assert notification_watcher.is_matching_notification(close_record, config) is True

    sold_record = dict(close_record)
    sold_record["title"] = "SteveOTWS (#short-term-call-outs-same-week-or-1-week)"
    sold_record["body"] = "Sold 2 @ 3.26"
    assert notification_watcher.is_matching_notification(sold_record, config) is True

    keyword_config = dict(config)
    keyword_config["capture_all_author_notifications"] = False
    keyword_config["body_keywords"] = ["CALL", "PUT", "SOLD", "CLOSED", "CLOSE", "STOPPED"]
    assert notification_watcher.is_matching_notification(sold_record, keyword_config) is True

    strict_config = dict(config)
    strict_config["require_alert_channel_id_match"] = True
    assert notification_watcher.is_matching_notification(steve_record, strict_config) is False
    steve_record["raw"]["thread"] = "492098253337264138"
    assert notification_watcher.is_matching_notification(steve_record, strict_config) is True


def test_live_pipeline_heartbeat() -> None:
    with tempfile.TemporaryDirectory() as tmp:
        heartbeat_file = Path(tmp) / "heartbeat.json"
        heartbeat_history_file = Path(tmp) / "heartbeats.jsonl"
        original = run_live_pipeline.HEARTBEAT_FILE
        original_history = run_live_pipeline.HEARTBEAT_HISTORY_FILE
        try:
            run_live_pipeline.HEARTBEAT_FILE = heartbeat_file
            run_live_pipeline.HEARTBEAT_HISTORY_FILE = heartbeat_history_file
            run_live_pipeline.write_heartbeat({"event_type": "live_pipeline_heartbeat", "capture_written": 0})
            run_live_pipeline.write_heartbeat({"event_type": "live_pipeline_heartbeat", "capture_written": 0})
            heartbeat = json.loads(heartbeat_file.read_text(encoding="utf-8"))
            history = read_jsonl(heartbeat_history_file)
            assert heartbeat["event_type"] == "live_pipeline_heartbeat"
            assert heartbeat["capture_written"] == 0
            assert heartbeat["history_appended"] is False
            assert len(history) == 1
            assert history[-1]["event_type"] == "live_pipeline_heartbeat"
            assert history[-1]["capture_written"] == 0
            snapshot_only = {
                "event_type": "live_pipeline_heartbeat",
                "capture_written": 0,
                "option_tracking": {"snapshots": 3, "human_exits": 0},
            }
            run_live_pipeline.write_heartbeat(snapshot_only)
            run_live_pipeline.write_heartbeat(snapshot_only)
            assert len(read_jsonl(heartbeat_history_file)) == 2
            run_live_pipeline.write_heartbeat({"event_type": "live_pipeline_heartbeat", "capture_written": 1})
            assert len(read_jsonl(heartbeat_history_file)) == 3
        finally:
            run_live_pipeline.HEARTBEAT_FILE = original
            run_live_pipeline.HEARTBEAT_HISTORY_FILE = original_history


def test_option_tracker_skips_junk_and_writes_lean_deduped_snapshots() -> None:
    with tempfile.TemporaryDirectory() as tmp:
        tmp_path = Path(tmp)
        patch_runtime_paths(tmp_path)
        original_enrich = option_validation.enrich_option_alert
        original_now_iso = option_validation.now_iso
        try:
            option_validation.enrich_option_alert = fake_snapshot
            option_validation.now_iso = lambda tz_name="America/Detroit": "2026-05-29T10:00:00-04:00"
            for row in (
                {
                    "event_type": "shadow_option_position",
                    "position_id": "shadow-synthetic",
                    "opened_at": "2026-05-29T09:55:00-04:00",
                    "source_dedupe_key": "full-option-synthetic",
                    "ticker": "AAPL",
                    "contract_symbol": "AAPL260619C00150000",
                    "expiration_date": "2026-06-19",
                    "option_type": "call",
                    "strike_price": 150.0,
                    "contracts": 1,
                },
                {
                    "event_type": "shadow_option_position",
                    "position_id": "shadow-expired",
                    "opened_at": "2026-05-29T09:55:00-04:00",
                    "source_dedupe_key": "real-expired",
                    "ticker": "AAPL",
                    "contract_symbol": "AAPL260515C00150000",
                    "expiration_date": "2026-05-15",
                    "option_type": "call",
                    "strike_price": 150.0,
                    "contracts": 1,
                },
                {
                    "event_type": "shadow_option_position",
                    "position_id": "shadow-real",
                    "opened_at": "2026-05-29T09:55:00-04:00",
                    "source_dedupe_key": "real-option-1",
                    "ticker": "AAPL",
                    "contract_symbol": "AAPL260619C00150000",
                    "expiration_date": "2026-06-19",
                    "option_type": "call",
                    "strike_price": 150.0,
                    "contracts": 1,
                },
            ):
                append_jsonl(option_validation.SHADOW_POSITIONS_FILE, row)

            counts = option_validation.track_open_positions_once()
            assert counts["snapshots"] == 1
            assert counts["skipped_synthetic"] == 1
            assert counts["skipped_expired"] == 1
            rows = read_jsonl(option_validation.QUOTE_SNAPSHOTS_FILE)
            assert len(rows) == 1
            assert rows[0]["storage_profile"] == "tracking_core_v1"
            assert "recent_news" not in rows[0]
            assert "spy_indicators" not in rows[0]
            state = json.loads(option_validation.TRACKING_STATE_FILE.read_text(encoding="utf-8"))
            assert state["positions"]["shadow-real"]["latest_price"] == 0.88
            assert state["positions"]["shadow-real"]["max_price"] == 0.88
            assert state["positions"]["shadow-real"]["min_price"] == 0.88

            second_counts = option_validation.track_open_positions_once()
            assert second_counts["skipped_not_due"] == 1
            assert len(read_jsonl(option_validation.QUOTE_SNAPSHOTS_FILE)) == 1
            scorecard = data_hygiene.data_hygiene_scorecard(tmp_path)
            assert scorecard["files"]["option_quote_snapshots.jsonl"]["synthetic_rows"] == 0
            assert scorecard["files"]["option_tracking_state.json"]["positions"] == 1
        finally:
            option_validation.enrich_option_alert = original_enrich
            option_validation.now_iso = original_now_iso


def test_browser_channel_watcher_filters_and_backfills() -> None:
    with tempfile.TemporaryDirectory() as tmp:
        tmp_path = Path(tmp)
        original_data_dir = discord_browser_channel_watcher.DATA_DIR
        original_messages = discord_browser_channel_watcher.BROWSER_MESSAGES_FILE
        original_state = discord_browser_channel_watcher.BROWSER_STATE_FILE
        try:
            discord_browser_channel_watcher.DATA_DIR = tmp_path
            discord_browser_channel_watcher.BROWSER_MESSAGES_FILE = tmp_path / "discord_browser_messages.jsonl"
            discord_browser_channel_watcher.BROWSER_STATE_FILE = tmp_path / "discord_browser_state.json"
            message = {
                "id": "chat-messages-1-2",
                "text": "OTWSteve\n#CVX May 22 200 call @ 1.59 Bought 3 #Lotto\nTuesday, May 19, 2026 at 3:19 PM",
            }
            timestamp = discord_browser_channel_watcher.extract_message_timestamp(message["text"])
            assert timestamp is not None
            candidates = discord_browser_channel_watcher.filter_candidate_messages(
                [message],
                ["OTWSteve", "SteveOTWS"],
                max_age_minutes=0,
                tz_name="America/Detroit",
                allow_unknown_time=False,
            )
            assert len(candidates) == 1
            counts = discord_browser_channel_watcher.process_browser_messages(
                "492098253337264138",
                "https://discord.com/channels/483483452180791296/492098253337264138",
                candidates,
                mode="live",
                source_prefix="browser_channel",
                process_raw=False,
            )
            assert counts["messages_new"] == 1
            assert counts["raw_backfilled"] == 1
            raw = read_jsonl(tmp_path / "raw_notifications.jsonl")
            assert raw[0]["source_app"] == "DiscordUI"
            assert raw[0]["dedupe_key"].startswith("ui-")
            messages = read_jsonl(discord_browser_channel_watcher.BROWSER_MESSAGES_FILE)
            assert messages[0]["capture_mode"] == "live"
            assert messages[0]["raw_record_keys"] == [raw[0]["dedupe_key"]]
        finally:
            discord_browser_channel_watcher.DATA_DIR = original_data_dir
            discord_browser_channel_watcher.BROWSER_MESSAGES_FILE = original_messages
            discord_browser_channel_watcher.BROWSER_STATE_FILE = original_state


def test_browser_channel_watcher_keeps_identical_text_on_new_message_ids() -> None:
    with tempfile.TemporaryDirectory() as tmp:
        tmp_path = Path(tmp)
        original_data_dir = discord_browser_channel_watcher.DATA_DIR
        original_messages = discord_browser_channel_watcher.BROWSER_MESSAGES_FILE
        original_state = discord_browser_channel_watcher.BROWSER_STATE_FILE
        try:
            discord_browser_channel_watcher.DATA_DIR = tmp_path
            discord_browser_channel_watcher.BROWSER_MESSAGES_FILE = tmp_path / "discord_browser_messages.jsonl"
            discord_browser_channel_watcher.BROWSER_STATE_FILE = tmp_path / "discord_browser_state.json"
            first = {
                "id": "chat-messages-562178552984764436-1509647449315213454",
                "text": "OTWSteve\n#AAPL Jun 18 310 call @ 4.70 Bought 3 #swing\nThursday, May 28, 2026 at 3:59 PM",
            }
            second = {
                "id": "chat-messages-562178552984764436-1511450171756642355",
                "text": "OTWSteve\n#AAPL Jun 18 310 call @ 4.70 Bought 3 #swing\nTuesday, June 2, 2026 at 3:23 PM",
            }
            candidates = discord_browser_channel_watcher.filter_candidate_messages(
                [first, second],
                ["OTWSteve", "SteveOTWS"],
                max_age_minutes=0,
                tz_name="America/Detroit",
                allow_unknown_time=False,
            )
            counts = discord_browser_channel_watcher.process_browser_messages(
                "562178552984764436",
                "https://discord.com/channels/483483452180791296/562178552984764436",
                candidates,
                mode="live",
                source_prefix="browser_channel",
                process_raw=False,
            )
            assert counts["messages_new"] == 2
            assert counts["raw_backfilled"] == 2
            raw = read_jsonl(tmp_path / "raw_notifications.jsonl")
            assert len(raw) == 2
            assert raw[0]["body"] == raw[1]["body"]
            assert raw[0]["dedupe_key"] != raw[1]["dedupe_key"]
        finally:
            discord_browser_channel_watcher.DATA_DIR = original_data_dir
            discord_browser_channel_watcher.BROWSER_MESSAGES_FILE = original_messages
            discord_browser_channel_watcher.BROWSER_STATE_FILE = original_state


def test_browser_channel_watcher_reprocesses_identical_text_as_distinct_buys() -> None:
    with tempfile.TemporaryDirectory() as tmp:
        tmp_path = Path(tmp)
        patch_runtime_paths(tmp_path)
        original_browser_data_dir = discord_browser_channel_watcher.DATA_DIR
        original_messages = discord_browser_channel_watcher.BROWSER_MESSAGES_FILE
        original_state = discord_browser_channel_watcher.BROWSER_STATE_FILE
        original_pipeline_data_dir = run_pipeline_once.DATA_DIR
        original_processed_file = run_pipeline_once.PROCESSED_FILE
        original_summary = run_pipeline_once.write_openclaw_summary
        original_enrich = option_validation.enrich_option_alert
        original_load_bot_config = steve_trade_bot.load_bot_config
        try:
            discord_browser_channel_watcher.DATA_DIR = tmp_path
            discord_browser_channel_watcher.BROWSER_MESSAGES_FILE = tmp_path / "discord_browser_messages.jsonl"
            discord_browser_channel_watcher.BROWSER_STATE_FILE = tmp_path / "discord_browser_state.json"
            run_pipeline_once.DATA_DIR = tmp_path
            run_pipeline_once.PROCESSED_FILE = tmp_path / "processed_notifications.jsonl"
            run_pipeline_once.write_openclaw_summary = lambda decision: None
            option_validation.enrich_option_alert = fake_snapshot
            steve_trade_bot.load_bot_config = lambda required=False: None

            candidates = discord_browser_channel_watcher.filter_candidate_messages(
                [
                    {
                        "id": "chat-messages-562178552984764436-1509647449315213454",
                        "text": "OTWSteve\n#AAPL Jun 18 310 call @ 4.70 Bought 3 #swing\nThursday, May 28, 2026 at 3:59 PM",
                    },
                    {
                        "id": "chat-messages-562178552984764436-1511450171756642355",
                        "text": "OTWSteve\n#AAPL Jun 18 310 call @ 4.70 Bought 3 #swing\nTuesday, June 2, 2026 at 3:23 PM",
                    },
                ],
                ["OTWSteve", "SteveOTWS"],
                max_age_minutes=0,
                tz_name="America/Detroit",
                allow_unknown_time=False,
            )
            counts = discord_browser_channel_watcher.process_browser_messages(
                "562178552984764436",
                "https://discord.com/channels/483483452180791296/562178552984764436",
                candidates,
                mode="live",
                source_prefix="browser_channel",
                process_raw=False,
            )
            assert counts["messages_new"] == 2
            assert counts["raw_backfilled"] == 2

            pipeline_counts = process_raw_notifications(
                read_jsonl(tmp_path / "raw_notifications.jsonl"),
                dry_run_orders=True,
                prior_decisions_override=[],
            )
            assert pipeline_counts["raw_new"] == 2
            assert pipeline_counts["parsed"] == 2
            parsed_buys = [
                row
                for row in read_jsonl(tmp_path / "parsed_alerts.jsonl")
                if row.get("side") == "buy" and row.get("ticker") == "AAPL"
            ]
            assert len(parsed_buys) == 2
            assert {row["source_dedupe_key"] for row in parsed_buys} == {
                row["dedupe_key"] for row in read_jsonl(tmp_path / "raw_notifications.jsonl")
            }
        finally:
            discord_browser_channel_watcher.DATA_DIR = original_browser_data_dir
            discord_browser_channel_watcher.BROWSER_MESSAGES_FILE = original_messages
            discord_browser_channel_watcher.BROWSER_STATE_FILE = original_state
            run_pipeline_once.DATA_DIR = original_pipeline_data_dir
            run_pipeline_once.PROCESSED_FILE = original_processed_file
            run_pipeline_once.write_openclaw_summary = original_summary
            option_validation.enrich_option_alert = original_enrich
            steve_trade_bot.load_bot_config = original_load_bot_config


def test_pipeline_health_pinpoints_stage_failures() -> None:
    with tempfile.TemporaryDirectory() as tmp:
        tmp_path = Path(tmp)
        originals = {
            "RAW_FILE": pipeline_health_monitor.RAW_FILE,
            "PROCESSED_FILE": pipeline_health_monitor.PROCESSED_FILE,
            "PARSED_FILE": pipeline_health_monitor.PARSED_FILE,
            "APPROVAL_CARDS_FILE": pipeline_health_monitor.APPROVAL_CARDS_FILE,
            "AUTO_BUY_REPORTS_FILE": pipeline_health_monitor.AUTO_BUY_REPORTS_FILE,
            "HUMAN_POSITIONS_FILE": pipeline_health_monitor.HUMAN_POSITIONS_FILE,
            "STEVE_EXITS_FILE": pipeline_health_monitor.STEVE_EXITS_FILE,
        }
        try:
            old_time = (dt.datetime.now(ZoneInfo("America/Detroit")) - dt.timedelta(minutes=5)).isoformat(timespec="seconds")
            pipeline_health_monitor.RAW_FILE = tmp_path / "raw_notifications.jsonl"
            pipeline_health_monitor.PROCESSED_FILE = tmp_path / "processed_notifications.jsonl"
            pipeline_health_monitor.PARSED_FILE = tmp_path / "parsed_alerts.jsonl"
            pipeline_health_monitor.APPROVAL_CARDS_FILE = tmp_path / "steve_approval_cards.jsonl"
            pipeline_health_monitor.AUTO_BUY_REPORTS_FILE = tmp_path / "steve_auto_buy_reports.jsonl"
            pipeline_health_monitor.HUMAN_POSITIONS_FILE = tmp_path / "human_paper_positions.jsonl"
            pipeline_health_monitor.STEVE_EXITS_FILE = tmp_path / "steve_option_exits.jsonl"
            append_jsonl(
                pipeline_health_monitor.RAW_FILE,
                {
                    "dedupe_key": "raw-missed",
                    "captured_at": old_time,
                    "body": "#CVX May 22 200 call @ 1.59 Bought 3 #Lotto",
                },
            )
            raw_issues = pipeline_health_monitor.check_raw_processing(90, "America/Detroit")
            assert any(issue.code == "raw_not_processed" for issue in raw_issues)

            append_jsonl(
                pipeline_health_monitor.PARSED_FILE,
                {
                    "source_dedupe_key": "raw-missed",
                    "parsed_at": old_time,
                    "instrument_type": "option",
                    "side": "buy",
                    "ticker": "CVX",
                    "tags": ["lotto"],
                    "raw_text": "#CVX May 22 200 call @ 1.59 Bought 3 #Lotto",
                },
            )
            routing_issues = pipeline_health_monitor.check_routing(90, "America/Detroit")
            assert any(issue.code == "non_hedge_missing_auto_buy" for issue in routing_issues)
        finally:
            for name, value in originals.items():
                setattr(pipeline_health_monitor, name, value)


def test_nightly_review_detects_recursive_improvement_issues() -> None:
    with tempfile.TemporaryDirectory() as tmp:
        tmp_path = Path(tmp)
        patch_runtime_paths(tmp_path)
        day = "2026-05-20"
        assert nightly_review.contract_key({"contract_symbol": "FRVO260717C00045000"}) == "FRVO|2026-07-17|45|call"
        append_jsonl(
            nightly_review.BROWSER_MESSAGES_FILE,
            {
                "event_type": "discord_browser_message",
                "captured_at": f"{day}T15:59:38-04:00",
                "message_timestamp": f"{day}T15:59:00-04:00",
                "channel_id": "1124441863848476863",
                "message_key": "msg-frvo",
                "text_preview": "OTWSteve\n#FRVO July 17 45 call @ 4.95 Bought 4 #swing",
            },
        )
        append_jsonl(
            nightly_review.BROWSER_MESSAGES_FILE,
            {
                "event_type": "discord_browser_message",
                "captured_at": f"{day}T10:55:16-04:00",
                "message_timestamp": f"{day}T10:55:00-04:00",
                "channel_id": "562178552984764436",
                "message_key": "msg-msft-add",
                "text_preview": "OTWSteve\n#MSFT Jun 18 450 call @ 6.35 bought 4 #swing\nadded 2 @ 3.70 #swing",
            },
        )
        append_jsonl(
            nightly_review.BROWSER_MESSAGES_FILE,
            {
                "event_type": "discord_browser_message",
                "captured_at": f"{day}T11:16:56-04:00",
                "message_timestamp": f"{day}T11:16:00-04:00",
                "channel_id": "492098253337264138",
                "message_key": "msg-xom-stop",
                "text_preview": "OTWSteve\n#XOM May 22 162.50 call @ 1.14 Bought 4 #lotto\nstopped out",
            },
        )
        append_jsonl(
            nightly_review.PARSED_FILE,
            {
                "source_dedupe_key": "frvo-parsed",
                "parsed_at": f"{day}T15:59:38-04:00",
                "instrument_type": "option",
                "side": "buy",
                "ticker": "FRVO",
                "expiration_date": "2026-07-17",
                "strike_price": 45.0,
                "option_type": "call",
                "entry_price": 4.95,
                "contracts": 4,
                "tags": ["swing"],
            },
        )
        for position_id in ("human-frvo-1", "human-frvo-2"):
            append_jsonl(
                nightly_review.HUMAN_POSITIONS_FILE,
                {
                    "event_type": "human_paper_option_position",
                    "opened_at": f"{day}T15:59:39-04:00",
                    "position_id": position_id,
                    "ticker": "FRVO",
                    "expiration_date": "2026-07-17",
                    "strike_price": 45.0,
                    "option_type": "call",
                    "entry_price": 5.36,
                    "contracts": 4,
                },
            )
        append_jsonl(
            nightly_review.ORDERS_FILE,
            {
                "event_type": "alpaca_option_paper_order_audit",
                "recorded_at": f"{day}T13:21:15-04:00",
                "status": "blocked",
                "ticker": "CVX",
                "contract_symbol": "CVX260522C00200000",
                "reason": 'Alpaca HTTP 403: {"message":"account not eligible to trade uncovered option contracts"}',
                "payload": {"side": "sell", "qty": "3"},
            },
        )

        report = nightly_review.review_day(day, refresh_browser=False)
        codes = {item["code"] for item in report["issues"]}
        assert "duplicate_paper_position" in codes
        assert "entry_price_worse_than_alert" in codes
        assert "scale_in_not_supported" in codes
        assert "contextual_stop_not_executed" in codes
        assert "broker_position_reconciliation_failed" in codes
        assert report["counts"]["truth_buys"] == 3


def test_nightly_review_capture_method_scorecard() -> None:
    with tempfile.TemporaryDirectory() as tmp:
        tmp_path = Path(tmp)
        patch_runtime_paths(tmp_path)
        day = "2026-05-20"
        append_jsonl(
            nightly_review.BROWSER_MESSAGES_FILE,
            {
                "event_type": "discord_browser_message",
                "captured_at": f"{day}T10:30:08-04:00",
                "message_timestamp": f"{day}T10:30:00-04:00",
                "channel_id": "492098253337264138",
                "message_key": "msg-two-alerts",
                "text_preview": "\n".join(
                    [
                        "OTWSteve",
                        "#TSLA May 22 390 put @ 4.90 Bought 2 #hedge",
                        "#SPY May 22 730 put @ 3.71 Bought 4 #hedge",
                    ]
                ),
            },
        )
        for body, second in (
            ("#TSLA May 22 390 put @ 4.90 Bought 2 #hedge", "08"),
            ("#SPY May 22 730 put @ 3.71 Bought 4 #hedge", "09"),
        ):
            append_jsonl(
                nightly_review.RAW_FILE,
                {
                    "event_type": "raw_discord_ui_backfill",
                    "captured_at": f"{day}T10:30:{second}-04:00",
                    "source_app": "DiscordUI",
                    "bundle_id": "browser_or_clipboard",
                    "title": "OTWSteve",
                    "subtitle": "browser_channel:492098253337264138",
                    "body": body,
                    "raw": {"source": "browser_channel:492098253337264138"},
                    "dedupe_key": f"ui-{second}",
                },
            )
        for index, second in enumerate(("45", "50"), start=1):
            append_jsonl(
                nightly_review.RAW_FILE,
                {
                    "event_type": "raw_discord_notification",
                    "captured_at": f"{day}T10:30:{second}-04:00",
                    "notification_timestamp": f"{day}T10:30:{second}-04:00",
                    "source_app": "Discord",
                    "bundle_id": "com.hnc.Discord",
                    "title": "OTWSteve",
                    "subtitle": "short-term-call-outs-same-week-or-1-week",
                    "body": "#TSLA May 22 390 put @ 4.90 Bought 2 #hedge",
                    "raw": {"thread": "492098253337264138"},
                    "dedupe_key": f"notif-tsla-{index}",
                },
            )

        report = nightly_review.review_day(day, refresh_browser=False)
        scorecard = report["capture_method_scorecard"]
        browser = scorecard["methods"]["browser"]
        notification = scorecard["methods"]["notification"]
        assert scorecard["truth_event_count"] == 2
        assert browser["matched_truth_events"] == 2
        assert browser["capture_rate"] == 1.0
        assert browser["latency"]["avg_seconds"] == 8.5
        assert notification["matched_truth_events"] == 1
        assert notification["capture_rate"] == 0.5
        assert notification["duplicate_event_records"] == 1
        assert scorecard["cross_source_duplicate_truth_events"] == 1
        assert scorecard["recommendation"]["recommended_primary"] == "browser"


def test_nightly_review_browser_refresh_overrides_stale_health_and_surfaces_current_errors() -> None:
    with tempfile.TemporaryDirectory() as tmp:
        tmp_path = Path(tmp)
        patch_runtime_paths(tmp_path)
        day = "2026-05-26"
        append_jsonl(
            nightly_review.PIPELINE_HEALTH_FILE,
            {
                "event_type": "pipeline_health_check",
                "recorded_at": f"{day}T10:00:00-04:00",
                "status": "critical",
                "issues": [
                    {
                        "stage": "browser_capture",
                        "code": "browser_health_stale",
                        "severity": "critical",
                        "message": "Browser capture health is stale.",
                    },
                    {
                        "stage": "browser_capture",
                        "code": "browser_capture_degraded",
                        "severity": "critical",
                        "message": "Browser capture reported channel read errors.",
                    },
                ],
            },
        )
        original_truth_events_from_chrome = nightly_review.truth_events_from_chrome
        try:
            nightly_review.truth_events_from_chrome = lambda *args, **kwargs: (
                [],
                [{"channel_id": "492098253337264138", "status": "ok", "events": 0}],
            )
            healthy_report = nightly_review.review_day(day, refresh_browser=True)
            healthy_codes = {item["code"] for item in healthy_report["issues"]}
            assert "health_browser_health_stale" not in healthy_codes
            assert "health_browser_capture_degraded" not in healthy_codes

            nightly_review.truth_events_from_chrome = lambda *args, **kwargs: (
                [],
                [{"channel_id": "492098253337264138", "status": "error", "reason": "RuntimeError:Chrome AppleScript read timed out", "events": 0}],
            )
            error_report = nightly_review.review_day(day, refresh_browser=True)
            error_codes = {item["code"] for item in error_report["issues"]}
            assert "browser_refresh_channel_error" in error_codes
        finally:
            nightly_review.truth_events_from_chrome = original_truth_events_from_chrome


def test_nightly_review_ignores_synthetic_full_pipeline_artifacts() -> None:
    with tempfile.TemporaryDirectory() as tmp:
        tmp_path = Path(tmp)
        patch_runtime_paths(tmp_path)
        day = "2026-05-21"
        for key in ("full-option-abc123", "full-option-def456"):
            append_jsonl(
                nightly_review.HUMAN_POSITIONS_FILE,
                {
                    "event_type": "human_paper_option_position",
                    "opened_at": f"{day}T00:10:00-04:00",
                    "position_id": f"pos-{key}",
                    "source_dedupe_key": key,
                    "ticker": "NVDA",
                    "expiration_date": "2026-05-15",
                    "strike_price": 215.0,
                    "option_type": "call",
                    "entry_price": 7.15,
                    "contracts": 3,
                },
            )
            append_jsonl(
                nightly_review.ORDERS_FILE,
                {
                    "event_type": "alpaca_option_paper_order_audit",
                    "recorded_at": f"{day}T00:10:01-04:00",
                    "source_dedupe_key": key,
                    "status": "blocked",
                    "ticker": "NVDA",
                    "contract_symbol": "NVDA260515C00215000",
                    "reason": 'Alpaca HTTP 422: {"code": 42210000, "message": "asset not found"}',
                    "payload": {"side": "buy", "qty": "3"},
                },
            )

        report = nightly_review.review_day(day, refresh_browser=False)
        issue_codes = {item["code"] for item in report["issues"]}
        assert "duplicate_paper_position" not in issue_codes
        assert "broker_error" not in issue_codes
        assert report["counts"]["filtered_test_artifacts"] >= 4


def test_nightly_review_compares_steve_local_and_broker_pl() -> None:
    with tempfile.TemporaryDirectory() as tmp:
        tmp_path = Path(tmp)
        patch_runtime_paths(tmp_path)
        day = "2026-05-28"
        append_jsonl(
            nightly_review.ORDERS_FILE,
            {
                "event_type": "alpaca_option_paper_order_audit",
                "recorded_at": f"{day}T15:59:59-04:00",
                "status": "submitted",
                "ticker": "AAPL",
                "contract_symbol": "AAPL260618C00310000",
                "position_id": "human-unfilled",
                "payload": {"side": "buy", "qty": "3"},
                "response": {"id": "order-unfilled", "status": "accepted"},
                "source_dedupe_key": "ui-unfilled",
            },
        )
        append_jsonl(
            nightly_review.HUMAN_POSITIONS_FILE,
            {
                "event_type": "human_paper_option_position",
                "opened_at": f"{day}T10:00:00-04:00",
                "position_id": "human-broker-pl",
                "source_dedupe_key": "ui-broker-pl",
                "ticker": "MS",
                "contract_symbol": "MS260529C00200000",
                "option_type": "call",
                "expiration_date": "2026-05-29",
                "strike_price": 200.0,
                "contracts": 1,
                "entry_price": 2.0,
            },
        )
        append_jsonl(
            nightly_review.HUMAN_EXITS_FILE,
            {
                "event_type": "human_paper_option_exit",
                "recorded_at": f"{day}T11:00:00-04:00",
                "position_id": "human-broker-pl",
                "ticker": "MS",
                "contract_symbol": "MS260529C00200000",
                "option_type": "call",
                "expiration_date": "2026-05-29",
                "strike_price": 200.0,
                "contracts": 1,
                "entry_price": 2.0,
                "exit_price": 3.0,
                "pnl_dollars": 100.0,
                "broker_client_order_id": "exit-broker-pl",
            },
        )
        for row in (
            {
                "event_type": "broker_order_status_report",
                "recorded_at": f"{day}T10:00:05-04:00",
                "order_id": "entry-broker-pl",
                "client_order_id": "entry-broker-pl",
                "broker_status": "filled",
                "position_id": "human-broker-pl",
                "source_dedupe_key": "ui-broker-pl",
                "contract_symbol": "MS260529C00200000",
                "side": "buy",
                "filled_qty": "1",
                "filled_avg_price": "2.00",
            },
            {
                "event_type": "broker_order_status_report",
                "recorded_at": f"{day}T11:00:05-04:00",
                "order_id": "exit-broker-pl",
                "client_order_id": "exit-broker-pl",
                "broker_status": "filled",
                "position_id": "human-broker-pl",
                "source_dedupe_key": "ui-broker-pl",
                "contract_symbol": "MS260529C00200000",
                "side": "sell",
                "filled_qty": "1",
                "filled_avg_price": "1.00",
            },
        ):
            append_jsonl(nightly_review.BROKER_STATUS_FILE, row)
        report = nightly_review.review_day(day, refresh_browser=False)
        codes = {item["code"] for item in report["issues"]}
        assert "submitted_broker_order_unresolved" in codes
        assert "local_pnl_differs_from_broker_fills" in codes
        assert report["broker_fill_pl"]["realized_pnl"] == -100.0
        assert report["steve_alert_pl"]["basis"] == "steve_buy_alert_and_steve_sell_alert_prices"
        assert any(item["auto_fixable"] for item in report["recursive_improvement_plan"])


def test_nightly_review_recommended_actions_include_health_fallbacks() -> None:
    issues = [
        {
            "severity": "critical",
            "code": "health_browser_capture_degraded",
            "message": "Browser capture reported channel read errors.",
            "recommendation": "Restart browser watcher and verify channel tab access before market open.",
        },
        {
            "severity": "critical",
            "code": "health_browser_health_stale",
            "message": "Browser capture health is stale.",
            "recommendation": "Restart browser watcher and verify channel tab access before market open.",
        },
        {
            "severity": "warning",
            "code": "broker_terminal_not_filled",
            "message": "Broker order reached a terminal status without a fill.",
            "recommendation": "Report expired/unfilled orders separately from real positions; consider no-new-entry cutoff near close.",
        },
    ]
    actions = nightly_review.recommended_next_actions(issues)
    assert actions == [
        "Report expired/unfilled orders separately from real positions; consider no-new-entry cutoff near close.",
        "Restart browser watcher and verify channel tab access before market open.",
    ]


def test_nightly_review_writes_markdown_and_summary() -> None:
    with tempfile.TemporaryDirectory() as tmp:
        tmp_path = Path(tmp)
        patch_runtime_paths(tmp_path)
        report = {
            "event_type": "nightly_pipeline_review",
            "day": "2026-05-20",
            "generated_at": "2026-05-20T17:30:00-04:00",
            "truth_events": [],
            "counts": {"truth_buys": 0, "truth_exits": 0, "truth_adds": 0, "truth_context_stops": 0, "matched_buys": 0, "paper_entries": 0, "broker_filled_buys": 0},
            "issue_counts": {"critical": 1},
            "capture_method_scorecard": {
                "truth_event_count": 1,
                "methods": {
                    "browser": {
                        "matched_truth_events": 1,
                        "capture_rate": 1.0,
                        "latency": {"avg_seconds": 4.0},
                        "raw_records": 1,
                        "duplicate_event_records": 0,
                    },
                    "notification": {
                        "matched_truth_events": 0,
                        "capture_rate": 0.0,
                        "latency": {"avg_seconds": None},
                        "raw_records": 0,
                        "duplicate_event_records": 0,
                    },
                    "other": {
                        "matched_truth_events": 0,
                        "capture_rate": 0.0,
                        "latency": {"avg_seconds": None},
                        "raw_records": 0,
                        "duplicate_event_records": 0,
                    },
                },
                "cross_source_duplicate_truth_events": 0,
                "recommendation": {"recommended_primary": "browser", "reason": "test", "browser_interval_seconds": 5},
            },
            "issues": [
                {
                    "severity": "critical",
                    "code": "duplicate_paper_position",
                    "message": "Duplicate paper position.",
                    "recommendation": "Canonicalize dedupe across sources.",
                }
            ],
            "daily_pl": {"total_pnl": -25.0, "realized_pnl": -30.0, "open_pnl": 5.0},
            "all_time_pl": {"total_pnl": 125.0, "realized_pnl": 100.0, "open_pnl": 25.0},
            "recommended_next_actions": ["Canonicalize dedupe across sources."],
        }
        json_path, md_path = nightly_review.write_report(report)
        assert json_path.exists()
        assert md_path.exists()
        assert "duplicate_paper_position" in md_path.read_text(encoding="utf-8")
        summary_rows = read_jsonl(nightly_review.NIGHTLY_SUMMARY_FILE)
        assert len(summary_rows) == 1
        message = nightly_review.telegram_summary(report)
        assert "NIGHTLY PIPELINE REVIEW" in message
        assert "Issues: 1 critical" in message
        assert "All-time P/L: +$125" in message
        sent_messages: list[str] = []
        original_sender = steve_trade_bot.send_message_to_configured_chats
        try:
            steve_trade_bot.send_message_to_configured_chats = lambda message: (
                sent_messages.append(message) or ("sent", "", [{"chat_id": "123", "message_id": 1, "status": "sent"}])
            )
            first_delivery = nightly_review.send_telegram_report(report)
            second_delivery = nightly_review.send_telegram_report(report)
        finally:
            steve_trade_bot.send_message_to_configured_chats = original_sender
        assert first_delivery["status"] == "sent"
        assert second_delivery["status"] == "already_sent"
        assert len(sent_messages) == 1


def test_nightly_review_markdown_interval_na_text() -> None:
    report = {
        "event_type": "nightly_pipeline_review",
        "day": "2026-05-26",
        "generated_at": "2026-05-26T17:30:00-04:00",
        "truth_events": [],
        "counts": {"truth_buys": 0, "truth_exits": 0, "truth_adds": 0, "truth_context_stops": 0, "matched_buys": 0, "paper_entries": 0, "broker_filled_buys": 0},
        "issue_counts": {"critical": 1},
        "capture_method_scorecard": {
            "truth_event_count": 0,
            "methods": {
                "browser": {"matched_truth_events": 0, "capture_rate": 0.0, "latency": {"avg_seconds": None}, "raw_records": 0, "duplicate_event_records": 0},
                "notification": {"matched_truth_events": 0, "capture_rate": 0.0, "latency": {"avg_seconds": None}, "raw_records": 0, "duplicate_event_records": 0},
                "other": {"matched_truth_events": 0, "capture_rate": 0.0, "latency": {"avg_seconds": None}, "raw_records": 0, "duplicate_event_records": 0},
            },
            "cross_source_duplicate_truth_events": 0,
            "recommendation": {"recommended_primary": "insufficient_data", "reason": "none", "browser_interval_seconds": None},
        },
        "issues": [
            {
                "severity": "critical",
                "code": "health_browser_health_stale",
                "message": "Browser capture health is stale.",
                "recommendation": "Restart browser watcher before market open.",
            }
        ],
        "daily_pl": {},
        "recommended_next_actions": ["Restart browser watcher before market open."],
    }
    markdown = nightly_review.markdown_report(report)
    assert "browser interval target: n/a" in markdown
    assert "browser interval target: Nones" not in markdown


def test_nightly_review_broker_reason_classification() -> None:
    assert nightly_review.classify_broker_reason("client_order_id must be unique") == "duplicate_broker_order"
    assert nightly_review.classify_broker_reason("options market orders are only allowed during market hours") == "broker_market_closed"
    assert nightly_review.classify_broker_reason("account not eligible to trade uncovered option contracts") == "broker_position_reconciliation_failed"
    assert nightly_review.classify_broker_reason("asset \"GS260522C00200000\" not found") == "broker_contract_not_found"
    assert nightly_review.classify_broker_reason("paper_order_submission_disabled") == "paper_order_disabled"
    assert nightly_review.broker_issue_recommendation("broker_contract_not_found").startswith("Validate option contract symbol")
    assert nightly_review.broker_issue_recommendation("broker_market_closed").startswith("Skip option submits outside market hours")


def test_option_sell_order_is_blocked_when_market_closed() -> None:
    original_load_adapter_config = alpaca_options.load_adapter_config
    original_require_paper_environment = alpaca_options.require_paper_environment
    original_options_market_open = alpaca_options.options_market_open
    original_alpaca_request = alpaca_options.alpaca_request
    try:
        alpaca_options.load_adapter_config = lambda: ({}, {})
        alpaca_options.require_paper_environment = lambda config, env_file, require_keys=True: {
            "base_url": "https://paper-api.alpaca.markets",
            "key_id": "paper-key",
            "secret_key": "paper-secret",
            "submit_enabled": True,
        }
        alpaca_options.options_market_open = lambda env: (False, "options_market_closed:next_open=2026-05-29T09:30:00-04:00")
        alpaca_options.alpaca_request = lambda method, path, env, body=None: (_ for _ in ()).throw(AssertionError("should not submit order when closed"))
        audit = alpaca_options.submit_option_paper_sell_order(
            {
                "position_id": "human-test-closed",
                "source_dedupe_key": "closed-source",
                "ticker": "AAPL",
                "contract_symbol": "AAPL260618C00310000",
            },
            1,
            "stop_loss",
            "stop-loss-test",
        )
        assert audit["status"] == "blocked"
        assert str(audit.get("reason") or "").startswith("options_market_closed")
        assert (audit.get("payload") or {}).get("side") == "sell"
    finally:
        alpaca_options.load_adapter_config = original_load_adapter_config
        alpaca_options.require_paper_environment = original_require_paper_environment
        alpaca_options.options_market_open = original_options_market_open
        alpaca_options.alpaca_request = original_alpaca_request


def test_browser_snapshot_retries_increase_timeout_and_delay() -> None:
    original_reader = discord_browser_channel_watcher.read_channel_snapshot
    original_sleep = discord_browser_channel_watcher.time.sleep
    attempts: list[tuple[int, float]] = []
    try:
        def fake_reader(channel_url: str, timeout: int = 15, first_load_delay: float = 4.0) -> dict[str, Any]:
            attempts.append((timeout, round(first_load_delay, 2)))
            if len(attempts) < 3:
                raise RuntimeError("temporary read error")
            return {"messages": []}

        discord_browser_channel_watcher.read_channel_snapshot = fake_reader
        discord_browser_channel_watcher.time.sleep = lambda _seconds: None
        snapshot = discord_browser_channel_watcher.read_channel_snapshot_with_retries(
            "https://discord.com/channels/1/2",
            timeout=10,
            first_load_delay=2.0,
            retries=2,
        )
        assert snapshot == {"messages": []}
        assert attempts == [(10, 2.0), (15, 2.75), (20, 3.5)]
    finally:
        discord_browser_channel_watcher.read_channel_snapshot = original_reader
        discord_browser_channel_watcher.time.sleep = original_sleep


def test_browser_snapshot_timeout_does_not_retry() -> None:
    original_reader = discord_browser_channel_watcher.read_channel_snapshot
    original_sleep = discord_browser_channel_watcher.time.sleep
    attempts: list[tuple[int, float]] = []
    sleeps: list[int] = []
    try:
        def fake_reader(channel_url: str, timeout: int = 15, first_load_delay: float = 4.0) -> dict[str, Any]:
            attempts.append((timeout, round(first_load_delay, 2)))
            raise RuntimeError("Chrome AppleScript read timed out")

        discord_browser_channel_watcher.read_channel_snapshot = fake_reader
        discord_browser_channel_watcher.time.sleep = lambda seconds: sleeps.append(int(seconds))
        try:
            discord_browser_channel_watcher.read_channel_snapshot_with_retries(
                "https://discord.com/channels/1/2",
                timeout=10,
                first_load_delay=2.0,
                retries=2,
            )
            raise AssertionError("expected timeout error")
        except RuntimeError as exc:
            assert "timed out" in str(exc).lower()
        assert attempts == [(10, 2.0)]
        assert sleeps == []
    finally:
        discord_browser_channel_watcher.read_channel_snapshot = original_reader
        discord_browser_channel_watcher.time.sleep = original_sleep


def main() -> int:
    test_parser()
    test_validation_and_approval()
    test_non_hedge_auto_paper_buy()
    test_non_hedge_bad_entry_requires_approval()
    test_non_hedge_mixed_buy_exit_requires_approval()
    test_fill_price_caps_excessive_slippage()
    test_exit_plan_contract_allocation()
    test_multi_destination_approval_cards()
    test_close_report_message_and_delivery()
    test_human_exit_rules_and_steve_catch_up()
    test_steve_alert_pl_summary_uses_steve_prices()
    test_option_exit_reply_matches_shadow_context()
    test_pipeline_processes_close_reply_as_option_exit()
    test_backfill_text_audit_matches_contextual_exits()
    test_chrome_visible_capture_filters_history_by_default()
    test_option_order_payload()
    test_broker_order_monitor_reports_terminal_fills()
    test_daily_pl_summary_short_report()
    test_watcher_steve_filters()
    test_live_pipeline_heartbeat()
    test_option_tracker_skips_junk_and_writes_lean_deduped_snapshots()
    test_browser_channel_watcher_filters_and_backfills()
    test_pipeline_health_pinpoints_stage_failures()
    test_nightly_review_detects_recursive_improvement_issues()
    test_nightly_review_capture_method_scorecard()
    test_nightly_review_browser_refresh_overrides_stale_health_and_surfaces_current_errors()
    test_nightly_review_ignores_synthetic_full_pipeline_artifacts()
    test_nightly_review_compares_steve_local_and_broker_pl()
    test_nightly_review_recommended_actions_include_health_fallbacks()
    test_nightly_review_writes_markdown_and_summary()
    test_nightly_review_markdown_interval_na_text()
    test_nightly_review_broker_reason_classification()
    test_option_sell_order_is_blocked_when_market_closed()
    test_browser_snapshot_retries_increase_timeout_and_delay()
    test_browser_snapshot_timeout_does_not_retry()
    print("Steve options MVP tests passed")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())

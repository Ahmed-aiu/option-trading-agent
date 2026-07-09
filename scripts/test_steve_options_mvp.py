#!/usr/bin/env python3
"""Focused tests for the Steve options validation MVP."""

from __future__ import annotations

import datetime as dt
import json
import os
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
from steve_options_test_support import fake_snapshot, parsed_records, patch_runtime_paths


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
        option_validation.enrich_option_alert = lambda alert: {
            **fake_snapshot(alert),
            "option_quote": {
                **fake_snapshot(alert)["option_quote"],
                "ask": 1.2,
                "mark": 1.1,
            },
        }
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
            approval_chat_id="123456789",
            owner_chat_id="123456789",
            owner_user_id="123456789",
            approval_chat_ids=("123456789", "-1001112223334"),
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
                "chat": {"id": "123456789"},
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
                "chat": {"id": "123456789"},
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
                "chat": {"id": "123456789"},
                "from": {"id": 123456789},
                "text": "buy contracts=1 stop=35 take=50",
                "reply_to_message": {"message_id": 100},
            },
            config,
        )
        assert approved["action"] == "approved"
        positions = read_jsonl(steve_trade_bot.HUMAN_POSITIONS_FILE)
        assert positions == []
        actions = read_jsonl(steve_trade_bot.APPROVAL_ACTIONS_FILE)
        assert actions[-1]["broker_status"] == "blocked"
        assert actions[-1]["broker_reason"] == "paper_order_submission_disabled"

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
        assert duplicate["action"] == "unauthorized"
        assert duplicate["reason"] == "unauthorized_chat"

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


def test_hedge_auto_paper_buy() -> None:
    with tempfile.TemporaryDirectory() as tmp:
        tmp_path = Path(tmp)
        patch_runtime_paths(tmp_path)
        option_validation.enrich_option_alert = fake_snapshot
        steve_trade_bot.load_bot_config = lambda required=False: steve_trade_bot.BotConfig(
            token="test",
            approval_chat_id="123456789",
            owner_chat_id="123456789",
            owner_user_id="123456789",
            approval_chat_ids=("123456789",),
        )
        steve_trade_bot.send_telegram_message = lambda config, text, chat_id=None: {
            "ok": True,
            "result": {"message_id": 1, "chat": {"id": chat_id or config.approval_chat_id}},
        }
        steve_trade_bot.submit_option_paper_order = lambda position: {
            "status": "submitted",
            "reason": "",
            "position_id": position.get("position_id"),
        }

        alert = parsed_records(
            parse_trade_alert(
                {
                    "captured_at": "2026-05-08T13:09:00-04:00",
                    "dedupe_key": "screen-hedge-auto-001",
                    "body": "#CRWV MAY 8 113 call @ .88 Bought 5 #hedge",
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
        reports = read_jsonl(steve_trade_bot.AUTO_BUY_REPORTS_FILE)
        assert len(reports) == 1
        assert reports[0]["broker_status"] == "submitted"


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
        assert len(sent_messages) == 1
        assert sent_messages[0][0] == "123456789"

        duplicate = option_validation.handle_option_entry(alert, send_approval=True)
        assert duplicate["auto_buy"]["created"] is False
        assert len(read_jsonl(steve_trade_bot.HUMAN_POSITIONS_FILE)) == 1
        assert len(read_jsonl(steve_trade_bot.AUTO_BUY_REPORTS_FILE)) == 1


def test_non_hedge_auto_paper_buy_does_not_persist_blocked_broker_position() -> None:
    with tempfile.TemporaryDirectory() as tmp:
        tmp_path = Path(tmp)
        patch_runtime_paths(tmp_path)
        option_validation.enrich_option_alert = fake_snapshot
        steve_trade_bot.load_bot_config = lambda required=False: steve_trade_bot.BotConfig(
            token="test",
            approval_chat_id="123456789",
            owner_chat_id="123456789",
            owner_user_id="123456789",
            approval_chat_ids=("123456789",),
        )
        steve_trade_bot.send_telegram_message = lambda config, text, chat_id=None: {
            "ok": True,
            "result": {"message_id": 1, "chat": {"id": chat_id or config.approval_chat_id}},
        }
        steve_trade_bot.submit_option_paper_order = lambda position: {
            "status": "blocked",
            "reason": "Alpaca HTTP 422: expires soon",
            "position_id": position.get("position_id"),
        }

        alert = parsed_records(
            parse_trade_alert(
                {
                    "captured_at": "2026-05-08T13:09:00-04:00",
                    "dedupe_key": "screen-auto-blocked-001",
                    "body": "#SPY MAY 8 745 call @ 1.66 Bought 4 #swing",
                }
            )
        )[0]
        result = option_validation.handle_option_entry(alert, send_approval=True)
        assert result["route"] == "auto_paper_buy"
        assert result["auto_buy"]["created"] is True
        assert read_jsonl(steve_trade_bot.HUMAN_POSITIONS_FILE) == []
        actions = read_jsonl(steve_trade_bot.APPROVAL_ACTIONS_FILE)
        assert actions[-1]["broker_status"] == "blocked"
        assert actions[-1]["broker_reason"] == "Alpaca HTTP 422: expires soon"
        reports = read_jsonl(steve_trade_bot.AUTO_BUY_REPORTS_FILE)
        assert reports[-1]["broker_status"] == "blocked"


def test_non_hedge_auto_paper_buy_duplicate_blocked_alert_is_idempotent() -> None:
    with tempfile.TemporaryDirectory() as tmp:
        tmp_path = Path(tmp)
        patch_runtime_paths(tmp_path)
        option_validation.enrich_option_alert = fake_snapshot
        steve_trade_bot.load_bot_config = lambda required=False: steve_trade_bot.BotConfig(
            token="test",
            approval_chat_id="123456789",
            owner_chat_id="123456789",
            owner_user_id="123456789",
            approval_chat_ids=("123456789",),
        )
        steve_trade_bot.send_telegram_message = lambda config, text, chat_id=None: {
            "ok": True,
            "result": {"message_id": 1, "chat": {"id": chat_id or config.approval_chat_id}},
        }
        call_count = {"submit": 0}

        def blocked_submit(position: dict[str, object]) -> dict[str, object]:
            call_count["submit"] += 1
            return {
                "status": "blocked",
                "reason": "Alpaca HTTP 422: expires soon",
                "position_id": position.get("position_id"),
            }

        steve_trade_bot.submit_option_paper_order = blocked_submit
        alert = parsed_records(
            parse_trade_alert(
                {
                    "captured_at": "2026-05-08T13:09:00-04:00",
                    "dedupe_key": "screen-auto-blocked-dup-001",
                    "body": "#SPY MAY 8 745 call @ 1.66 Bought 4 #swing",
                }
            )
        )[0]
        first = option_validation.handle_option_entry(alert, send_approval=True)
        second = option_validation.handle_option_entry(alert, send_approval=True)
        assert first["auto_buy"]["created"] is True
        assert second["auto_buy"]["created"] is False
        assert call_count["submit"] == 1
        assert len(read_jsonl(steve_trade_bot.AUTO_BUY_REPORTS_FILE)) == 1
        actions = read_jsonl(steve_trade_bot.APPROVAL_ACTIONS_FILE)
        assert actions[-1]["broker_reason"] == "duplicate_auto_paper_buy_already_processed"


def test_option_order_payload_rounds_limit_price_to_two_decimals() -> None:
    payload = alpaca_options.build_option_order_payload(
        {
            "contracts": 3,
            "entry_price": 4.019,
            "source_dedupe_key": "ui-rounding-001",
            "position_id": "human-rounding-001",
            "contract_symbol": "HOOD260618C00090000",
        }
    )
    assert payload["limit_price"] == "4.02"


def test_option_entry_order_skips_existing_client_order_id_without_resubmit() -> None:
    with tempfile.TemporaryDirectory() as tmp:
        tmp_path = Path(tmp)
        patch_runtime_paths(tmp_path)
        position = {
            "position_id": "human-existing-order",
            "source_dedupe_key": "ui-existing-order",
            "ticker": "NFLX",
            "contract_symbol": "NFLX260717C00080000",
            "contracts": 5,
            "entry_price": 1.95,
        }
        payload = alpaca_options.build_option_order_payload(position)
        append_jsonl(
            alpaca_options.ORDERS_FILE,
            {
                "event_type": "alpaca_option_paper_order_audit",
                "action": "paper_entry_order",
                "recorded_at": "2026-06-26T12:15:34-04:00",
                "status": "submitted",
                "reason": "",
                "position_id": position["position_id"],
                "source_dedupe_key": position["source_dedupe_key"],
                "ticker": position["ticker"],
                "contract_symbol": position["contract_symbol"],
                "payload": payload,
                "response": {"id": "order-existing", "client_order_id": payload["client_order_id"]},
            },
        )
        call_count = {"request": 0}
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
            alpaca_options.options_market_open = lambda env: (True, "")

            def fake_request(method: str, path: str, env: dict[str, str], body: dict[str, object] | None = None):
                call_count["request"] += 1
                raise AssertionError("duplicate order should not be submitted")

            alpaca_options.alpaca_request = fake_request
            audit = alpaca_options.submit_option_paper_order(position)
        finally:
            alpaca_options.load_adapter_config = original_load_adapter_config
            alpaca_options.require_paper_environment = original_require_paper_environment
            alpaca_options.options_market_open = original_options_market_open
            alpaca_options.alpaca_request = original_alpaca_request
        assert audit["status"] == "skipped"
        assert audit["reason"] == "duplicate_client_order_id_already_audited"
        assert audit["duplicate_of_recorded_at"] == "2026-06-26T12:15:34-04:00"
        assert call_count["request"] == 0
        assert len(read_jsonl(alpaca_options.ORDERS_FILE)) == 1


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


def test_dm_only_approval_and_executive_group_routing() -> None:
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
        assert steve_trade_bot.configured_approval_chat_ids(config) == ("123456789",)
        assert steve_trade_bot.configured_executive_chat_ids(config) == ("-1001234567890",)

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
        assert sent_chat_ids == ["123456789"]
        assert [(row["chat_id"], row["message_id"]) for row in card["telegram_messages"]] == [
            ("123456789", 10),
        ]

        group_skip = steve_trade_bot.process_approval_message(
            {
                "message_id": 21,
                "chat": {"id": "-1001234567890"},
                "from": {"id": 222333444},
                "text": "skip",
                "reply_to_message": {"message_id": 10},
            },
            config,
        )
        assert group_skip["action"] == "unauthorized"
        assert group_skip["reason"] == "unauthorized_chat"

        owner_skip = steve_trade_bot.process_approval_message(
            {
                "message_id": 22,
                "chat": {"id": "123456789"},
                "from": {"id": 123456789},
                "text": "skip",
                "reply_to_message": {"message_id": 10},
            },
            config,
        )
        assert owner_skip["action"] == "skipped"
        assert owner_skip["authorization_scope"] == "owner_dm"
        assert owner_skip["approval_id"] == card["approval_id"]

        status, reason, messages = steve_trade_bot.send_message_to_executive_chats("EXECUTIVE SUMMARY")
        assert status == "sent"
        assert reason == ""
        assert [(row["chat_id"], row["message_id"]) for row in messages] == [
            ("-1001234567890", 20),
        ]


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
        assert [row[0] for row in sent] == ["123456789"]
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


def test_contextual_stop_closes_human_position_at_local_stop() -> None:
    with tempfile.TemporaryDirectory() as tmp:
        tmp_path = Path(tmp)
        patch_runtime_paths(tmp_path)
        steve_trade_bot.load_bot_config = lambda required=False: None
        shadow = {
            "event_type": "shadow_option_position",
            "position_id": "shadow-googl-stop",
            "opened_at": "2026-06-22T11:02:00-04:00",
            "source_dedupe_key": "ui-googl-stop",
            "ticker": "GOOGL",
            "contract_symbol": "GOOGL260717C00390000",
            "option_type": "call",
            "expiration_date": "2026-07-17",
            "strike_price": 390.0,
            "contracts": 3,
        }
        human = {
            "event_type": "human_paper_option_position",
            "position_id": "human-googl-stop",
            "approval_id": "auto-googl-stop",
            "opened_at": "2026-06-22T11:02:01-04:00",
            "source_dedupe_key": "ui-googl-stop",
            "ticker": "GOOGL",
            "contract_symbol": "GOOGL260717C00390000",
            "option_type": "call",
            "expiration_date": "2026-07-17",
            "strike_price": 390.0,
            "contracts": 3,
            "entry_price": 1.63,
            "risk_type": "percent",
            "stop_percent": 35.0,
        }
        append_jsonl(option_validation.SHADOW_POSITIONS_FILE, shadow)
        append_jsonl(option_validation.HUMAN_POSITIONS_FILE, human)
        raw = {
            "event_type": "raw_discord_ui_backfill",
            "dedupe_key": "ui-googl-stop-exit",
            "captured_at": "2026-06-22T11:02:12-04:00",
            "notification_timestamp": "2026-06-22T11:02:00-04:00",
            "title": "OTWSteve",
            "subtitle": "browser_channel:562178552984764436",
            "body": "#GOOGL July 17 390 call @ 4.80 Bought 3\nstopped out",
        }
        parsed = parse_trade_alert(raw)
        assert parsed["exit_action"] == "stopped_out"
        result = option_validation.handle_option_exit(parsed)
        assert result["created"] is True
        assert result["human_exits"] == 1
        steve_exits = read_jsonl(option_validation.STEVE_EXITS_FILE)
        assert steve_exits[0]["exit_action"] == "stopped_out"
        assert steve_exits[0]["exit_price"] is None
        human_exits = read_jsonl(option_validation.HUMAN_EXITS_FILE)
        assert len(human_exits) == 1
        assert human_exits[0]["contracts"] == 3
        assert human_exits[0]["exit_price"] == 1.0595
        assert human_exits[0]["exit_price_source"] == "local_stop_price_contextual_steve_stop"


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


def test_option_exit_duplicate_capture_is_idempotent() -> None:
    with tempfile.TemporaryDirectory() as tmp:
        tmp_path = Path(tmp)
        patch_runtime_paths(tmp_path)
        steve_trade_bot.load_bot_config = lambda required=False: None
        shadow = {
            "event_type": "shadow_option_position",
            "position_id": "shadow-xom",
            "opened_at": "2026-05-18T10:00:00-04:00",
            "source_dedupe_key": "source-xom",
            "ticker": "XOM",
            "contract_symbol": "XOM260522C00160000",
            "option_type": "call",
            "expiration_date": "2026-05-22",
            "strike_price": 160.0,
            "contracts": 10,
        }
        human = {
            "event_type": "human_paper_option_position",
            "position_id": "human-xom",
            "approval_id": "auto-xom",
            "opened_at": "2026-05-18T10:00:01-04:00",
            "source_dedupe_key": "source-xom",
            "ticker": "XOM",
            "contract_symbol": "XOM260522C00160000",
            "option_type": "call",
            "expiration_date": "2026-05-22",
            "strike_price": 160.0,
            "contracts": 10,
            "entry_price": 1.62,
        }
        append_jsonl(option_validation.SHADOW_POSITIONS_FILE, shadow)
        append_jsonl(option_validation.HUMAN_POSITIONS_FILE, human)
        base_raw = {
            "captured_at": "2026-05-18T14:17:01-04:00",
            "notification_timestamp": "2026-05-18T14:17:00-04:00",
            "body": "@OTWSteve\n#XOM MAY 22 160 call @ 1.62 Bought 10 #swing\nSteveOTWS\nSold 2 @ 3.26",
        }
        first = parse_trade_alert({**base_raw, "dedupe_key": "exit-xom-browser"})
        second = parse_trade_alert({**base_raw, "dedupe_key": "exit-xom-notification"})
        first_result = option_validation.handle_option_exit(first)
        second_result = option_validation.handle_option_exit(second)
        assert first_result["created"] is True
        assert second_result["created"] is False
        assert len(read_jsonl(option_validation.STEVE_EXITS_FILE)) == 1
        assert len(read_jsonl(option_validation.HUMAN_EXITS_FILE)) == 1
        exits = read_jsonl(option_validation.STEVE_EXITS_FILE)
        assert option_validation.applied_exit_contracts("shadow-xom", exits + [dict(exits[0], exit_id="legacy-dupe")]) == 2


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


def test_pipeline_skips_stale_browser_backfill_before_routing() -> None:
    with tempfile.TemporaryDirectory() as tmp:
        tmp_path = Path(tmp)
        patch_runtime_paths(tmp_path)
        original_data_dir = run_pipeline_once.DATA_DIR
        original_processed_file = run_pipeline_once.PROCESSED_FILE
        original_summary = run_pipeline_once.write_openclaw_summary
        original_enrich = option_validation.enrich_option_alert
        original_load_bot_config = steve_trade_bot.load_bot_config
        original_submit_order = steve_trade_bot.submit_option_paper_order
        try:
            run_pipeline_once.DATA_DIR = tmp_path
            run_pipeline_once.PROCESSED_FILE = tmp_path / "processed_notifications.jsonl"
            run_pipeline_once.write_openclaw_summary = lambda decision: None
            option_validation.enrich_option_alert = fake_snapshot
            steve_trade_bot.load_bot_config = lambda required=False: None
            steve_trade_bot.submit_option_paper_order = lambda position: {
                "status": "submitted",
                "reason": "",
                "position_id": position.get("position_id"),
            }
            counts = run_pipeline_once.process_raw_notifications(
                [
                    {
                        "event_type": "raw_discord_ui_backfill",
                        "dedupe_key": "stale-browser-tsla",
                        "captured_at": "2026-07-08T10:21:27-04:00",
                        "notification_timestamp": "2026-07-08T09:32:00-04:00",
                        "source_app": "DiscordUI",
                        "bundle_id": "browser_or_clipboard",
                        "title": "OTWSteve",
                        "subtitle": "browser_channel:492098253337264138",
                        "body": "#TSLA JULY 8 400 call @ 2.70 Bought 2 #Lotto",
                    }
                ],
                dry_run_orders=False,
                prior_decisions_override=[],
            )
            assert counts["raw_new"] == 1
            assert counts["stale_skipped"] == 1
            assert counts["parsed"] == 0
            assert counts["option_auto_buys"] == 0
            assert read_jsonl(option_validation.SHADOW_POSITIONS_FILE) == []
            assert read_jsonl(steve_trade_bot.HUMAN_POSITIONS_FILE) == []
            rejected = read_jsonl(tmp_path / "rejected_alerts.jsonl")
            assert rejected[0]["reason"] == "stale_raw_alert"
            processed = read_jsonl(run_pipeline_once.PROCESSED_FILE)
            assert processed[0]["status"] == "skipped:stale_raw_alert"
        finally:
            run_pipeline_once.DATA_DIR = original_data_dir
            run_pipeline_once.PROCESSED_FILE = original_processed_file
            run_pipeline_once.write_openclaw_summary = original_summary
            option_validation.enrich_option_alert = original_enrich
            steve_trade_bot.load_bot_config = original_load_bot_config
            steve_trade_bot.submit_option_paper_order = original_submit_order


def test_pipeline_close_reply_does_not_duplicate_original_buy() -> None:
    with tempfile.TemporaryDirectory() as tmp:
        tmp_path = Path(tmp)
        patch_runtime_paths(tmp_path)
        original_data_dir = run_pipeline_once.DATA_DIR
        original_processed_file = run_pipeline_once.PROCESSED_FILE
        original_summary = run_pipeline_once.write_openclaw_summary
        original_enrich = option_validation.enrich_option_alert
        original_load_bot_config = steve_trade_bot.load_bot_config
        original_submit_order = steve_trade_bot.submit_option_paper_order
        original_max_raw_age = os.environ.get("OPENCLAW_MAX_RAW_ALERT_AGE_SECONDS")
        try:
            run_pipeline_once.DATA_DIR = tmp_path
            run_pipeline_once.PROCESSED_FILE = tmp_path / "processed_notifications.jsonl"
            run_pipeline_once.write_openclaw_summary = lambda decision: None
            os.environ["OPENCLAW_MAX_RAW_ALERT_AGE_SECONDS"] = "999999999"
            option_validation.enrich_option_alert = fake_snapshot
            steve_trade_bot.load_bot_config = lambda required=False: None
            steve_trade_bot.submit_option_paper_order = lambda position: {
                "status": "submitted",
                "reason": "",
                "position_id": position.get("position_id"),
            }
            original_raw = {
                "event_type": "raw_discord_notification",
                "dedupe_key": "orig-ms-buy",
                "captured_at": "2026-06-05T11:30:01-04:00",
                "notification_timestamp": "2026-06-05T11:30:00-04:00",
                "source_app": "Discord",
                "bundle_id": "com.hnc.Discord",
                "title": "OTWSteve",
                "subtitle": "short-term-call-outs-same-week-or-1-week",
                "body": "#MS Jun 5 210 call @ 2.14 Bought 5 #swing",
            }
            append_jsonl(tmp_path / "raw_notifications.jsonl", original_raw)
            first_counts = run_pipeline_once.process_raw_notifications(
                read_jsonl(tmp_path / "raw_notifications.jsonl"),
                dry_run_orders=False,
                prior_decisions_override=[],
            )
            assert first_counts["option_shadow_positions"] == 1
            assert first_counts["option_auto_buys"] == 1

            reply_text = "\n".join(
                [
                    "OTWSteve",
                    "#MS Jun 5 210 call @ 2.14 Bought 5 #swing",
                    "OTWSteve",
                    " — ",
                    "11:40 AM",
                    "Friday, June 5, 2026 at 11:40 AM",
                    "closed 2 @ 5.15",
                ]
            )
            reply_records = backfill_steve_text.build_raw_records(
                reply_text,
                "browser_channel:492098253337264138",
                dedupe_scope="chat-messages-492098253337264138-1512481362844848178",
                suppress_context_entries=True,
                source_time="2026-06-05T11:40:00-04:00",
            )
            assert len(reply_records) == 1
            assert reply_records[0]["body"] == "#MS Jun 5 210 call @ 2.14 Bought 5\nclosed 2 @ 5.15"
            append_jsonl(tmp_path / "raw_notifications.jsonl", reply_records[0])
            second_counts = run_pipeline_once.process_raw_notifications(
                read_jsonl(tmp_path / "raw_notifications.jsonl"),
                dry_run_orders=False,
                prior_decisions_override=[],
            )
            assert second_counts["raw_new"] == 1
            assert second_counts["option_auto_buys"] == 0
            assert second_counts["option_exits"] == 1
            assert len(read_jsonl(option_validation.SHADOW_POSITIONS_FILE)) == 1
            assert len(read_jsonl(steve_trade_bot.HUMAN_POSITIONS_FILE)) == 1
            assert len(read_jsonl(option_validation.STEVE_EXITS_FILE)) == 1
            assert len(read_jsonl(option_validation.HUMAN_EXITS_FILE)) == 1
        finally:
            run_pipeline_once.DATA_DIR = original_data_dir
            run_pipeline_once.PROCESSED_FILE = original_processed_file
            run_pipeline_once.write_openclaw_summary = original_summary
            option_validation.enrich_option_alert = original_enrich
            steve_trade_bot.load_bot_config = original_load_bot_config
            steve_trade_bot.submit_option_paper_order = original_submit_order
            if original_max_raw_age is None:
                os.environ.pop("OPENCLAW_MAX_RAW_ALERT_AGE_SECONDS", None)
            else:
                os.environ["OPENCLAW_MAX_RAW_ALERT_AGE_SECONDS"] = original_max_raw_age


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


def test_backfill_raw_records_suppress_reply_context_entry() -> None:
    text = "\n".join(
        [
            "OTWSteve",
            "#JPM Jun 18 310 call @ 3.25 Bought 3 #swing \\",
            "OTWSteve",
            " — ",
            "12:22 PM",
            "Friday, June 5, 2026 at 12:22 PM",
            "closed @ 6.40",
        ]
    )
    records = backfill_steve_text.build_raw_records(
        text,
        "browser_channel:562178552984764436",
        dedupe_scope="chat-messages-562178552984764436-1512491799023980595",
        suppress_context_entries=True,
    )
    assert len(records) == 1
    assert records[0]["body"] == "#JPM Jun 18 310 call @ 3.25 Bought 3\nclosed @ 6.40"


def test_backfill_raw_records_materialize_contextual_stop() -> None:
    text = "\n".join(
        [
            "OTWSteve",
            "#GOOGL July 17 390 call @ 4.80 Bought 3 #swing",
            "OTWSteve",
            " — ",
            "11:02 AM",
            "Monday, June 22, 2026 at 11:02 AM",
            "stopped out",
        ]
    )
    records = backfill_steve_text.build_raw_records(
        text,
        "browser_channel:562178552984764436",
        dedupe_scope="chat-messages-562178552984764436-1518632199602311168",
        suppress_context_entries=True,
        source_time="2026-06-22T11:02:00-04:00",
    )
    assert len(records) == 1
    assert records[0]["body"] == "#GOOGL July 17 390 call @ 4.80 Bought 3\nstopped out"
    parsed = parse_trade_alert(records[0])
    assert parsed["side"] == "exit"
    assert parsed["exit_action"] == "stopped_out"
    assert parsed["exit_price"] is None
    assert parsed["ticker"] == "GOOGL"


def test_backfill_raw_records_materialize_add_context() -> None:
    text = "\n".join(
        [
            "OTWSteve",
            "#MU Jun 18 950 put @ 10.20 Bought 1 #Lotto",
            "added 3 @ 2.70 #Lotto",
        ]
    )
    records = backfill_steve_text.build_raw_records(
        text,
        "browser_channel:492098253337264138",
        dedupe_scope="chat-messages-492098253337264138-add-test",
    )
    assert len(records) == 2
    assert records[0]["body"] == "#MU Jun 18 950 put @ 10.20 Bought 1 #Lotto"
    assert records[1]["body"] == "#MU Jun 18 950 put @ 2.7 added 3 #lotto"


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
            approval_chat_ids=("123456789", "-1001112223334"),
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
            steve_trade_bot.PARSED_ALERTS_FILE,
            {
                "source_dedupe_key": "broker-fill-key",
                "matched_text": "#CVX May 22 200 call @ .52 Bought 3",
                "notification_timestamp": "2026-05-20T10:51:37-04:00",
                "entry_price": 0.52,
            },
        )
        append_jsonl(
            broker_order_monitor.HUMAN_POSITIONS_FILE,
            {
                "position_id": "human-test",
                "source_dedupe_key": "broker-fill-key",
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
                "source_dedupe_key": "broker-fill-key",
                "contract_symbol": "CVX260522C00200000",
                "payload": {"client_order_id": "openclaw-opt-test", "symbol": "CVX260522C00200000", "side": "buy", "qty": "3"},
                "response": {"id": "order-test"},
            },
        )
        counts = broker_order_monitor.check_broker_order_statuses_once(max_age_hours=24)
        assert counts["reported"] == 1
        reports = read_jsonl(broker_order_monitor.ORDER_STATUS_FILE)
        assert reports[0]["broker_status"] == "filled"
        assert [row[0] for row in sent_messages] == ["123456789", "-1001112223334"]
        executive_message = sent_messages[1][1]
        assert "BOUGHT FILLED [PAPER]" in executive_message
        assert "Alert May 20 10:51:37 ET" in executive_message
        assert "#CVX May 22 200 call @ .52 Bought 3" in executive_message
        assert "Filled May 20 10:54:05 ET" in executive_message
        assert "Bought 3 @ 0.52 avg" in executive_message
        assert "Invested: $156" in executive_message
        assert "Alert -> fill: +$0 / +0.0%" in executive_message
        assert "Latency: 2m 28s" in executive_message


def test_sell_fill_executive_message_includes_realized_pl() -> None:
    with tempfile.TemporaryDirectory() as tmp:
        tmp_path = Path(tmp)
        patch_runtime_paths(tmp_path)
        position = {
            "position_id": "human-sell-test",
            "source_dedupe_key": "sell-source",
            "approval_id": "auto-sell-test",
            "alert_text": "#MSFT Jun 18 410 call @ 1.00 Bought 5",
            "alert_time": "2026-06-18T10:00:00-04:00",
            "ticker": "MSFT",
            "expiration_date": "2026-06-18",
            "option_type": "call",
            "strike_price": 410,
            "contract_symbol": "MSFT260618C00410000",
            "contracts": 5,
            "entry_price": 1.0,
        }
        append_jsonl(steve_trade_bot.HUMAN_POSITIONS_FILE, position)
        buy_fill = {
            "order_id": "buy-order",
            "position_id": "human-sell-test",
            "broker_status": "filled",
            "side": "buy",
            "filled_qty": "5",
            "filled_avg_price": "1.00",
            "filled_at": "2026-06-18T14:01:00Z",
        }
        sell_fill = {
            "order_id": "sell-order",
            "position_id": "human-sell-test",
            "broker_status": "filled",
            "side": "sell",
            "label": "MSFT Jun 18 410C",
            "filled_qty": "2",
            "filled_avg_price": "1.80",
            "filled_at": "2026-06-18T16:30:00Z",
            "exit_reason": "take_profit",
        }
        append_jsonl(steve_trade_bot.BROKER_STATUS_REPORTS_FILE, buy_fill)
        append_jsonl(steve_trade_bot.BROKER_STATUS_REPORTS_FILE, sell_fill)
        message = steve_trade_bot.broker_fill_executive_message(sell_fill)
        assert "SOLD FILLED [PAPER]" in message
        assert "Alert Jun 18 10:00:00 ET" in message
        assert "#MSFT Jun 18 410 call @ 1.00 Bought 5" in message
        assert "Filled Jun 18 12:30:00 ET" in message
        assert "Sold 2 @ 1.80 avg" in message
        assert "Proceeds: $360" in message
        assert "Realized P/L: +$160 / +80.0%" in message
        assert "Remaining: 3 contracts" in message
        assert "Reason: take profit" in message
        assert "Held: 2h 30m" in message


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


def test_browser_health_activity_does_not_force_duplicate_history_rows() -> None:
    record = {
        "event_type": "discord_browser_capture_health",
        "status": "ok",
        "errors": [],
        "totals": {
            "messages_new": 2,
            "raw_backfilled": 2,
            "raw_processed": 2,
        },
    }
    assert data_hygiene.browser_health_is_interesting(record) is False


def test_browser_health_history_record_ignores_viewport_only_churn() -> None:
    first = {
        "event_type": "discord_browser_capture_health",
        "recorded_at": "2026-07-08T09:30:00-04:00",
        "mode": "live",
        "status": "degraded",
        "errors": [{"channel_id": "562178552984764436", "reason": "RuntimeError:Chrome AppleScript read timed out"}],
        "totals": {
            "channels": 5,
            "channels_ok": 3,
            "visible_messages": 74,
            "candidate_messages": 0,
            "messages_new": 0,
            "raw_backfilled": 0,
            "raw_processed": 0,
        },
        "channels": [
            {
                "channel_id": "492098253337264138",
                "status": "ok",
                "title": "Discord | Steve",
                "visible_messages": 22,
                "candidate_messages": 0,
                "messages_new": 0,
                "raw_backfilled": 0,
                "raw_processed": 0,
            },
            {
                "channel_id": "562178552984764436",
                "status": "error",
                "reason": "RuntimeError:Chrome AppleScript read timed out",
            },
        ],
    }
    second = {
        **first,
        "totals": {**first["totals"], "visible_messages": 73},
        "channels": [
            {
                **first["channels"][0],
                "title": "Discord | Steve (1)",
                "visible_messages": 21,
            },
            first["channels"][1],
        ],
    }
    normalized_first = discord_browser_channel_watcher.browser_health_history_record(first)
    normalized_second = discord_browser_channel_watcher.browser_health_history_record(second)
    assert normalized_first == normalized_second
    assert normalized_first["recorded_at"] == "2026-07-08T09:30:00-04:00"


def test_browser_capture_js_scrolls_before_sampling_messages() -> None:
    js = discord_browser_channel_watcher.VISIBLE_MESSAGES_JS
    assert "scrollTop = scroller.scrollHeight" in js
    assert "scrollableAncestor" in js


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


def test_browser_channel_watcher_suppresses_reply_context_buy_duplicates() -> None:
    with tempfile.TemporaryDirectory() as tmp:
        tmp_path = Path(tmp)
        original_data_dir = discord_browser_channel_watcher.DATA_DIR
        original_messages = discord_browser_channel_watcher.BROWSER_MESSAGES_FILE
        original_state = discord_browser_channel_watcher.BROWSER_STATE_FILE
        try:
            discord_browser_channel_watcher.DATA_DIR = tmp_path
            discord_browser_channel_watcher.BROWSER_MESSAGES_FILE = tmp_path / "discord_browser_messages.jsonl"
            discord_browser_channel_watcher.BROWSER_STATE_FILE = tmp_path / "discord_browser_state.json"
            message_text = "\n".join(
                [
                    "OTWSteve",
                    "#MS Jun 5 210 call @ 2.14 Bought 5 #swing",
                    "OTWSteve",
                    " — ",
                    "11:40 AM",
                    "Friday, June 5, 2026 at 11:40 AM",
                    "closed @ 5.15",
                ]
            )
            first = {
                "id": "chat-messages-492098253337264138-1512481362844848178",
                "text": message_text,
            }
            duplicate_dom_alias = {
                "id": "chat-messages___chat-messages-492098253337264138-1512481362844848178",
                "text": message_text,
            }
            counts = discord_browser_channel_watcher.process_browser_messages(
                "492098253337264138",
                "https://discord.com/channels/483483452180791296/492098253337264138",
                [first, duplicate_dom_alias],
                mode="live",
                source_prefix="browser_channel",
                process_raw=False,
            )
            assert counts["messages_new"] == 1
            assert counts["raw_backfilled"] == 1
            raw = read_jsonl(tmp_path / "raw_notifications.jsonl")
            assert len(raw) == 1
            assert raw[0]["body"] == "#MS Jun 5 210 call @ 2.14 Bought 5\nclosed @ 5.15"
        finally:
            discord_browser_channel_watcher.DATA_DIR = original_data_dir
            discord_browser_channel_watcher.BROWSER_MESSAGES_FILE = original_messages
            discord_browser_channel_watcher.BROWSER_STATE_FILE = original_state


def test_browser_channel_watcher_reprocesses_edited_message_add_lines() -> None:
    with tempfile.TemporaryDirectory() as tmp:
        tmp_path = Path(tmp)
        original_data_dir = discord_browser_channel_watcher.DATA_DIR
        original_messages = discord_browser_channel_watcher.BROWSER_MESSAGES_FILE
        original_state = discord_browser_channel_watcher.BROWSER_STATE_FILE
        try:
            discord_browser_channel_watcher.DATA_DIR = tmp_path
            discord_browser_channel_watcher.BROWSER_MESSAGES_FILE = tmp_path / "discord_browser_messages.jsonl"
            discord_browser_channel_watcher.BROWSER_STATE_FILE = tmp_path / "discord_browser_state.json"
            base_message = {
                "id": "chat-messages-492098253337264138-1518995973685252106",
                "message_timestamp": "2026-06-23T10:06:00-04:00",
                "text": "\n".join(
                    [
                        "OTWSteve",
                        "#AMZN Jun 26 240 call @ 1.74 Bought 6 #Lotto",
                        "Tuesday, June 23, 2026 at 10:06 AM",
                    ]
                ),
            }
            first_counts = discord_browser_channel_watcher.process_browser_messages(
                "492098253337264138",
                "https://discord.com/channels/483483452180791296/492098253337264138",
                [base_message],
                mode="live",
                source_prefix="browser_channel",
                process_raw=False,
            )
            edited_message = dict(base_message)
            edited_message["text"] = "\n".join(
                [
                    "OTWSteve",
                    "#AMZN Jun 26 240 call @ 1.74 Bought 6 #Lotto",
                    "added 4 @ 1.35 #Lotto",
                    "Tuesday, June 23, 2026 at 10:06 AM",
                ]
            )
            second_counts = discord_browser_channel_watcher.process_browser_messages(
                "492098253337264138",
                "https://discord.com/channels/483483452180791296/492098253337264138",
                [edited_message],
                mode="live",
                source_prefix="browser_channel",
                process_raw=False,
            )
            unchanged_counts = discord_browser_channel_watcher.process_browser_messages(
                "492098253337264138",
                "https://discord.com/channels/483483452180791296/492098253337264138",
                [edited_message],
                mode="live",
                source_prefix="browser_channel",
                process_raw=False,
            )
            raw = read_jsonl(tmp_path / "raw_notifications.jsonl")
            assert first_counts["raw_backfilled"] == 1
            assert second_counts["messages_new"] == 1
            assert second_counts["raw_backfilled"] == 1
            assert unchanged_counts["messages_new"] == 0
            assert len(raw) == 2
            assert raw[0]["body"] == "#AMZN Jun 26 240 call @ 1.74 Bought 6 #Lotto"
            assert raw[1]["body"] == "#AMZN Jun 26 240 call @ 1.35 added 4 #lotto"
        finally:
            discord_browser_channel_watcher.DATA_DIR = original_data_dir
            discord_browser_channel_watcher.BROWSER_MESSAGES_FILE = original_messages
            discord_browser_channel_watcher.BROWSER_STATE_FILE = original_state


def test_browser_channel_watcher_selects_changed_seen_messages_outside_age_window() -> None:
    with tempfile.TemporaryDirectory() as tmp:
        tmp_path = Path(tmp)
        original_state = discord_browser_channel_watcher.BROWSER_STATE_FILE
        try:
            discord_browser_channel_watcher.BROWSER_STATE_FILE = tmp_path / "discord_browser_state.json"
            channel_id = "492098253337264138"
            original_message = {
                "id": f"chat-messages-{channel_id}-1519403201357549639",
                "text": "\n".join(
                    [
                        "OTWSteve",
                        "#tsla Jun 24 370 put @ 4 bought 2 #lotto (edited)",
                        "Wednesday, June 24, 2026 at 2:05 PM",
                    ]
                ),
            }
            original_key = discord_browser_channel_watcher.message_key(channel_id, original_message)
            original_fingerprint = discord_browser_channel_watcher.message_fingerprint(original_message)
            discord_browser_channel_watcher.save_state(
                {
                    "seen_message_keys": [original_key],
                    "seen_message_fingerprints": {original_key: original_fingerprint},
                }
            )

            edited_message = dict(original_message)
            edited_message["text"] = "\n".join(
                [
                    "OTWSteve",
                    "#tsla Jun 26 370 put @ 4 bought 2 #lotto (edited)",
                    "Wednesday, June 24, 2026 at 2:05 PM",
                    "sold 1 @ 4.05",
                    "Wednesday, June 24, 2026 at 2:14 PM",
                ]
            )
            recent_candidates = discord_browser_channel_watcher.filter_candidate_messages(
                [edited_message],
                ["OTWSteve", "SteveOTWS"],
                max_age_minutes=1,
                tz_name="America/Detroit",
                allow_unknown_time=False,
            )
            selected = discord_browser_channel_watcher.include_changed_seen_messages(
                channel_id,
                recent_candidates,
                [edited_message],
                discord_browser_channel_watcher.load_state(),
                author_names=["OTWSteve", "SteveOTWS"],
                tz_name="America/Detroit",
            )
            assert recent_candidates == []
            assert len(selected) == 1
            assert selected[0]["id"] == edited_message["id"]
        finally:
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
        assert report["counts"]["truth_buys"] == 1
        assert report["counts"]["truth_adds"] == 1
        assert report["counts"]["truth_context_stops"] == 1


def test_nightly_review_labels_truth_buy_capture_miss_before_parser() -> None:
    with tempfile.TemporaryDirectory() as tmp:
        tmp_path = Path(tmp)
        patch_runtime_paths(tmp_path)
        day = "2026-05-20"
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

        report = nightly_review.review_day(day, refresh_browser=False)
        issues_by_code = {item["code"]: item for item in report["issues"]}
        assert "truth_buy_not_captured" in issues_by_code
        assert "truth_buy_not_parsed" not in issues_by_code
        assert issues_by_code["truth_buy_not_captured"]["evidence"]["raw_match_count"] == 0


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


def test_nightly_review_distinguishes_guarded_buy_from_dropped_buy() -> None:
    with tempfile.TemporaryDirectory() as tmp:
        tmp_path = Path(tmp)
        patch_runtime_paths(tmp_path)
        day = "2026-06-15"
        append_jsonl(
            nightly_review.BROWSER_MESSAGES_FILE,
            {
                "event_type": "discord_browser_message",
                "captured_at": f"{day}T11:26:33-04:00",
                "message_timestamp": f"{day}T11:26:00-04:00",
                "channel_id": "492098253337264138",
                "message_key": "msg-googl-guarded",
                "text_preview": "OTWSteve\n#GOOGL Jun 18 380 calls @ 2.10 bought 2 #lotto",
            },
        )
        append_jsonl(
            nightly_review.PARSED_FILE,
            {
                "event_type": "parsed_trade_alert",
                "source_dedupe_key": "ui-googl-guarded",
                "parsed_at": f"{day}T11:26:33-04:00",
                "notification_timestamp": f"{day}T11:26:00-04:00",
                "instrument_type": "option",
                "side": "buy",
                "ticker": "GOOGL",
                "expiration_date": "2026-06-18",
                "strike_price": 380.0,
                "option_type": "call",
                "entry_price": 2.10,
                "contracts": 2,
                "matched_text": "#GOOGL Jun 18 380 calls @ 2.10 bought 2",
                "raw_text": "OTWSteve browser_channel:492098253337264138 #GOOGL Jun 18 380 calls @ 2.10 bought 2 #lotto",
                "tags": ["lotto"],
            },
        )
        append_jsonl(
            nightly_review.APPROVAL_CARDS_FILE,
            {
                "event_type": "steve_approval_card",
                "approval_id": "approval-googl-guarded",
                "created_at": f"{day}T11:26:34-04:00",
                "status": "sent",
                "source_dedupe_key": "ui-googl-guarded",
                "alert": {
                    "source_dedupe_key": "ui-googl-guarded",
                    "auto_entry_guard": {
                        "ok": False,
                        "reasons": ["entry_price_above_alert_threshold"],
                        "alert_entry_price": 2.10,
                        "observed_entry_price": 2.33,
                    },
                },
            },
        )

        report = nightly_review.review_day(day, refresh_browser=False)
        issues_by_code = {item["code"]: item for item in report["issues"]}
        assert "parsed_buy_not_paper_traded" not in issues_by_code
        guarded = issues_by_code["parsed_buy_held_by_auto_entry_guard"]
        assert guarded["severity"] == "warning"
        assert guarded["evidence"]["approval_status"] == "sent"
        assert "entry_price_above_alert_threshold" in guarded["evidence"]["guard"]["reasons"]


def test_nightly_review_does_not_require_local_position_for_blocked_auto_buy() -> None:
    with tempfile.TemporaryDirectory() as tmp:
        tmp_path = Path(tmp)
        patch_runtime_paths(tmp_path)
        day = "2026-06-25"
        append_jsonl(
            nightly_review.BROWSER_MESSAGES_FILE,
            {
                "event_type": "discord_browser_message",
                "captured_at": f"{day}T15:59:38-04:00",
                "message_timestamp": f"{day}T15:59:00-04:00",
                "channel_id": "562178552984764436",
                "message_key": "msg-tem",
                "text_preview": "OTWSteve\n#TEM JULY 17 55 call @ 4.30 Bought 2 #swingt",
            },
        )
        append_jsonl(
            nightly_review.PARSED_FILE,
            {
                "event_type": "parsed_trade_alert",
                "source_dedupe_key": "ui-tem-blocked",
                "parsed_at": f"{day}T15:59:38-04:00",
                "notification_timestamp": f"{day}T15:59:00-04:00",
                "instrument_type": "option",
                "side": "buy",
                "ticker": "TEM",
                "expiration_date": "2026-07-17",
                "strike_price": 55.0,
                "option_type": "call",
                "entry_price": 4.30,
                "contracts": 2,
                "matched_text": "#TEM JULY 17 55 call @ 4.30 Bought 2",
                "raw_text": "OTWSteve browser_channel:562178552984764436 #TEM JULY 17 55 call @ 4.30 Bought 2 #swingt",
                "tags": ["swingt"],
            },
        )
        append_jsonl(
            nightly_review.AUTO_BUY_REPORTS_FILE,
            {
                "event_type": "steve_auto_buy_report",
                "auto_paper_id": "auto-tem-blocked",
                "position_id": "human-tem-blocked",
                "source_dedupe_key": "ui-tem-blocked",
                "created_at": f"{day}T15:59:39-04:00",
                "broker_status": "blocked",
                "broker_reason": "options_market_closed:next_open=2026-06-26T09:30:00-04:00",
            },
        )

        report = nightly_review.review_day(day, refresh_browser=False)
        codes = {item["code"] for item in report["issues"]}
        assert "auto_buy_missing_local_position" not in codes
        assert "parsed_buy_not_paper_traded" not in codes
        assert report["counts"]["truth_buys"] == 1
        assert report["counts"]["paper_entries"] == 0


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


def test_nightly_review_keeps_browser_recovery_issue_when_live_watcher_is_stuck() -> None:
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
        nightly_review.BROWSER_HEALTH_LATEST_FILE.write_text(
            json.dumps(
                {
                    "event_type": "discord_browser_capture_health",
                    "recorded_at": f"{day}T11:40:27-04:00",
                    "status": "failed",
                    "channels": [{"channel_id": "492098253337264138"}, {"channel_id": "562178552984764436"}],
                    "errors": [
                        {"channel_id": "492098253337264138", "reason": "RuntimeError:Chrome AppleScript read timed out"},
                        {"channel_id": "562178552984764436", "reason": "RuntimeError:Chrome AppleScript read timed out"},
                    ],
                }
            ),
            encoding="utf-8",
        )
        original_truth_events_from_chrome = nightly_review.truth_events_from_chrome
        try:
            nightly_review.truth_events_from_chrome = lambda *args, **kwargs: (
                [],
                [{"channel_id": "492098253337264138", "status": "ok", "events": 0}],
            )
            report = nightly_review.review_day(day, refresh_browser=True)
            codes = {item["code"] for item in report["issues"]}
            assert "health_browser_health_stale" in codes
            assert "health_browser_capture_degraded" in codes
            assert "browser_foreground_recovery_needed" in codes
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


def test_nightly_review_duplicate_scorecard_prefers_canonical_keys_and_quarantines_legacy_collisions() -> None:
    rows = [
        {
            "position_id": "shadow-1",
            "source_dedupe_key": "ui-a",
            "ticker": "MU",
            "expiration_date": "2026-06-18",
            "strike_price": 800.0,
            "option_type": "put",
            "entry_price": 16.60,
            "contracts": 3,
            "canonical_entry_key": "entry|MU|2026-06-18|800|put|16.60|3|2026-06-10T09:54-04:00",
        },
        {
            "position_id": "shadow-2",
            "source_dedupe_key": "ui-b",
            "ticker": "MU",
            "expiration_date": "2026-06-18",
            "strike_price": 800.0,
            "option_type": "put",
            "entry_price": 16.60,
            "contracts": 3,
            "canonical_entry_key": "entry|MU|2026-06-18|800|put|16.60|3|2026-06-10T10:15-04:00",
        },
        {
            "position_id": "legacy-1",
            "source_dedupe_key": "ui-c",
            "ticker": "FRVO",
            "expiration_date": "2026-07-17",
            "strike_price": 45.0,
            "option_type": "call",
            "entry_price": 5.36,
            "contracts": 4,
        },
        {
            "position_id": "legacy-2",
            "source_dedupe_key": "ui-d",
            "ticker": "FRVO",
            "expiration_date": "2026-07-17",
            "strike_price": 45.0,
            "option_type": "call",
            "entry_price": 5.36,
            "contracts": 4,
        },
    ]
    stats = nightly_review.build_ledger_duplicate_scorecard(
        rows,
        fallback_key_func=nightly_review.position_duplicate_key,
        id_key="position_id",
        canonical_keys=("canonical_entry_key",),
    )
    assert stats["duplicate_groups"] == 0
    assert stats["duplicate_rows"] == 0
    assert stats["legacy_collision_groups"] == 1
    assert stats["legacy_collision_rows"] == 1
    assert stats["canonical_rows"] == 2
    assert stats["legacy_rows"] == 2


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
            "executive_activity": {
                "filled_buys": 1,
                "filled_sells": 1,
                "contracts_bought": 3,
                "contracts_sold": 1,
                "invested": 600.0,
                "proceeds": 420.0,
                "realized_pnl": 220.0,
                "best": {"label": "MSFT Jun 18 410C", "pnl_dollars": 220.0, "pnl_pct": 110.0},
                "worst": {"label": "MSFT Jun 18 410C", "pnl_dollars": 220.0, "pnl_pct": 110.0},
                "unfilled_terminal": 0,
            },
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
        executive_message = nightly_review.executive_telegram_summary(report)
        assert "DAILY PAPER SUMMARY - May 20" in executive_message
        assert "Filled buys: 1 (3 contracts) | Filled sells: 1 (1 contracts)" in executive_message
        assert "Invested: $600 | Proceeds: $420" in executive_message
        assert "Realized P/L: +$220" in executive_message
        assert "Improvement loop:" in executive_message
        sent_messages: list[tuple[str, str]] = []
        original_sender = steve_trade_bot.send_message_to_approval_chats
        original_executive_sender = steve_trade_bot.send_message_to_executive_chats
        try:
            steve_trade_bot.send_message_to_approval_chats = lambda message: (
                sent_messages.append(("dm", message)) or ("sent", "", [{"chat_id": "123", "message_id": 1, "status": "sent"}])
            )
            steve_trade_bot.send_message_to_executive_chats = lambda message: (
                sent_messages.append(("group", message)) or ("sent", "", [{"chat_id": "-100", "message_id": 2, "status": "sent"}])
            )
            first_delivery = nightly_review.send_telegram_report(report)
            second_delivery = nightly_review.send_telegram_report(report)
        finally:
            steve_trade_bot.send_message_to_approval_chats = original_sender
            steve_trade_bot.send_message_to_executive_chats = original_executive_sender
        assert first_delivery["status"] == "sent"
        assert second_delivery["status"] == "already_sent"
        assert [row[0] for row in sent_messages] == ["dm", "group"]
        assert "NIGHTLY PIPELINE REVIEW" in sent_messages[0][1]
        assert "DAILY PAPER SUMMARY" in sent_messages[1][1]


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
    test_hedge_auto_paper_buy()
    test_non_hedge_auto_paper_buy()
    test_non_hedge_auto_paper_buy_does_not_persist_blocked_broker_position()
    test_non_hedge_auto_paper_buy_duplicate_blocked_alert_is_idempotent()
    test_option_order_payload_rounds_limit_price_to_two_decimals()
    test_option_entry_order_skips_existing_client_order_id_without_resubmit()
    test_non_hedge_bad_entry_requires_approval()
    test_non_hedge_mixed_buy_exit_requires_approval()
    test_fill_price_caps_excessive_slippage()
    test_exit_plan_contract_allocation()
    test_dm_only_approval_and_executive_group_routing()
    test_close_report_message_and_delivery()
    test_human_exit_rules_and_steve_catch_up()
    test_steve_alert_pl_summary_uses_steve_prices()
    test_option_exit_reply_matches_shadow_context()
    test_option_exit_duplicate_capture_is_idempotent()
    test_pipeline_processes_close_reply_as_option_exit()
    test_pipeline_skips_stale_browser_backfill_before_routing()
    test_pipeline_close_reply_does_not_duplicate_original_buy()
    test_backfill_text_audit_matches_contextual_exits()
    test_backfill_raw_records_suppress_reply_context_entry()
    test_backfill_raw_records_materialize_add_context()
    test_chrome_visible_capture_filters_history_by_default()
    test_option_order_payload()
    test_broker_order_monitor_reports_terminal_fills()
    test_sell_fill_executive_message_includes_realized_pl()
    test_daily_pl_summary_short_report()
    test_watcher_steve_filters()
    test_live_pipeline_heartbeat()
    test_option_tracker_skips_junk_and_writes_lean_deduped_snapshots()
    test_browser_channel_watcher_filters_and_backfills()
    test_browser_channel_watcher_suppresses_reply_context_buy_duplicates()
    test_pipeline_health_pinpoints_stage_failures()
    test_nightly_review_detects_recursive_improvement_issues()
    test_nightly_review_labels_truth_buy_capture_miss_before_parser()
    test_nightly_review_capture_method_scorecard()
    test_nightly_review_distinguishes_guarded_buy_from_dropped_buy()
    test_nightly_review_does_not_require_local_position_for_blocked_auto_buy()
    test_nightly_review_browser_refresh_overrides_stale_health_and_surfaces_current_errors()
    test_nightly_review_keeps_browser_recovery_issue_when_live_watcher_is_stuck()
    test_nightly_review_ignores_synthetic_full_pipeline_artifacts()
    test_nightly_review_compares_steve_local_and_broker_pl()
    test_nightly_review_recommended_actions_include_health_fallbacks()
    test_nightly_review_duplicate_scorecard_prefers_canonical_keys_and_quarantines_legacy_collisions()
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

#!/usr/bin/env python3
"""Focused tests for the Steve options validation MVP."""

from __future__ import annotations

import json
import tempfile
from pathlib import Path

import alpaca_options
import notification_watcher
import option_validation
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
    option_validation.STEVE_EXITS_FILE = tmp_path / "steve_option_exits.jsonl"
    option_validation.HUMAN_POSITIONS_FILE = tmp_path / "human_paper_positions.jsonl"
    option_validation.HUMAN_EXITS_FILE = tmp_path / "human_paper_exits.jsonl"
    option_validation.DAILY_SUMMARIES_FILE = tmp_path / "daily_option_summaries.jsonl"
    steve_trade_bot.APPROVAL_CARDS_FILE = tmp_path / "steve_approval_cards.jsonl"
    steve_trade_bot.APPROVAL_ACTIONS_FILE = tmp_path / "steve_approval_actions.jsonl"
    steve_trade_bot.HUMAN_POSITIONS_FILE = tmp_path / "human_paper_positions.jsonl"
    steve_trade_bot.BOT_STATE_FILE = tmp_path / "steve_trade_bot_state.json"


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
                    "body": "#CRWV MAY 8 113 call @ .88 Bought 10 #Lotto",
                }
            )
        )[0]
        result = option_validation.handle_option_entry(alert, send_approval=True)
        assert result["shadow_position_created"] is True
        cards = read_jsonl(steve_trade_bot.APPROVAL_CARDS_FILE)
        assert len(cards) == 1
        assert cards[0]["status"] == "telegram_disabled"
        assert cards[0]["message_text"].startswith("Alert: #CRWV MAY 8 113 call @ .88 Bought 10")
        assert "\nbuy\n" in cards[0]["message_text"]
        cards[0]["telegram_message_id"] = 100
        steve_trade_bot.APPROVAL_CARDS_FILE.write_text(json.dumps(cards[0], sort_keys=True) + "\n", encoding="utf-8")

        default_buy = steve_trade_bot.parse_approval_command("buy")
        assert default_buy["ok"] is True
        assert default_buy["contracts"] is None
        assert default_buy["stop_percent"] == 35.0
        assert default_buy["take_percent"] == 80.0
        assert default_buy["used_default_contracts"] is True
        assert default_buy["used_default_risk"] is True

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
            {"action": "sell", "contracts": 1, "take_percent": 50.0, "take_price": 1.32}
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


def test_human_exit_rules_and_steve_catch_up() -> None:
    with tempfile.TemporaryDirectory() as tmp:
        tmp_path = Path(tmp)
        patch_runtime_paths(tmp_path)
        position = {
            "event_type": "human_paper_option_position",
            "position_id": "human-test",
            "approval_id": "approval-test",
            "opened_at": "2026-05-08T13:09:00-04:00",
            "source_dedupe_key": "source-test",
            "ticker": "MSFT",
            "contract_symbol": "MSFT260717C00475000",
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


def test_option_order_payload() -> None:
    payload = alpaca_options.build_option_order_payload(
        {
            "position_id": "human-test",
            "source_dedupe_key": "source-test",
            "contract_symbol": "QQQ260515P00710000",
            "contracts": 2,
            "entry_price": 5.86,
        }
    )
    assert payload["symbol"] == "QQQ260515P00710000"
    assert payload["qty"] == "2"
    assert payload["type"] == "limit"
    assert "notional" not in payload


def test_watcher_steve_filters() -> None:
    config = {
        "app_names": ["Discord"],
        "bundle_ids": ["com.hnc.Discord"],
        "alert_author_names": ["OTWSteve"],
        "alert_channel_ids": ["492098253337264138"],
        "require_alert_channel_id_match": False,
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

    strict_config = dict(config)
    strict_config["require_alert_channel_id_match"] = True
    assert notification_watcher.is_matching_notification(steve_record, strict_config) is False
    steve_record["raw"]["thread"] = "492098253337264138"
    assert notification_watcher.is_matching_notification(steve_record, strict_config) is True


def main() -> int:
    test_parser()
    test_validation_and_approval()
    test_exit_plan_contract_allocation()
    test_multi_destination_approval_cards()
    test_human_exit_rules_and_steve_catch_up()
    test_option_order_payload()
    test_watcher_steve_filters()
    print("Steve options MVP tests passed")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())

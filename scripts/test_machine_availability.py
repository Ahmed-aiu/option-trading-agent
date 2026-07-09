#!/usr/bin/env python3
"""Focused tests for nightly machine-availability latency classification."""

from __future__ import annotations

import tempfile
from pathlib import Path
from typing import Any

import nightly_review
import option_validation
from pipeline_common import append_jsonl


DAY = "2026-05-20"
SOURCE_TIME = f"{DAY}T09:30:00-04:00"
ALERT_BODY = "#FRVO July 17 45 call @ 4.95 Bought 4 #swing"
CONTRACT = {
    "ticker": "FRVO",
    "expiration_date": "2026-07-17",
    "strike_price": 45.0,
    "option_type": "call",
}


def patch_runtime_paths(tmp_path: Path) -> None:
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
    option_validation.SHADOW_POSITIONS_FILE = tmp_path / "shadow_option_positions.jsonl"


def append_buy_case(
    *,
    raw_key: str,
    raw_captured_at: str | None,
    order_time: str,
) -> None:
    append_jsonl(
        nightly_review.BROWSER_MESSAGES_FILE,
        {
            "event_type": "discord_browser_message",
            "captured_at": raw_captured_at or SOURCE_TIME,
            "message_timestamp": SOURCE_TIME,
            "channel_id": "chan-1",
            "message_key": f"msg-{raw_key}",
            "text_preview": f"OTWSteve\n{ALERT_BODY}",
        },
    )
    raw_row: dict[str, Any] = {
        "event_type": "raw_discord_ui_backfill",
        "notification_timestamp": SOURCE_TIME,
        "source_app": "DiscordUI",
        "bundle_id": "browser_or_clipboard",
        "title": "OTWSteve",
        "subtitle": "browser_channel:chan-1",
        "body": ALERT_BODY,
        "raw": {"source": "browser_channel:chan-1"},
        "dedupe_key": raw_key,
    }
    if raw_captured_at is not None:
        raw_row["captured_at"] = raw_captured_at
    append_jsonl(nightly_review.RAW_FILE, raw_row)
    append_jsonl(
        nightly_review.PARSED_FILE,
        {
            "event_type": "parsed_trade_alert",
            "source_dedupe_key": raw_key,
            "parsed_at": raw_captured_at or SOURCE_TIME,
            "instrument_type": "option",
            "side": "buy",
            **CONTRACT,
            "entry_price": 4.95,
            "contracts": 4,
            "tags": ["swing"],
            "notification_timestamp": SOURCE_TIME,
            "raw_text": ALERT_BODY,
        },
    )
    append_jsonl(
        nightly_review.HUMAN_POSITIONS_FILE,
        {
            "event_type": "human_paper_option_position",
            "opened_at": order_time,
            "position_id": f"human-{raw_key}",
            "source_dedupe_key": raw_key,
            **CONTRACT,
            "entry_price": 4.95,
            "contracts": 4,
        },
    )
    append_jsonl(
        nightly_review.ORDERS_FILE,
        {
            "event_type": "alpaca_option_paper_order_audit",
            "action": "paper_entry_order",
            "recorded_at": order_time,
            "status": "submitted",
            "position_id": f"human-{raw_key}",
            "source_dedupe_key": raw_key,
            **CONTRACT,
            "payload": {"side": "buy", "qty": "4"},
            "response": {},
        },
    )


def only_issue(report: dict[str, Any], code: str) -> dict[str, Any]:
    matches = [item for item in report["issues"] if item.get("code") == code]
    assert len(matches) == 1, report["issues"]
    return matches[0]


def test_startup_backfill_is_machine_availability() -> None:
    with tempfile.TemporaryDirectory() as tmp:
        patch_runtime_paths(Path(tmp))
        append_buy_case(
            raw_key="raw-startup",
            raw_captured_at=f"{DAY}T09:40:00-04:00",
            order_time=f"{DAY}T09:40:45-04:00",
        )

        report = nightly_review.review_day(DAY, refresh_browser=False)
        codes = {item["code"] for item in report["issues"]}
        assert "machine_unavailable_startup_backfill" in codes
        assert "slow_order_submission" not in codes
        assert report["latency_classification_counts"] == {
            "machine_unavailable_startup_backfill": 1,
            "slow_order_submission": 0,
        }
        assert report["counts"]["machine_availability_findings"] == 1

        evidence = only_issue(report, "machine_unavailable_startup_backfill")["evidence"]
        assert evidence["source_time"] == SOURCE_TIME
        assert evidence["raw_captured_at"] == f"{DAY}T09:40:00-04:00"
        assert evidence["order_time"] == f"{DAY}T09:40:45-04:00"
        assert evidence["source_to_capture_seconds"] == 600.0
        assert evidence["capture_to_order_seconds"] == 45.0
        assert evidence["total_latency_seconds"] == 645.0
        assert evidence["contract"]["contract_key"] == "FRVO|2026-07-17|45|call"
        assert evidence["raw_dedupe_keys"] == ["raw-startup"]
        assert evidence["parsed_source_keys"] == ["raw-startup"]


def test_true_live_delay_stays_slow_order_submission() -> None:
    with tempfile.TemporaryDirectory() as tmp:
        patch_runtime_paths(Path(tmp))
        append_buy_case(
            raw_key="raw-live-delay",
            raw_captured_at=f"{DAY}T09:30:20-04:00",
            order_time=f"{DAY}T09:34:00-04:00",
        )

        report = nightly_review.review_day(DAY, refresh_browser=False)
        codes = {item["code"] for item in report["issues"]}
        assert "slow_order_submission" in codes
        assert "machine_unavailable_startup_backfill" not in codes
        assert report["latency_classification_counts"] == {
            "machine_unavailable_startup_backfill": 0,
            "slow_order_submission": 1,
        }

        evidence = only_issue(report, "slow_order_submission")["evidence"]
        assert evidence["source_to_capture_seconds"] == 20.0
        assert evidence["capture_to_order_seconds"] == 220.0
        assert evidence["total_latency_seconds"] == 240.0
        assert evidence["raw_captured_at"] == f"{DAY}T09:30:20-04:00"


def test_missing_raw_capture_time_falls_back_to_slow_order_submission() -> None:
    with tempfile.TemporaryDirectory() as tmp:
        patch_runtime_paths(Path(tmp))
        append_buy_case(
            raw_key="raw-missing-capture",
            raw_captured_at=None,
            order_time=f"{DAY}T09:34:00-04:00",
        )

        report = nightly_review.review_day(DAY, refresh_browser=False)
        codes = {item["code"] for item in report["issues"]}
        assert "slow_order_submission" in codes
        assert "machine_unavailable_startup_backfill" not in codes

        evidence = only_issue(report, "slow_order_submission")["evidence"]
        assert evidence["raw_match_count"] == 1
        assert evidence["raw_dedupe_keys"] == ["raw-missing-capture"]
        assert evidence["raw_captured_at"] is None
        assert evidence["source_to_capture_seconds"] is None
        assert evidence["capture_to_order_seconds"] is None
        assert evidence["total_latency_seconds"] == 240.0


if __name__ == "__main__":
    test_startup_backfill_is_machine_availability()
    test_true_live_delay_stays_slow_order_submission()
    test_missing_raw_capture_time_falls_back_to_slow_order_submission()
    print("test_machine_availability.py passed")

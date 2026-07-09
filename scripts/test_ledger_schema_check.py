#!/usr/bin/env python3
"""Focused tests for the ledger schema checker."""

from __future__ import annotations

import json
import subprocess
import sys
import tempfile
from pathlib import Path

import ledger_schema_check


SECRET_SENTINEL = "do-not-print-secret-alert-text"


def append_jsonl(path: Path, row: dict) -> None:
    with path.open("a", encoding="utf-8") as handle:
        handle.write(json.dumps(row, sort_keys=True) + "\n")


def populate_valid_ledgers(data_dir: Path) -> None:
    append_jsonl(
        data_dir / "raw_notifications.jsonl",
        {
            "event_type": "raw_discord_notification",
            "dedupe_key": "raw-1",
            "captured_at": "2026-07-08T09:30:00-04:00",
            "notification_timestamp": "2026-07-08T09:29:59-04:00",
            "source_app": "Discord",
            "bundle_id": "com.hnc.Discord",
            "title": "OTWSteve",
            "subtitle": "alerts",
            "body": SECRET_SENTINEL,
            "raw": {},
        },
    )
    append_jsonl(
        data_dir / "parsed_alerts.jsonl",
        {
            "event_type": "parsed_trade_alert",
            "source_dedupe_key": "raw-1",
            "parsed_at": "2026-07-08T09:30:01-04:00",
            "ticker": "QQQ",
            "side": "buy",
            "instrument_type": "option",
            "entry_type": "limit",
            "entry_price": 5.86,
            "stop_price": None,
            "target_price": None,
            "time_in_force": "day",
            "confidence": "high",
            "raw_text": SECRET_SENTINEL,
            "parser_version": "test",
            "notification_timestamp": "2026-07-08T09:29:59-04:00",
        },
    )
    append_jsonl(
        data_dir / "shadow_option_positions.jsonl",
        {
            "event_type": "shadow_option_position",
            "position_id": "shadow-1",
            "validation_id": "validation-1",
            "canonical_entry_key": "QQQ|2026-07-17|call|500|5.86|1",
            "created_at": "2026-07-08T09:30:02-04:00",
            "opened_at": "2026-07-08T09:29:59-04:00",
            "source_dedupe_key": "raw-1",
            "ticker": "QQQ",
            "contract_symbol": "QQQ260717C00500000",
            "option_type": "call",
            "expiration_date": "2026-07-17",
            "strike_price": 500.0,
            "contracts": 1,
            "remaining_contracts": 1,
            "primary_tag": "swing",
            "tags": ["swing"],
            "alert_entry_price": 5.86,
            "bot_entry_price": 5.9,
            "bot_entry_price_source": "mark",
            "shadow_models": ["steve_exit"],
            "raw_text": SECRET_SENTINEL,
        },
    )
    append_jsonl(
        data_dir / "human_paper_positions.jsonl",
        {
            "event_type": "human_paper_option_position",
            "position_id": "human-1",
            "approval_id": "auto-1",
            "canonical_entry_key": "QQQ|2026-07-17|call|500|5.86|1",
            "opened_at": "2026-07-08T09:30:03-04:00",
            "source_dedupe_key": "raw-1",
            "alert_text": SECRET_SENTINEL,
            "alert_time": "2026-07-08T09:29:59-04:00",
            "alert_price": 5.86,
            "ticker": "QQQ",
            "contract_symbol": "QQQ260717C00500000",
            "option_type": "call",
            "expiration_date": "2026-07-17",
            "strike_price": 500.0,
            "contracts": 1,
            "entry_price": 5.9,
            "entry_price_source": "mark",
            "risk_type": "percent",
            "stop_percent": 35.0,
            "take_percent": 80.0,
            "stop_price": None,
            "take_price": None,
            "used_default_contracts": True,
            "used_default_risk": True,
            "alert_contracts": 1,
            "exit_plan": [{"contracts": 1, "take_percent": 80.0}],
            "exit_plan_notes": ["test"],
            "status": "open",
        },
    )
    append_jsonl(
        data_dir / "human_paper_exits.jsonl",
        {
            "event_type": "human_paper_option_exit",
            "exit_id": "human-exit-1",
            "position_id": "human-1",
            "approval_id": "auto-1",
            "recorded_at": "2026-07-08T10:00:00-04:00",
            "source_dedupe_key": "raw-1",
            "ticker": "QQQ",
            "contract_symbol": "QQQ260717C00500000",
            "option_type": "call",
            "expiration_date": "2026-07-17",
            "strike_price": 500.0,
            "position_contracts": 1,
            "reason": "target_80",
            "contracts": 1,
            "exit_price": 10.62,
            "entry_price": 5.9,
            "pnl_percent": 80.0,
            "pnl_dollars": 472.0,
            "remaining_after_exit": 0,
            "broker_status": "submitted",
            "broker_reason": "",
            "broker_order_id": "order-sell-1",
            "broker_client_order_id": "client-sell-1",
        },
    )
    append_jsonl(
        data_dir / "orders_paper.jsonl",
        {
            "event_type": "alpaca_option_paper_order_audit",
            "action": "paper_entry_order",
            "recorded_at": "2026-07-08T09:30:04-04:00",
            "status": "submitted",
            "reason": "",
            "exit_reason": "",
            "position_id": "human-1",
            "source_dedupe_key": "raw-1",
            "ticker": "QQQ",
            "contract_symbol": "QQQ260717C00500000",
            "payload": {"client_order_id": "client-buy-1"},
            "response": {"id": "order-buy-1", "client_order_id": "client-buy-1"},
        },
    )
    append_jsonl(
        data_dir / "broker_order_status_reports.jsonl",
        {
            "event_type": "broker_order_status_report",
            "recorded_at": "2026-07-08T09:31:00-04:00",
            "order_id": "order-buy-1",
            "client_order_id": "client-buy-1",
            "broker_status": "filled",
            "action": "paper_entry_order",
            "exit_reason": "",
            "position_id": "human-1",
            "source_dedupe_key": "raw-1",
            "contract_symbol": "QQQ260717C00500000",
            "label": "QQQ Jul 17 500C",
            "side": "buy",
            "qty": "1",
            "filled_qty": "1",
            "filled_avg_price": "5.90",
            "limit_price": "5.90",
            "submitted_at": "2026-07-08T09:30:04-04:00",
            "filled_at": "2026-07-08T09:30:05-04:00",
            "raw_order": {"id": "order-buy-1"},
        },
    )
    append_jsonl(
        data_dir / "nightly_review_reports.jsonl",
        {
            "event_type": "nightly_review_report",
            "day": "2026-07-08",
            "created_at": "2026-07-08T17:00:00-04:00",
            "json_path": "data/nightly_reviews/2026-07-08.json",
            "markdown_path": "data/nightly_reviews/2026-07-08.md",
            "counts": {},
            "issue_counts": {},
            "capture_method_scorecard": {},
            "all_time_pl": {},
            "steve_alert_pl": {},
            "broker_fill_pl": {},
            "executive_activity": {},
            "storage_hygiene": {},
            "recursive_improvement_plan": [],
            "recommended_next_actions": [],
        },
    )


def test_valid_ledgers_pass() -> None:
    with tempfile.TemporaryDirectory() as tmp:
        data_dir = Path(tmp)
        populate_valid_ledgers(data_dir)
        report = ledger_schema_check.run_check(data_dir)
        assert report["status"] == "ok"
        assert report["totals"]["rows"] == len(ledger_schema_check.SCHEMAS)
        assert report["totals"]["missing_required_key_count"] == 0


def test_missing_keys_and_bad_json_are_reported_without_values() -> None:
    with tempfile.TemporaryDirectory() as tmp:
        data_dir = Path(tmp)
        populate_valid_ledgers(data_dir)
        (data_dir / "parsed_alerts.jsonl").write_text(
            json.dumps({"event_type": "parsed_trade_alert", "raw_text": SECRET_SENTINEL}) + "\n",
            encoding="utf-8",
        )
        with (data_dir / "orders_paper.jsonl").open("a", encoding="utf-8") as handle:
            handle.write("{bad json\n")
            handle.write(json.dumps({"event_type": "alpaca_option_paper_order_audit", "action": "paper_entry_order", "recorded_at": "now", "status": "blocked", "reason": "", "source_dedupe_key": "", "ticker": "", "payload": {}, "response": {}}) + "\n")

        report = ledger_schema_check.run_check(data_dir)
        assert report["status"] == "issues"
        assert report["files"]["parsed_alerts.jsonl"]["missing_required_keys"]["source_dedupe_key"] == 1
        assert report["files"]["orders_paper.jsonl"]["invalid_json_lines"] == 1
        assert report["files"]["orders_paper.jsonl"]["missing_trace_key_group_count"] == 1
        assert SECRET_SENTINEL not in json.dumps(report)


def test_cli_print_json_and_write_report() -> None:
    with tempfile.TemporaryDirectory() as tmp:
        data_dir = Path(tmp)
        populate_valid_ledgers(data_dir)
        script = Path(__file__).with_name("ledger_schema_check.py")
        result = subprocess.run(
            [sys.executable, str(script), "--data-dir", str(data_dir), "--print-json", "--write-report"],
            check=False,
            capture_output=True,
            text=True,
        )
        assert result.returncode == 0, result.stderr
        parsed = json.loads(result.stdout)
        assert parsed["status"] == "ok"
        assert SECRET_SENTINEL not in result.stdout
        report_path = data_dir / ledger_schema_check.REPORT_FILE_NAME
        reports = [json.loads(line) for line in report_path.read_text(encoding="utf-8").splitlines()]
        assert len(reports) == 1
        assert reports[0]["event_type"] == "ledger_schema_check_report"


def main() -> int:
    test_valid_ledgers_pass()
    test_missing_keys_and_bad_json_are_reported_without_values()
    test_cli_print_json_and_write_report()
    print("Ledger schema checker tests passed.")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())

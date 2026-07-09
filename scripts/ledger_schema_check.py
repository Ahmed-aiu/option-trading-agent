#!/usr/bin/env python3
"""Validate core JSONL ledger trace keys without printing sensitive contents."""

from __future__ import annotations

import argparse
import datetime as dt
import json
from dataclasses import dataclass
from pathlib import Path
from typing import Any
from zoneinfo import ZoneInfo


ROOT = Path(__file__).resolve().parents[1]
DEFAULT_DATA_DIR = ROOT / "data"
DEFAULT_TZ = "America/Detroit"
REPORT_FILE_NAME = "ledger_schema_reports.jsonl"
MAX_FINDINGS_PER_FILE = 50
MISSING = object()


@dataclass(frozen=True)
class LedgerSchema:
    filename: str
    required_keys: tuple[str, ...]
    nonempty_keys: tuple[str, ...]
    trace_key_groups: tuple[tuple[str, ...], ...] = ()


SCHEMAS: tuple[LedgerSchema, ...] = (
    LedgerSchema(
        filename="raw_notifications.jsonl",
        required_keys=(
            "event_type",
            "dedupe_key",
            "captured_at",
            "notification_timestamp",
            "source_app",
            "bundle_id",
            "title",
            "subtitle",
            "body",
            "raw",
        ),
        nonempty_keys=("event_type", "dedupe_key", "captured_at", "body"),
    ),
    LedgerSchema(
        filename="parsed_alerts.jsonl",
        required_keys=(
            "event_type",
            "source_dedupe_key",
            "parsed_at",
            "ticker",
            "side",
            "instrument_type",
            "entry_type",
            "entry_price",
            "stop_price",
            "target_price",
            "time_in_force",
            "confidence",
            "raw_text",
            "parser_version",
            "notification_timestamp",
        ),
        nonempty_keys=("event_type", "source_dedupe_key", "parsed_at", "side", "instrument_type", "raw_text"),
    ),
    LedgerSchema(
        filename="shadow_option_positions.jsonl",
        required_keys=(
            "event_type",
            "position_id",
            "validation_id",
            "canonical_entry_key",
            "created_at",
            "opened_at",
            "source_dedupe_key",
            "ticker",
            "contract_symbol",
            "option_type",
            "expiration_date",
            "strike_price",
            "contracts",
            "remaining_contracts",
            "primary_tag",
            "tags",
            "alert_entry_price",
            "bot_entry_price",
            "bot_entry_price_source",
            "shadow_models",
            "raw_text",
        ),
        nonempty_keys=(
            "event_type",
            "position_id",
            "validation_id",
            "canonical_entry_key",
            "created_at",
            "opened_at",
            "source_dedupe_key",
            "ticker",
            "contract_symbol",
        ),
    ),
    LedgerSchema(
        filename="human_paper_positions.jsonl",
        required_keys=(
            "event_type",
            "position_id",
            "approval_id",
            "canonical_entry_key",
            "opened_at",
            "source_dedupe_key",
            "alert_text",
            "alert_time",
            "alert_price",
            "ticker",
            "contract_symbol",
            "option_type",
            "expiration_date",
            "strike_price",
            "contracts",
            "entry_price",
            "entry_price_source",
            "risk_type",
            "stop_percent",
            "take_percent",
            "stop_price",
            "take_price",
            "used_default_contracts",
            "used_default_risk",
            "alert_contracts",
            "exit_plan",
            "exit_plan_notes",
            "status",
        ),
        nonempty_keys=(
            "event_type",
            "position_id",
            "approval_id",
            "canonical_entry_key",
            "opened_at",
            "source_dedupe_key",
            "ticker",
            "contract_symbol",
            "status",
        ),
    ),
    LedgerSchema(
        filename="human_paper_exits.jsonl",
        required_keys=(
            "event_type",
            "exit_id",
            "position_id",
            "approval_id",
            "recorded_at",
            "source_dedupe_key",
            "ticker",
            "contract_symbol",
            "option_type",
            "expiration_date",
            "strike_price",
            "position_contracts",
            "reason",
            "contracts",
            "exit_price",
            "entry_price",
            "pnl_percent",
            "pnl_dollars",
            "remaining_after_exit",
            "broker_status",
            "broker_reason",
            "broker_order_id",
            "broker_client_order_id",
        ),
        nonempty_keys=(
            "event_type",
            "exit_id",
            "position_id",
            "approval_id",
            "recorded_at",
            "source_dedupe_key",
            "ticker",
            "contract_symbol",
            "reason",
        ),
    ),
    LedgerSchema(
        filename="orders_paper.jsonl",
        required_keys=(
            "event_type",
            "action",
            "recorded_at",
            "status",
            "reason",
            "source_dedupe_key",
            "ticker",
            "payload",
            "response",
        ),
        nonempty_keys=("event_type", "action", "recorded_at", "status"),
        trace_key_groups=(("source_dedupe_key", "position_id", "payload.client_order_id", "response.client_order_id", "response.id"),),
    ),
    LedgerSchema(
        filename="broker_order_status_reports.jsonl",
        required_keys=(
            "event_type",
            "recorded_at",
            "order_id",
            "client_order_id",
            "broker_status",
            "action",
            "exit_reason",
            "position_id",
            "source_dedupe_key",
            "contract_symbol",
            "label",
            "side",
            "qty",
            "filled_qty",
            "filled_avg_price",
            "limit_price",
            "submitted_at",
            "filled_at",
            "raw_order",
        ),
        nonempty_keys=("event_type", "recorded_at", "order_id", "client_order_id", "broker_status", "action"),
    ),
    LedgerSchema(
        filename="nightly_review_reports.jsonl",
        required_keys=(
            "event_type",
            "day",
            "created_at",
            "json_path",
            "markdown_path",
            "counts",
            "issue_counts",
            "capture_method_scorecard",
            "all_time_pl",
            "steve_alert_pl",
            "broker_fill_pl",
            "executive_activity",
            "storage_hygiene",
            "recursive_improvement_plan",
            "recommended_next_actions",
        ),
        nonempty_keys=("event_type", "day", "created_at", "json_path", "markdown_path"),
    ),
)


def now_iso() -> str:
    return dt.datetime.now(ZoneInfo(DEFAULT_TZ)).isoformat(timespec="seconds")


def value_at_path(row: dict[str, Any], path: str) -> Any:
    value: Any = row
    for part in path.split("."):
        if not isinstance(value, dict) or part not in value:
            return MISSING
        value = value[part]
    return value


def is_empty(value: Any) -> bool:
    return value is MISSING or value is None or value == "" or value == [] or value == {}


def increment(counter: dict[str, int], key: str) -> None:
    counter[key] = int(counter.get(key, 0)) + 1


def add_finding(findings: list[dict[str, Any]], finding: dict[str, Any]) -> None:
    if len(findings) < MAX_FINDINGS_PER_FILE:
        findings.append(finding)


def check_jsonl_file(path: Path, schema: LedgerSchema) -> dict[str, Any]:
    result: dict[str, Any] = {
        "exists": path.exists(),
        "rows": 0,
        "invalid_json_lines": 0,
        "non_object_lines": 0,
        "missing_required_key_count": 0,
        "missing_required_keys": {},
        "empty_join_key_count": 0,
        "empty_join_keys": {},
        "missing_trace_key_group_count": 0,
        "missing_trace_key_groups": {},
        "findings": [],
    }
    if not path.exists():
        result["missing_file"] = True
        add_finding(result["findings"], {"kind": "missing_file", "file": schema.filename})
        return result

    with path.open("r", encoding="utf-8") as handle:
        for line_no, line in enumerate(handle, 1):
            text = line.strip()
            if not text:
                continue
            try:
                row = json.loads(text)
            except json.JSONDecodeError:
                result["invalid_json_lines"] += 1
                add_finding(result["findings"], {"kind": "invalid_json", "line": line_no})
                continue
            if not isinstance(row, dict):
                result["non_object_lines"] += 1
                add_finding(result["findings"], {"kind": "non_object_json", "line": line_no})
                continue

            result["rows"] += 1
            missing = [key for key in schema.required_keys if value_at_path(row, key) is MISSING]
            if missing:
                result["missing_required_key_count"] += len(missing)
                for key in missing:
                    increment(result["missing_required_keys"], key)
                add_finding(result["findings"], {"kind": "missing_required_keys", "line": line_no, "keys": missing})

            empty_join_keys = [key for key in schema.nonempty_keys if is_empty(value_at_path(row, key))]
            if empty_join_keys:
                result["empty_join_key_count"] += len(empty_join_keys)
                for key in empty_join_keys:
                    increment(result["empty_join_keys"], key)
                add_finding(result["findings"], {"kind": "empty_join_keys", "line": line_no, "keys": empty_join_keys})

            for group in schema.trace_key_groups:
                if all(is_empty(value_at_path(row, key)) for key in group):
                    group_key = "|".join(group)
                    result["missing_trace_key_group_count"] += 1
                    increment(result["missing_trace_key_groups"], group_key)
                    add_finding(result["findings"], {"kind": "missing_trace_key_group", "line": line_no, "keys": list(group)})

    return result


def run_check(data_dir: Path) -> dict[str, Any]:
    files: dict[str, Any] = {}
    totals = {
        "files_checked": len(SCHEMAS),
        "missing_files": 0,
        "rows": 0,
        "invalid_json_lines": 0,
        "non_object_lines": 0,
        "missing_required_key_count": 0,
        "empty_join_key_count": 0,
        "missing_trace_key_group_count": 0,
    }
    for schema in SCHEMAS:
        file_result = check_jsonl_file(data_dir / schema.filename, schema)
        files[schema.filename] = file_result
        if not file_result.get("exists"):
            totals["missing_files"] += 1
        for key in (
            "rows",
            "invalid_json_lines",
            "non_object_lines",
            "missing_required_key_count",
            "empty_join_key_count",
            "missing_trace_key_group_count",
        ):
            totals[key] += int(file_result.get(key) or 0)

    issue_count = (
        totals["missing_files"]
        + totals["invalid_json_lines"]
        + totals["non_object_lines"]
        + totals["missing_required_key_count"]
        + totals["empty_join_key_count"]
        + totals["missing_trace_key_group_count"]
    )
    return {
        "event_type": "ledger_schema_check_report",
        "generated_at": now_iso(),
        "data_dir": str(data_dir),
        "status": "ok" if issue_count == 0 else "issues",
        "totals": totals,
        "files": files,
    }


def append_report(data_dir: Path, report: dict[str, Any]) -> Path:
    data_dir.mkdir(parents=True, exist_ok=True)
    path = data_dir / REPORT_FILE_NAME
    with path.open("a", encoding="utf-8") as handle:
        handle.write(json.dumps(report, sort_keys=True, separators=(",", ":")) + "\n")
    return path


def print_summary(report: dict[str, Any]) -> None:
    totals = report.get("totals") or {}
    print(f"Ledger schema check status: {report.get('status')}")
    print(f"Data dir: {report.get('data_dir')}")
    print(f"Rows checked: {totals.get('rows', 0)}")
    print(f"Missing files: {totals.get('missing_files', 0)}")
    print(f"Invalid JSON lines: {totals.get('invalid_json_lines', 0)}")
    print(f"Non-object JSON lines: {totals.get('non_object_lines', 0)}")
    print(f"Missing required keys: {totals.get('missing_required_key_count', 0)}")
    print(f"Empty join keys: {totals.get('empty_join_key_count', 0)}")
    print(f"Missing trace-key groups: {totals.get('missing_trace_key_group_count', 0)}")


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--data-dir", default=str(DEFAULT_DATA_DIR), help="Directory containing JSONL ledgers")
    parser.add_argument("--print-json", action="store_true", help="Print the full JSON report to stdout")
    parser.add_argument(
        "--write-report",
        action="store_true",
        help=f"Append the report to {REPORT_FILE_NAME} in --data-dir",
    )
    return parser.parse_args()


def main() -> int:
    args = parse_args()
    data_dir = Path(args.data_dir)
    report = run_check(data_dir)
    if args.write_report:
        append_report(data_dir, report)
    if args.print_json:
        print(json.dumps(report, indent=2, sort_keys=True))
    else:
        print_summary(report)
    return 0 if report.get("status") == "ok" else 1


if __name__ == "__main__":
    raise SystemExit(main())

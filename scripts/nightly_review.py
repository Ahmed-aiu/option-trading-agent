#!/usr/bin/env python3
"""Nightly source-of-truth review for Steve option alert automation."""

from __future__ import annotations

import argparse
import datetime as dt
import json
import re
from collections import defaultdict
from pathlib import Path
from typing import Any
from zoneinfo import ZoneInfo

from backfill_steve_text import useful_lines
from data_hygiene import compact_runtime_ledgers, data_hygiene_scorecard
from parse_alert import ParseRejected, parse_trade_alert, raw_text_from_record
from pipeline_common import CONFIG_DIR, DATA_DIR, append_jsonl, load_simple_yaml, now_iso, parse_datetime, read_jsonl, stable_hash
from run_pipeline_once import normalize_parsed


NIGHTLY_DIR = DATA_DIR / "nightly_reviews"
NIGHTLY_SUMMARY_FILE = DATA_DIR / "nightly_review_reports.jsonl"
NIGHTLY_TELEGRAM_FILE = DATA_DIR / "nightly_telegram_reports.jsonl"
STEVE_ALERT_PL_FILE = DATA_DIR / "steve_alert_pl_reports.jsonl"
BROKER_FILL_PL_FILE = DATA_DIR / "broker_fill_pl_reports.jsonl"
BROWSER_MESSAGES_FILE = DATA_DIR / "discord_browser_messages.jsonl"
RAW_FILE = DATA_DIR / "raw_notifications.jsonl"
PARSED_FILE = DATA_DIR / "parsed_alerts.jsonl"
REJECTED_FILE = DATA_DIR / "rejected_alerts.jsonl"
APPROVAL_CARDS_FILE = DATA_DIR / "steve_approval_cards.jsonl"
AUTO_BUY_REPORTS_FILE = DATA_DIR / "steve_auto_buy_reports.jsonl"
HUMAN_POSITIONS_FILE = DATA_DIR / "human_paper_positions.jsonl"
HUMAN_EXITS_FILE = DATA_DIR / "human_paper_exits.jsonl"
ORDERS_FILE = DATA_DIR / "orders_paper.jsonl"
BROKER_STATUS_FILE = DATA_DIR / "broker_order_status_reports.jsonl"
STEVE_EXITS_FILE = DATA_DIR / "steve_option_exits.jsonl"
PIPELINE_HEALTH_FILE = DATA_DIR / "pipeline_health_checks.jsonl"
DAILY_PL_FILE = DATA_DIR / "daily_pl_reports.jsonl"
BROWSER_HEALTH_LATEST_FILE = DATA_DIR / "discord_browser_health_latest.json"
PREMARKET_READINESS_FILE = DATA_DIR / "premarket_readiness_reports.jsonl"

PRICE_RE = r"(?:\d+(?:\.\d+)?|\.\d+)"
ADD_RE = re.compile(r"\b(?P<action>aaded|added|add)\s+(?P<contracts>\d+)\s+(?:@|at)\s*(?P<price>" + PRICE_RE + r")", re.I)
STOPPED_OUT_RE = re.compile(r"\bstopped?\s+out\b", re.I)
OCC_SYMBOL_RE = re.compile(r"^(?P<ticker>[A-Z]+)(?P<year>\d{2})(?P<month>\d{2})(?P<day>\d{2})(?P<option_type>[CP])(?P<strike>\d{8})$")
SYNTHETIC_TEST_SOURCE_PREFIXES = ("full-stock-", "full-option-", "full-add-")
SYNTHETIC_TEST_VALUES = {
    "closed-source",
    "approval-test",
    "approval-test-2",
    "manual-dm-close-report-test",
    "source-test",
}
SLOW_ORDER_SUBMISSION_SECONDS = 90.0
STARTUP_BACKFILL_SOURCE_CAPTURE_SECONDS = 300.0
STARTUP_BACKFILL_CAPTURE_ORDER_SECONDS = 120.0


def safe_float(value: Any) -> float | None:
    if value in (None, ""):
        return None
    try:
        return float(value)
    except (TypeError, ValueError):
        return None


def safe_int(value: Any) -> int | None:
    if value in (None, ""):
        return None
    try:
        return int(float(value))
    except (TypeError, ValueError):
        return None


def business_day_for_run(tz_name: str) -> str:
    now = dt.datetime.now(ZoneInfo(tz_name))
    day = now.date()
    if now.weekday() >= 5 or now.hour < 9:
        day -= dt.timedelta(days=1)
        while day.weekday() >= 5:
            day -= dt.timedelta(days=1)
    return day.isoformat()


def date_key(value: Any, tz_name: str = "America/Detroit") -> str:
    parsed = parse_datetime(value, tz_name)
    if parsed is not None:
        return parsed.date().isoformat()
    text = str(value or "")
    return text[:10] if re.match(r"\d{4}-\d{2}-\d{2}", text) else ""


def row_time(row: dict[str, Any]) -> str:
    for key in ("recorded_at", "created_at", "opened_at", "parsed_at", "captured_at", "notification_timestamp", "submitted_at"):
        if row.get(key):
            return str(row.get(key))
    return ""


def rows_for_day(path: Path, day: str, tz_name: str = "America/Detroit") -> list[dict[str, Any]]:
    return [row for row in read_jsonl(path) if date_key(row_time(row), tz_name) == day]


def load_json(path: Path) -> dict[str, Any]:
    if not path.exists():
        return {}
    try:
        value = json.loads(path.read_text(encoding="utf-8"))
    except json.JSONDecodeError:
        return {}
    return value if isinstance(value, dict) else {}


def synthetic_test_value(value: Any) -> bool:
    text = str(value or "")
    if not text:
        return False
    if text in SYNTHETIC_TEST_VALUES:
        return True
    if any(text.startswith(prefix) for prefix in SYNTHETIC_TEST_SOURCE_PREFIXES):
        return True
    return text.startswith(("test-", "human-test", "shadow-test", "exit-test", "order-test"))


def is_synthetic_test_row(row: dict[str, Any]) -> bool:
    values = [
        row.get("source_dedupe_key"),
        row.get("dedupe_key"),
        row.get("position_id"),
        row.get("approval_id"),
        row.get("exit_id"),
        row.get("order_id"),
    ]
    payload = row.get("payload")
    if isinstance(payload, dict):
        values.append(payload.get("client_order_id"))
    return any(synthetic_test_value(value) for value in values)


def option_side(value: Any) -> str:
    text = str(value or "").lower()
    if text.startswith("call"):
        return "call"
    if text.startswith("put"):
        return "put"
    return text


def option_fields_from_symbol(symbol: Any) -> dict[str, Any]:
    match = OCC_SYMBOL_RE.match(str(symbol or "").upper())
    if not match:
        return {}
    year = 2000 + int(match.group("year"))
    strike = int(match.group("strike")) / 1000
    return {
        "ticker": match.group("ticker"),
        "expiration_date": f"{year:04d}-{int(match.group('month')):02d}-{int(match.group('day')):02d}",
        "strike_price": strike,
        "option_type": "call" if match.group("option_type") == "C" else "put",
    }


def option_fields(row: dict[str, Any]) -> dict[str, Any]:
    fields = {
        "ticker": row.get("ticker"),
        "expiration_date": row.get("expiration_date"),
        "strike_price": row.get("strike_price"),
        "option_type": row.get("option_type"),
    }
    if all(value not in (None, "") for value in fields.values()):
        return fields
    symbol_fields = option_fields_from_symbol(row.get("contract_symbol") or row.get("symbol") or (row.get("payload") or {}).get("symbol"))
    return {**symbol_fields, **{key: value for key, value in fields.items() if value not in (None, "")}}


def strike_text(value: Any) -> str:
    number = safe_float(value)
    return f"{number:g}" if number is not None else ""


def contract_key(row: dict[str, Any]) -> str:
    fields = option_fields(row)
    return "|".join(
        [
            str(fields.get("ticker") or "").upper(),
            str(fields.get("expiration_date") or ""),
            strike_text(fields.get("strike_price")),
            option_side(fields.get("option_type")),
        ]
    )


def price_key(value: Any) -> str:
    number = safe_float(value)
    return f"{number:.2f}" if number is not None else ""


def event_key(event: dict[str, Any]) -> str:
    kind = str(event.get("kind") or "")
    base = [kind, contract_key(event)]
    if kind in {"buy", "add"}:
        base.extend([price_key(event.get("entry_price") or event.get("add_price")), str(event.get("contracts") or "")])
    elif kind == "exit":
        base.extend([price_key(event.get("exit_price")), str(event.get("contracts") or "")])
    elif kind == "context_stop":
        base.append(str(event.get("source_time") or "")[:16])
    return "|".join(base)


def is_noise_line(line: str) -> bool:
    cleaned = " ".join(line.split()).strip()
    lowered = cleaned.lower()
    if not cleaned or cleaned in {"—", "-", "|"}:
        return True
    if lowered in {"today", "yesterday", "new messages"}:
        return True
    if re.fullmatch(r"\d{1,2}:\d{2}\s*(?:am|pm)", lowered):
        return True
    if re.search(r"^[A-Za-z]+,\s+[A-Za-z]+\s+\d{1,2},\s+\d{4}\s+at\s+\d{1,2}:\d{2}\s+(?:AM|PM)$", cleaned):
        return True
    return False


def raw_record_for_line(line: str, dedupe_key: str, source_time: str, source: str) -> dict[str, Any]:
    return {
        "event_type": "nightly_truth_raw",
        "captured_at": source_time or now_iso(),
        "notification_timestamp": source_time or "",
        "source_app": "DiscordBrowserTruth",
        "bundle_id": "nightly_review",
        "title": "OTWSteve",
        "subtitle": source,
        "body": line,
        "raw": {"source": source},
        "dedupe_key": dedupe_key,
    }


def parsed_for_body(body: str, source_time: str, source: str) -> list[dict[str, Any]]:
    key = "truth-" + stable_hash([source, source_time, body])[:24]
    try:
        parsed = parse_trade_alert(raw_record_for_line(body, key, source_time, source))
    except ParseRejected:
        return []
    return normalize_parsed(parsed)


def truth_event_from_parsed(parsed: dict[str, Any], source: dict[str, Any], kind: str | None = None) -> dict[str, Any]:
    event = {
        "kind": kind or ("exit" if parsed.get("side") == "exit" else "buy"),
        "source": source.get("source"),
        "channel_id": source.get("channel_id"),
        "message_key": source.get("message_key"),
        "source_time": source.get("source_time"),
        "raw_text": parsed.get("raw_text"),
        "matched_text": parsed.get("matched_text"),
        "ticker": parsed.get("ticker"),
        "expiration_date": parsed.get("expiration_date"),
        "strike_price": parsed.get("strike_price"),
        "option_type": parsed.get("option_type"),
        "contracts": parsed.get("contracts"),
        "entry_price": parsed.get("entry_price"),
        "exit_price": parsed.get("exit_price"),
        "tags": parsed.get("tags") or [],
        "primary_tag": parsed.get("primary_tag"),
        "confidence": parsed.get("confidence"),
    }
    event["event_key"] = event_key(event)
    event["contract_key"] = contract_key(event)
    return event


def parse_truth_events_from_text(text: str, source: dict[str, Any]) -> list[dict[str, Any]]:
    events: list[dict[str, Any]] = []
    last_entry: dict[str, Any] | None = None
    for line in useful_lines(text):
        if is_noise_line(line):
            continue
        add_match = ADD_RE.search(line)
        if add_match and last_entry:
            event = {
                "kind": "add",
                "source": source.get("source"),
                "channel_id": source.get("channel_id"),
                "message_key": source.get("message_key"),
                "source_time": source.get("source_time"),
                "raw_text": line,
                "ticker": last_entry.get("ticker"),
                "expiration_date": last_entry.get("expiration_date"),
                "strike_price": last_entry.get("strike_price"),
                "option_type": last_entry.get("option_type"),
                "contracts": int(add_match.group("contracts")),
                "add_price": float(add_match.group("price")),
                "tags": last_entry.get("tags") or [],
                "primary_tag": last_entry.get("primary_tag"),
            }
            event["event_key"] = event_key(event)
            event["contract_key"] = contract_key(event)
            events.append(event)
            continue
        if STOPPED_OUT_RE.search(line) and last_entry:
            event = {
                "kind": "context_stop",
                "source": source.get("source"),
                "channel_id": source.get("channel_id"),
                "message_key": source.get("message_key"),
                "source_time": source.get("source_time"),
                "raw_text": line,
                "ticker": last_entry.get("ticker"),
                "expiration_date": last_entry.get("expiration_date"),
                "strike_price": last_entry.get("strike_price"),
                "option_type": last_entry.get("option_type"),
                "contracts": last_entry.get("contracts"),
                "tags": last_entry.get("tags") or [],
                "primary_tag": last_entry.get("primary_tag"),
            }
            event["event_key"] = event_key(event)
            event["contract_key"] = contract_key(event)
            events.append(event)
            continue

        parsed_items = parsed_for_body(line, str(source.get("source_time") or ""), str(source.get("source") or "nightly"))
        parsed_needs_context = (
            parsed_items
            and last_entry
            and all(item.get("side") == "exit" and not item.get("ticker") for item in parsed_items)
        )
        if (not parsed_items or parsed_needs_context) and last_entry:
            context = str(last_entry.get("matched_text") or last_entry.get("raw_text") or "")
            parsed_items = parsed_for_body(f"{context}\n{line}", str(source.get("source_time") or ""), str(source.get("source") or "nightly"))
        for parsed in parsed_items:
            if parsed.get("instrument_type") != "option":
                continue
            if parsed.get("side") == "buy":
                event = truth_event_from_parsed(parsed, source, "buy")
                events.append(event)
                last_entry = parsed
            elif parsed.get("side") == "exit":
                events.append(truth_event_from_parsed(parsed, source, "exit"))
    if any(event.get("kind") in {"add", "exit", "context_stop"} for event in events):
        events = [event for event in events if event.get("kind") != "buy"]
    return events


def normalize_message_key(value: Any) -> str:
    return re.sub(r"^chat-messages___", "", str(value or ""))


def truth_events_from_browser_ledger(day: str, tz_name: str) -> list[dict[str, Any]]:
    events: list[dict[str, Any]] = []
    seen_messages: set[str] = set()
    for row in read_jsonl(BROWSER_MESSAGES_FILE):
        if date_key(row.get("message_timestamp") or row.get("captured_at"), tz_name) != day:
            continue
        key = normalize_message_key(row.get("message_key") or row.get("message_id"))
        if key in seen_messages:
            continue
        seen_messages.add(key)
        source = {
            "source": "browser_ledger",
            "channel_id": row.get("channel_id"),
            "message_key": key,
            "source_time": row.get("message_timestamp") or row.get("captured_at"),
        }
        events.extend(parse_truth_events_from_text(str(row.get("text_preview") or ""), source))
    return dedupe_truth_events(events)


def truth_events_from_chrome(day: str, tz_name: str, max_age_minutes: float, timeout: int, retries: int) -> tuple[list[dict[str, Any]], list[dict[str, Any]]]:
    from discord_browser_channel_watcher import channel_urls, filter_candidate_messages, read_channel_snapshot_with_retries

    config = load_simple_yaml(CONFIG_DIR / "watcher.yaml")
    author_names = [str(item) for item in config.get("alert_author_names", [])]
    events: list[dict[str, Any]] = []
    channel_reports: list[dict[str, Any]] = []
    for item in channel_urls(config):
        channel_id = item["channel_id"]
        channel_report: dict[str, Any] = {"channel_id": channel_id, "status": "ok", "events": 0}
        try:
            snapshot = read_channel_snapshot_with_retries(item["url"], timeout=timeout, first_load_delay=2.0, retries=retries)
            candidates = filter_candidate_messages(
                list(snapshot.get("messages") or []),
                author_names=author_names,
                max_age_minutes=max_age_minutes,
                tz_name=tz_name,
                allow_unknown_time=False,
            )
            day_candidates = [
                message
                for message in candidates
                if date_key(message.get("message_timestamp"), tz_name) == day
            ]
            channel_report["visible_messages"] = len(snapshot.get("messages") or [])
            channel_report["candidate_messages"] = len(day_candidates)
            for message in day_candidates:
                source = {
                    "source": "browser_refresh",
                    "channel_id": channel_id,
                    "message_key": normalize_message_key(message.get("id")),
                    "source_time": message.get("message_timestamp"),
                }
                parsed = parse_truth_events_from_text(str(message.get("text") or ""), source)
                channel_report["events"] += len(parsed)
                events.extend(parsed)
        except Exception as exc:  # noqa: BLE001
            channel_report["status"] = "error"
            channel_report["reason"] = f"{type(exc).__name__}:{exc}"
        channel_reports.append(channel_report)
    return dedupe_truth_events(events), channel_reports


def dedupe_truth_events(events: list[dict[str, Any]]) -> list[dict[str, Any]]:
    seen: set[str] = set()
    deduped: list[dict[str, Any]] = []
    for event in sorted(events, key=lambda row: str(row.get("source_time") or "")):
        key = str(event.get("event_key") or "")
        if not key or key in seen:
            continue
        seen.add(key)
        deduped.append(event)
    return deduped


def parsed_buy_key(row: dict[str, Any]) -> str:
    return "|".join(["buy", contract_key(row), price_key(row.get("entry_price")), str(row.get("contracts") or "")])


def position_duplicate_key(row: dict[str, Any]) -> str:
    entry_price = row.get("entry_price")
    if entry_price in (None, ""):
        entry_price = row.get("alert_entry_price")
    return "|".join(["buy", contract_key(row), price_key(entry_price), str(row.get("contracts") or "")])


def steve_exit_duplicate_key(row: dict[str, Any]) -> str:
    return "|".join(
        [
            "exit",
            str(row.get("matched_shadow_position_id") or ""),
            contract_key(row),
            price_key(row.get("exit_price")),
            str(row.get("contracts") or ""),
        ]
    )


def canonical_or_fallback_key(
    row: dict[str, Any],
    *,
    canonical_keys: tuple[str, ...],
    fallback_key_func: Any,
) -> tuple[str, str]:
    for key_name in canonical_keys:
        value = str(row.get(key_name) or "")
        if value:
            return "canonical", value
    fallback = str(fallback_key_func(row) or "")
    return ("legacy", fallback) if fallback else ("", "")


def duplicate_group_summary(rows: list[dict[str, Any]], key_func: Any, id_key: str) -> dict[str, Any]:
    groups: dict[str, list[dict[str, Any]]] = defaultdict(list)
    for row in rows:
        key = key_func(row)
        if not key or key.count("|") and not key.replace("|", ""):
            continue
        groups[key].append(row)
    duplicates = {key: grouped for key, grouped in groups.items() if len(grouped) > 1}
    examples = []
    for key, grouped in sorted(duplicates.items(), key=lambda item: len(item[1]), reverse=True)[:10]:
        examples.append(
            {
                "key": key,
                "count": len(grouped),
                "ids": [row.get(id_key) for row in grouped[:8]],
                "source_dedupe_keys": [row.get("source_dedupe_key") for row in grouped[:8]],
            }
        )
    return {
        "rows": len(rows),
        "duplicate_groups": len(duplicates),
        "duplicate_rows": sum(len(grouped) - 1 for grouped in duplicates.values()),
        "examples": examples,
    }


def build_ledger_duplicate_scorecard(
    rows: list[dict[str, Any]],
    *,
    fallback_key_func: Any,
    id_key: str,
    canonical_keys: tuple[str, ...],
) -> dict[str, Any]:
    canonical_groups: dict[str, list[dict[str, Any]]] = defaultdict(list)
    legacy_groups: dict[str, list[dict[str, Any]]] = defaultdict(list)
    for row in rows:
        bucket, key = canonical_or_fallback_key(
            row,
            canonical_keys=canonical_keys,
            fallback_key_func=fallback_key_func,
        )
        if not bucket or not key:
            continue
        target = canonical_groups if bucket == "canonical" else legacy_groups
        target[key].append(row)

    def summarize(groups: dict[str, list[dict[str, Any]]], limit: int = 10) -> tuple[int, int, list[dict[str, Any]]]:
        duplicates = {key: grouped for key, grouped in groups.items() if len(grouped) > 1}
        examples: list[dict[str, Any]] = []
        for key, grouped in sorted(duplicates.items(), key=lambda item: len(item[1]), reverse=True)[:limit]:
            examples.append(
                {
                    "key": key,
                    "count": len(grouped),
                    "ids": [row.get(id_key) for row in grouped[:8]],
                    "source_dedupe_keys": [row.get("source_dedupe_key") for row in grouped[:8]],
                }
            )
        return len(duplicates), sum(len(grouped) - 1 for grouped in duplicates.values()), examples

    canonical_groups_count, canonical_duplicate_rows, canonical_examples = summarize(canonical_groups)
    legacy_groups_count, legacy_duplicate_rows, legacy_examples = summarize(legacy_groups)
    return {
        "rows": len(rows),
        "canonical_rows": sum(1 for row in rows if any(str(row.get(key) or "") for key in canonical_keys)),
        "legacy_rows": sum(1 for row in rows if not any(str(row.get(key) or "") for key in canonical_keys)),
        "duplicate_groups": canonical_groups_count,
        "duplicate_rows": canonical_duplicate_rows,
        "examples": canonical_examples,
        "legacy_collision_groups": legacy_groups_count,
        "legacy_collision_rows": legacy_duplicate_rows,
        "legacy_examples": legacy_examples,
    }


def parsed_exit_key(row: dict[str, Any]) -> str:
    return "|".join(["exit", contract_key(row), price_key(row.get("exit_price")), str(row.get("contracts") or "")])


def raw_capture_method(row: dict[str, Any]) -> str:
    source_app = str(row.get("source_app") or "").lower()
    bundle_id = str(row.get("bundle_id") or "").lower()
    raw_source = str((row.get("raw") or {}).get("source") or "").lower()
    if source_app in {"discordui", "discordbrowsertruth"} or bundle_id == "browser_or_clipboard" or raw_source.startswith("browser_channel"):
        return "browser"
    if "discord" in source_app or bundle_id == "com.hnc.discord":
        return "notification"
    return "other"


def parsed_event_keys_for_raw(row: dict[str, Any]) -> list[str]:
    try:
        parsed_items = normalize_parsed(parse_trade_alert(row))
    except ParseRejected:
        return []
    keys: list[str] = []
    for parsed in parsed_items:
        if parsed.get("instrument_type") != "option":
            continue
        if parsed.get("side") == "buy":
            keys.append(parsed_buy_key(parsed))
        elif parsed.get("side") == "exit":
            keys.append(parsed_exit_key(parsed))
    return keys


def first_capture_time(values: list[str], tz_name: str) -> str:
    parsed_values = [(parse_datetime(value, tz_name), value) for value in values if value]
    valid = [(parsed, value) for parsed, value in parsed_values if parsed is not None]
    if not valid:
        return min(values) if values else ""
    return min(valid, key=lambda item: item[0])[1]


def summarize_latency(values: list[float]) -> dict[str, Any]:
    if not values:
        return {"avg_seconds": None, "max_seconds": None}
    return {
        "avg_seconds": round(sum(values) / len(values), 1),
        "max_seconds": round(max(values), 1),
    }


def capture_recommendation(methods: dict[str, dict[str, Any]], truth_event_count: int) -> dict[str, Any]:
    if truth_event_count == 0:
        return {
            "recommended_primary": "insufficient_data",
            "reason": "No parseable Steve buy/exit truth events were found for the day.",
            "browser_interval_seconds": None,
        }
    browser = methods.get("browser", {})
    notification = methods.get("notification", {})
    browser_rate = safe_float(browser.get("capture_rate")) or 0.0
    notification_rate = safe_float(notification.get("capture_rate")) or 0.0
    browser_latency = safe_float((browser.get("latency") or {}).get("avg_seconds"))
    notification_latency = safe_float((notification.get("latency") or {}).get("avg_seconds"))

    if browser_rate >= 0.95 and (browser_rate >= notification_rate + 0.10 or notification_rate < 0.80):
        interval = 5
        if browser_latency is not None and browser_latency > 90:
            interval = 3
        return {
            "recommended_primary": "browser",
            "reason": "Browser capture covered materially more Steve alerts than macOS notifications.",
            "browser_interval_seconds": interval,
        }
    if notification_rate >= browser_rate + 0.20 and notification_rate >= 0.80:
        return {
            "recommended_primary": "notification",
            "reason": "macOS notifications covered materially more Steve alerts than browser capture.",
            "browser_interval_seconds": 10,
        }
    if browser_rate >= notification_rate:
        interval = 5 if browser_latency is None or browser_latency > 15 else 10
        return {
            "recommended_primary": "browser_with_notification_backup",
            "reason": "Browser capture was at least as complete as notifications; keep notifications as a dedupe/backup signal.",
            "browser_interval_seconds": interval,
        }
    return {
        "recommended_primary": "auto_dual",
        "reason": "Neither capture method clearly dominated; keep both active and compare again with more sessions.",
        "browser_interval_seconds": 5,
    }


def capture_method_scorecard(truth_events: list[dict[str, Any]], raw_rows: list[dict[str, Any]], tz_name: str) -> dict[str, Any]:
    scored_truth = [event for event in truth_events if event.get("kind") in {"buy", "exit"}]
    truth_by_key = {str(event.get("event_key") or event_key(event)): event for event in scored_truth}
    truth_keys = set(truth_by_key)
    methods: dict[str, dict[str, Any]] = {}
    event_times: dict[str, dict[str, list[str]]] = defaultdict(lambda: defaultdict(list))
    cross_source_methods: dict[str, set[str]] = defaultdict(set)

    for method in ("browser", "notification", "other"):
        methods[method] = {
            "method": method,
            "raw_records": 0,
            "parsed_event_records": 0,
            "unique_event_records": 0,
            "duplicate_event_records": 0,
            "matched_truth_events": 0,
            "missed_truth_events": len(truth_keys),
            "capture_rate": 0.0,
            "latency": {"avg_seconds": None, "max_seconds": None},
            "sample_missed_event_keys": [],
        }

    event_counts: dict[str, dict[str, int]] = defaultdict(lambda: defaultdict(int))
    for row in raw_rows:
        method = raw_capture_method(row)
        if method not in methods:
            method = "other"
        methods[method]["raw_records"] += 1
        for key in parsed_event_keys_for_raw(row):
            methods[method]["parsed_event_records"] += 1
            event_counts[method][key] += 1
            event_times[method][key].append(row_time(row))
            if key in truth_keys:
                cross_source_methods[key].add(method)

    for method, stats in methods.items():
        keys = set(event_counts[method])
        matched = keys & truth_keys
        missed = truth_keys - matched
        duplicate_count = sum(max(0, count - 1) for count in event_counts[method].values())
        latencies: list[float] = []
        for key in matched:
            seen_time = first_capture_time(event_times[method].get(key, []), tz_name)
            seconds = latency_seconds(truth_by_key[key].get("source_time"), seen_time, tz_name)
            if seconds is None:
                continue
            if seconds < 0 and seconds >= -300:
                seconds = 0.0
            latencies.append(seconds)
        stats.update(
            {
                "unique_event_records": len(keys),
                "duplicate_event_records": duplicate_count,
                "matched_truth_events": len(matched),
                "missed_truth_events": len(missed),
                "capture_rate": round(len(matched) / len(truth_keys), 4) if truth_keys else 0.0,
                "latency": summarize_latency(latencies),
                "sample_missed_event_keys": sorted(missed)[:5],
            }
        )

    scorecard = {
        "truth_event_count": len(truth_keys),
        "methods": methods,
        "cross_source_duplicate_truth_events": sum(1 for method_set in cross_source_methods.values() if len(method_set) > 1),
    }
    scorecard["recommendation"] = capture_recommendation(methods, len(truth_keys))
    return scorecard


def group_by_key(rows: list[dict[str, Any]], key_func) -> dict[str, list[dict[str, Any]]]:
    grouped: dict[str, list[dict[str, Any]]] = defaultdict(list)
    for row in rows:
        grouped[key_func(row)].append(row)
    return grouped


def parsed_source_keys(rows: list[dict[str, Any]]) -> list[str]:
    keys: list[str] = []
    seen: set[str] = set()
    for row in rows:
        key = str(row.get("source_dedupe_key") or "")
        if key and key not in seen:
            seen.add(key)
            keys.append(key)
    return keys


def raw_matches_for_truth_buy(
    event: dict[str, Any],
    raw_rows: list[dict[str, Any]],
) -> list[dict[str, Any]]:
    buy_key = event_key(event)
    raw_matches: list[dict[str, Any]] = []
    for raw in raw_rows:
        for parsed in parsed_for_body(
            str(raw.get("raw_text") or raw_text_from_record(raw)),
            str(raw.get("notification_timestamp") or raw.get("captured_at") or ""),
            str((raw.get("raw") or {}).get("source") or raw.get("subtitle") or raw.get("source_app") or ""),
        ):
            if str(parsed.get("side") or "") != "buy":
                continue
            if parsed_buy_key(parsed) == buy_key:
                raw_matches.append(raw)
                break
    return raw_matches


def truth_buy_pipeline_evidence(
    event: dict[str, Any],
    raw_rows: list[dict[str, Any]],
    rejected_rows: list[dict[str, Any]],
) -> dict[str, Any]:
    raw_matches = raw_matches_for_truth_buy(event, raw_rows)
    rejected_matches: list[dict[str, Any]] = []
    raw_keys = {
        str(row.get("dedupe_key") or row.get("source_dedupe_key") or "")
        for row in raw_matches
        if row.get("dedupe_key") or row.get("source_dedupe_key")
    }
    for rejected in rejected_rows:
        key = str(rejected.get("source_dedupe_key") or "")
        if key and key in raw_keys:
            rejected_matches.append(rejected)
    return {
        "raw_matches": raw_matches,
        "rejected_matches": rejected_matches,
        "raw_match_count": len(raw_matches),
        "rejected_match_count": len(rejected_matches),
    }


def alert_source_key(row: dict[str, Any]) -> str:
    alert = row.get("alert")
    if isinstance(alert, dict) and alert.get("source_dedupe_key"):
        return str(alert.get("source_dedupe_key"))
    return str(row.get("source_dedupe_key") or "")


def unique_rows_by_key(rows: list[dict[str, Any]], key_name: str) -> list[dict[str, Any]]:
    seen: set[str] = set()
    unique: list[dict[str, Any]] = []
    for row in rows:
        key = str(row.get(key_name) or "")
        if key and key in seen:
            continue
        if key:
            seen.add(key)
        unique.append(row)
    return unique


def issue(severity: str, code: str, message: str, evidence: dict[str, Any], recommendation: str) -> dict[str, Any]:
    return {
        "severity": severity,
        "code": code,
        "message": message,
        "evidence": evidence,
        "recommendation": recommendation,
    }


def browser_foreground_recovery_evidence(latest_health: dict[str, Any]) -> dict[str, Any] | None:
    if not latest_health:
        return None
    errors = latest_health.get("errors") or []
    timeout_errors = [row for row in errors if "timed out" in str(row.get("reason") or "").lower()]
    if len(timeout_errors) < 2:
        return None
    return {
        "recorded_at": latest_health.get("recorded_at"),
        "status": latest_health.get("status"),
        "timeout_count": len(timeout_errors),
        "channel_count": len(latest_health.get("channels") or []),
        "timeout_channels": [row.get("channel_id") for row in timeout_errors],
        "errors": timeout_errors,
    }


def first_time(rows: list[dict[str, Any]]) -> str:
    values = [row_time(row) for row in rows if row_time(row)]
    return min(values) if values else ""


def first_row_by_time(rows: list[dict[str, Any]], tz_name: str) -> dict[str, Any] | None:
    valid: list[tuple[dt.datetime, dict[str, Any]]] = []
    fallback: list[dict[str, Any]] = []
    for row in rows:
        value = row_time(row)
        if not value:
            continue
        parsed = parse_datetime(value, tz_name)
        if parsed is None:
            fallback.append(row)
        else:
            valid.append((parsed, row))
    if valid:
        return min(valid, key=lambda item: item[0])[1]
    return min(fallback, key=row_time) if fallback else None


def latency_seconds(start: Any, end: Any, tz_name: str) -> float | None:
    start_dt = parse_datetime(start, tz_name)
    end_dt = parse_datetime(end, tz_name)
    if start_dt is None or end_dt is None:
        return None
    return (end_dt - start_dt).total_seconds()


def normalized_latency_seconds(start: Any, end: Any, tz_name: str) -> float | None:
    seconds = latency_seconds(start, end, tz_name)
    if seconds is not None and seconds < 0 and seconds >= -300:
        return 0.0
    return seconds


def rounded_seconds(value: float | None) -> float | None:
    return round(value, 1) if value is not None else None


def first_raw_capture_match(raw_matches: list[dict[str, Any]], tz_name: str) -> dict[str, Any] | None:
    valid: list[tuple[dt.datetime, dict[str, Any]]] = []
    fallback: list[dict[str, Any]] = []
    for row in raw_matches:
        captured_at = str(row.get("captured_at") or "")
        if not captured_at:
            continue
        parsed = parse_datetime(captured_at, tz_name)
        if parsed is None:
            fallback.append(row)
        else:
            valid.append((parsed, row))
    if valid:
        return min(valid, key=lambda item: item[0])[1]
    return min(fallback, key=lambda row: str(row.get("captured_at") or "")) if fallback else None


def source_label_for_raw(row: dict[str, Any]) -> str:
    raw_payload = row.get("raw") if isinstance(row.get("raw"), dict) else {}
    return str(raw_payload.get("source") or row.get("subtitle") or row.get("source_app") or "")


def contract_identity(row: dict[str, Any]) -> dict[str, Any]:
    fields = option_fields(row)
    return {
        "ticker": fields.get("ticker"),
        "expiration_date": fields.get("expiration_date"),
        "strike_price": fields.get("strike_price"),
        "option_type": option_side(fields.get("option_type")),
        "contract_key": contract_key(row),
    }


def late_order_latency_evidence(
    event: dict[str, Any],
    order: dict[str, Any],
    raw_matches: list[dict[str, Any]],
    parsed_keys: list[str],
    tz_name: str,
) -> dict[str, Any]:
    source_time = str(event.get("source_time") or "")
    order_time = row_time(order)
    raw_match = first_raw_capture_match(raw_matches, tz_name)
    raw_captured_at = str((raw_match or {}).get("captured_at") or "")
    total_seconds = normalized_latency_seconds(source_time, order_time, tz_name)
    source_to_capture_seconds = (
        normalized_latency_seconds(source_time, raw_captured_at, tz_name)
        if raw_captured_at
        else None
    )
    capture_to_order_seconds = (
        normalized_latency_seconds(raw_captured_at, order_time, tz_name)
        if raw_captured_at
        else None
    )
    raw_dedupe_keys = sorted(
        {
            str(row.get("dedupe_key") or "")
            for row in raw_matches
            if row.get("dedupe_key")
        }
    )
    raw_source_dedupe_keys = sorted(
        {
            str(row.get("source_dedupe_key") or "")
            for row in raw_matches
            if row.get("source_dedupe_key")
        }
    )
    raw_sources = sorted({source_label_for_raw(row) for row in raw_matches if source_label_for_raw(row)})
    return {
        "event": event,
        "contract": contract_identity(event),
        "event_key": event.get("event_key") or event_key(event),
        "contract_key": event.get("contract_key") or contract_key(event),
        "source": event.get("source"),
        "channel_id": event.get("channel_id"),
        "message_key": event.get("message_key"),
        "source_time": source_time or None,
        "raw_captured_at": raw_captured_at or None,
        "order_time": order_time or None,
        "source_to_capture_seconds": rounded_seconds(source_to_capture_seconds),
        "capture_to_order_seconds": rounded_seconds(capture_to_order_seconds),
        "total_latency_seconds": rounded_seconds(total_seconds),
        "latency_seconds": rounded_seconds(total_seconds),
        "raw_match_count": len(raw_matches),
        "raw_dedupe_keys": raw_dedupe_keys,
        "raw_source_dedupe_keys": raw_source_dedupe_keys,
        "raw_sources": raw_sources,
        "selected_raw_dedupe_key": (raw_match or {}).get("dedupe_key"),
        "selected_raw_source_dedupe_key": (raw_match or {}).get("source_dedupe_key"),
        "parsed_source_keys": parsed_keys,
        "order_source_dedupe_key": order.get("source_dedupe_key"),
        "order_position_id": order.get("position_id"),
        "order_id": order_response_id(order),
        "order_status": order.get("status"),
        "order_action": order.get("action"),
        "order_side": order_payload_side(order),
    }


def late_order_issue(
    event: dict[str, Any],
    order: dict[str, Any],
    raw_matches: list[dict[str, Any]],
    parsed_keys: list[str],
    tz_name: str,
) -> dict[str, Any] | None:
    evidence = late_order_latency_evidence(event, order, raw_matches, parsed_keys, tz_name)
    total_seconds = safe_float(evidence.get("total_latency_seconds"))
    if total_seconds is None or total_seconds <= SLOW_ORDER_SUBMISSION_SECONDS:
        return None
    severity = "warning" if total_seconds <= 300 else "critical"
    source_to_capture_seconds = safe_float(evidence.get("source_to_capture_seconds"))
    capture_to_order_seconds = safe_float(evidence.get("capture_to_order_seconds"))
    if (
        source_to_capture_seconds is not None
        and capture_to_order_seconds is not None
        and source_to_capture_seconds >= STARTUP_BACKFILL_SOURCE_CAPTURE_SECONDS
        and capture_to_order_seconds <= STARTUP_BACKFILL_CAPTURE_ORDER_SECONDS
    ):
        return issue(
            severity,
            "machine_unavailable_startup_backfill",
            "Paper order was late because the raw alert was captured long after Steve's source message, then ordered quickly after capture.",
            evidence,
            "Treat this as machine availability/startup backfill latency; add or verify premarket readiness instead of tuning parser or broker submission speed.",
        )
    return issue(
        severity,
        "slow_order_submission",
        "Paper order was submitted too long after Steve's alert.",
        evidence,
        "Measure live capture, enrichment, and broker-submit latency; keep max-buy-latency and price-buffer rules focused on true live delays.",
    )


def latency_classification_summary(issues: list[dict[str, Any]]) -> dict[str, Any]:
    machine_count = sum(1 for item in issues if item.get("code") == "machine_unavailable_startup_backfill")
    slow_count = sum(1 for item in issues if item.get("code") == "slow_order_submission")
    return {
        "machine_unavailable_startup_backfill": machine_count,
        "slow_order_submission": slow_count,
    }


def find_orders_for_position(position: dict[str, Any], orders: list[dict[str, Any]]) -> list[dict[str, Any]]:
    position_id = position.get("position_id")
    return [row for row in orders if row.get("position_id") == position_id]


def summarize_daily_pl(day: str) -> dict[str, Any]:
    reports = [row for row in read_jsonl(DAILY_PL_FILE) if str(row.get("day") or (row.get("summary") or {}).get("day") or "") == day]
    return reports[-1].get("summary") if reports and isinstance(reports[-1].get("summary"), dict) else {}


def summarize_all_time_pl() -> dict[str, Any]:
    positions = [row for row in read_jsonl(HUMAN_POSITIONS_FILE) if not is_synthetic_test_row(row)]
    exits = [row for row in read_jsonl(HUMAN_EXITS_FILE) if not is_synthetic_test_row(row)]
    realized_pnl = sum(float(row.get("pnl_dollars") or 0) for row in exits)
    open_positions = 0
    for position in positions:
        position_id = position.get("position_id")
        contracts = int(position.get("contracts") or 0)
        closed = sum(int(row.get("contracts") or 0) for row in exits if row.get("position_id") == position_id)
        if contracts - closed > 0:
            open_positions += 1
    return {
        "realized_pnl": realized_pnl,
        "open_pnl": 0.0,
        "total_pnl": realized_pnl,
        "open_positions": open_positions,
        "marked_open_positions": 0,
        "total_exits": len(exits),
        "contracts_closed": sum(int(row.get("contracts") or 0) for row in exits),
    }


def broker_report_time(row: dict[str, Any]) -> str:
    return str(row.get("recorded_at") or row.get("filled_at") or row.get("submitted_at") or "")


def broker_fill_price(row: dict[str, Any]) -> float | None:
    return safe_float(row.get("filled_avg_price"))


def broker_fill_qty(row: dict[str, Any]) -> int:
    quantity = safe_int(row.get("filled_qty"))
    if quantity is None:
        quantity = safe_int(row.get("qty"))
    return max(0, quantity or 0)


def summarize_broker_fill_pl(day: str, broker_reports: list[dict[str, Any]], all_time: bool = False, tz_name: str = "America/Detroit") -> dict[str, Any]:
    reports = [
        row
        for row in broker_reports
        if str(row.get("broker_status") or "").lower() == "filled"
    ]
    entry_fills: dict[str, dict[str, Any]] = {}
    sell_fills: list[dict[str, Any]] = []
    for report in sorted(reports, key=broker_report_time):
        position_id = str(report.get("position_id") or "")
        if not position_id:
            continue
        side = str(report.get("side") or "").lower()
        if side == "buy" and position_id not in entry_fills:
            entry_fills[position_id] = report
        elif side == "sell":
            sell_fills.append(report)

    realized_pnl = 0.0
    matched_exit_fills = 0
    contracts_closed = 0
    for sell in sell_fills:
        sell_day = date_key(broker_report_time(sell), tz_name)
        if not all_time and sell_day != day:
            continue
        position_id = str(sell.get("position_id") or "")
        entry = entry_fills.get(position_id)
        entry_price = broker_fill_price(entry or {})
        exit_price = broker_fill_price(sell)
        quantity = broker_fill_qty(sell)
        if quantity <= 0:
            continue
        if entry_price is not None and exit_price is not None:
            realized_pnl += (exit_price - entry_price) * quantity * 100
        matched_exit_fills += 1
        contracts_closed += quantity

    return {
        "event_type": "broker_fill_pl_summary",
        "period": "all_time" if all_time else "day",
        "day": day,
        "generated_at": now_iso(),
        "basis": "alpaca_paper_filled_prices",
        "realized_pnl": realized_pnl,
        "open_pnl": 0.0,
        "total_pnl": realized_pnl,
        "matched_exit_fills": matched_exit_fills,
        "contracts_closed": contracts_closed,
        "open_positions": 0,
        "open_contracts": 0,
        "marked_open_positions": 0,
        "unfilled_local_positions": 0,
        "exit_details": [],
    }


def broker_fill_notional(row: dict[str, Any]) -> float | None:
    price = broker_fill_price(row)
    qty = broker_fill_qty(row)
    if price is None or qty <= 0:
        return None
    return price * qty * 100


def broker_fill_activity_summary(
    day: str,
    broker_reports: list[dict[str, Any]],
    all_broker_reports: list[dict[str, Any]],
    tz_name: str = "America/Detroit",
) -> dict[str, Any]:
    filled_reports = [row for row in broker_reports if str(row.get("broker_status") or "").lower() == "filled"]
    day_fills = [
        row
        for row in filled_reports
        if date_key(row.get("filled_at") or broker_report_time(row), tz_name) == day
    ]
    buy_fills = [row for row in day_fills if str(row.get("side") or "").lower() == "buy"]
    sell_fills = [row for row in day_fills if str(row.get("side") or "").lower() == "sell"]

    all_entry_fills: dict[str, dict[str, Any]] = {}
    for row in sorted(all_broker_reports, key=broker_report_time):
        if str(row.get("broker_status") or "").lower() != "filled" or str(row.get("side") or "").lower() != "buy":
            continue
        position_id = str(row.get("position_id") or "")
        if position_id and position_id not in all_entry_fills:
            all_entry_fills[position_id] = row

    invested = sum(broker_fill_notional(row) or 0.0 for row in buy_fills)
    proceeds = sum(broker_fill_notional(row) or 0.0 for row in sell_fills)
    realized_pnl = 0.0
    realized_rows: list[dict[str, Any]] = []
    for sell in sell_fills:
        entry = all_entry_fills.get(str(sell.get("position_id") or ""))
        entry_price = broker_fill_price(entry or {})
        exit_price = broker_fill_price(sell)
        qty = broker_fill_qty(sell)
        pnl_dollars = None
        pnl_pct = None
        if entry_price is not None and exit_price is not None and qty > 0:
            pnl_dollars = (exit_price - entry_price) * qty * 100
            realized_pnl += pnl_dollars
            if entry_price > 0:
                pnl_pct = ((exit_price - entry_price) / entry_price) * 100
        realized_rows.append(
            {
                "label": sell.get("label") or sell.get("contract_symbol") or "OPTION",
                "qty": qty,
                "pnl_dollars": pnl_dollars,
                "pnl_pct": pnl_pct,
            }
        )
    ranked = [row for row in realized_rows if row.get("pnl_dollars") is not None]
    ranked.sort(key=lambda row: float(row.get("pnl_dollars") or 0))
    terminal_not_filled = [
        row
        for row in broker_reports
        if str(row.get("broker_status") or "").lower() in {"expired", "rejected", "failed", "canceled"}
    ]
    return {
        "filled_buys": len(buy_fills),
        "filled_sells": len(sell_fills),
        "contracts_bought": sum(broker_fill_qty(row) for row in buy_fills),
        "contracts_sold": sum(broker_fill_qty(row) for row in sell_fills),
        "invested": invested,
        "proceeds": proceeds,
        "realized_pnl": realized_pnl,
        "best": ranked[-1] if ranked else {},
        "worst": ranked[0] if ranked else {},
        "unfilled_terminal": len(terminal_not_filled),
    }


def order_payload_side(row: dict[str, Any]) -> str:
    return str((row.get("payload") or {}).get("side") or row.get("side") or "").lower()


def order_response_id(row: dict[str, Any]) -> str:
    return str((row.get("response") or {}).get("id") or row.get("order_id") or "")


def classify_broker_reason(reason: str) -> str:
    lowered = reason.lower()
    if "client_order_id must be unique" in lowered:
        return "duplicate_broker_order"
    if "options_market_closed" in lowered or "options market orders are only allowed during market hours" in lowered:
        return "broker_market_closed"
    if "uncovered option contracts" in lowered:
        return "broker_position_reconciliation_failed"
    if "asset" in lowered and "not found" in lowered:
        return "broker_contract_not_found"
    if "paper_order_submission_disabled" in lowered:
        return "paper_order_disabled"
    return "broker_error"


def broker_issue_recommendation(code: str) -> str:
    if code == "duplicate_broker_order":
        return "Prevent duplicate broker submits by reusing deterministic client_order_id and skipping already-audited order attempts."
    if code == "broker_market_closed":
        return "Skip option submits outside market hours and queue/manual-review exits for the next session open."
    if code == "broker_position_reconciliation_failed":
        return "Reconcile Alpaca option positions before exit submits and avoid uncovered close attempts."
    if code == "broker_contract_not_found":
        return "Validate option contract symbol construction (ticker/expiration/strike/type) and skip stale expirations before paper submit."
    if code == "paper_order_disabled":
        return "Paper order submission is disabled; keep dry-run mode and verify config before market open."
    return "Reconcile Alpaca open positions before broker exits and prevent duplicate client order submissions."


def storage_hygiene_issue(scorecard: dict[str, Any]) -> dict[str, Any] | None:
    recommendations = list(scorecard.get("recommendations") or [])
    tracked_bytes = int(scorecard.get("total_tracked_bytes") or 0)
    if not recommendations and tracked_bytes < 250 * 1024 * 1024:
        return None
    severity = "critical" if tracked_bytes >= 1024 * 1024 * 1024 else "warning"
    return issue(
        severity,
        "ledger_storage_hygiene",
        "Runtime ledgers are growing or contain compactable repeated telemetry.",
        {
            "total_tracked_bytes": tracked_bytes,
            "recommendations": recommendations,
            "largest_files": sorted(
                (
                    {"name": name, "bytes": int((item or {}).get("bytes") or 0)}
                    for name, item in (scorecard.get("files") or {}).items()
                ),
                key=lambda row: row["bytes"],
                reverse=True,
            )[:5],
        },
        "Keep trading fact ledgers, but archive-first compact quote snapshots, health history, and heartbeat telemetry after nightly review.",
    )


def premarket_readiness_issue(reports: list[dict[str, Any]]) -> dict[str, Any] | None:
    if not reports:
        return None
    latest = reports[-1]
    status = str(latest.get("status") or "")
    if status == "ok":
        return None
    checks = [
        {
            "name": row.get("name"),
            "status": row.get("status"),
            "summary": row.get("summary"),
            "recommendation": row.get("recommendation"),
        }
        for row in (latest.get("checks") or [])
        if row.get("status") != "ok"
    ]
    return issue(
        "critical" if status == "failed" else "warning",
        "premarket_readiness_failed",
        "Premarket readiness check did not pass before the session.",
        {
            "status": status,
            "generated_at": latest.get("generated_at"),
            "checks": checks[:8],
            "recommendations": list(latest.get("recommendations") or [])[:8],
        },
        "Run `python3 scripts/premarket_readiness.py --print-json` before market open and resolve failed checks before relying on live paper entries.",
    )


def recursive_improvement_plan(issues: list[dict[str, Any]], report: dict[str, Any]) -> list[dict[str, Any]]:
    auto_fixable_codes = {
        "entry_price_worse_than_alert",
        "slow_order_submission",
        "machine_unavailable_startup_backfill",
        "premarket_readiness_failed",
        "local_position_without_broker_fill",
        "submitted_broker_order_unresolved",
        "local_pnl_differs_from_broker_fills",
        "broker_market_closed",
        "truth_buy_not_parsed",
        "truth_exit_not_recorded",
        "duplicate_shadow_position_all_time",
        "duplicate_paper_position_all_time",
        "duplicate_steve_exit_all_time",
        "browser_foreground_recovery_needed",
        "duplicate_paper_position",
        "broker_terminal_not_filled",
        "ledger_repeated_health_history",
        "ledger_storage_hygiene",
    }
    plan: list[dict[str, Any]] = []
    seen: set[str] = set()
    for item in issues:
        code = str(item.get("code") or "")
        if code in seen:
            continue
        seen.add(code)
        plan.append(
            {
                "code": code,
                "severity": item.get("severity"),
                "status": "open",
                "confidence": "medium",
                "priority": "high" if item.get("severity") == "critical" else "medium",
                "area": "pipeline",
                "auto_fixable": code in auto_fixable_codes,
                "observed_evidence": item.get("evidence") or {},
                "validation": [
                    "python3 scripts/test_pipeline.py",
                    "python3 scripts/test_full_pipeline.py",
                    "python3 scripts/test_steve_options_mvp.py",
                    "python3 -m py_compile scripts/*.py",
                ],
                "rollback_criteria": [
                    "tests fail",
                    "new duplicate or missed Steve alert appears in the next real session",
                    "paper-only guard or Alpaca paper endpoint guard is weakened",
                ],
            }
        )
    return plan[:8]


def review_day(day: str, refresh_browser: bool = False, tz_name: str = "America/Detroit", max_age_minutes: float = 720, timeout: int = 30, retries: int = 2) -> dict[str, Any]:
    truth_events = truth_events_from_browser_ledger(day, tz_name)
    browser_refresh: list[dict[str, Any]] = []
    if refresh_browser:
        refreshed, browser_refresh = truth_events_from_chrome(day, tz_name, max_age_minutes, timeout, retries)
        truth_events = dedupe_truth_events(truth_events + refreshed)

    parsed_rows = rows_for_day(PARSED_FILE, day, tz_name)
    parsed_buys = [row for row in parsed_rows if row.get("instrument_type") == "option" and row.get("side") == "buy"]
    parsed_exits = [row for row in parsed_rows if row.get("instrument_type") == "option" and row.get("side") == "exit"]
    positions = rows_for_day(HUMAN_POSITIONS_FILE, day, tz_name)
    exits = rows_for_day(HUMAN_EXITS_FILE, day, tz_name)
    steve_exits = rows_for_day(STEVE_EXITS_FILE, day, tz_name)
    orders = rows_for_day(ORDERS_FILE, day, tz_name)
    broker_reports = rows_for_day(BROKER_STATUS_FILE, day, tz_name)
    raw_rows = rows_for_day(RAW_FILE, day, tz_name)
    rejected = rows_for_day(REJECTED_FILE, day, tz_name)
    approval_cards = rows_for_day(APPROVAL_CARDS_FILE, day, tz_name)
    filtered_counts = {
        "parsed_rows": sum(1 for row in parsed_rows if is_synthetic_test_row(row)),
        "positions": sum(1 for row in positions if is_synthetic_test_row(row)),
        "exits": sum(1 for row in exits if is_synthetic_test_row(row)),
        "steve_exits": sum(1 for row in steve_exits if is_synthetic_test_row(row)),
        "orders": sum(1 for row in orders if is_synthetic_test_row(row)),
        "broker_reports": sum(1 for row in broker_reports if is_synthetic_test_row(row)),
        "raw_rows": sum(1 for row in raw_rows if is_synthetic_test_row(row)),
        "rejected": sum(1 for row in rejected if is_synthetic_test_row(row)),
        "approval_cards": sum(1 for row in approval_cards if is_synthetic_test_row(row)),
    }
    parsed_rows = [row for row in parsed_rows if not is_synthetic_test_row(row)]
    parsed_buys = [row for row in parsed_rows if row.get("instrument_type") == "option" and row.get("side") == "buy"]
    parsed_exits = [row for row in parsed_rows if row.get("instrument_type") == "option" and row.get("side") == "exit"]
    positions = [row for row in positions if not is_synthetic_test_row(row)]
    exits = [row for row in exits if not is_synthetic_test_row(row)]
    steve_exits = [row for row in steve_exits if not is_synthetic_test_row(row)]
    orders = [row for row in orders if not is_synthetic_test_row(row)]
    broker_reports = [row for row in broker_reports if not is_synthetic_test_row(row)]
    raw_rows = [row for row in raw_rows if not is_synthetic_test_row(row)]
    rejected = [row for row in rejected if not is_synthetic_test_row(row)]
    approval_cards = [row for row in approval_cards if not is_synthetic_test_row(row)]
    auto_buy_reports = rows_for_day(AUTO_BUY_REPORTS_FILE, day, tz_name)
    filtered_counts["auto_buy_reports"] = sum(1 for row in auto_buy_reports if is_synthetic_test_row(row))
    auto_buy_reports = [row for row in auto_buy_reports if not is_synthetic_test_row(row)]
    health_checks = rows_for_day(PIPELINE_HEALTH_FILE, day, tz_name)
    premarket_readiness_reports = rows_for_day(PREMARKET_READINESS_FILE, day, tz_name)
    capture_scorecard = capture_method_scorecard(truth_events, raw_rows, tz_name)

    buys_by_key = group_by_key(parsed_buys, parsed_buy_key)
    approval_cards_by_source = group_by_key(approval_cards, alert_source_key)
    auto_buy_reports_by_source = group_by_key(auto_buy_reports, alert_source_key)
    exits_by_contract = group_by_key(parsed_exits, contract_key)
    steve_exits_by_contract = group_by_key(steve_exits, contract_key)
    positions_by_contract = group_by_key(positions, contract_key)
    positions_by_buy_key = group_by_key(positions, lambda row: "|".join(["buy", contract_key(row), price_key(row.get("entry_price")), str(row.get("contracts") or "")]))
    orders_by_contract = group_by_key(orders, contract_key)
    broker_by_contract = group_by_key(broker_reports, contract_key)
    issues: list[dict[str, Any]] = []
    matched_buys = 0
    paper_entries = 0
    broker_filled_buys = 0
    from option_validation import SHADOW_POSITIONS_FILE, compute_steve_alert_pl_summary

    all_shadow_positions = [row for row in read_jsonl(SHADOW_POSITIONS_FILE) if not is_synthetic_test_row(row)]
    all_human_positions = [row for row in read_jsonl(HUMAN_POSITIONS_FILE) if not is_synthetic_test_row(row)]
    all_steve_exits = [row for row in read_jsonl(STEVE_EXITS_FILE) if not is_synthetic_test_row(row)]
    ledger_duplicate_scorecard = {
        "shadow_positions": build_ledger_duplicate_scorecard(
            all_shadow_positions,
            fallback_key_func=position_duplicate_key,
            id_key="position_id",
            canonical_keys=("canonical_entry_key",),
        ),
        "human_positions": build_ledger_duplicate_scorecard(
            all_human_positions,
            fallback_key_func=position_duplicate_key,
            id_key="position_id",
            canonical_keys=("canonical_entry_key",),
        ),
        "steve_exits": build_ledger_duplicate_scorecard(
            all_steve_exits,
            fallback_key_func=steve_exit_duplicate_key,
            id_key="exit_id",
            canonical_keys=("canonical_exit_key", "canonical_raw_exit_key"),
        ),
    }
    steve_alert_pl = compute_steve_alert_pl_summary(day, positions=all_shadow_positions, exits=all_steve_exits, snapshots=[])
    all_time_steve_alert_pl = compute_steve_alert_pl_summary(day, all_time=True, positions=all_shadow_positions, exits=all_steve_exits, snapshots=[])
    broker_fill_pl = summarize_broker_fill_pl(day, broker_reports, tz_name=tz_name)
    all_broker_status_reports = [row for row in read_jsonl(BROKER_STATUS_FILE) if not is_synthetic_test_row(row)]
    all_time_broker_fill_pl = summarize_broker_fill_pl(day, all_broker_status_reports, all_time=True, tz_name=tz_name)
    executive_activity = broker_fill_activity_summary(day, broker_reports, all_broker_status_reports, tz_name=tz_name)

    truth_buys = [event for event in truth_events if event.get("kind") == "buy"]
    truth_exits = [event for event in truth_events if event.get("kind") == "exit"]
    truth_adds = [event for event in truth_events if event.get("kind") == "add"]
    truth_context_stops = [event for event in truth_events if event.get("kind") == "context_stop"]

    for event in truth_buys:
        buy_key = event_key(event)
        parsed_matches = buys_by_key.get(buy_key, [])
        if parsed_matches:
            matched_buys += 1
        else:
            pipeline_evidence = truth_buy_pipeline_evidence(event, raw_rows, rejected)
            if pipeline_evidence["raw_matches"]:
                issues.append(
                    issue(
                        "critical",
                        "truth_buy_not_parsed",
                        "Steve buy alert exists in Discord truth and in raw capture, but no matching parsed buy exists.",
                        {"event": event, **pipeline_evidence},
                        "Fix parser or raw-to-parsed normalization so this captured Steve buy reaches parsed_alerts before next session.",
                    )
                )
            else:
                issues.append(
                    issue(
                        "critical",
                        "truth_buy_not_captured",
                        "Steve buy alert exists in Discord truth but never reached raw_notifications.",
                        {"event": event, **pipeline_evidence},
                        "Fix browser/notification capture reliability before market open; this miss happened before parser routing.",
                    )
                )
            continue
        contract_positions = positions_by_contract.get(event["contract_key"], [])
        parsed_keys = parsed_source_keys(parsed_matches)
        related_cards = unique_rows_by_key(
            [row for key in parsed_keys for row in approval_cards_by_source.get(key, [])],
            "approval_id",
        )
        related_auto_buys = unique_rows_by_key(
            [row for key in parsed_keys for row in auto_buy_reports_by_source.get(key, [])],
            "auto_paper_id",
        )
        if contract_positions:
            paper_entries += 1
        elif related_auto_buys and any(
            str(row.get("broker_status") or "").lower() in {"submitted", "filled"} for row in related_auto_buys
        ):
            issues.append(
                issue(
                    "critical",
                    "auto_buy_missing_local_position",
                    "Auto paper buy was accepted by the broker path for a parsed Steve buy, but no local paper position was recorded.",
                    {
                        "event": event,
                        "parsed_keys": parsed_keys,
                        "auto_buy_ids": [row.get("auto_paper_id") for row in related_auto_buys],
                        "broker_statuses": [row.get("broker_status") for row in related_auto_buys],
                    },
                    "Keep auto-buy and local position creation atomic so paper entries never disappear after broker submission.",
                )
            )
        elif related_auto_buys:
            pass
        elif related_cards:
            latest_card = max(related_cards, key=row_time)
            alert_payload = latest_card.get("alert") if isinstance(latest_card.get("alert"), dict) else {}
            guard = alert_payload.get("auto_entry_guard") if isinstance(alert_payload, dict) else {}
            approval_status = str(latest_card.get("status") or "")
            if guard:
                delivery_failed = approval_status in {"telegram_disabled", "send_failed"}
                issues.append(
                    issue(
                        "critical" if delivery_failed else "warning",
                        "parsed_buy_held_by_auto_entry_guard",
                        "Parsed Steve buy was held by the auto-entry guard and routed to Telegram approval instead of immediate paper entry.",
                        {
                            "event": event,
                            "parsed_keys": parsed_keys,
                            "approval_id": latest_card.get("approval_id"),
                            "approval_status": approval_status or "unknown",
                            "guard": guard,
                        },
                        (
                            "Fix Telegram approval delivery before market open so guarded buys are still actionable."
                            if delivery_failed
                            else "Keep the guard explicit in nightly output; only loosen slippage or quote-age thresholds after comparing missed fills against bad chases."
                        ),
                    )
                )
            else:
                issues.append(
                    issue(
                        "critical" if approval_status in {"telegram_disabled", "send_failed"} else "warning",
                        "parsed_buy_waiting_on_approval",
                        "Parsed Steve buy was routed to Telegram approval instead of immediate paper entry.",
                        {
                            "event": event,
                            "parsed_keys": parsed_keys,
                            "approval_id": latest_card.get("approval_id"),
                            "approval_status": approval_status or "unknown",
                        },
                        (
                            "Restore Telegram approval delivery before market open."
                            if approval_status in {"telegram_disabled", "send_failed"}
                            else "Review whether this approval path is still justified for unambiguous Steve buys."
                        ),
                    )
                )
        else:
            issues.append(
                issue(
                    "critical",
                    "parsed_buy_not_paper_traded",
                    "Parsed Steve buy did not create a local paper position.",
                    {"event": event, "parsed_keys": parsed_keys},
                    "Route every non-ambiguous Steve option buy to immediate paper entry; only ask Telegram for genuinely ambiguous context.",
                )
            )
        tags = {str(tag).lower() for tag in event.get("tags") or []}
        if "hedge" in tags and not contract_positions:
            issues.append(
                issue(
                    "critical",
                    "hedge_policy_blocks_immediate_following",
                    "Hedge alert was parsed but current policy may require approval instead of immediate paper entry.",
                    {"event": event},
                    "For paper validation, auto-enter hedges too unless the alert lacks contract details or conflicts with account constraints.",
                )
            )
        related_orders = orders_by_contract.get(event["contract_key"], [])
        filled_buys = [
            row
            for row in broker_by_contract.get(event["contract_key"], [])
            if str(row.get("side")).lower() == "buy" and str(row.get("broker_status")).lower() == "filled"
        ]
        if filled_buys:
            broker_filled_buys += 1
        if related_orders:
            first_order = first_row_by_time(related_orders, tz_name)
            if first_order:
                latency_issue = late_order_issue(
                    event,
                    first_order,
                    raw_matches_for_truth_buy(event, raw_rows),
                    parsed_keys,
                    tz_name,
                )
                if latency_issue:
                    issues.append(latency_issue)
        for position in contract_positions:
            alert_price = safe_float(event.get("entry_price"))
            entry_price = safe_float(position.get("entry_price"))
            if alert_price and entry_price and entry_price > alert_price * 1.05:
                issues.append(
                    issue(
                        "warning",
                        "entry_price_worse_than_alert",
                        "Paper entry price was more than 5% worse than Steve's alert price.",
                        {"event": event, "position_id": position.get("position_id"), "entry_price": entry_price},
                        "Add configurable max slippage/buffer behavior instead of blindly crossing stale or wide quotes.",
                    )
                )

    for event in truth_adds:
        if not positions_by_contract.get(event["contract_key"]):
            issues.append(
                issue(
                    "critical",
                    "scale_in_not_supported",
                    "Steve add/scale-in message was visible but did not create a paper add.",
                    {"event": event},
                    "Implement add-alert parsing and position scaling with the same contract context as the parent alert.",
                )
            )

    for event in truth_context_stops:
        matching_exits = [row for row in exits if contract_key(row) == event["contract_key"]]
        if not matching_exits:
            issues.append(
                issue(
                    "critical",
                    "contextual_stop_not_executed",
                    "Steve contextual stopped-out message was visible but no matching local paper exit was recorded.",
                    {"event": event},
                    "Use browser message context to resolve ticker/contract for bare 'stopped out' messages and close remaining paper contracts.",
                )
            )

    for event in truth_exits:
        if not exits_by_contract.get(event["contract_key"]) and not steve_exits_by_contract.get(event["contract_key"]):
            issues.append(
                issue(
                    "critical",
                    "truth_exit_not_recorded",
                    "Steve exit alert exists in Discord truth but no matching Steve exit was recorded.",
                    {"event": event},
                    "Fix exit parsing/matching and make Steve closes cumulative against local paper exits.",
                )
            )

    for key, grouped_positions in positions_by_buy_key.items():
        if len(grouped_positions) > 1:
            issues.append(
                issue(
                    "critical",
                    "duplicate_paper_position",
                    "Multiple local paper positions were opened for what appears to be the same Steve buy.",
                    {"buy_key": key, "position_ids": [row.get("position_id") for row in grouped_positions]},
                    "Canonicalize dedupe across browser and macOS notification sources using contract, price, contracts, channel, and message time.",
                )
            )

    for label, code, message, recommendation in (
        (
            "shadow_positions",
            "duplicate_shadow_position_all_time",
            "All-time shadow ledger contains duplicate Steve buy groups.",
            "Confirm canonical entry keys stopped growing duplicate shadow rows after the latest dedupe change.",
        ),
        (
            "human_positions",
            "duplicate_paper_position_all_time",
            "All-time human paper ledger contains duplicate Steve buy groups.",
            "Confirm canonical entry keys stopped growing duplicate human paper rows after the latest dedupe change.",
        ),
        (
            "steve_exits",
            "duplicate_steve_exit_all_time",
            "All-time Steve close ledger contains duplicate close groups.",
            "Confirm canonical close keys stopped growing duplicate close rows and cumulative exits count unique close events only.",
        ),
    ):
        stats = ledger_duplicate_scorecard.get(label) or {}
        if int(stats.get("duplicate_groups") or 0) > 0:
            issues.append(
                issue(
                    "critical" if label == "human_positions" else "warning",
                    code,
                    message,
                    {label: stats},
                    recommendation,
                )
            )
        elif int(stats.get("legacy_collision_groups") or 0) > 0:
            issues.append(
                issue(
                    "warning",
                    f"legacy_{code}",
                    message.replace("contains duplicate", "contains pre-canonical coarse-key collisions for"),
                    {label: stats},
                    "Treat these as historical cleanup candidates; only escalate again if canonical-key duplicates reappear.",
                )
            )

    for order in orders:
        reason = str(order.get("reason") or "")
        if order.get("status") == "blocked" or reason:
            code = classify_broker_reason(reason)
            issues.append(
                issue(
                    "warning" if code in {"paper_order_disabled", "broker_market_closed"} else "critical",
                    code,
                    "Broker/order audit recorded a blocked order.",
                    {"ticker": order.get("ticker"), "contract_symbol": order.get("contract_symbol"), "reason": reason, "payload": order.get("payload")},
                    broker_issue_recommendation(code),
                )
            )

    reported_order_ids = {str(report.get("order_id") or "") for report in broker_reports if report.get("order_id")}
    for order in orders:
        order_id = order_response_id(order)
        if order.get("status") != "submitted" or not order_id or order_id in reported_order_ids:
            continue
        side = order_payload_side(order)
        response_status = str((order.get("response") or {}).get("status") or "")
        issues.append(
            issue(
                "critical" if side == "buy" else "warning",
                "submitted_broker_order_unresolved",
                "Submitted broker order did not have a terminal fill/cancel/reject status by nightly review.",
                {
                    "ticker": order.get("ticker"),
                    "contract_symbol": order.get("contract_symbol"),
                    "order_id": order_id,
                    "side": side,
                    "response_status": response_status,
                    "recorded_at": order.get("recorded_at"),
                },
                "Continue broker polling for accepted/pending orders and keep unfilled entries out of active paper position P/L until filled.",
            )
        )

    for report in broker_reports:
        status = str(report.get("broker_status") or "").lower()
        if status in {"expired", "rejected", "failed", "canceled"}:
            submitted_at = parse_datetime(report.get("submitted_at"), tz_name)
            near_close = submitted_at is not None and submitted_at.hour == 15 and submitted_at.minute >= 55
            issues.append(
                issue(
                    "warning" if status in {"expired", "canceled"} else "critical",
                    "broker_terminal_not_filled",
                    "Broker order reached a terminal status without a fill.",
                    {"status": status, "contract_symbol": report.get("contract_symbol"), "submitted_at": report.get("submitted_at"), "near_close": near_close},
                    "Report expired/unfilled orders separately from real positions; consider no-new-entry cutoff near close.",
                )
            )

    local_realized_pnl = sum(float(row.get("pnl_dollars") or 0) for row in exits)
    broker_realized_pnl = safe_float(broker_fill_pl.get("realized_pnl")) or 0.0
    if int(broker_fill_pl.get("matched_exit_fills") or 0) > 0 and abs(local_realized_pnl - broker_realized_pnl) > 1.0:
        issues.append(
            issue(
                "warning",
                "local_pnl_differs_from_broker_fills",
                "Local policy P/L differs from broker-fill P/L for the same review day.",
                {
                    "local_realized_pnl": local_realized_pnl,
                    "broker_fill_realized_pnl": broker_realized_pnl,
                    "difference": broker_realized_pnl - local_realized_pnl,
                },
                "Keep local policy P/L, broker-fill P/L, and Steve-alert P/L as separate comparison ledgers.",
            )
        )

    browser_refresh_errors = [item for item in browser_refresh if str(item.get("status") or "") == "error"]
    refresh_browser_healthy = refresh_browser and bool(browser_refresh) and not browser_refresh_errors
    latest_browser_health = load_json(BROWSER_HEALTH_LATEST_FILE)
    foreground_recovery = browser_foreground_recovery_evidence(latest_browser_health)
    seen_health_issue_keys: set[str] = set()
    for check in health_checks:
        if check.get("status") != "ok":
            for item in check.get("issues") or []:
                issue_code = str(item.get("code") or "")
                if refresh_browser_healthy and not foreground_recovery and issue_code in {"browser_health_stale", "browser_capture_degraded"}:
                    # Nightly browser refresh succeeded; suppress stale browser-health alarms from earlier checks.
                    continue
                health_key = f"{item.get('stage')}:{item.get('code')}"
                if health_key in seen_health_issue_keys:
                    continue
                seen_health_issue_keys.add(health_key)
                issues.append(
                    issue(
                        str(item.get("severity") or "warning"),
                        f"health_{item.get('code')}",
                        str(item.get("message") or "Pipeline health issue."),
                        {"health_issue": item, "recorded_at": check.get("recorded_at")},
                        "Keep health failures in the nightly root-cause list and make stale monitors fail loudly before market open.",
                    )
                )
    if browser_refresh_errors:
        issues.append(
            issue(
                "critical",
                "browser_refresh_channel_error",
                "Nightly browser refresh could not read one or more configured Discord channels.",
                {"errors": browser_refresh_errors},
                "Recover the stuck Discord tabs/Chrome session and rerun browser watcher before market open.",
            )
        )
    elif foreground_recovery:
        issues.append(
            issue(
                "critical",
                "browser_foreground_recovery_needed",
                "Live browser watcher still shows repeated Chrome AppleScript timeouts across channels.",
                {
                    "latest_browser_health": foreground_recovery,
                    "nightly_browser_refresh_ok": refresh_browser_healthy,
                },
                "Restart browser capture with `scripts/run_browser_watcher_foreground.sh` before market open and verify Chrome channel reads in the foreground.",
            )
        )

    storage_hygiene = data_hygiene_scorecard(NIGHTLY_SUMMARY_FILE.parent)
    storage_issue = storage_hygiene_issue(storage_hygiene)
    if storage_issue:
        issues.append(storage_issue)
    readiness_issue = premarket_readiness_issue(premarket_readiness_reports)
    if readiness_issue:
        issues.append(readiness_issue)

    severities = defaultdict(int)
    for item in issues:
        severities[str(item.get("severity") or "warning")] += 1
    latency_summary = latency_classification_summary(issues)

    report = {
        "event_type": "nightly_pipeline_review",
        "day": day,
        "generated_at": now_iso(tz_name),
        "source_of_truth": "browser_discord",
        "truth_events": truth_events,
        "browser_refresh": browser_refresh,
        "counts": {
            "truth_buys": len(truth_buys),
            "truth_exits": len(truth_exits),
            "truth_adds": len(truth_adds),
            "truth_context_stops": len(truth_context_stops),
            "raw_records": len(raw_rows),
            "parsed_buys": len(parsed_buys),
            "parsed_exits": len(parsed_exits),
            "matched_buys": matched_buys,
            "paper_entries": paper_entries,
            "broker_filled_buys": broker_filled_buys,
            "local_exits": len(exits),
            "steve_exits": len(steve_exits),
            "approval_cards": len(approval_cards),
            "broker_status_reports": len(broker_reports),
            "rejected": len(rejected),
            "filtered_test_artifacts": sum(filtered_counts.values()),
            "machine_availability_findings": latency_summary["machine_unavailable_startup_backfill"],
            "slow_order_submission_findings": latency_summary["slow_order_submission"],
            "premarket_readiness_reports": len(premarket_readiness_reports),
        },
        "premarket_readiness": premarket_readiness_reports[-1] if premarket_readiness_reports else {},
        "latency_classification_counts": latency_summary,
        "issue_counts": dict(severities),
        "issues": issues,
        "capture_method_scorecard": capture_scorecard,
        "ledger_duplicate_scorecard": ledger_duplicate_scorecard,
        "daily_pl": summarize_daily_pl(day),
        "all_time_pl": summarize_all_time_pl(),
        "steve_alert_pl": steve_alert_pl,
        "all_time_steve_alert_pl": all_time_steve_alert_pl,
        "broker_fill_pl": broker_fill_pl,
        "all_time_broker_fill_pl": all_time_broker_fill_pl,
        "executive_activity": executive_activity,
        "storage_hygiene": storage_hygiene,
        "recursive_improvement_plan": [],
        "recommended_next_actions": recommended_next_actions(issues),
    }
    report["recursive_improvement_plan"] = recursive_improvement_plan(issues, report)
    return report


def recommended_next_actions(issues: list[dict[str, Any]]) -> list[str]:
    ordered_codes = [
        "duplicate_paper_position",
        "duplicate_paper_position_all_time",
        "duplicate_shadow_position_all_time",
        "duplicate_steve_exit_all_time",
        "browser_foreground_recovery_needed",
        "machine_unavailable_startup_backfill",
        "premarket_readiness_failed",
        "scale_in_not_supported",
        "contextual_stop_not_executed",
        "truth_buy_not_parsed",
        "parsed_buy_not_paper_traded",
        "hedge_missing_approval_card",
        "broker_position_reconciliation_failed",
        "broker_contract_not_found",
        "duplicate_broker_order",
        "broker_market_closed",
        "local_position_without_broker_fill",
        "submitted_broker_order_unresolved",
        "local_pnl_differs_from_broker_fills",
        "slow_order_submission",
        "entry_price_worse_than_alert",
        "broker_terminal_not_filled",
        "health_browser_capture_degraded",
        "health_browser_health_stale",
        "health_browser_message_not_raw",
        "health_notification_db_row_not_raw",
        "health_raw_not_processed",
        "health_non_hedge_missing_auto_buy",
        "health_option_exit_not_recorded",
        "ledger_storage_hygiene",
    ]
    by_code = {str(item.get("code")): item for item in issues}
    actions: list[str] = []
    for code in ordered_codes:
        item = by_code.get(code)
        recommendation = str((item or {}).get("recommendation") or "").strip()
        if recommendation and recommendation not in actions:
            actions.append(recommendation)
    for item in issues:
        recommendation = str(item.get("recommendation") or "").strip()
        if recommendation and recommendation not in actions:
            actions.append(recommendation)
    return actions[:6]


def format_money(value: Any) -> str:
    number = safe_float(value)
    if number is None:
        return "n/a"
    sign = "+" if number >= 0 else "-"
    return f"{sign}${abs(number):,.0f}"


def format_amount(value: Any) -> str:
    number = safe_float(value)
    if number is None:
        return "n/a"
    return f"${number:,.0f}"


def format_pct(value: Any) -> str:
    number = safe_float(value)
    if number is None:
        return "n/a"
    return f"{number:+.1f}%"


def executive_trade_line(label: str, row: dict[str, Any]) -> str:
    if not row:
        return f"{label}: n/a"
    return (
        f"{label}: {row.get('label') or 'OPTION'} "
        f"{format_money(row.get('pnl_dollars'))} / {format_pct(row.get('pnl_pct'))}"
    )


def executive_loop_line(report: dict[str, Any]) -> list[str]:
    issues = report.get("issues") or []
    plan = report.get("recursive_improvement_plan") or []
    top_issue = issues[0] if issues else {}
    found = top_issue.get("code") or "no critical issues"
    fixed_items = [item for item in plan if str(item.get("status") or "").lower() in {"fixed", "complete", "validated"}]
    fixed = fixed_items[0].get("code") if fixed_items else ("no changes needed" if not issues else "none auto-applied")
    actions = report.get("recommended_next_actions") or []
    next_action = str(actions[0])[:90] if actions else "monitor tomorrow's capture and fills"
    return [
        "Improvement loop:",
        f"Found: {found}",
        f"Fixed: {fixed}",
        f"Next: {next_action}",
    ]


def executive_telegram_summary(report: dict[str, Any]) -> str:
    activity = report.get("executive_activity") or {}
    capture = report.get("capture_method_scorecard") or {}
    methods = capture.get("methods") or {}
    browser_capture = methods.get("browser") or {}
    recommendation = capture.get("recommendation") or {}
    day = str(report.get("day") or "")
    try:
        day_label = dt.date.fromisoformat(day).strftime("%b %d")
    except ValueError:
        day_label = day or "today"
    lines = [
        f"DAILY PAPER SUMMARY - {day_label}",
        (
            f"Filled buys: {activity.get('filled_buys', 0)} "
            f"({activity.get('contracts_bought', 0)} contracts) | "
            f"Filled sells: {activity.get('filled_sells', 0)} "
            f"({activity.get('contracts_sold', 0)} contracts)"
        ),
        (
            f"Invested: {format_amount(activity.get('invested'))} | "
            f"Proceeds: {format_amount(activity.get('proceeds'))}"
        ),
        f"Realized P/L: {format_money(activity.get('realized_pnl'))}",
        executive_trade_line("Best", activity.get("best") or {}),
        executive_trade_line("Worst", activity.get("worst") or {}),
        f"Unfilled/expired: {activity.get('unfilled_terminal', 0)}",
    ]
    if capture:
        lines.append(
            "Capture: "
            f"browser {browser_capture.get('matched_truth_events', 0)}/{capture.get('truth_event_count', 0)} | "
            f"best {recommendation.get('recommended_primary') or 'n/a'}"
        )
    lines.extend(executive_loop_line(report))
    return "\n".join(lines)


def telegram_summary(report: dict[str, Any]) -> str:
    counts = report.get("counts") or {}
    issue_counts = report.get("issue_counts") or {}
    daily_pl = report.get("daily_pl") or {}
    all_time_pl = report.get("all_time_pl") or {}
    steve_pl = report.get("steve_alert_pl") or {}
    broker_pl = report.get("broker_fill_pl") or {}
    capture = report.get("capture_method_scorecard") or {}
    storage = report.get("storage_hygiene") or {}
    methods = capture.get("methods") or {}
    browser_capture = methods.get("browser") or {}
    notification_capture = methods.get("notification") or {}
    recommendation = capture.get("recommendation") or {}
    top_issue = (report.get("issues") or [{}])[0]
    lines = [
        "NIGHTLY PIPELINE REVIEW",
        f"Day: {report.get('day')}",
        (
            f"Steve: {counts.get('truth_buys', 0)} buys, "
            f"{counts.get('truth_exits', 0)} exits, {counts.get('truth_adds', 0)} adds"
        ),
        (
            f"Parsed: {counts.get('matched_buys', 0)}/{counts.get('truth_buys', 0)} | "
            f"Paper: {counts.get('paper_entries', 0)} | Broker fills: {counts.get('broker_filled_buys', 0)}"
        ),
        f"Issues: {issue_counts.get('critical', 0)} critical, {issue_counts.get('warning', 0)} warnings",
    ]
    if capture:
        lines.append(
            "Capture: "
            f"browser {browser_capture.get('matched_truth_events', 0)}/{capture.get('truth_event_count', 0)} "
            f"avg {browser_capture.get('latency', {}).get('avg_seconds') or 'n/a'}s | "
            f"notif {notification_capture.get('matched_truth_events', 0)}/{capture.get('truth_event_count', 0)} "
            f"avg {notification_capture.get('latency', {}).get('avg_seconds') or 'n/a'}s | "
            f"best {recommendation.get('recommended_primary') or 'n/a'}"
        )
    if daily_pl:
        lines.append(
            "Local P/L: "
            f"{format_money(daily_pl.get('total_pnl'))} "
            f"(realized {format_money(daily_pl.get('realized_pnl'))}, open {format_money(daily_pl.get('open_pnl'))})"
        )
    if steve_pl or broker_pl:
        lines.append(
            "Compare P/L: "
            f"Steve {format_money(steve_pl.get('total_pnl'))} | "
            f"Broker {format_money(broker_pl.get('total_pnl'))}"
        )
    if storage:
        tracked_mb = (safe_float(storage.get("total_tracked_bytes")) or 0) / (1024 * 1024)
        recommendations = storage.get("recommendations") or []
        lines.append(f"Storage: {tracked_mb:.1f} MB tracked | recs {len(recommendations)}")
    if all_time_pl:
        lines.append(
            "All-time P/L: "
            f"{format_money(all_time_pl.get('total_pnl'))} "
            f"(realized {format_money(all_time_pl.get('realized_pnl'))}, open {format_money(all_time_pl.get('open_pnl'))})"
        )
    if top_issue:
        lines.append(f"Top: {top_issue.get('code')}")
    actions = report.get("recommended_next_actions") or []
    if actions:
        lines.append(f"Next: {actions[0][:110]}")
    return "\n".join(lines)


def markdown_report(report: dict[str, Any]) -> str:
    counts = report.get("counts") or {}
    capture = report.get("capture_method_scorecard") or {}
    capture_methods = capture.get("methods") or {}
    capture_reco = capture.get("recommendation") or {}
    lines = [
        f"# Nightly Pipeline Review - {report.get('day')}",
        "",
        "## Summary",
        "",
        f"- Generated: `{report.get('generated_at')}`",
        f"- Steve truth: {counts.get('truth_buys', 0)} buys, {counts.get('truth_exits', 0)} exits, {counts.get('truth_adds', 0)} adds, {counts.get('truth_context_stops', 0)} context stops",
        f"- Pipeline: {counts.get('matched_buys', 0)}/{counts.get('truth_buys', 0)} buys parsed, {counts.get('paper_entries', 0)} paper entries, {counts.get('broker_filled_buys', 0)} broker buy fills",
        f"- Issues: {report.get('issue_counts') or {}}",
        f"- Latency classifications: startup_backfill={counts.get('machine_availability_findings', 0)}, slow_order={counts.get('slow_order_submission_findings', 0)}",
        f"- Premarket readiness reports: {counts.get('premarket_readiness_reports', 0)}",
        f"- Capture recommendation: {capture_reco.get('recommended_primary', 'n/a')} ({capture_reco.get('reason', 'n/a')})",
        "",
        "## Steve Truth Timeline",
        "",
    ]
    for event in report.get("truth_events") or []:
        descriptor = event.get("entry_price") or event.get("exit_price") or event.get("add_price") or ""
        lines.append(
            f"- `{event.get('source_time')}` {str(event.get('kind')).upper()} "
            f"{event.get('ticker')} {event.get('expiration_date')} {strike_text(event.get('strike_price'))} "
            f"{option_side(event.get('option_type'))} contracts={event.get('contracts')} price={descriptor} channel={event.get('channel_id')}"
        )
    lines.extend(["", "## Capture Method Scorecard", ""])
    if capture_methods:
        lines.extend(
            [
                "| Method | Matched Truth | Capture Rate | Avg Latency | Raw Records | Duplicates |",
                "| --- | ---: | ---: | ---: | ---: | ---: |",
            ]
        )
        for method in ("browser", "notification", "other"):
            stats = capture_methods.get(method) or {}
            latency = (stats.get("latency") or {}).get("avg_seconds")
            latency_text = f"{latency}s" if latency is not None else "n/a"
            lines.append(
                f"| {method} | {stats.get('matched_truth_events', 0)}/{capture.get('truth_event_count', 0)} "
                f"| {safe_float(stats.get('capture_rate')) or 0:.0%} "
                f"| {latency_text} "
                f"| {stats.get('raw_records', 0)} "
                f"| {stats.get('duplicate_event_records', 0)} |"
            )
        lines.append(f"- Cross-source duplicate truth events: {capture.get('cross_source_duplicate_truth_events', 0)}")
        interval_seconds = capture_reco.get("browser_interval_seconds")
        interval_text = f"{interval_seconds}s" if interval_seconds is not None else "n/a"
        lines.append(
            f"- Recommended primary: {capture_reco.get('recommended_primary', 'n/a')}; "
            f"browser interval target: {interval_text}"
        )
    else:
        lines.append("- No capture scorecard available.")
    duplicate_scorecard = report.get("ledger_duplicate_scorecard") or {}
    if duplicate_scorecard:
        lines.extend(["", "## Ledger Duplicate Scorecard", ""])
        for label in ("shadow_positions", "human_positions", "steve_exits"):
            stats = duplicate_scorecard.get(label) or {}
            lines.append(
                f"- {label}: groups={stats.get('duplicate_groups', 0)} "
                f"extra_rows={stats.get('duplicate_rows', 0)} rows={stats.get('rows', 0)} "
                f"legacy_groups={stats.get('legacy_collision_groups', 0)} "
                f"legacy_extra_rows={stats.get('legacy_collision_rows', 0)}"
            )
    lines.extend(["", "## Issues", ""])
    if not report.get("issues"):
        lines.append("- No issues detected.")
    for item in report.get("issues") or []:
        lines.append(f"- **{item.get('severity')} `{item.get('code')}`**: {item.get('message')}")
        lines.append(f"  Recommendation: {item.get('recommendation')}")
    lines.extend(["", "## Recommended Next Actions", ""])
    for action in report.get("recommended_next_actions") or ["No action needed."]:
        lines.append(f"- {action}")
    lines.extend(["", "## Daily P/L", "", "```json", json.dumps(report.get("daily_pl") or {}, indent=2, sort_keys=True), "```", ""])
    lines.extend(["", "## All-Time P/L", "", "```json", json.dumps(report.get("all_time_pl") or {}, indent=2, sort_keys=True), "```", ""])
    lines.extend(["", "## Steve Alert-Price P/L", "", "```json", json.dumps(report.get("steve_alert_pl") or {}, indent=2, sort_keys=True), "```", ""])
    lines.extend(["", "## Broker Fill P/L", "", "```json", json.dumps(report.get("broker_fill_pl") or {}, indent=2, sort_keys=True), "```", ""])
    lines.extend(["", "## Storage Hygiene", "", "```json", json.dumps(report.get("storage_hygiene") or {}, indent=2, sort_keys=True), "```", ""])
    lines.extend(["", "## Recursive Improvement Plan", ""])
    for item in report.get("recursive_improvement_plan") or []:
        lines.append(f"- **{item.get('priority')} `{item.get('code')}`** ({item.get('area')}, confidence={item.get('confidence', 'n/a')})")
    return "\n".join(lines)


def write_report(report: dict[str, Any]) -> tuple[Path, Path]:
    NIGHTLY_DIR.mkdir(parents=True, exist_ok=True)
    day = str(report.get("day"))
    json_path = NIGHTLY_DIR / f"{day}.json"
    md_path = NIGHTLY_DIR / f"{day}.md"
    json_path.write_text(json.dumps(report, indent=2, sort_keys=True) + "\n", encoding="utf-8")
    md_path.write_text(markdown_report(report), encoding="utf-8")
    append_jsonl(
        NIGHTLY_SUMMARY_FILE,
        {
            "event_type": "nightly_review_report",
            "day": day,
            "created_at": report.get("generated_at"),
            "json_path": str(json_path),
            "markdown_path": str(md_path),
            "counts": report.get("counts"),
            "premarket_readiness": report.get("premarket_readiness"),
            "latency_classification_counts": report.get("latency_classification_counts"),
            "issue_counts": report.get("issue_counts"),
            "capture_method_scorecard": report.get("capture_method_scorecard"),
            "all_time_pl": report.get("all_time_pl"),
            "steve_alert_pl": report.get("steve_alert_pl"),
            "broker_fill_pl": report.get("broker_fill_pl"),
            "executive_activity": report.get("executive_activity"),
            "storage_hygiene": report.get("storage_hygiene"),
            "recursive_improvement_plan": report.get("recursive_improvement_plan"),
            "recommended_next_actions": report.get("recommended_next_actions"),
        },
    )
    if report.get("steve_alert_pl"):
        append_jsonl(STEVE_ALERT_PL_FILE, report["steve_alert_pl"])
    if report.get("all_time_steve_alert_pl"):
        append_jsonl(STEVE_ALERT_PL_FILE, report["all_time_steve_alert_pl"])
    if report.get("broker_fill_pl"):
        append_jsonl(BROKER_FILL_PL_FILE, report["broker_fill_pl"])
    if report.get("all_time_broker_fill_pl"):
        append_jsonl(BROKER_FILL_PL_FILE, report["all_time_broker_fill_pl"])
    return json_path, md_path


def nightly_telegram_already_sent(day: str) -> bool:
    return any(
        row.get("day") == day and row.get("status") in {"sent", "partial_sent"}
        for row in read_jsonl(NIGHTLY_TELEGRAM_FILE)
    )


def send_telegram_report(report: dict[str, Any], force: bool = False) -> dict[str, Any]:
    from steve_trade_bot import delivery_status_from_messages, send_message_to_approval_chats, send_message_to_executive_chats

    day = str(report.get("day") or "")
    if day and not force and nightly_telegram_already_sent(day):
        return {"status": "already_sent", "reason": "nightly_review_already_sent_for_day", "telegram_messages": []}
    message = telegram_summary(report)
    status, reason, messages = send_message_to_approval_chats(message)
    executive_message = executive_telegram_summary(report)
    executive_status, executive_reason, executive_messages = send_message_to_executive_chats(executive_message)
    if executive_messages:
        messages.extend(executive_messages)
        status, reason = delivery_status_from_messages(messages)
    elif status != "sent" and executive_reason:
        reason = "; ".join(part for part in [reason, executive_reason] if part)
    delivery = {
        "event_type": "nightly_telegram_report",
        "day": day,
        "created_at": now_iso(),
        "status": status,
        "reason": reason,
        "message_text": message,
        "executive_message_text": executive_message,
        "executive_status": executive_status,
        "telegram_messages": messages,
    }
    append_jsonl(NIGHTLY_TELEGRAM_FILE, delivery)
    return {key: value for key, value in delivery.items() if key != "message_text"}


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--day", default="auto")
    parser.add_argument("--timezone", default="America/Detroit")
    parser.add_argument("--refresh-browser", action="store_true")
    parser.add_argument("--max-age-minutes", type=float, default=720.0)
    parser.add_argument("--timeout", type=int, default=30)
    parser.add_argument("--retries", type=int, default=2)
    parser.add_argument("--send-telegram", action="store_true")
    parser.add_argument("--force-telegram", action="store_true")
    parser.add_argument("--skip-storage-compact", action="store_true")
    parser.add_argument("--storage-compact-min-saved-bytes", type=int, default=64 * 1024)
    parser.add_argument("--print-json", action="store_true")
    args = parser.parse_args()

    day = business_day_for_run(args.timezone) if args.day == "auto" else args.day
    report = review_day(
        day,
        refresh_browser=args.refresh_browser,
        tz_name=args.timezone,
        max_age_minutes=args.max_age_minutes,
        timeout=args.timeout,
        retries=args.retries,
    )
    json_path, md_path = write_report(report)
    delivery = send_telegram_report(report, force=args.force_telegram) if args.send_telegram else {"status": "not_sent"}
    storage_compaction = (
        {"applied": False, "reason": "skipped"}
        if args.skip_storage_compact
        else compact_runtime_ledgers(NIGHTLY_SUMMARY_FILE.parent, apply=True, min_saved_bytes=int(args.storage_compact_min_saved_bytes))
    )
    result = {
        "day": day,
        "json_path": str(json_path),
        "markdown_path": str(md_path),
        "issue_counts": report.get("issue_counts"),
        "counts": report.get("counts"),
        "storage_hygiene": report.get("storage_hygiene"),
        "storage_compaction": storage_compaction,
        "telegram": delivery,
    }
    print(json.dumps(result if args.print_json else {"day": day, "issues": report.get("issue_counts"), "markdown_path": str(md_path)}, sort_keys=True))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())

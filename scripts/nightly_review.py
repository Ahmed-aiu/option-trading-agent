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
from parse_alert import ParseRejected, parse_trade_alert
from pipeline_common import CONFIG_DIR, DATA_DIR, append_jsonl, load_simple_yaml, now_iso, parse_datetime, read_jsonl, stable_hash
from run_pipeline_once import normalize_parsed


NIGHTLY_DIR = DATA_DIR / "nightly_reviews"
NIGHTLY_SUMMARY_FILE = DATA_DIR / "nightly_review_reports.jsonl"
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

PRICE_RE = r"(?:\d+(?:\.\d+)?|\.\d+)"
ADD_RE = re.compile(r"\b(?P<action>aaded|added|add)\s+(?P<contracts>\d+)\s+(?:@|at)\s*(?P<price>" + PRICE_RE + r")", re.I)
STOPPED_OUT_RE = re.compile(r"\bstopped?\s+out\b", re.I)
OCC_SYMBOL_RE = re.compile(r"^(?P<ticker>[A-Z]+)(?P<year>\d{2})(?P<month>\d{2})(?P<day>\d{2})(?P<option_type>[CP])(?P<strike>\d{8})$")


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


def issue(severity: str, code: str, message: str, evidence: dict[str, Any], recommendation: str) -> dict[str, Any]:
    return {
        "severity": severity,
        "code": code,
        "message": message,
        "evidence": evidence,
        "recommendation": recommendation,
    }


def first_time(rows: list[dict[str, Any]]) -> str:
    values = [row_time(row) for row in rows if row_time(row)]
    return min(values) if values else ""


def latency_seconds(start: Any, end: Any, tz_name: str) -> float | None:
    start_dt = parse_datetime(start, tz_name)
    end_dt = parse_datetime(end, tz_name)
    if start_dt is None or end_dt is None:
        return None
    return (end_dt - start_dt).total_seconds()


def find_orders_for_position(position: dict[str, Any], orders: list[dict[str, Any]]) -> list[dict[str, Any]]:
    position_id = position.get("position_id")
    return [row for row in orders if row.get("position_id") == position_id]


def summarize_daily_pl(day: str) -> dict[str, Any]:
    reports = [row for row in read_jsonl(DAILY_PL_FILE) if str(row.get("day") or (row.get("summary") or {}).get("day") or "") == day]
    return reports[-1].get("summary") if reports and isinstance(reports[-1].get("summary"), dict) else {}


def classify_broker_reason(reason: str) -> str:
    lowered = reason.lower()
    if "client_order_id must be unique" in lowered:
        return "duplicate_broker_order"
    if "uncovered option contracts" in lowered:
        return "broker_position_reconciliation_failed"
    if "paper_order_submission_disabled" in lowered:
        return "paper_order_disabled"
    return "broker_error"


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
    health_checks = rows_for_day(PIPELINE_HEALTH_FILE, day, tz_name)
    capture_scorecard = capture_method_scorecard(truth_events, raw_rows, tz_name)

    buys_by_key = group_by_key(parsed_buys, parsed_buy_key)
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
            issues.append(
                issue(
                    "critical",
                    "truth_buy_not_parsed",
                    "Steve buy alert exists in Discord truth but no matching parsed buy exists.",
                    {"event": event},
                    "Fix parser/canonical dedupe so this Steve buy enters parsed_alerts before next session.",
                )
            )
            continue
        contract_positions = positions_by_contract.get(event["contract_key"], [])
        if contract_positions:
            paper_entries += 1
        else:
            issues.append(
                issue(
                    "critical",
                    "parsed_buy_not_paper_traded",
                    "Parsed Steve buy did not create a local paper position.",
                    {"event": event, "parsed_keys": [row.get("source_dedupe_key") for row in parsed_matches]},
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
            order_time = first_time(related_orders)
            seconds = latency_seconds(event.get("source_time"), order_time, tz_name)
            if seconds is not None and seconds > 90:
                issues.append(
                    issue(
                        "warning" if seconds <= 300 else "critical",
                        "slow_order_submission",
                        "Paper order was submitted too long after Steve's alert.",
                        {"event": event, "order_time": order_time, "latency_seconds": round(seconds, 1)},
                        "Measure capture and enrichment latency; consider max-buy-latency and price-buffer rules when late.",
                    )
                )
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

    for order in orders:
        reason = str(order.get("reason") or "")
        if order.get("status") == "blocked" or reason:
            code = classify_broker_reason(reason)
            issues.append(
                issue(
                    "critical" if code != "paper_order_disabled" else "warning",
                    code,
                    "Broker/order audit recorded a blocked order.",
                    {"ticker": order.get("ticker"), "contract_symbol": order.get("contract_symbol"), "reason": reason, "payload": order.get("payload")},
                    "Reconcile Alpaca open positions before broker exits and prevent duplicate client order submissions.",
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

    seen_health_issue_keys: set[str] = set()
    for check in health_checks:
        if check.get("status") != "ok":
            for item in check.get("issues") or []:
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

    severities = defaultdict(int)
    for item in issues:
        severities[str(item.get("severity") or "warning")] += 1

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
            "broker_status_reports": len(broker_reports),
            "rejected": len(rejected),
        },
        "issue_counts": dict(severities),
        "issues": issues,
        "capture_method_scorecard": capture_scorecard,
        "daily_pl": summarize_daily_pl(day),
        "recommended_next_actions": recommended_next_actions(issues),
    }
    return report


def recommended_next_actions(issues: list[dict[str, Any]]) -> list[str]:
    ordered_codes = [
        "duplicate_paper_position",
        "scale_in_not_supported",
        "contextual_stop_not_executed",
        "truth_buy_not_parsed",
        "parsed_buy_not_paper_traded",
        "hedge_policy_blocks_immediate_following",
        "broker_position_reconciliation_failed",
        "duplicate_broker_order",
        "slow_order_submission",
        "entry_price_worse_than_alert",
        "broker_terminal_not_filled",
    ]
    by_code = {str(item.get("code")): item for item in issues}
    actions = []
    for code in ordered_codes:
        item = by_code.get(code)
        if item and item.get("recommendation") not in actions:
            actions.append(str(item.get("recommendation")))
    return actions[:6]


def format_money(value: Any) -> str:
    number = safe_float(value)
    if number is None:
        return "n/a"
    sign = "+" if number >= 0 else "-"
    return f"{sign}${abs(number):,.0f}"


def telegram_summary(report: dict[str, Any]) -> str:
    counts = report.get("counts") or {}
    issue_counts = report.get("issue_counts") or {}
    daily_pl = report.get("daily_pl") or {}
    capture = report.get("capture_method_scorecard") or {}
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
        lines.append(f"P/L local: {format_money(daily_pl.get('total_pnl'))}")
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
        lines.append(
            f"- Recommended primary: {capture_reco.get('recommended_primary', 'n/a')}; "
            f"browser interval target: {capture_reco.get('browser_interval_seconds', 'n/a')}s"
        )
    else:
        lines.append("- No capture scorecard available.")
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
            "issue_counts": report.get("issue_counts"),
            "capture_method_scorecard": report.get("capture_method_scorecard"),
            "recommended_next_actions": report.get("recommended_next_actions"),
        },
    )
    return json_path, md_path


def send_telegram_report(report: dict[str, Any]) -> dict[str, Any]:
    from steve_trade_bot import send_message_to_configured_chats

    status, reason, messages = send_message_to_configured_chats(telegram_summary(report))
    return {"status": status, "reason": reason, "telegram_messages": messages}


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--day", default="auto")
    parser.add_argument("--timezone", default="America/Detroit")
    parser.add_argument("--refresh-browser", action="store_true")
    parser.add_argument("--max-age-minutes", type=float, default=720.0)
    parser.add_argument("--timeout", type=int, default=30)
    parser.add_argument("--retries", type=int, default=2)
    parser.add_argument("--send-telegram", action="store_true")
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
    delivery = send_telegram_report(report) if args.send_telegram else {"status": "not_sent"}
    result = {
        "day": day,
        "json_path": str(json_path),
        "markdown_path": str(md_path),
        "issue_counts": report.get("issue_counts"),
        "counts": report.get("counts"),
        "telegram": delivery,
    }
    print(json.dumps(result if args.print_json else {"day": day, "issues": report.get("issue_counts"), "markdown_path": str(md_path)}, sort_keys=True))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())

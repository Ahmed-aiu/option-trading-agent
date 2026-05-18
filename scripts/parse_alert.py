#!/usr/bin/env python3
"""Deterministically parse raw notification records into strict trade alerts."""

from __future__ import annotations

import argparse
import datetime as dt
import json
import re
import sys
from pathlib import Path
from typing import Any

from pipeline_common import (
    CONFIG_DIR,
    DATA_DIR,
    LOG_DIR,
    append_jsonl,
    load_simple_yaml,
    now_iso,
    parse_datetime,
    read_jsonl,
    setup_logging,
)


PARSER_VERSION = "v1"
TICKER_RE = r"(?P<ticker>[A-Z]{1,5})"
PRICE_VALUE_RE = r"(?:\d+(?:\.\d+)?|\.\d+)"
PRICE_RE = rf"(?P<{{name}}>{PRICE_VALUE_RE})"
STOP_RE = rf"(?:stop|stp|sl|stop loss)\s+(?P<stop>{PRICE_VALUE_RE})"
TARGET_RE = rf"(?:target|tgt|tp)\s+(?P<target>{PRICE_VALUE_RE})"
ENTRY_WORDS = r"(?:over|above|under|below|at|@)"

ENTRY_PATTERNS = [
    re.compile(
        rf"\b(?P<side>BUY|LONG|SELL|SHORT)\s+{TICKER_RE}\s+{ENTRY_WORDS}\s+{PRICE_RE.format(name='entry')}",
        re.I,
    ),
    re.compile(
        rf"\b(?P<side>BOUGHT|SOLD)\s+{TICKER_RE}\s+(?:at\s+|@\s+)?{PRICE_RE.format(name='entry')}",
        re.I,
    ),
]
EXIT_RE = re.compile(r"\b(?P<action>trim|exit|close|closed|sold|take profit|taking profit)\b", re.I)
MONTHS = {
    "jan": 1,
    "january": 1,
    "feb": 2,
    "february": 2,
    "mar": 3,
    "march": 3,
    "apr": 4,
    "april": 4,
    "may": 5,
    "jun": 6,
    "june": 6,
    "jul": 7,
    "july": 7,
    "aug": 8,
    "august": 8,
    "sep": 9,
    "sept": 9,
    "september": 9,
    "oct": 10,
    "october": 10,
    "nov": 11,
    "november": 11,
    "dec": 12,
    "december": 12,
}
OPTION_LINE_RE = re.compile(
    r"(?:^|\s)#?(?P<ticker>[A-Z]{1,5})\s+"
    r"(?P<month>Jan(?:uary)?|Feb(?:ruary)?|Mar(?:ch)?|Apr(?:il)?|May|Jun(?:e)?|Jul(?:y)?|Aug(?:ust)?|Sep(?:t(?:ember)?)?|Oct(?:ober)?|Nov(?:ember)?|Dec(?:ember)?)\s+"
    r"(?P<day>\d{1,2})\s+"
    r"(?P<strike>\d+(?:\.\d+)?)\s+"
    r"(?P<option_type>calls?|puts?)\s+@\s*"
    rf"(?P<entry>{PRICE_VALUE_RE})\s+"
    r"(?P<action>bought|buy|added|add)\s*"
    r"(?P<quantity>\d+)?",
    re.I,
)
OPTION_EXIT_PRICE_RE = re.compile(
    rf"\b(?P<action>sold|sell|trim(?:med)?|trim|closed?|exit(?:ed)?)\s*"
    rf"(?P<quantity>\d+)?\s*(?:@|at)\s*(?P<exit_price>{PRICE_VALUE_RE})",
    re.I,
)
HASHTAG_RE = re.compile(r"#(?P<tag>[A-Za-z][A-Za-z0-9_-]*)")


class ParseRejected(Exception):
    def __init__(self, reason: str):
        super().__init__(reason)
        self.reason = reason


def raw_text_from_record(record: dict[str, Any]) -> str:
    parts = [record.get("title"), record.get("subtitle"), record.get("body")]
    return " ".join(str(part).strip() for part in parts if part).strip()


def normalized_side(value: str) -> str:
    value = value.lower()
    if value in {"buy", "long", "bought"}:
        return "buy"
    if value in {"sell", "short", "sold"}:
        return "short"
    return value


def infer_entry_type(text: str) -> str:
    lowered = text.lower()
    if any(word in lowered for word in ("over", "above")):
        return "breakout"
    if any(word in lowered for word in ("under", "below")):
        return "breakdown"
    return "limit"


def extract_float(pattern: re.Pattern[str], text: str, group: str) -> float | None:
    match = pattern.search(text)
    if not match:
        return None
    return float(match.group(group))


def extract_tags(text: str) -> list[str]:
    tags: list[str] = []
    for match in HASHTAG_RE.finditer(text):
        tag = match.group("tag").lower()
        if tag and tag not in tags:
            tags.append(tag)
    return tags


def line_for_match(text: str, match: re.Match[str]) -> str:
    anchor = match.start()
    while anchor < len(text) and text[anchor].isspace():
        anchor += 1
    line_start = text.rfind("\n", 0, anchor) + 1
    line_end = text.find("\n", match.end())
    if line_end == -1:
        line_end = len(text)
    return text[line_start:line_end]


def option_tags_for_line(line: str, ticker: str) -> list[str]:
    return [tag for tag in extract_tags(line) if tag != ticker.lower()]


def option_expiration(month_name: str, day_text: str, config: dict[str, Any], record: dict[str, Any] | None = None) -> str:
    month = MONTHS[month_name.lower()]
    day = int(day_text)
    year_setting = str(config.get("default_option_expiration_year", "auto"))
    base_datetime = parse_datetime((record or {}).get("notification_timestamp")) or parse_datetime((record or {}).get("captured_at"))
    base_date = base_datetime.date() if base_datetime else dt.date.today()
    year = base_date.year if year_setting == "auto" else int(year_setting)
    expiration = dt.date(year, month, day)
    if year_setting == "auto" and expiration < base_date:
        expiration = dt.date(year + 1, month, day)
    return expiration.isoformat()


def parse_option_alerts(raw_text: str, record: dict[str, Any], config: dict[str, Any]) -> list[dict[str, Any]]:
    if re.search(r"\badded?\s+\d+\s+@\s*\d", raw_text, re.I) and not OPTION_LINE_RE.search(raw_text):
        raise ParseRejected("option_add_missing_original_contract")
    matches = list(OPTION_LINE_RE.finditer(raw_text))
    if not matches:
        return []
    alerts: list[dict[str, Any]] = []
    source_key = record.get("dedupe_key") or record.get("source_dedupe_key") or ""
    for index, match in enumerate(matches, 1):
        ticker = match.group("ticker").upper()
        tags = option_tags_for_line(line_for_match(raw_text, match), ticker)
        option_type = "call" if match.group("option_type").lower().startswith("call") else "put"
        expiration = option_expiration(match.group("month"), match.group("day"), config, record)
        quantity = int(match.group("quantity") or 1)
        alerts.append(
            {
                "event_type": "parsed_trade_alert",
                "source_dedupe_key": f"{source_key}:option:{index}" if len(matches) > 1 else source_key,
                "parsed_at": now_iso(),
                "ticker": ticker,
                "side": "buy",
                "instrument_type": "option",
                "option_type": option_type,
                "expiration_date": expiration,
                "strike_price": float(match.group("strike")),
                "contracts": quantity,
                "tags": tags,
                "primary_tag": next((tag for tag in tags if tag in {"lotto", "swing", "hedge"}), tags[0] if tags else None),
                "entry_type": "limit",
                "entry_price": float(match.group("entry")),
                "stop_price": None,
                "target_price": None,
                "time_in_force": config.get("default_time_in_force", "day"),
                "confidence": "high",
                "raw_text": raw_text,
                "matched_text": match.group(0).strip(),
                "parser_version": PARSER_VERSION,
                "notification_timestamp": record.get("notification_timestamp"),
            }
        )
    return alerts


def parse_option_exit(raw_text: str, record: dict[str, Any], config: dict[str, Any]) -> dict[str, Any] | None:
    match = OPTION_EXIT_PRICE_RE.search(raw_text)
    if not match:
        return None
    source_key = record.get("dedupe_key") or record.get("source_dedupe_key") or ""
    ticker_match = re.search(r"#?(?P<ticker>[A-Z]{1,5})\b", raw_text)
    return {
        "event_type": "parsed_trade_alert",
        "source_dedupe_key": source_key,
        "parsed_at": now_iso(),
        "ticker": ticker_match.group("ticker").upper() if ticker_match else None,
        "side": "exit",
        "instrument_type": "option",
        "entry_type": "exit_candidate",
        "exit_action": match.group("action").lower(),
        "exit_price": float(match.group("exit_price")),
        "contracts": int(match.group("quantity")) if match.group("quantity") else None,
        "entry_price": None,
        "stop_price": None,
        "target_price": None,
        "time_in_force": config.get("default_time_in_force", "day"),
        "confidence": "medium" if ticker_match else "low",
        "raw_text": raw_text,
        "parser_version": PARSER_VERSION,
        "notification_timestamp": record.get("notification_timestamp"),
    }


def parse_trade_alert(record: dict[str, Any], config: dict[str, Any] | None = None) -> dict[str, Any] | list[dict[str, Any]]:
    config = config or load_simple_yaml(CONFIG_DIR / "parser_patterns.yaml")
    raw_text = (record.get("raw_text") or raw_text_from_record(record)).strip()
    lowered = raw_text.lower()
    if not raw_text:
        raise ParseRejected("empty_alert")
    if re.search(r"\b(CALLS?|PUTS?)\b", raw_text, re.I):
        if not config.get("allow_options", False):
            raise ParseRejected("options_disabled")
        option_alerts = parse_option_alerts(raw_text, record, config)
        if option_alerts:
            return option_alerts

    option_exit = parse_option_exit(raw_text, record, config)
    if option_exit:
        return option_exit

    for phrase in config.get("ambiguous_phrases", []):
        if phrase.lower() in lowered:
            raise ParseRejected("ambiguous_phrase")

    source_key = record.get("dedupe_key") or record.get("source_dedupe_key") or ""
    exit_match = EXIT_RE.search(raw_text)
    if exit_match and not any(re.search(rf"\b{word}\b", raw_text, re.I) for word in ("BUY", "LONG", "SHORT")):
        ticker_match = re.search(r"\b[A-Z]{1,5}\b", raw_text)
        if not ticker_match:
            raise ParseRejected("exit_missing_ticker")
        return {
            "event_type": "parsed_trade_alert",
            "source_dedupe_key": source_key,
            "parsed_at": now_iso(),
            "ticker": ticker_match.group(0),
            "side": "exit",
            "instrument_type": "stock",
            "entry_type": "exit_candidate",
            "entry_price": None,
            "stop_price": None,
            "target_price": None,
            "time_in_force": config.get("default_time_in_force", "day"),
            "confidence": "medium",
            "raw_text": raw_text,
            "parser_version": PARSER_VERSION,
            "notification_timestamp": record.get("notification_timestamp"),
        }

    match = None
    for pattern in ENTRY_PATTERNS:
        match = pattern.search(raw_text)
        if match:
            break
    if not match:
        if not re.search(r"\b(BUY|SELL|LONG|SHORT|BOUGHT|SOLD)\b", raw_text, re.I):
            raise ParseRejected("missing_explicit_side")
        if not re.search(r"\b[A-Z]{1,5}\b", raw_text):
            raise ParseRejected("missing_ticker")
        raise ParseRejected("ambiguous_no_explicit_entry")

    ticker = match.group("ticker").upper()
    allowlist = [str(item).upper() for item in config.get("ticker_allowlist", [])]
    blocklist = [str(item).upper() for item in config.get("ticker_blocklist", [])]
    if allowlist and ticker not in allowlist:
        raise ParseRejected("ticker_not_allowlisted")
    if ticker in blocklist:
        raise ParseRejected("ticker_blocklisted")

    stop_price = extract_float(re.compile(STOP_RE, re.I), raw_text, "stop")
    target_price = extract_float(re.compile(TARGET_RE, re.I), raw_text, "target")
    if config.get("require_stop_loss", True) and stop_price is None:
        raise ParseRejected("missing_stop_loss")

    side = normalized_side(match.group("side"))
    if side == "sell":
        entry_type = "breakdown" if re.search(r"\b(below|under)\b", raw_text, re.I) else "limit"
    else:
        entry_type = infer_entry_type(raw_text)
    return {
        "event_type": "parsed_trade_alert",
        "source_dedupe_key": source_key,
        "parsed_at": now_iso(),
        "ticker": ticker,
        "side": side,
        "instrument_type": "stock",
        "entry_type": entry_type,
        "entry_price": float(match.group("entry")),
        "stop_price": stop_price,
        "target_price": target_price,
        "time_in_force": config.get("default_time_in_force", "day"),
        "confidence": "high",
        "raw_text": raw_text,
        "parser_version": PARSER_VERSION,
        "notification_timestamp": record.get("notification_timestamp"),
    }


def rejected_record(record: dict[str, Any], reason: str) -> dict[str, Any]:
    return {
        "event_type": "rejected_alert",
        "source_dedupe_key": record.get("dedupe_key") or record.get("source_dedupe_key") or "",
        "rejected_at": now_iso(),
        "reason": reason,
        "raw_text": record.get("raw_text") or raw_text_from_record(record),
    }


def parse_records(records: list[dict[str, Any]], write: bool = False) -> tuple[list[dict[str, Any]], list[dict[str, Any]]]:
    logger = setup_logging("parser", LOG_DIR / "parser.log")
    config = load_simple_yaml(CONFIG_DIR / "parser_patterns.yaml")
    parsed: list[dict[str, Any]] = []
    rejected: list[dict[str, Any]] = []
    for record in records:
        try:
            parsed_record = parse_trade_alert(record, config)
            parsed_records = parsed_record if isinstance(parsed_record, list) else [parsed_record]
            for item in parsed_records:
                parsed.append(item)
                if write:
                    append_jsonl(DATA_DIR / "parsed_alerts.jsonl", item)
        except ParseRejected as exc:
            reject = rejected_record(record, exc.reason)
            rejected.append(reject)
            if write:
                append_jsonl(DATA_DIR / "rejected_alerts.jsonl", reject)
            logger.info("Rejected alert: %s", exc.reason)
        except Exception as exc:
            reject = rejected_record(record, f"parser_error:{type(exc).__name__}")
            rejected.append(reject)
            if write:
                append_jsonl(DATA_DIR / "rejected_alerts.jsonl", reject)
            logger.exception("Parser error")
    return parsed, rejected


def load_input(args: argparse.Namespace) -> list[dict[str, Any]]:
    if args.text:
        return [{"raw_text": args.text}]
    if args.json:
        value = json.loads(args.json)
        return [value] if isinstance(value, dict) else value
    if args.input:
        return read_jsonl(Path(args.input))
    data = sys.stdin.read().strip()
    if data:
        value = json.loads(data)
        return [value] if isinstance(value, dict) else value
    return []


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--input", help="Raw notification JSONL file")
    parser.add_argument("--json", help="Single raw notification JSON object")
    parser.add_argument("--text", help="Single raw alert text")
    parser.add_argument("--write", action="store_true", help="Append parsed/rejected records to data files")
    args = parser.parse_args()
    parsed, rejected = parse_records(load_input(args), write=args.write)
    for record in parsed + rejected:
        print(json.dumps(record, sort_keys=True))
    return 0 if parsed else 1


if __name__ == "__main__":
    raise SystemExit(main())

#!/usr/bin/env python3
"""Backfill Steve Discord text into the validation ledgers.

Default mode is audit-only: it records parsed entries/exits and shadow outcomes
without sending Telegram approval cards or submitting paper orders. Use live mode
only for near-real-time browser capture where auto paper routing is intended.
"""

from __future__ import annotations

import argparse
import datetime as dt
import re
import sys
from pathlib import Path
from typing import Any

from option_validation import handle_option_entry, handle_option_exit, is_option_entry, is_option_exit
from parse_alert import ParseRejected, parse_trade_alert
from pipeline_common import DATA_DIR, append_jsonl, now_iso, read_jsonl, stable_hash
from run_pipeline_once import normalize_parsed, process_raw_notifications


BACKFILLS_FILE = DATA_DIR / "discord_text_backfills.jsonl"
AUTHOR_PREFIXES = ("OTWSteve", "SteveOTWS", "@OTWSteve", "@SteveOTWS")
ADD_LINE_RE = re.compile(r"\b(?:aaded|added|add)\s+(?P<contracts>\d+)\s+(?:@|at)\s*(?P<price>\d+(?:\.\d+)?|\.\d+)", re.I)
MONTH_ABBR = {
    1: "Jan",
    2: "Feb",
    3: "Mar",
    4: "Apr",
    5: "May",
    6: "Jun",
    7: "Jul",
    8: "Aug",
    9: "Sep",
    10: "Oct",
    11: "Nov",
    12: "Dec",
}


def clean_line(raw_line: str) -> str:
    line = " ".join(raw_line.replace("\u202f", " ").split()).strip()
    for author in AUTHOR_PREFIXES:
        if line == author:
            return ""
        if line.startswith(author + " — "):
            return line.split("—", 1)[1].strip()
        if line.startswith(author + " "):
            return line[len(author) :].strip()
    return line


def useful_lines(text: str) -> list[str]:
    lines = []
    for raw_line in text.splitlines():
        line = clean_line(raw_line)
        if not line:
            continue
        lowered = line.lower()
        if lowered in {"today", "yesterday", "new messages"}:
            continue
        if lowered.endswith("am") or lowered.endswith("pm"):
            continue
        lines.append(line)
    return lines


def unique_records(records: list[dict[str, Any]]) -> list[dict[str, Any]]:
    seen: set[str] = set()
    unique: list[dict[str, Any]] = []
    for record in records:
        key = str(record.get("dedupe_key") or "")
        if not key or key in seen:
            continue
        seen.add(key)
        unique.append(record)
    return unique


def parsed_items_for_body(body: str, dedupe_key: str, source_time: str | None = None) -> list[dict[str, Any]]:
    captured_at = now_iso()
    try:
        value = parse_trade_alert(
            {
                "event_type": "raw_discord_ui_backfill",
                "captured_at": captured_at,
                "notification_timestamp": source_time or "",
                "source_app": "DiscordUI",
                "bundle_id": "browser_or_clipboard",
                "title": "OTWSteve",
                "subtitle": "discord_text_backfill",
                "body": body,
                "raw": {},
                "dedupe_key": dedupe_key,
            }
        )
    except ParseRejected:
        return []
    return normalize_parsed(value)


def materialize_add_body(entry: dict[str, Any], add_line: str) -> str | None:
    match = ADD_LINE_RE.search(add_line)
    if not match:
        return None
    expiration_text = str(entry.get("expiration_date") or "")
    try:
        expiration = dt.date.fromisoformat(expiration_text)
    except ValueError:
        return None
    month = MONTH_ABBR.get(expiration.month)
    if not month:
        return None
    ticker = str(entry.get("ticker") or "").upper()
    option_type = str(entry.get("option_type") or "").lower()
    strike = entry.get("strike_price")
    if not ticker or not option_type or strike in (None, ""):
        return None
    tags = " ".join(f"#{tag}" for tag in (entry.get("tags") or []) if tag)
    add_body = (
        f"#{ticker} {month} {expiration.day} {float(strike):g} {option_type} "
        f"@ {float(match.group('price')):g} added {int(match.group('contracts'))}"
    )
    return f"{add_body} {tags}".strip()


def build_raw_records(
    text: str,
    source: str,
    dedupe_scope: str | None = None,
    suppress_context_entries: bool = False,
    source_time: str | None = None,
) -> list[dict[str, Any]]:
    records: list[dict[str, Any]] = []
    last_entry_context = ""
    last_entry_alert: dict[str, Any] | None = None
    dedupe_seed = dedupe_scope or source
    captured_at = now_iso()
    parsed_lines: list[tuple[str, list[dict[str, Any]], list[dict[str, Any]]]] = []
    has_exit_context = False
    for line in useful_lines(text):
        key = "ui-" + stable_hash([dedupe_seed, line])[:24]
        parsed = parsed_items_for_body(line, key, source_time=source_time)
        entries = [item for item in parsed if is_option_entry(item)]
        exits = [item for item in parsed if is_option_exit(item)]
        if exits:
            has_exit_context = True
        parsed_lines.append((line, entries, exits))
    for line, entries, exits in parsed_lines:
        if entries:
            body = line
            last_entry_context = entries[-1].get("matched_text") or line
            last_entry_alert = entries[-1]
            if suppress_context_entries and has_exit_context:
                continue
        elif exits:
            body = f"{last_entry_context}\n{line}" if last_entry_context else line
        elif last_entry_alert:
            add_body = materialize_add_body(last_entry_alert, line)
            if not add_body:
                continue
            body = add_body
        else:
            continue
        records.append(
            {
                "event_type": "raw_discord_ui_backfill",
                "captured_at": captured_at,
                "notification_timestamp": source_time or "",
                "source_app": "DiscordUI",
                "bundle_id": "browser_or_clipboard",
                "title": "OTWSteve",
                "subtitle": source,
                "body": body,
                "raw": {"source": source, "line": line},
                "dedupe_key": "ui-" + stable_hash([dedupe_seed, body])[:24],
            }
        )
    return unique_records(records)


def existing_keys(path: Path) -> set[str]:
    return {str(row.get("dedupe_key")) for row in read_jsonl(path) if row.get("dedupe_key")}


def process_audit(records: list[dict[str, Any]]) -> dict[str, int]:
    counts = {"raw": 0, "parsed": 0, "entries": 0, "exits": 0, "rejected": 0}
    seen_backfills = existing_keys(BACKFILLS_FILE)
    for raw in records:
        if raw["dedupe_key"] in seen_backfills:
            continue
        seen_backfills.add(raw["dedupe_key"])
        append_jsonl(BACKFILLS_FILE, raw)
        counts["raw"] += 1
        try:
            parsed_items = normalize_parsed(parse_trade_alert(raw))
        except ParseRejected:
            counts["rejected"] += 1
            continue
        for parsed in parsed_items:
            append_jsonl(DATA_DIR / "parsed_alerts.jsonl", parsed)
            counts["parsed"] += 1
            if is_option_entry(parsed):
                handle_option_entry(parsed, send_approval=False)
                counts["entries"] += 1
            elif is_option_exit(parsed):
                result = handle_option_exit(parsed)
                if result.get("created"):
                    counts["exits"] += 1
    return counts


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--input", help="Text file to read. Defaults to stdin.")
    parser.add_argument("--source", default="manual_text_backfill")
    parser.add_argument("--mode", choices=["audit", "live"], default="audit")
    parser.add_argument("--dry-run", action="store_true")
    args = parser.parse_args()

    text = Path(args.input).read_text(encoding="utf-8") if args.input else sys.stdin.read()
    records = build_raw_records(text, args.source)
    if args.dry_run:
        for record in records:
            print(record["body"])
            print("---")
        print({"records": len(records), "mode": args.mode, "dry_run": True})
        return 0
    if args.mode == "live":
        existing = existing_keys(DATA_DIR / "raw_notifications.jsonl")
        new_records = []
        for record in records:
            if record["dedupe_key"] in existing:
                continue
            existing.add(record["dedupe_key"])
            new_records.append(record)
        for record in new_records:
            append_jsonl(DATA_DIR / "raw_notifications.jsonl", record)
        counts = process_raw_notifications(read_jsonl(DATA_DIR / "raw_notifications.jsonl"))
        counts["raw_backfilled"] = len(new_records)
    else:
        counts = process_audit(records)
    print(counts)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())

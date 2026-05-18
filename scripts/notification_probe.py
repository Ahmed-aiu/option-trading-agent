#!/usr/bin/env python3
"""Probe local macOS notifications without touching Discord APIs."""

from __future__ import annotations

import argparse
import datetime as dt
import json
import os
import plistlib
import re
import sqlite3
import subprocess
import unicodedata
from pathlib import Path
from typing import Any
from zoneinfo import ZoneInfo

from pipeline_common import DEFAULT_TZ, parse_datetime


TEXT_KEYS = {
    "title",
    "subtitle",
    "body",
    "message",
    "informativetext",
    "thread",
    "category",
}


def clean_notification_text(value: Any) -> str:
    if value is None:
        return ""
    text = str(value)
    text = text.replace("\u2068", "").replace("\u2069", "")
    return " ".join(text.split()).strip()


def candidate_notification_paths() -> list[Path]:
    home = Path.home()
    roots = [
        home / "Library/Application Support/NotificationCenter",
        home / "Library/Group Containers/group.com.apple.usernoted",
        home / "Library/Group Containers/group.com.apple.UserNotifications",
    ]
    candidates: list[Path] = []
    for root in roots:
        try:
            root.stat()
        except PermissionError:
            candidates.append(root)
            continue
        except FileNotFoundError:
            continue
        try:
            for path in root.rglob("*"):
                if path.is_file() and path.name.lower() in {"db", "db2", "database.sqlite"}:
                    candidates.append(path)
                elif path.is_file() and path.suffix.lower() in {".db", ".sqlite", ".sqlite3"}:
                    candidates.append(path)
        except PermissionError:
            candidates.append(root)
    var_root = Path("/private/var/folders")
    try:
        for path in var_root.glob("*/*/0/com.apple.notificationcenter*/**/*"):
            if path.is_file() and (path.name.lower() == "db" or path.suffix.lower() in {".db", ".sqlite"}):
                candidates.append(path)
    except PermissionError:
        candidates.append(var_root)
    unique: list[Path] = []
    seen: set[str] = set()
    for path in candidates:
        key = str(path)
        if key not in seen:
            unique.append(path)
            seen.add(key)
    return unique


def protected_root_diagnostics() -> list[str]:
    home = Path.home()
    diagnostics: list[str] = []
    for root in [
        home / "Library/Group Containers/group.com.apple.usernoted",
        home / "Library/Group Containers/group.com.apple.UserNotifications",
    ]:
        try:
            root.stat()
            next(root.iterdir(), None)
        except PermissionError:
            diagnostics.append(f"Permission denied scanning {root}")
        except StopIteration:
            diagnostics.append(f"Notification root is readable but empty: {root}")
        except FileNotFoundError:
            diagnostics.append(f"Notification root does not exist: {root}")
        except OSError as exc:
            diagnostics.append(f"Could not scan {root}: {exc}")
    return diagnostics


def flatten_text(value: Any) -> list[str]:
    texts: list[str] = []
    if value is None:
        return texts
    if isinstance(value, bytes):
        for parser in (plistlib.loads, json.loads):
            try:
                parsed = parser(value)
                texts.extend(flatten_text(parsed))
                return texts
            except Exception:
                pass
        for encoding in ("utf-8", "utf-16", "latin-1"):
            try:
                decoded = value.decode(encoding, errors="ignore")
            except Exception:
                continue
            texts.extend(re.findall(r"[A-Za-z0-9][A-Za-z0-9 .:_@$%/#'\"!?()\\-]{2,}", decoded))
        return texts
    if isinstance(value, dict):
        for key, item in value.items():
            if str(key).lower() in TEXT_KEYS or isinstance(item, (dict, list, tuple, bytes)):
                texts.extend(flatten_text(item))
        return texts
    if isinstance(value, (list, tuple)):
        for item in value:
            texts.extend(flatten_text(item))
        return texts
    if isinstance(value, str):
        stripped = value.strip()
        if stripped:
            texts.append(stripped)
    return texts


def pick_text(texts: list[str], app: str) -> dict[str, str]:
    cleaned: list[str] = []
    for text in texts:
        normalized = " ".join(text.split())
        if len(normalized) < 3 or normalized in cleaned:
            continue
        if normalized.lower() in {"null", "true", "false"}:
            continue
        cleaned.append(normalized)
    app_matches = [t for t in cleaned if app.lower() in t.lower()]
    title = app_matches[0] if app_matches else (cleaned[0] if cleaned else "")
    subtitle = cleaned[1] if len(cleaned) > 1 else ""
    body = ""
    tradeish = re.compile(r"\b(BUY|SELL|LONG|SHORT|STOP|TARGET|SL|TP|ABOVE|BELOW|UNDER|OVER)\b", re.I)
    for text in cleaned:
        if text != title and tradeish.search(text):
            body = text
            break
    if not body and len(cleaned) > 2:
        body = cleaned[2]
    elif not body and len(cleaned) > 1:
        body = cleaned[1]
    return {"title": title, "subtitle": subtitle, "body": body}


def rows_from_sqlite(path: Path, app: str, since: dt.datetime, tz_name: str) -> list[dict[str, Any]]:
    rows: list[dict[str, Any]] = []
    connection = sqlite3.connect(f"file:{path}?mode=ro", uri=True, timeout=1)
    connection.row_factory = sqlite3.Row
    try:
        tables = [
            row[0]
            for row in connection.execute(
                "select name from sqlite_master where type='table' and name not like 'sqlite_%'"
            )
        ]
        app_lookup: dict[Any, str] = {}
        for table in tables:
            columns = [row[1] for row in connection.execute(f'pragma table_info("{table}")')]
            if {"app_id", "identifier"}.issubset(columns):
                try:
                    for row in connection.execute(f'select app_id, identifier from "{table}"'):
                        app_lookup[row["app_id"]] = row["identifier"]
                except sqlite3.Error:
                    pass
        for table in tables:
            if table.lower() in {"app", "dbinfo", "categories", "delivered", "displayed", "requests"}:
                continue
            columns = [row[1] for row in connection.execute(f'pragma table_info("{table}")')]
            if not columns:
                continue
            timestamp_cols = [
                col
                for col in columns
                if any(token in col.lower() for token in ("date", "time", "timestamp", "delivered"))
            ]
            order_col = "delivered_date" if "delivered_date" in columns else (timestamp_cols[0] if timestamp_cols else columns[0])
            select_cols = ", ".join(f'"{col}"' for col in columns)
            try:
                query = f'select {select_cols} from "{table}" order by "{order_col}" desc limit 300'
                table_rows = connection.execute(query).fetchall()
            except sqlite3.Error:
                continue
            for row in table_rows:
                row_dict = dict(row)
                row_text = " ".join(str(v) for v in row_dict.values() if not isinstance(v, bytes))
                bundle_id = ""
                if "app_id" in row_dict and row_dict["app_id"] in app_lookup:
                    bundle_id = app_lookup[row_dict["app_id"]]
                    row_text += " " + bundle_id
                    if app.lower() not in bundle_id.lower():
                        continue
                if table.lower() == "record" and isinstance(row_dict.get("data"), bytes):
                    try:
                        payload = plistlib.loads(row_dict["data"])
                    except Exception:
                        payload = {}
                    payload_app = clean_notification_text(payload.get("app"))
                    if payload_app:
                        bundle_id = payload_app
                    req = payload.get("req") if isinstance(payload.get("req"), dict) else {}
                    if app.lower() in (bundle_id or "").lower() or app.lower() in row_text.lower():
                        timestamp = (
                            parse_datetime(payload.get("date"), tz_name)
                            or parse_datetime(row_dict.get("delivered_date"), tz_name)
                            or parse_datetime(row_dict.get("request_last_date"), tz_name)
                            or parse_datetime(row_dict.get("request_date"), tz_name)
                        )
                        if timestamp and timestamp < since:
                            continue
                        title = clean_notification_text(req.get("titl") or req.get("title"))
                        subtitle = clean_notification_text(req.get("subt") or req.get("subtitle") or req.get("thre"))
                        body = clean_notification_text(req.get("body") or req.get("message"))
                        if not title and not subtitle and not body:
                            continue
                        rows.append(
                            {
                                "source_app": app,
                                "bundle_id": bundle_id or ("com.hnc.Discord" if app.lower() == "discord" else ""),
                                "title": title,
                                "subtitle": subtitle,
                                "body": body,
                                "notification_timestamp": timestamp.isoformat(timespec="seconds") if timestamp else "",
                                "raw": {
                                    "method": "notification_db",
                                    "path": str(path),
                                    "table": table,
                                    "rec_id": row_dict.get("rec_id"),
                                    "thread": clean_notification_text(req.get("thre")),
                                    "identifier": clean_notification_text(req.get("iden")),
                                    "content_type": clean_notification_text(req.get("unct")),
                                },
                            }
                        )
                        continue
                if app.lower() not in row_text.lower() and "discord" not in bundle_id.lower():
                    blob_text = " ".join(flatten_text(list(row_dict.values())))
                    if app.lower() not in blob_text.lower() and "discord" not in blob_text.lower():
                        continue
                timestamp = None
                for col in timestamp_cols:
                    timestamp = parse_datetime(row_dict.get(col), tz_name)
                    if timestamp:
                        break
                if timestamp and timestamp < since:
                    continue
                text_fields = pick_text(flatten_text(list(row_dict.values())), app)
                if not text_fields["title"] and not text_fields["subtitle"] and not text_fields["body"]:
                    continue
                if text_fields["title"].lower() in {app.lower(), f"com.hnc.{app.lower()}", "com.hnc.discord"} and not text_fields["body"]:
                    continue
                rows.append(
                    {
                        "source_app": app,
                        "bundle_id": bundle_id or ("com.hnc.Discord" if app.lower() == "discord" else ""),
                        "title": text_fields["title"],
                        "subtitle": text_fields["subtitle"],
                        "body": text_fields["body"],
                        "notification_timestamp": timestamp.isoformat(timespec="seconds") if timestamp else "",
                        "raw": {"method": "notification_db", "path": str(path), "table": table},
                    }
                )
    finally:
        connection.close()
    return rows


def probe_notification_db(app: str, last_minutes: int, tz_name: str) -> tuple[list[dict[str, Any]], list[str]]:
    tz = ZoneInfo(tz_name)
    since = dt.datetime.now(tz) - dt.timedelta(minutes=last_minutes)
    found: list[dict[str, Any]] = []
    diagnostics: list[str] = []
    candidates = candidate_notification_paths()
    if not candidates:
        diagnostics.extend(protected_root_diagnostics())
        diagnostics.append("No readable Notification Center database candidates were discovered.")
        diagnostics.append(
            "Known protected roots include ~/Library/Group Containers/group.com.apple.usernoted "
            "and ~/Library/Group Containers/group.com.apple.UserNotifications."
        )
    for path in candidates:
        if path.is_dir():
            diagnostics.append(f"Permission denied while scanning {path}")
            continue
        try:
            found.extend(rows_from_sqlite(path, app, since, tz_name))
            diagnostics.append(f"Checked {path}")
        except PermissionError:
            diagnostics.append(f"Permission denied reading {path}")
        except sqlite3.DatabaseError as exc:
            diagnostics.append(f"Not a readable SQLite notification DB: {path} ({exc})")
        except OSError as exc:
            diagnostics.append(f"Could not read {path}: {exc}")
    return found, diagnostics


def probe_accessibility(app: str) -> tuple[list[dict[str, Any]], list[str]]:
    script = f'''
    tell application "System Events"
      set output to ""
      repeat with procName in {{"NotificationCenter", "{app}"}}
        if exists process procName then
          tell process procName
            try
              set output to output & procName & ": " & (name of UI elements as text) & linefeed
            on error errMsg
              set output to output & procName & ": ERROR " & errMsg & linefeed
            end try
          end tell
        end if
      end repeat
      return output
    end tell
    '''
    try:
        result = subprocess.run(
            ["osascript", "-e", script],
            check=False,
            text=True,
            capture_output=True,
            timeout=5,
        )
    except Exception as exc:
        return [], [f"Accessibility probe failed: {exc}"]
    diagnostics = [result.stdout.strip()] if result.stdout.strip() else []
    if result.stderr.strip():
        diagnostics.append(result.stderr.strip())
    return [], diagnostics


def print_notification(record: dict[str, Any]) -> None:
    print("FOUND Discord notification:")
    print(f"timestamp: {record.get('notification_timestamp') or '(unknown)'}")
    print(f"title: {record.get('title') or ''}")
    print(f"subtitle: {record.get('subtitle') or ''}")
    print(f"body: {record.get('body') or ''}")
    if record.get("raw"):
        print(f"raw: {json.dumps(record['raw'], sort_keys=True)}")
    print()


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--app", default="Discord")
    parser.add_argument("--last-minutes", type=int, default=30)
    parser.add_argument("--timezone", default=DEFAULT_TZ)
    parser.add_argument("--method", choices=["auto", "db", "accessibility"], default="auto")
    args = parser.parse_args()

    records: list[dict[str, Any]] = []
    diagnostics: list[str] = []
    if args.method in {"auto", "db"}:
        db_records, db_diagnostics = probe_notification_db(args.app, args.last_minutes, args.timezone)
        records.extend(db_records)
        diagnostics.extend(db_diagnostics)
    if args.method in {"auto", "accessibility"} and not records:
        _, accessibility_diagnostics = probe_accessibility(args.app)
        diagnostics.extend(["Accessibility fallback snapshot only; no parsing from UI was attempted."])
        diagnostics.extend(accessibility_diagnostics)

    if records:
        for record in records:
            print_notification(record)
        return 0

    print(f"No {args.app} notifications found in the last {args.last_minutes} minutes.")
    print()
    print("Troubleshooting:")
    print("- Confirm Discord notifications are enabled in macOS System Settings.")
    print("- Confirm the relevant Discord channel is set to All Messages.")
    print("- Disable Focus/Do Not Disturb while testing.")
    print("- Allow notification previews; hidden previews may remove body text from local records.")
    print("- Notification Center database paths are privacy-protected on recent macOS versions.")
    print("- Grant Full Disk Access to Terminal/Codex if DB diagnostics show permission errors.")
    print("- Accessibility permission is needed only for the isolated UI snapshot fallback.")
    print()
    print("Diagnostics:")
    for item in diagnostics[-20:]:
        print(f"- {item}")
    return 1


if __name__ == "__main__":
    raise SystemExit(main())

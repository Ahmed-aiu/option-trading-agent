#!/usr/bin/env python3
"""Continuously capture matching Discord notifications into append-only JSONL."""

from __future__ import annotations

import argparse
import json
import signal
import time
import unicodedata
from typing import Any

from notification_probe import probe_accessibility, probe_notification_db
from pipeline_common import (
    CONFIG_DIR,
    DATA_DIR,
    LOG_DIR,
    append_jsonl,
    atomic_touch_jsonl_files,
    load_seen_keys,
    load_simple_yaml,
    notification_dedupe_key,
    now_iso,
    setup_logging,
)


STOP = False


def request_stop(signum: int, frame: object) -> None:
    global STOP
    STOP = True


def text_contains_any(text: str, keywords: list[str]) -> bool:
    normalized_text = unicodedata.normalize("NFKC", text).upper()
    return any(unicodedata.normalize("NFKC", str(keyword)).upper() in normalized_text for keyword in keywords)


def raw_values(record: dict[str, Any]) -> list[str]:
    raw = record.get("raw") or {}
    values = [
        record.get("title"),
        record.get("subtitle"),
        raw.get("thread"),
        raw.get("identifier"),
        raw.get("conversation_id"),
        raw.get("target_content_identifier"),
    ]
    return [str(value) for value in values if value not in (None, "")]


def notification_has_channel_id(record: dict[str, Any], channel_ids: list[str]) -> bool:
    if not channel_ids:
        return True
    haystack = " ".join(raw_values(record))
    return any(str(channel_id) in haystack for channel_id in channel_ids)


def is_matching_notification(record: dict[str, Any], config: dict[str, Any]) -> bool:
    app_names = [str(item).lower() for item in config.get("app_names", [])]
    bundle_ids = [str(item).lower() for item in config.get("bundle_ids", [])]
    source_app = str(record.get("source_app", "")).lower()
    bundle_id = str(record.get("bundle_id", "")).lower()
    if app_names and not any(name in source_app for name in app_names):
        if bundle_ids and bundle_id not in bundle_ids:
            return False
    author_names = [str(item) for item in config.get("alert_author_names", [])]
    if author_names and not text_contains_any(str(record.get("title") or ""), author_names):
        return False
    channel_ids = [str(item) for item in config.get("alert_channel_ids", [])]
    if config.get("require_alert_channel_id_match", False) and not notification_has_channel_id(record, channel_ids):
        return False
    if config.get("write_all_discord_notifications", False):
        return True
    title_text = " ".join(str(record.get(key) or "") for key in ("title", "subtitle"))
    body_text = str(record.get("body") or "")
    title_keywords = [str(item) for item in config.get("title_keywords", [])]
    body_keywords = [str(item) for item in config.get("body_keywords", [])]
    title_match = text_contains_any(title_text, title_keywords) if title_keywords else True
    body_match = text_contains_any(body_text, body_keywords) if body_keywords else True
    return title_match or body_match


def normalize_raw(record: dict[str, Any], config: dict[str, Any]) -> dict[str, Any]:
    normalized = {
        "event_type": "raw_discord_notification",
        "captured_at": now_iso(config.get("timezone", "America/Detroit")),
        "notification_timestamp": record.get("notification_timestamp") or "",
        "source_app": record.get("source_app") or "Discord",
        "bundle_id": record.get("bundle_id") or "com.hnc.Discord",
        "title": record.get("title") or "",
        "subtitle": record.get("subtitle") or "",
        "body": record.get("body") or "",
        "raw": record.get("raw") or {},
    }
    normalized["dedupe_key"] = notification_dedupe_key(normalized)
    return normalized


def poll_once(config: dict[str, Any], seen: set[str], logger: Any) -> tuple[int, int]:
    app_name = str((config.get("app_names") or ["Discord"])[0])
    method = str(config.get("capture_method", "auto")).lower()
    diagnostics: list[str] = []
    records: list[dict[str, Any]] = []
    if method in {"auto", "db"}:
        db_records, db_diagnostics = probe_notification_db(app_name, last_minutes=120, tz_name=config.get("timezone", "America/Detroit"))
        records.extend(db_records)
        diagnostics.extend(db_diagnostics)
    if method in {"accessibility"}:
        _, accessibility_diagnostics = probe_accessibility(app_name)
        diagnostics.extend(accessibility_diagnostics)
    if not records and diagnostics:
        logger.debug("No notification rows found. Last diagnostic: %s", diagnostics[-1])

    written = 0
    duplicates = 0
    for record in records:
        try:
            normalized = normalize_raw(record, config)
            if not normalized["body"] and not normalized["title"]:
                continue
            if not is_matching_notification(normalized, config):
                continue
            key = normalized["dedupe_key"]
            if key in seen:
                duplicates += 1
                continue
            append_jsonl(DATA_DIR / "raw_notifications.jsonl", normalized)
            seen.add(key)
            written += 1
            logger.info("Captured Discord notification dedupe_key=%s title=%r", key, normalized["title"])
        except Exception:
            logger.exception("Failed to normalize/write a notification")
    return written, duplicates


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--once", action="store_true", help="Poll once and exit")
    parser.add_argument("--config", default=str(CONFIG_DIR / "watcher.yaml"))
    args = parser.parse_args()
    atomic_touch_jsonl_files()
    config = load_simple_yaml(CONFIG_DIR / "watcher.yaml")
    if args.config:
        config = load_simple_yaml(__import__("pathlib").Path(args.config))
    logger = setup_logging("watcher", LOG_DIR / "watcher.log")
    signal.signal(signal.SIGTERM, request_stop)
    signal.signal(signal.SIGINT, request_stop)
    seen = load_seen_keys(DATA_DIR / "raw_notifications.jsonl")
    logger.info("Watcher starting with %d existing dedupe keys", len(seen))
    interval = float(config.get("poll_interval_seconds", 1))
    while not STOP:
        try:
            written, duplicates = poll_once(config, seen, logger)
            if written or duplicates:
                logger.info("Poll complete written=%d duplicates=%d", written, duplicates)
        except Exception:
            logger.exception("Watcher poll failed; continuing")
        if args.once:
            break
        time.sleep(interval)
    logger.info("Watcher stopped")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())

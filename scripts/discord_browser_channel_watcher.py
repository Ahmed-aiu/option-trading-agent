#!/usr/bin/env python3
"""Capture Steve Discord messages from configured Chrome channel tabs.

This is a browser fallback for cases where Discord/macOS notifications do not
arrive. It does not use Discord APIs, account tokens, cookies, or passwords.
Chrome must already be logged into Discord and must allow JavaScript from Apple
Events.
"""

from __future__ import annotations

import argparse
import datetime as dt
import json
import re
import signal
import subprocess
import time
from pathlib import Path
from typing import Any
from zoneinfo import ZoneInfo

from backfill_steve_text import build_raw_records, existing_keys
from pipeline_common import CONFIG_DIR, DATA_DIR, LOG_DIR, append_jsonl, load_simple_yaml, now_iso, read_jsonl, setup_logging, stable_hash
from run_pipeline_once import process_raw_notifications


DEFAULT_GUILD_ID = "483483452180791296"
BROWSER_MESSAGES_FILE = DATA_DIR / "discord_browser_messages.jsonl"
BROWSER_HEALTH_FILE = DATA_DIR / "discord_browser_health.jsonl"
BROWSER_HEALTH_LATEST_FILE = DATA_DIR / "discord_browser_health_latest.json"
BROWSER_STATE_FILE = DATA_DIR / "discord_browser_channel_watcher_state.json"

STOP = False

APPLE_SCRIPT = r'''
on run argv
  set channelUrl to item 1 of argv
  set jsCode to item 2 of argv
  set firstLoadDelay to item 3 of argv as real
  tell application "Google Chrome"
    if not (exists front window) then
      make new window
    end if
    set targetWindow to missing value
    set targetTab to missing value
    set targetIndex to 0
    repeat with w in windows
      repeat with i from 1 to count tabs of w
        set candidateTab to tab i of w
        try
          if (URL of candidateTab as string) starts with channelUrl then
            set targetWindow to w
            set targetTab to candidateTab
            set targetIndex to i
            exit repeat
          end if
        end try
      end repeat
      if targetTab is not missing value then exit repeat
    end repeat
    if targetTab is missing value then
      set targetWindow to front window
      tell targetWindow
        set targetTab to make new tab at end of tabs with properties {URL:channelUrl}
        set targetIndex to count tabs
        set active tab index to targetIndex
      end tell
      set index of targetWindow to 1
      delay firstLoadDelay
    else
      tell targetWindow to set active tab index to targetIndex
      set index of targetWindow to 1
      delay 0.2
    end if
    return execute targetTab javascript jsCode
  end tell
end run
'''

VISIBLE_MESSAGES_JS = r'''
(() => {
  const selectors = [
    '[id^="chat-messages-"]',
    '[data-list-item-id*="chat-messages"]',
    'li[class*="messageListItem"]',
    'div[class*="messageListItem"]'
  ];
  const nodes = Array.from(document.querySelectorAll(selectors.join(',')));
  const messages = nodes.map((el) => {
    const text = (el.innerText || el.textContent || '')
      .replace(/\u202f/g, ' ')
      .replace(/\n{3,}/g, '\n')
      .trim();
    const id = el.id || el.getAttribute('data-list-item-id') || '';
    return { id, text };
  }).filter((row) => row.text);
  const channelId = (location.pathname.match(/\/channels\/[^/]+\/([^/]+)/) || [])[1] || '';
  return JSON.stringify({
    url: location.href,
    title: document.title,
    channel_id: channelId,
    logged_in: !document.body.innerText.includes('Welcome back!'),
    messages: messages.slice(-160)
  });
})()
'''

FULL_TS_RE = re.compile(
    r"(?P<weekday>[A-Za-z]+),\s+(?P<month>[A-Za-z]+)\s+(?P<day>\d{1,2}),\s+"
    r"(?P<year>\d{4})\s+at\s+(?P<hour>\d{1,2}):(?P<minute>\d{2})\s+(?P<ampm>AM|PM)",
    re.I,
)
RELATIVE_TS_RE = re.compile(r"\b(?P<day>Today|Yesterday)\s+at\s+(?P<hour>\d{1,2}):(?P<minute>\d{2})\s+(?P<ampm>AM|PM)", re.I)


def request_stop(signum: int, frame: object) -> None:
    global STOP
    STOP = True


def in_market_window(tz_name: str) -> bool:
    now = dt.datetime.now(ZoneInfo(tz_name))
    if now.weekday() >= 5:
        return False
    start = now.replace(hour=9, minute=25, second=0, microsecond=0)
    end = now.replace(hour=16, minute=10, second=0, microsecond=0)
    return start <= now <= end


def write_latest_json(path: Path, record: dict[str, Any]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    tmp_path = path.with_suffix(path.suffix + ".tmp")
    tmp_path.write_text(json.dumps(record, sort_keys=True, separators=(",", ":")) + "\n", encoding="utf-8")
    tmp_path.replace(path)


def read_channel_snapshot(channel_url: str, timeout: int = 15, first_load_delay: float = 4.0) -> dict[str, Any]:
    try:
        result = subprocess.run(
            ["osascript", "-e", APPLE_SCRIPT, channel_url, VISIBLE_MESSAGES_JS, str(first_load_delay)],
            text=True,
            capture_output=True,
            timeout=timeout,
            check=False,
        )
    except subprocess.TimeoutExpired as exc:
        raise RuntimeError("Chrome AppleScript read timed out") from exc
    if result.returncode != 0:
        raise RuntimeError(result.stderr.strip() or result.stdout.strip() or "Chrome AppleScript read failed")
    try:
        return json.loads(result.stdout)
    except json.JSONDecodeError as exc:
        raise RuntimeError(f"Chrome returned non-JSON output: {result.stdout[:500]}") from exc


def read_channel_snapshot_with_retries(
    channel_url: str,
    timeout: int,
    first_load_delay: float,
    retries: int,
) -> dict[str, Any]:
    last_error: Exception | None = None
    for attempt in range(max(1, retries + 1)):
        try:
            return read_channel_snapshot(channel_url, timeout=timeout, first_load_delay=first_load_delay)
        except Exception as exc:  # noqa: BLE001
            last_error = exc
            if attempt < retries:
                time.sleep(1)
    raise RuntimeError(str(last_error) if last_error else "Chrome read failed")


def channel_urls(config: dict[str, Any]) -> list[dict[str, str]]:
    guild_id = str(config.get("discord_guild_id") or config.get("guild_id") or DEFAULT_GUILD_ID)
    channel_ids = config.get("browser_channel_ids") or config.get("alert_channel_ids") or []
    urls = []
    for channel_id in channel_ids:
        clean_channel_id = str(channel_id).strip()
        if clean_channel_id:
            urls.append(
                {
                    "channel_id": clean_channel_id,
                    "url": f"https://discord.com/channels/{guild_id}/{clean_channel_id}",
                }
            )
    return urls


def load_state() -> dict[str, Any]:
    if not BROWSER_STATE_FILE.exists():
        return {"seen_message_keys": []}
    try:
        return json.loads(BROWSER_STATE_FILE.read_text(encoding="utf-8"))
    except json.JSONDecodeError:
        return {"seen_message_keys": []}


def save_state(state: dict[str, Any]) -> None:
    state["updated_at"] = now_iso()
    state["seen_count"] = len(state.get("seen_message_keys") or [])
    write_latest_json(BROWSER_STATE_FILE, state)


def message_key(channel_id: str, message: dict[str, Any]) -> str:
    return str(message.get("id") or stable_hash([channel_id, message.get("text")])[:28])


def detect_author(text: str, author_names: list[str]) -> str:
    upper_text = text.upper()
    for author in author_names:
        if str(author).upper() in upper_text:
            return str(author)
    return ""


def extract_message_timestamp(text: str, tz_name: str = "America/Detroit") -> dt.datetime | None:
    tz = ZoneInfo(tz_name)
    full_match = FULL_TS_RE.search(text)
    if full_match:
        stamp = " ".join(
            [
                full_match.group("month"),
                full_match.group("day"),
                full_match.group("year"),
                f"{full_match.group('hour')}:{full_match.group('minute')}",
                full_match.group("ampm").upper(),
            ]
        )
        try:
            return dt.datetime.strptime(stamp, "%B %d %Y %I:%M %p").replace(tzinfo=tz)
        except ValueError:
            return None
    relative_match = RELATIVE_TS_RE.search(text)
    if relative_match:
        now = dt.datetime.now(tz)
        base_date = now.date()
        if relative_match.group("day").lower() == "yesterday":
            base_date = base_date - dt.timedelta(days=1)
        hour = int(relative_match.group("hour"))
        minute = int(relative_match.group("minute"))
        if relative_match.group("ampm").upper() == "PM" and hour != 12:
            hour += 12
        if relative_match.group("ampm").upper() == "AM" and hour == 12:
            hour = 0
        return dt.datetime.combine(base_date, dt.time(hour, minute), tzinfo=tz)
    return None


def is_recent_message(message: dict[str, Any], max_age_minutes: float, tz_name: str, allow_unknown_time: bool) -> bool:
    timestamp = extract_message_timestamp(str(message.get("text") or ""), tz_name)
    if timestamp is None:
        return allow_unknown_time
    if max_age_minutes <= 0:
        return True
    now = dt.datetime.now(ZoneInfo(tz_name))
    age_seconds = (now - timestamp).total_seconds()
    return -300 <= age_seconds <= max_age_minutes * 60


def filter_candidate_messages(
    messages: list[dict[str, Any]],
    author_names: list[str],
    max_age_minutes: float,
    tz_name: str,
    allow_unknown_time: bool,
) -> list[dict[str, Any]]:
    candidates: list[dict[str, Any]] = []
    current_author = ""
    for message in messages:
        text = str(message.get("text") or "")
        detected_author = detect_author(text, author_names)
        if detected_author:
            current_author = detected_author
        if not detected_author and not current_author:
            continue
        if not is_recent_message(message, max_age_minutes, tz_name, allow_unknown_time):
            continue
        copy = dict(message)
        copy["detected_author"] = detected_author or current_author
        copy["message_timestamp"] = (
            extract_message_timestamp(text, tz_name).isoformat(timespec="seconds")
            if extract_message_timestamp(text, tz_name)
            else ""
        )
        candidates.append(copy)
    return candidates


def append_browser_message(
    channel_id: str,
    channel_url: str,
    message: dict[str, Any],
    raw_records: list[dict[str, Any]],
    mode: str,
) -> None:
    append_jsonl(
        BROWSER_MESSAGES_FILE,
        {
            "event_type": "discord_browser_message",
            "captured_at": now_iso(),
            "channel_id": channel_id,
            "channel_url": channel_url,
            "capture_mode": mode,
            "message_key": message_key(channel_id, message),
            "message_id": message.get("id") or "",
            "message_timestamp": message.get("message_timestamp") or "",
            "detected_author": message.get("detected_author") or "",
            "text_preview": str(message.get("text") or "")[:500],
            "raw_record_keys": [record.get("dedupe_key") for record in raw_records],
        },
    )


def process_browser_messages(
    channel_id: str,
    channel_url: str,
    messages: list[dict[str, Any]],
    mode: str,
    source_prefix: str,
    process_raw: bool,
) -> dict[str, int]:
    state = load_state()
    seen = set(str(item) for item in state.get("seen_message_keys") or [])
    raw_seen = existing_keys(DATA_DIR / "raw_notifications.jsonl")
    counts = {
        "messages_seen": len(messages),
        "messages_new": 0,
        "raw_backfilled": 0,
        "raw_processed": 0,
    }
    source = f"{source_prefix}:{channel_id}"
    for message in messages:
        key = message_key(channel_id, message)
        if key in seen:
            continue
        records = build_raw_records(str(message.get("text") or ""), source)
        if mode != "mark":
            append_browser_message(channel_id, channel_url, message, records, mode)
        counts["messages_new"] += 1
        if mode == "live":
            for record in records:
                raw_key = str(record.get("dedupe_key") or "")
                if raw_key and raw_key not in raw_seen:
                    append_jsonl(DATA_DIR / "raw_notifications.jsonl", record)
                    raw_seen.add(raw_key)
                    counts["raw_backfilled"] += 1
        seen.add(key)
    state["seen_message_keys"] = sorted(seen)
    save_state(state)
    if mode == "live" and process_raw and counts["raw_backfilled"]:
        before = len(read_jsonl(DATA_DIR / "raw_notifications.jsonl"))
        pipeline_counts = process_raw_notifications(read_jsonl(DATA_DIR / "raw_notifications.jsonl"))
        counts["raw_processed"] = pipeline_counts.get("raw_new", 0)
        counts["raw_seen_after_process"] = before
    return counts


def capture_once(
    config: dict[str, Any],
    mode: str,
    max_age_minutes: float,
    timeout: int,
    first_load_delay: float,
    retries: int,
    allow_unknown_time: bool,
    source_prefix: str,
    process_raw: bool,
) -> dict[str, Any]:
    tz_name = str(config.get("timezone") or "America/Detroit")
    author_names = [str(item) for item in config.get("alert_author_names", [])]
    record: dict[str, Any] = {
        "event_type": "discord_browser_capture_health",
        "recorded_at": now_iso(tz_name),
        "mode": mode,
        "status": "ok",
        "channels": [],
        "errors": [],
        "totals": {
            "channels": 0,
            "channels_ok": 0,
            "visible_messages": 0,
            "candidate_messages": 0,
            "messages_new": 0,
            "raw_backfilled": 0,
            "raw_processed": 0,
        },
    }
    urls = channel_urls(config)
    record["totals"]["channels"] = len(urls)
    for item in urls:
        channel_id = item["channel_id"]
        channel_url = item["url"]
        channel_record: dict[str, Any] = {"channel_id": channel_id, "url": channel_url, "status": "ok"}
        try:
            snapshot = read_channel_snapshot_with_retries(
                channel_url,
                timeout=timeout,
                first_load_delay=first_load_delay,
                retries=retries,
            )
            visible = list(snapshot.get("messages") or [])
            candidates = filter_candidate_messages(
                visible,
                author_names=author_names,
                max_age_minutes=max_age_minutes,
                tz_name=tz_name,
                allow_unknown_time=allow_unknown_time,
            )
            counts = process_browser_messages(
                channel_id,
                channel_url,
                candidates,
                mode=mode,
                source_prefix=source_prefix,
                process_raw=process_raw,
            )
            channel_record.update(
                {
                    "title": snapshot.get("title") or "",
                    "visible_messages": len(visible),
                    "candidate_messages": len(candidates),
                    "messages_new": counts["messages_new"],
                    "raw_backfilled": counts["raw_backfilled"],
                    "raw_processed": counts["raw_processed"],
                }
            )
            record["totals"]["channels_ok"] += 1
            record["totals"]["visible_messages"] += len(visible)
            record["totals"]["candidate_messages"] += len(candidates)
            record["totals"]["messages_new"] += counts["messages_new"]
            record["totals"]["raw_backfilled"] += counts["raw_backfilled"]
            record["totals"]["raw_processed"] += counts["raw_processed"]
        except Exception as exc:  # noqa: BLE001
            channel_record["status"] = "error"
            channel_record["reason"] = f"{type(exc).__name__}:{exc}"
            record["errors"].append({"channel_id": channel_id, "reason": channel_record["reason"]})
        record["channels"].append(channel_record)
    if record["errors"]:
        record["status"] = "degraded" if record["totals"]["channels_ok"] else "failed"
    append_jsonl(BROWSER_HEALTH_FILE, record)
    write_latest_json(BROWSER_HEALTH_LATEST_FILE, record)
    return record


def mark_existing(config: dict[str, Any], timeout: int, first_load_delay: float, max_age_minutes: float, allow_unknown_time: bool) -> dict[str, Any]:
    return capture_once(
        config,
        mode="mark",
        max_age_minutes=max_age_minutes,
        timeout=timeout,
        first_load_delay=first_load_delay,
        retries=1,
        allow_unknown_time=allow_unknown_time,
        source_prefix="browser_channel",
        process_raw=False,
    )


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--config", default=str(CONFIG_DIR / "watcher.yaml"))
    parser.add_argument("--mode", choices=["live", "audit"], default="live")
    parser.add_argument("--once", action="store_true")
    parser.add_argument("--interval", type=float, default=None)
    parser.add_argument("--timeout", type=int, default=15)
    parser.add_argument("--retries", type=int, default=1)
    parser.add_argument("--first-load-delay", type=float, default=4.0)
    parser.add_argument("--max-age-minutes", type=float, default=5.0)
    parser.add_argument("--allow-unknown-time", action="store_true")
    parser.add_argument("--mark-existing", action="store_true")
    parser.add_argument("--market-hours-only", action="store_true")
    parser.add_argument("--no-process", action="store_true", help="Append browser raw records but let the live pipeline process them")
    args = parser.parse_args()
    config = load_simple_yaml(Path(args.config))
    interval = float(args.interval if args.interval is not None else config.get("browser_watcher_interval_seconds") or 5.0)
    logger = setup_logging("discord_browser_channel_watcher", LOG_DIR / "discord_browser_channel_watcher.log")
    signal.signal(signal.SIGTERM, request_stop)
    signal.signal(signal.SIGINT, request_stop)

    if args.mark_existing:
        record = mark_existing(config, args.timeout, args.first_load_delay, args.max_age_minutes, True)
        print(json.dumps(record["totals"], sort_keys=True))
        return 0

    while not STOP:
        started = time.monotonic()
        if not args.market_hours_only or in_market_window(str(config.get("timezone") or "America/Detroit")):
            record = capture_once(
                config,
                mode=args.mode,
                max_age_minutes=args.max_age_minutes,
                timeout=args.timeout,
                first_load_delay=args.first_load_delay,
                retries=args.retries,
                allow_unknown_time=args.allow_unknown_time,
                source_prefix="browser_channel",
                process_raw=not args.no_process,
            )
            logger.info("browser_capture status=%s totals=%s errors=%s", record["status"], record["totals"], record["errors"])
            print(json.dumps({"status": record["status"], **record["totals"]}, sort_keys=True))
        if args.once:
            break
        sleep_for = max(0.1, interval - (time.monotonic() - started))
        time.sleep(sleep_for)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())

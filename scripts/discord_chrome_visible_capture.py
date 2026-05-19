#!/usr/bin/env python3
"""Capture visible Steve Discord messages from the active Chrome tab.

This is an optional second capture source for a logged-in Discord web tab. It
does not use Discord APIs or account tokens. Chrome must allow JavaScript from
Apple Events, and the active tab must be a Discord channel.
"""

from __future__ import annotations

import argparse
import datetime as dt
import json
import subprocess
import time
from typing import Any

from backfill_steve_text import build_raw_records, existing_keys, process_audit
from pipeline_common import DATA_DIR, append_jsonl, now_iso, read_jsonl, stable_hash
from run_pipeline_once import process_raw_notifications


STATE_FILE = DATA_DIR / "discord_chrome_visible_capture_state.json"

APPLE_SCRIPT = r'''
on run argv
  set jsCode to item 1 of argv
  tell application "Google Chrome"
    if not (exists front window) then error "No Chrome window is open"
    set currentUrl to URL of active tab of front window
    if currentUrl does not contain "discord.com/channels/" then error "Active Chrome tab is not a Discord channel: " & currentUrl
    return execute active tab of front window javascript jsCode
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
  return JSON.stringify({
    url: location.href,
    title: document.title,
    messages: messages.slice(-120)
  });
})()
'''


def read_visible_discord(timeout: int = 8) -> dict[str, Any]:
    try:
        result = subprocess.run(
            ["osascript", "-e", APPLE_SCRIPT, VISIBLE_MESSAGES_JS],
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


def today_label() -> str:
    now = dt.datetime.now()
    return f"{now:%A}, {now:%B} {now.day}, {now:%Y}"


def filter_visible_messages(snapshot: dict[str, Any], include_history: bool) -> list[dict[str, Any]]:
    messages = list(snapshot.get("messages") or [])
    if include_history:
        return messages
    label = today_label()
    return [row for row in messages if label in str(row.get("text") or "")]


def message_key(message: dict[str, Any]) -> str:
    return str(message.get("id") or stable_hash([message.get("text")])[:24])


def load_state() -> dict[str, Any]:
    if not STATE_FILE.exists():
        return {"seen_message_keys": []}
    try:
        return json.loads(STATE_FILE.read_text(encoding="utf-8"))
    except json.JSONDecodeError:
        return {"seen_message_keys": []}


def save_state(state: dict[str, Any]) -> None:
    STATE_FILE.parent.mkdir(parents=True, exist_ok=True)
    state["updated_at"] = now_iso()
    STATE_FILE.write_text(json.dumps(state, indent=2, sort_keys=True) + "\n", encoding="utf-8")


def mark_messages_seen(messages: list[dict[str, Any]]) -> dict[str, Any]:
    state = load_state()
    seen = set(str(item) for item in state.get("seen_message_keys") or [])
    for message in messages:
        seen.add(message_key(message))
    state["seen_message_keys"] = sorted(seen)
    state["seen_count"] = len(seen)
    save_state(state)
    return state


def unseen_messages(messages: list[dict[str, Any]]) -> list[dict[str, Any]]:
    state = load_state()
    seen = set(str(item) for item in state.get("seen_message_keys") or [])
    return [message for message in messages if message_key(message) not in seen]


def process_visible_snapshot(snapshot: dict[str, Any], mode: str, source: str, include_history: bool = False) -> dict[str, int]:
    messages = filter_visible_messages(snapshot, include_history)
    text = "\n".join(str(row.get("text") or "") for row in messages)
    records = build_raw_records(text, source)
    if mode == "audit":
        counts = process_audit(records)
    else:
        existing = existing_keys(DATA_DIR / "raw_notifications.jsonl")
        new_records = [record for record in records if record["dedupe_key"] not in existing]
        for record in new_records:
            append_jsonl(DATA_DIR / "raw_notifications.jsonl", record)
        counts = process_raw_notifications(read_jsonl(DATA_DIR / "raw_notifications.jsonl"))
        counts["raw_backfilled"] = len(new_records)
    counts["visible_messages"] = len(snapshot.get("messages") or [])
    counts["processed_visible_messages"] = len(messages)
    return counts


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--mode", choices=["audit", "live"], default="audit")
    parser.add_argument("--source", default="chrome_visible_discord")
    parser.add_argument("--interval", type=float, default=5.0)
    parser.add_argument("--once", action="store_true")
    parser.add_argument("--timeout", type=int, default=8)
    parser.add_argument("--include-history", action="store_true", help="Process all visible messages instead of only today's messages")
    parser.add_argument("--include-seen", action="store_true", help="Live mode also processes messages already present in the capture state")
    parser.add_argument("--mark-existing", action="store_true", help="Record current visible messages as seen and exit without processing")
    args = parser.parse_args()

    while True:
        snapshot = read_visible_discord(timeout=args.timeout)
        visible_messages = filter_visible_messages(snapshot, args.include_history)
        if args.mark_existing:
            state = mark_messages_seen(visible_messages)
            print(
                json.dumps(
                    {
                        "url": snapshot.get("url"),
                        "mode": "mark_existing",
                        "visible_messages": len(snapshot.get("messages") or []),
                        "processed_visible_messages": len(visible_messages),
                        "seen_count": state.get("seen_count"),
                    },
                    sort_keys=True,
                )
            )
            return 0
        if args.mode == "live" and not args.include_seen:
            visible_messages = unseen_messages(visible_messages)
            snapshot = dict(snapshot)
            snapshot["messages"] = visible_messages
        counts = process_visible_snapshot(snapshot, args.mode, args.source, include_history=args.include_history)
        if args.mode == "live":
            mark_messages_seen(visible_messages)
        print(json.dumps({"url": snapshot.get("url"), "mode": args.mode, **counts}, sort_keys=True))
        if args.once:
            return 0
        time.sleep(args.interval)


if __name__ == "__main__":
    raise SystemExit(main())

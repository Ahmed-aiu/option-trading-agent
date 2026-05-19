#!/usr/bin/env python3
"""Capture visible Steve Discord messages from the active Chrome tab.

This is an optional second capture source for a logged-in Discord web tab. It
does not use Discord APIs or account tokens. Chrome must allow JavaScript from
Apple Events, and the active tab must be a Discord channel.
"""

from __future__ import annotations

import argparse
import json
import subprocess
import time
from typing import Any

from backfill_steve_text import build_raw_records, existing_keys, process_audit
from pipeline_common import DATA_DIR, append_jsonl, read_jsonl
from run_pipeline_once import process_raw_notifications


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
    result = subprocess.run(
        ["osascript", "-e", APPLE_SCRIPT, VISIBLE_MESSAGES_JS],
        text=True,
        capture_output=True,
        timeout=timeout,
        check=False,
    )
    if result.returncode != 0:
        raise RuntimeError(result.stderr.strip() or result.stdout.strip() or "Chrome AppleScript read failed")
    try:
        return json.loads(result.stdout)
    except json.JSONDecodeError as exc:
        raise RuntimeError(f"Chrome returned non-JSON output: {result.stdout[:500]}") from exc


def process_visible_snapshot(snapshot: dict[str, Any], mode: str, source: str) -> dict[str, int]:
    text = "\n".join(str(row.get("text") or "") for row in snapshot.get("messages") or [])
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
    return counts


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--mode", choices=["audit", "live"], default="audit")
    parser.add_argument("--source", default="chrome_visible_discord")
    parser.add_argument("--interval", type=float, default=5.0)
    parser.add_argument("--once", action="store_true")
    parser.add_argument("--timeout", type=int, default=8)
    args = parser.parse_args()

    while True:
        snapshot = read_visible_discord(timeout=args.timeout)
        counts = process_visible_snapshot(snapshot, args.mode, args.source)
        print(json.dumps({"url": snapshot.get("url"), "mode": args.mode, **counts}, sort_keys=True))
        if args.once:
            return 0
        time.sleep(args.interval)


if __name__ == "__main__":
    raise SystemExit(main())

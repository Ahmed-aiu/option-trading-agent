#!/usr/bin/env python3
"""Read-only Accessibility snapshot for visible Discord text.

This does not click, type, send, or call Discord APIs. It only asks macOS
Accessibility for currently visible static text in the Discord desktop app.
"""

from __future__ import annotations

import argparse
import subprocess


SCRIPT = r'''
on collectText(e)
  set outText to ""
  tell application "System Events"
    try
      set roleName to role of e
    on error
      set roleName to ""
    end try
    if roleName is "AXStaticText" then
      try
        set outText to outText & value of e & linefeed
      end try
    end if
    try
      repeat with child in UI elements of e
        set outText to outText & my collectText(child)
      end repeat
    end try
  end tell
  return outText
end collectText

tell application "System Events"
  if not (exists process "Discord") then
    return "ERROR: Discord process is not running"
  end if
  tell process "Discord"
    set output to ""
    repeat with w in windows
      set output to output & my collectText(w)
    end repeat
    return output
  end tell
end tell
'''


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--contains", help="Only print lines containing this text")
    parser.add_argument("--limit", type=int, default=80)
    args = parser.parse_args()
    result = subprocess.run(
        ["osascript", "-e", SCRIPT],
        text=True,
        capture_output=True,
        timeout=15,
        check=False,
    )
    if result.returncode != 0:
        print(result.stderr.strip() or "Accessibility read failed")
        return result.returncode
    lines: list[str] = []
    seen: set[str] = set()
    for raw_line in result.stdout.splitlines():
        line = " ".join(raw_line.split()).strip()
        if not line or line in seen:
            continue
        if args.contains and args.contains.lower() not in line.lower():
            continue
        seen.add(line)
        lines.append(line)
    for line in lines[-args.limit :]:
        print(line)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())

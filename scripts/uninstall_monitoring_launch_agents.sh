#!/bin/sh
set -eu

for label in ai.openclaw.discord-browser-watcher ai.openclaw.pipeline-health-monitor ai.openclaw.nightly-review
do
  plist="$HOME/Library/LaunchAgents/$label.plist"
  launchctl bootout "gui/$(id -u)" "$plist" >/dev/null 2>&1 || true
  rm -f "$plist"
  echo "Uninstalled $label"
done

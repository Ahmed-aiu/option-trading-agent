#!/bin/sh
set -eu

LABEL="ai.openclaw.trading-alert-watcher"
PLIST_DST="$HOME/Library/LaunchAgents/$LABEL.plist"

launchctl bootout "gui/$(id -u)" "$PLIST_DST" >/dev/null 2>&1 || true
rm -f "$PLIST_DST"

echo "Uninstalled $LABEL"

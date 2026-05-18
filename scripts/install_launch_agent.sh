#!/bin/sh
set -eu

PROJECT_DIR="$(CDPATH= cd -- "$(dirname -- "$0")/.." && pwd)"
LABEL="ai.openclaw.trading-alert-watcher"
PLIST_SRC="$PROJECT_DIR/launchd/$LABEL.plist"
PLIST_DST="$HOME/Library/LaunchAgents/$LABEL.plist"
PYTHON_BIN="${PYTHON_BIN:-$(command -v python3)}"

mkdir -p "$HOME/Library/LaunchAgents" "$PROJECT_DIR/logs"
sed \
  -e "s|__PROJECT_DIR__|$PROJECT_DIR|g" \
  -e "s|__PYTHON_BIN__|$PYTHON_BIN|g" \
  "$PLIST_SRC" > "$PLIST_DST"

launchctl bootout "gui/$(id -u)" "$PLIST_DST" >/dev/null 2>&1 || true
launchctl bootstrap "gui/$(id -u)" "$PLIST_DST"
launchctl enable "gui/$(id -u)/$LABEL"

echo "Installed $PLIST_DST"
echo "Status: launchctl print gui/$(id -u)/$LABEL"
echo "Logs:"
echo "  $PROJECT_DIR/logs/live_pipeline.launchd.out.log"
echo "  $PROJECT_DIR/logs/live_pipeline.launchd.err.log"

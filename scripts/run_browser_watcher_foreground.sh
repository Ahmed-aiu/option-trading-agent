#!/bin/sh
set -eu

PROJECT_DIR="$(CDPATH= cd -- "$(dirname -- "$0")/.." && pwd)"
cd "$PROJECT_DIR"
mkdir -p logs
BROWSER_WATCHER_INTERVAL_SECONDS="${BROWSER_WATCHER_INTERVAL_SECONDS:-5}"

echo "Seeding current visible Discord history as already seen..."
python3 scripts/discord_browser_channel_watcher.py --mark-existing --max-age-minutes 0

echo "Starting foreground Discord browser watcher. Keep this Terminal session open."
echo "Browser polling interval: ${BROWSER_WATCHER_INTERVAL_SECONDS}s"
echo "Log: $PROJECT_DIR/logs/discord_browser_watcher.foreground.log"
exec caffeinate -dimsu python3 scripts/discord_browser_channel_watcher.py \
  --mode live \
  --interval "$BROWSER_WATCHER_INTERVAL_SECONDS" \
  --max-age-minutes 5 \
  --timeout 30 \
  --retries 2 \
  --market-hours-only \
  >> "$PROJECT_DIR/logs/discord_browser_watcher.foreground.log" 2>&1

#!/bin/sh
set -eu

PROJECT_DIR="$(CDPATH= cd -- "$(dirname -- "$0")/.." && pwd)"
PYTHON_BIN="${PYTHON_BIN:-$(command -v python3)}"

install_one() {
  label="$1"
  plist_src="$PROJECT_DIR/launchd/$label.plist"
  plist_dst="$HOME/Library/LaunchAgents/$label.plist"
  mkdir -p "$HOME/Library/LaunchAgents" "$PROJECT_DIR/logs"
  sed \
    -e "s|__PROJECT_DIR__|$PROJECT_DIR|g" \
    -e "s|__PYTHON_BIN__|$PYTHON_BIN|g" \
    "$plist_src" > "$plist_dst"
  launchctl bootout "gui/$(id -u)" "$plist_dst" >/dev/null 2>&1 || true
  launchctl bootstrap "gui/$(id -u)" "$plist_dst"
  launchctl enable "gui/$(id -u)/$label"
  echo "Installed $label"
}

install_one "ai.openclaw.pipeline-health-monitor"
install_one "ai.openclaw.nightly-review"

if [ "${INSTALL_BROWSER_WATCHER_LAUNCH_AGENT:-0}" = "1" ]; then
  install_one "ai.openclaw.discord-browser-watcher"
else
  echo "Skipped ai.openclaw.discord-browser-watcher LaunchAgent; run it in a foreground Terminal session instead."
fi

echo "Status:"
echo "  launchctl print gui/$(id -u)/ai.openclaw.pipeline-health-monitor"
echo "  launchctl print gui/$(id -u)/ai.openclaw.nightly-review"
echo "Foreground browser watcher:"
echo "  scripts/run_browser_watcher_foreground.sh"

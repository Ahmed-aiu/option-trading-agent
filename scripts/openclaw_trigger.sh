#!/bin/sh
set -eu

PROJECT_DIR="$(CDPATH= cd -- "$(dirname -- "$0")/.." && pwd)"
DECISIONS_FILE="$PROJECT_DIR/data/trade_decisions.jsonl"
WORKSPACE_DIR="$HOME/.openclaw/workspace/trading_alerts"
SUMMARY_FILE="$WORKSPACE_DIR/latest_trade_decision.md"

if [ "$#" -lt 1 ]; then
  echo "usage: $0 '<decision-json-or-path>'" >&2
  exit 2
fi

INPUT="$1"
mkdir -p "$PROJECT_DIR/data" "$WORKSPACE_DIR"
touch "$DECISIONS_FILE"

python3 - "$INPUT" "$DECISIONS_FILE" "$SUMMARY_FILE" <<'PY'
import json
import sys
from pathlib import Path

raw_arg, decisions_file, summary_file = sys.argv[1:4]
candidate = Path(raw_arg)
if candidate.exists():
    text = candidate.read_text(encoding="utf-8").strip()
else:
    text = raw_arg.strip()
decision = json.loads(text)
source_key = decision.get("source_dedupe_key")
decisions_path = Path(decisions_file)
already_present = False
for line in decisions_path.read_text(encoding="utf-8").splitlines():
    if not line.strip():
        continue
    try:
        existing = json.loads(line)
    except json.JSONDecodeError:
        continue
    if source_key and existing.get("source_dedupe_key") == source_key:
        already_present = True
        break
if not already_present:
    with decisions_path.open("a", encoding="utf-8") as handle:
        handle.write(json.dumps(decision, sort_keys=True, separators=(",", ":")) + "\n")

order = decision.get("would_place_order") or {}
summary = f"""# Trading Alert Decision

Time: {decision.get('decided_at', '')}
Source: {decision.get('source_dedupe_key', '')}
Ticker: {decision.get('ticker', '')}
Side: {decision.get('side', '')}
Allowed: {decision.get('allowed', False)}
Reason: {decision.get('reason', '')}
Raw Alert: {decision.get('raw_text', '')}
Would-place order: {json.dumps(order, sort_keys=True)}
Risk config: {json.dumps(decision.get('risk_config', {}), sort_keys=True)}
"""
Path(summary_file).write_text(summary, encoding="utf-8")
PY

if command -v openclaw >/dev/null 2>&1; then
  openclaw status >/dev/null 2>&1 || true
fi

echo "Wrote $DECISIONS_FILE"
echo "Wrote $SUMMARY_FILE"

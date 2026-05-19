#!/bin/sh
set -eu

PROJECT_DIR="$(CDPATH= cd -- "$(dirname -- "$0")/.." && pwd)"
ARCHIVE_DIR="$PROJECT_DIR/data/archive/$(date +%Y%m%d-%H%M%S)"
mkdir -p "$ARCHIVE_DIR"

for file in \
  raw_notifications.jsonl \
  discord_text_backfills.jsonl \
  parsed_alerts.jsonl \
  rejected_alerts.jsonl \
  trade_decisions.jsonl \
  orders_paper.jsonl \
  processed_notifications.jsonl \
  shadow_option_positions.jsonl \
  option_quote_snapshots.jsonl \
  steve_option_exits.jsonl \
  steve_approval_cards.jsonl \
  steve_approval_actions.jsonl \
  steve_close_reports.jsonl \
  steve_auto_buy_reports.jsonl \
  human_paper_positions.jsonl \
  human_paper_exits.jsonl \
  option_validation_errors.jsonl \
  daily_option_summaries.jsonl
do
  if [ -f "$PROJECT_DIR/data/$file" ]; then
    mv "$PROJECT_DIR/data/$file" "$ARCHIVE_DIR/$file"
  fi
  : > "$PROJECT_DIR/data/$file"
done

echo "Archived runtime JSONL files to $ARCHIVE_DIR"

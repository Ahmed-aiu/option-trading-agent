# Architecture

## Purpose

This project validates Steve-style Discord option alerts without auto-trading. It captures local Discord notifications, parses option alerts, enriches them with Alpaca-first market data, sends Telegram approval prompts, and records local paper outcomes.

## Boundaries

- Paper and validation first.
- Human approval required for human paper entries.
- Alpaca paper order submission is disabled unless explicitly enabled with `OPENCLAW_ENABLE_PAPER_ORDERS=true`.
- Runtime files are append-only JSONL ledgers for auditability.
- Missing data is logged instead of guessed.

## Pipeline

```text
notification_watcher.py
  captures matching macOS Discord notifications

run_pipeline_once.py
  dedupes raw records and routes parsed entries/exits

parse_alert.py
  parses option buys like "#QQQ May 15 710 put @ 5.86 Bought 4 #hedge"
  parses exits like "sold 2 @ 4.11"

option_validation.py
  creates shadow buy-all positions
  appends option quote snapshots
  computes validation metrics and daily summaries
  applies local paper exit rules

steve_trade_bot.py
  sends Telegram approval cards
  accepts group/owner replies
  creates human paper positions

alpaca_options.py
  builds OCC option symbols
  fetches Alpaca option/stock data
  optionally attempts paper option order submission
```

`run_live_pipeline.py` runs the whole loop continuously and also polls Telegram replies.

## Data Model

All state is append-only JSONL. This makes the system easy to debug with `tail`, `jq`, or one-off scripts.

Core ledgers:

- `raw_notifications.jsonl`: captured local notifications.
- `processed_notifications.jsonl`: raw notification dedupe ledger.
- `parsed_alerts.jsonl`: normalized alerts and exits.
- `shadow_option_positions.jsonl`: Steve buy-all validation positions.
- `option_quote_snapshots.jsonl`: option and underlying snapshots.
- `steve_option_exits.jsonl`: parsed Steve closes matched to shadow positions.
- `steve_approval_cards.jsonl`: Telegram card audit.
- `steve_approval_actions.jsonl`: Telegram reply audit.
- `steve_close_reports.jsonl`: Telegram close-report audit.
- `human_paper_positions.jsonl`: approved human paper entries.
- `human_paper_exits.jsonl`: local paper exits from targets, stops, or Steve catch-up.
- `orders_paper.jsonl`: Alpaca paper order attempts or blocked attempts.

## Exit Logic

Approved human paper positions carry an `exit_plan`:

- `contracts=1`: one tranche at +80%.
- `contracts>1`: `floor(total / 2)` at +80%, `floor(remaining / 2)` at +120%, rest at +200%.

When Steve sends partial closes, the system treats Steve's closed contract count as cumulative. It only records a local paper exit when Steve's cumulative closed amount is greater than the local paper amount already closed.

## Configuration

Tracked config:

- `config/broker.yaml`
- `config/parser_patterns.yaml`
- `config/risk.yaml`
- `config/watcher.example.yaml`

Ignored local config:

- `config/watcher.yaml`
- `.env.local`

To set up a new machine, copy `config/watcher.example.yaml` to `config/watcher.yaml`, then fill in local Discord channel IDs and alert author names.

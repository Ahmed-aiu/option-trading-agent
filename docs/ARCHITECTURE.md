# Architecture

## Purpose

This project validates Steve-style Discord option alerts with local paper trading. It captures local Discord notifications, parses option alerts, enriches them with Alpaca-first market data, routes hedge alerts to Telegram approval, auto-routes non-hedge alerts to paper trading, and records local paper outcomes.

## Boundaries

- Paper and validation first.
- Non-hedge option alerts are allowed to auto-enter paper trades.
- Hedge option alerts require human approval because they may not make sense without an existing position to hedge.
- Alpaca paper order submission is disabled unless explicitly enabled with `OPENCLAW_ENABLE_PAPER_ORDERS=true`.
- Broker-side paper buys can be attempted when enabled; broker-side paper sells are not yet wired as the source of truth.
- Runtime files are append-only JSONL ledgers for auditability.
- Missing data is logged instead of guessed.
- Discord capture is local notification/browser reading only. The system does not use Discord user tokens or private Discord APIs.

## Pipeline

```text
notification_watcher.py
  captures matching macOS Discord notifications

discord_browser_channel_watcher.py
  fallback-captures visible Steve messages from logged-in Chrome Discord channel tabs

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
  creates approved paper positions
  creates auto paper positions for non-hedge alerts
  sends auto-buy and close reports

alpaca_options.py
  builds OCC option symbols
  fetches Alpaca option/stock data
  optionally attempts paper option order submission

pipeline_health_monitor.py
  pinpoints failures across notification, browser, raw, parse, routing, Telegram, and broker stages

nightly_review.py
  uses browser Discord truth after close to reconcile the full day and produce improvement actions
```

`run_live_pipeline.py` runs the whole loop continuously and also polls Telegram replies.

## Capture Sources

The pipeline intentionally keeps more than one capture source because macOS and Discord notification behavior is not stable enough to trust blindly.

- `notification_watcher.py` reads local macOS Notification Center records when macOS permits it.
- `discord_browser_channel_watcher.py` reads visible logged-in Chrome Discord channel tabs through Apple Events.
- Both sources write normalized raw records to `raw_notifications.jsonl`.
- Downstream dedupe is based on canonical option identity and source dedupe keys so the same alert can arrive from both methods without creating duplicate positions.

The nightly review computes a capture-method scorecard:

- matched Steve truth events
- capture rate
- average/max latency
- same-source duplicates
- cross-source duplicates
- recommended primary source and browser polling interval

Use the scorecard rather than intuition when changing capture priority or polling frequency.

## Data Model

All state is append-only JSONL. This makes the system easy to debug with `tail`, `jq`, or one-off scripts.

Core ledgers:

- `raw_notifications.jsonl`: captured local notifications.
- `discord_browser_messages.jsonl`: browser-visible Steve messages and derived raw keys.
- `discord_browser_health.jsonl`: browser capture health history.
- `processed_notifications.jsonl`: raw notification dedupe ledger.
- `parsed_alerts.jsonl`: normalized alerts and exits.
- `shadow_option_positions.jsonl`: Steve buy-all validation positions.
- `option_quote_snapshots.jsonl`: option and underlying snapshots.
- `steve_option_exits.jsonl`: parsed Steve closes matched to shadow positions.
- `steve_approval_cards.jsonl`: Telegram card audit.
- `steve_approval_actions.jsonl`: Telegram reply audit.
- `steve_auto_buy_reports.jsonl`: Telegram auto paper buy report audit.
- `steve_close_reports.jsonl`: Telegram close-report audit.
- `human_paper_positions.jsonl`: approved human paper entries.
- `human_paper_exits.jsonl`: local paper exits from targets, stops, or Steve catch-up.
- `orders_paper.jsonl`: Alpaca paper order attempts or blocked attempts.
- `pipeline_health_checks.jsonl`: exact stage health checks.
- `pipeline_health_alerts.jsonl`: Telegram health alert delivery audit.
- `nightly_review_reports.jsonl`: post-market review summaries and recommended improvements.

## Exit Logic

Paper positions currently carry an `exit_plan`:

- `contracts=1`: one tranche at +80%.
- `contracts>1`: `floor(total / 2)` at +80%, `floor(remaining / 2)` at +120%, rest at +200%.

The default stop is -35% for percent-risk entries. When the stop is hit, the local exit manager closes all remaining contracts in the paper ledger.

When Steve sends partial closes, the system treats Steve's closed contract count as cumulative. It only records a local paper exit when Steve's cumulative closed amount is greater than the local paper amount already closed.

This exit logic is intentionally conservative for short-dated options. It may leave gains on the table for longer-dated swing contracts, so future changes should be made through an explicit exit policy per position rather than changing the global target ladder loosely.

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

## LLM/Codex Operating Context

Human readers should start with `README.md` and `docs/OPERATIONS.md`.

LLM agents should start with:

- `AGENTS.md` for hard safety rules.
- `SKILL.md` for the recursive nightly improvement loop.
- This architecture document for module boundaries and ledgers.

Any automated improvement should preserve paper-only execution, add or update tests, and leave enough JSONL/Markdown audit data to explain the next day's behavior.

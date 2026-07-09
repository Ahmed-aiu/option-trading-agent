# Architecture

## Purpose

This project validates Steve-style Discord option alerts with local paper trading. It captures local Discord notifications and browser-visible Discord messages, parses option entries/exits, enriches them with Alpaca-first market data, auto-routes fresh unambiguous option buys to local paper trading when guardrails pass, falls back to owner-DM Telegram approval when context or price quality is not good enough, and records local paper plus broker-audit outcomes.

## Boundaries

- Paper and validation first.
- Fresh unambiguous option buys, including `#hedge`, are allowed to auto-enter paper trades when the auto-entry guard passes.
- Telegram approval is the fallback for guard-blocked, context-incomplete, ambiguous, or otherwise uncertain option alerts.
- Alpaca paper order submission is disabled unless explicitly enabled with `OPENCLAW_ENABLE_PAPER_ORDERS=true`.
- Broker-side paper buys and sells can be attempted when enabled, but local policy state, broker-fill state, and Steve-alert-price state remain separate ledgers.
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
  sends owner-DM Telegram approval cards when guardrails block auto-entry
  accepts owner replies only
  creates approved paper positions
  creates auto paper positions for guard-passing option buys
  sends owner operational reports and executive filled-trade summaries

alpaca_options.py
  builds OCC option symbols
  fetches Alpaca option/stock data
  optionally attempts Alpaca paper option buy/sell order submission

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

Machine availability is a separate root cause from pipeline speed. If the laptop is asleep/off during market open, browser history can backfill Steve messages after startup for audit, but any resulting late capture should be classified as startup/backfill latency rather than a live-running pipeline delay.

## Data Model

All state is append-only JSONL. This makes the system easy to debug with `tail`, `jq`, or one-off scripts.

Core ledgers:

- `raw_notifications.jsonl`: captured local notifications.
- `discord_browser_messages.jsonl`: browser-visible Steve messages and derived raw keys.
- `discord_browser_health.jsonl`: browser capture health history.
- `discord_browser_health_latest.json`: latest browser capture liveness state.
- `processed_notifications.jsonl`: raw notification dedupe ledger.
- `parsed_alerts.jsonl`: normalized alerts and exits.
- `rejected_alerts.jsonl`: deterministic parser, stale-alert, or pipeline rejections.
- `shadow_option_positions.jsonl`: Steve buy-all validation positions.
- `option_quote_snapshots.jsonl`: option and underlying snapshots.
- `option_tracking_state.json`: latest/high/low/threshold state for open paper positions.
- `steve_option_exits.jsonl`: parsed Steve closes matched to shadow positions.
- `steve_approval_cards.jsonl`: Telegram card audit.
- `steve_approval_actions.jsonl`: Telegram reply audit.
- `steve_auto_buy_reports.jsonl`: Telegram auto paper buy report audit.
- `steve_broker_order_reports.jsonl`: Telegram broker-status report audit.
- `steve_close_reports.jsonl`: Telegram close-report audit.
- `human_paper_positions.jsonl`: approved human paper entries.
- `human_paper_exits.jsonl`: local paper exits from targets, stops, or Steve catch-up.
- `orders_paper.jsonl`: Alpaca paper order attempts or blocked attempts.
- `broker_order_status_reports.jsonl`: terminal broker order status and fill facts.
- `pipeline_health_checks.jsonl`: exact stage health checks.
- `pipeline_health_alerts.jsonl`: Telegram health alert delivery audit.
- `daily_pl_reports.jsonl`: local daily paper P/L report audit.
- `steve_alert_pl_reports.jsonl`: P/L using Steve alert prices.
- `broker_fill_pl_reports.jsonl`: P/L using actual broker fills.
- `data_hygiene_reports.jsonl`: storage scorecards and compaction reports.
- `nightly_review_reports.jsonl`: post-market review summaries and recommended improvements.

## Traceability

For one alert, follow this path:

1. Browser truth: `data/discord_browser_messages.jsonl` and the nightly `truth_events`.
2. Raw capture: `data/raw_notifications.jsonl` by `dedupe_key`, `source`, `notification_timestamp`, and `captured_at`.
3. Parser output: `data/parsed_alerts.jsonl` by `source_dedupe_key`.
4. Shadow validation: `data/shadow_option_positions.jsonl` by `canonical_entry_key` and `validation_id`.
5. Route decision: `data/steve_auto_buy_reports.jsonl` for auto entries, or `data/steve_approval_cards.jsonl` plus `data/steve_approval_actions.jsonl` for approval fallback.
6. Local paper state: `data/human_paper_positions.jsonl` and `data/human_paper_exits.jsonl`.
7. Broker audit: `data/orders_paper.jsonl` and `data/broker_order_status_reports.jsonl`.
8. Daily reconciliation: `data/nightly_reviews/YYYY-MM-DD.json` issue list, capture scorecard, ledger duplicate scorecard, and P/L summaries.

New code should preserve or improve those join keys instead of adding anonymous records. Prefer canonical keys over raw text when deduping, and include rollback-friendly evidence in nightly issues before changing behavior.

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
- `docs/OPERATIONS.md` for local commands, LaunchAgents, and health checks.
- `docs/GITHUB_PUBLISHING.md` before committing or pushing.

Any automated improvement should preserve paper-only execution, add or update tests, and leave enough JSONL/Markdown audit data to explain the next day's behavior.

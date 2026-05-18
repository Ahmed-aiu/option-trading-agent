# Trading Alert Executor

Local macOS capture and paper-trading decision pipeline for Discord desktop stock and option alert notifications.

This project reads local macOS notification artifacts when macOS permits it, stores every matching raw Discord notification before parsing, parses strict trade-alert formats into JSON, runs a paper-only risk gate, and writes local audit files for OpenClaw.

## What It Does

- Captures recent Discord notifications from local macOS Notification Center databases when readable.
- Provides a read-only Accessibility snapshot fallback for troubleshooting visible UI state.
- Stores raw notification records in append-only JSONL.
- Parses deterministic stock-alert patterns into structured JSON.
- Rejects ambiguous alerts and missing-risk alerts.
- Writes paper-only trade decisions and OpenClaw markdown summaries.
- Sends Steve-style option alerts to a dedicated Telegram approval bot.
- Tracks shadow buy-all option outcomes and human-approved local paper trades.
- Replays JSONL files for parser/risk testing.

## What It Does Not Do

- Does not use a Discord user token.
- Does not automate a normal Discord user account.
- Does not call Discord private or internal APIs.
- Does not send messages or actions back to Discord.
- Does not place live trades.
- Does not submit broker orders unless Alpaca paper orders are explicitly enabled.
- Does not use an LLM as a trade trigger.

## Safety Boundary

This is a local paper/simulation pipeline. It is designed to fail closed: if an alert is ambiguous, incomplete, duplicated, too old, unsupported, or outside configured risk limits, it is rejected or blocked. Broker execution is restricted to Alpaca paper endpoints and disabled by default.

## Repo Map

- `AGENTS.md`: short onboarding notes for Codex/LLM coding agents.
- `docs/ARCHITECTURE.md`: pipeline, ledgers, and module responsibilities.
- `docs/OPERATIONS.md`: local runbook for capture, Telegram approvals, and reports.
- `docs/GITHUB_PUBLISHING.md`: safe GitHub publishing checklist.
- `.env.example`: local environment variable template.
- `config/watcher.example.yaml`: sanitized watcher template. Copy it to ignored `config/watcher.yaml` for local use.

## macOS Setup

1. Enable Discord notifications in macOS System Settings.
2. In Discord, set the paid alert channel notification setting to `All Messages`.
3. Disable Focus/Do Not Disturb while testing.
4. Allow notification previews so the notification body is stored locally.
5. Keep the Mac awake while the watcher is running.
6. If DB reads are blocked, grant Full Disk Access to Terminal/Codex and rerun the probe.
7. Use Accessibility permission only if you intentionally test the fallback UI snapshot mode.

Recent macOS versions privacy-protect Notification Center storage. The probe will tell you if local database access is blocked.

## Quick Test

```sh
cd trading-alert-executor
cp .env.example .env.local
cp config/watcher.example.yaml config/watcher.yaml
python3 scripts/test_pipeline.py
python3 scripts/replay_alerts.py --input tests/sample_alerts.jsonl --expect tests/expected_parsed.jsonl --dry-run
```

Expected local test counts:

- total: 7
- parsed: 4
- rejected: 3
- allowed: 3
- blocked: 1, because short selling is disabled by default

## Notification Probe

```sh
python3 scripts/notification_probe.py --app Discord --last-minutes 30
```

If Discord notifications are readable, output includes timestamp, title, subtitle, and body. If not, the command prints actionable troubleshooting. Do not assume capture works until this command actually shows a Discord notification on this machine.

## Discord UI Fallback

If macOS notifications truncate an alert, the isolated read-only fallback can inspect visible Discord text through Accessibility:

```sh
python3 scripts/discord_ui_readonly_probe.py --contains swing
```

This fallback does not click, type, send messages, read tokens, or call Discord APIs. It only reads currently visible text from the Discord desktop UI, so it is less reliable than Notification Center capture and depends on the channel/message already being visible.

## Watcher

```sh
python3 scripts/notification_watcher.py
```

The watcher polls every second by default and appends matching Discord notifications to:

```text
data/raw_notifications.jsonl
```

It keeps an in-memory dedupe set and reloads previous dedupe keys on restart, so restarting does not reprocess existing raw notifications.

## Parser

Examples supported in v1:

```text
BUY TSLA over 182.50 stop 179.80 target 188
LONG AAPL above 198.50 SL 196.20 TP 203
SELL TSLA below 180 stop 183 target 174
SHORT NVDA under 910 stop 922 target 880
Bought AAPL 198.50 stop 196 target 203
```

Ambiguous examples are rejected:

```text
Watching TSLA here
Could take AAPL if it breaks
TSLA maybe long
BUY GME moon soon
```

## Risk Guard

Risk mode is `paper_only`. The guard only writes decisions to local JSONL and never calls a broker:

```sh
python3 scripts/risk_guard.py --input data/parsed_alerts.jsonl --write
```

Default policy blocks short selling, options, missing stops, stale timestamped alerts, duplicate trades, and more than three allowed trades per day.

## Full Pipeline

Process new raw notifications once through parser, risk guard, OpenClaw summary, and Alpaca paper dry-run audit:

```sh
python3 scripts/run_pipeline_once.py
```

This uses `data/processed_notifications.jsonl` as a ledger so the same raw notification is not processed twice. Stock alerts go through `risk_guard.py`; option alerts go through the Steve option validation and Telegram approval path.

Run capture and processing continuously:

```sh
python3 scripts/run_live_pipeline.py
```

Archive and clear runtime JSONL files before a clean market-hours test:

```sh
scripts/reset_runtime_data.sh
```

Full local test:

```sh
python3 scripts/test_full_pipeline.py
python3 scripts/test_steve_options_mvp.py
```

## Steve Options Approval Bot

The Steve options MVP uses a separate Telegram bot from the existing OpenClaw/EZ Telegram setup.

Set these locally in `.env.local` or your shell:

```sh
STEVE_TRADE_BOT_TOKEN=...
STEVE_TRADE_APPROVAL_CHAT_ID=-100...
STEVE_TRADE_APPROVAL_CHAT_IDS=123456789,-100...
STEVE_TRADE_OWNER_CHAT_ID=...
STEVE_TRADE_OWNER_USER_ID=...
```

`STEVE_TRADE_APPROVAL_CHAT_ID` is the primary Telegram destination. `STEVE_TRADE_APPROVAL_CHAT_IDS` is optional and can add comma-separated destinations, such as your owner DM plus one approval group. Any member who can write in an approval group can approve or skip. Outside configured approval chats, only the configured owner DM is accepted. Messages from any other chat are logged as unauthorized.

To find the group id:

1. Create the new Telegram bot with BotFather.
2. Add it to the specific approval group.
3. Send any message in the group, such as `hello`.
4. Run:

```sh
python3 scripts/steve_trade_bot.py discover-chats
```

5. Use the printed `chat_id` for `STEVE_TRADE_APPROVAL_CHAT_ID` or add multiple values to `STEVE_TRADE_APPROVAL_CHAT_IDS`. Telegram supergroup ids usually look like `-100...`. Use your private chat row for `STEVE_TRADE_OWNER_CHAT_ID` and `sender_user_id` for `STEVE_TRADE_OWNER_USER_ID`.

Legacy names `STEVE_TRADE_APPROVER_CHAT_ID` and `STEVE_TRADE_APPROVER_USER_ID` still work for the old one-person DM setup, but the group setup should use the new names above.

The live pipeline sends approval cards for parsed Steve-style option alerts and polls Telegram replies locally:

```text
skip
buy
buy contracts=1 stop=35% take=80%
buy contracts=1 stop_price=3.80 take_price=6.20
```

Approvals always write a local human paper ledger. Alpaca paper option submission is attempted only when paper credentials and `OPENCLAW_ENABLE_PAPER_ORDERS=true` are configured; otherwise the broker attempt is logged as blocked and local paper tracking continues.

The default local paper exit plan is staged: sell half the approved contracts at +80%, half of the remaining contracts at +120%, and the rest at +200%. For a single contract, the whole contract exits at +80%. Steve partial closes are treated as a cumulative catch-up target: if Steve has closed 2 contracts and the local paper plan already closed 2 or more, no extra sell is recorded; if Steve moves ahead of the local exits, the ledger closes only enough contracts to catch up.

Validation ledgers are append-only JSONL files under `data/`, including:

```text
shadow_option_positions.jsonl
option_quote_snapshots.jsonl
steve_option_exits.jsonl
steve_approval_cards.jsonl
steve_approval_actions.jsonl
steve_close_reports.jsonl
human_paper_positions.jsonl
human_paper_exits.jsonl
daily_option_summaries.jsonl
```

Generate a daily validation report:

```sh
python3 scripts/option_validation.py daily-summary
```

## OpenClaw Trigger

```sh
scripts/openclaw_trigger.sh '{"event_type":"trade_decision","decided_at":"2026-05-08T13:45:25-04:00","source_dedupe_key":"demo","ticker":"TSLA","side":"buy","allowed":true,"reason":"passed_all_risk_checks","raw_text":"BUY TSLA over 182.50 stop 179.80 target 188","would_place_order":{"symbol":"TSLA","side":"buy","notional":100,"order_type":"limit","limit_price":182.5,"time_in_force":"day"}}'
```

This appends locally and writes:

```text
~/.openclaw/workspace/trading_alerts/latest_trade_decision.md
```

If the `openclaw` command is not installed, the script still writes local files.

## Alpaca Paper Adapter

The Alpaca adapter is direct API integration for paper trading only. It refuses any endpoint other than:

```text
https://paper-api.alpaca.markets
```

Credentials are read from `.env.local` or the shell environment:

```sh
APCA_API_BASE_URL=https://paper-api.alpaca.markets
APCA_API_KEY_ID=...
APCA_API_SECRET_KEY=...
OPENCLAW_TRADING_MODE=paper
OPENCLAW_ENABLE_PAPER_ORDERS=false
```

Check paper account access:

```sh
python3 scripts/alpaca_paper_adapter.py check-account
```

Build a paper order payload without submitting:

```sh
python3 scripts/alpaca_paper_adapter.py process-latest --audit
```

Actual paper submission is disabled unless `OPENCLAW_ENABLE_PAPER_ORDERS=true` is set locally. Keep it `false` until dry-run decisions look correct.

Submit the latest allowed decision to Alpaca paper after explicitly enabling paper orders:

```sh
OPENCLAW_ENABLE_PAPER_ORDERS=true python3 scripts/alpaca_paper_adapter.py process-latest --submit
```

Paper order audits are appended to:

```text
data/orders_paper.jsonl
```

## LaunchAgent

Install:

```sh
scripts/install_launch_agent.sh
```

Uninstall:

```sh
scripts/uninstall_launch_agent.sh
```

Status:

```sh
launchctl print gui/$(id -u)/ai.openclaw.trading-alert-watcher
```

## Troubleshooting

- No probe results: generate a fresh Discord notification, then rerun the probe.
- Permission denied diagnostics: grant Full Disk Access to the terminal app running these scripts.
- Empty bodies: allow notification previews in macOS and Discord.
- Watcher captures nothing: set `write_all_discord_notifications: true` temporarily in `config/watcher.yaml`.
- Parser rejects too much: add deterministic patterns to `scripts/parse_alert.py` and samples to `tests/`.
- Replayed alerts blocked as stale: pass `--ignore-age` for offline replay.

## GitHub Publishing

Runtime ledgers, logs, local Telegram/Alpaca secrets, and local Discord channel config are ignored by git. See `docs/GITHUB_PUBLISHING.md` before creating a public or private GitHub repo.

## Next Phase

The next phase should focus on validation reporting and paper exit reconciliation before any stronger automation: cleaner daily P/L summaries, reviewable Telegram exit notifications, and stricter handling for stale option quotes.

# Operations

## Local Setup

```sh
cp .env.example .env.local
cp config/watcher.example.yaml config/watcher.yaml
```

Fill `.env.local` with Telegram and Alpaca paper credentials. Fill `config/watcher.yaml` with the Discord authors and channel IDs to watch.

For Telegram, `STEVE_TRADE_APPROVAL_CHAT_IDS` can contain comma-separated destinations. Use it when cards should go to both the owner DM and an approval group.

## Validate Capture

Generate a fresh Discord notification, then run:

```sh
python3 scripts/notification_probe.py --app Discord --last-minutes 30
```

If no Discord notification appears, check macOS notification permissions, Discord channel notification settings, Focus mode, notification previews, and Full Disk Access for the terminal app.

Steve close replies often arrive as short messages like `Closed @ 7.54` or `Sold 2 @ 3.26`, so the watcher should keep `capture_all_author_notifications: true`. That records every notification from configured Steve author names first, then lets the parser/audit layer decide whether the text is a supported entry, exit, or reject.

## Run Once

```sh
python3 scripts/run_live_pipeline.py --once
```

This is the safest smoke test because it uses the same path as the LaunchAgent but exits after one loop.

## Run Continuously

```sh
python3 scripts/run_live_pipeline.py
```

Install as a macOS LaunchAgent:

```sh
scripts/install_launch_agent.sh
```

Check status:

```sh
launchctl print gui/$(id -u)/ai.openclaw.trading-alert-watcher
```

Check that the live loop is not stalled:

```sh
cat data/live_pipeline_heartbeat.json
```

The `recorded_at` timestamp should refresh about every 30 seconds while the LaunchAgent is running.
For historical checks around a missed alert, use the append-only heartbeat history:

```sh
tail -n 20 data/live_pipeline_heartbeats.jsonl
```

If the heartbeat was fresh but `data/raw_notifications.jsonl` has no matching Discord row, the miss happened before parsing/Telegram, usually because Discord/macOS did not emit or persist a notification body for the channel message.

## Backfill Missed Discord Text

When Steve messages are visible in Discord but were missed by macOS notifications, paste the copied Discord text into the audit backfill path:

```sh
pbpaste | python3 scripts/backfill_steve_text.py --mode audit --source short-term-call-outs
```

Audit mode creates parsed alerts, shadow positions, and Steve exit records without sending approval cards or submitting paper orders. Use this for stale missed alerts. The raw copied text is also logged to `data/discord_text_backfills.jsonl`.

Near-real-time browser capture can use live mode only when the text is fresh enough to trade:

```sh
pbpaste | python3 scripts/backfill_steve_text.py --mode live --source browser_poll
```

Live mode writes records into `data/raw_notifications.jsonl`, so normal routing applies: non-hedge alerts can auto-paper-buy, while hedge alerts still request Telegram approval.

## Chrome Visible Discord Capture

For a second live capture source, keep Chrome logged into Discord and enable:

```text
Chrome menu -> View -> Developer -> Allow JavaScript from Apple Events
```

The preferred browser fallback is the multi-channel watcher. It opens/uses the configured Discord channel tabs from `config/watcher.yaml`, reads only visible browser text, and writes fresh Steve messages into the normal raw pipeline.

Before the first live run, mark current visible history as seen so old alerts are not traded:

```sh
python3 scripts/discord_browser_channel_watcher.py --mark-existing --max-age-minutes 0
```

Run one safe live pass:

```sh
python3 scripts/discord_browser_channel_watcher.py --once --mode live --max-age-minutes 5
```

Run continuously during market hours from a foreground Terminal session:

```sh
scripts/run_browser_watcher_foreground.sh
```

This foreground runner uses `caffeinate` and is preferred over LaunchAgent for browser capture because Chrome Apple Events can time out from a background LaunchAgent while succeeding from an interactive Terminal session. It polls every 5 seconds by default; override with `BROWSER_WATCHER_INTERVAL_SECONDS=3 scripts/run_browser_watcher_foreground.sh` only if the nightly capture scorecard shows browser latency is still too high.

The browser watcher writes:

```sh
data/discord_browser_messages.jsonl
data/discord_browser_health.jsonl
data/discord_browser_health_latest.json
```

If it sees a fresh Steve message, it writes a raw record to `data/raw_notifications.jsonl`; the existing parser/router then handles auto paper buys, hedge approvals, and Steve exits.

The nightly review reports a capture-method scorecard comparing browser capture and macOS notifications by coverage, latency, and duplicate rate:

```sh
python3 scripts/nightly_review.py --refresh-browser --print-json
```

Use that scorecard to decide whether browser should stay primary, whether polling needs to be faster, or whether macOS notifications are still adding useful coverage.

The older active-tab smoke test is still available:

```sh
python3 scripts/discord_chrome_visible_capture.py --once --mode audit
```

Run live polling only when the visible channel is current and fresh enough to trade:

```sh
python3 scripts/discord_chrome_visible_capture.py --mark-existing
python3 scripts/discord_chrome_visible_capture.py --mode live --interval 5
```

The `--mark-existing` step seeds `data/discord_chrome_visible_capture_state.json` so the first live loop does not trade older messages already visible in the channel. By default this reader only processes visible messages that contain today's full Discord date label. This avoids accidentally trading old visible history. Use `--include-history` only for audit/backfill work, not live trading.

Run this reader in a foreground terminal session. macOS may allow Chrome Apple Events from Terminal while blocking the same script under LaunchAgent. This reader only sees messages currently present in the active Chrome Discord tab. It is a fallback for missed macOS notifications, not a replacement for an official Discord bot/webhook.

## Pipeline Health Monitor

The health monitor checks each stage independently so a missed Telegram alert has an exact failure point:

```sh
python3 scripts/pipeline_health_monitor.py --once --no-telegram
```

It records:

```sh
data/pipeline_health_checks.jsonl
data/pipeline_health_latest.json
data/pipeline_health_alerts.jsonl
```

Failure codes identify where the miss happened:

- `notification_db_row_not_raw`: macOS stored a Discord notification, but the watcher did not capture it.
- `browser_message_not_raw`: browser saw a Steve message, but no raw pipeline record exists.
- `raw_not_processed`: raw exists, parser/router did not process it.
- `hedge_missing_approval_card`: parsed hedge alert did not create Telegram approval.
- `non_hedge_missing_auto_buy`: parsed non-hedge alert did not create auto paper-buy artifacts.
- `option_exit_not_recorded`: parsed Steve exit did not create an exit record.
- `*_send_failed`: Telegram delivery failed.

Install the health monitor LaunchAgent:

```sh
scripts/install_monitoring_launch_agents.sh
```

Check status:

```sh
launchctl print gui/$(id -u)/ai.openclaw.pipeline-health-monitor
```

The health monitor runs every 10 minutes during market hours. It sends Telegram only on new failures or recoveries unless run manually with `--no-telegram`. The browser watcher should be kept open in foreground Terminal; if it stops or fails, the health monitor reports `browser_health_stale` or `browser_capture_degraded`.

## Nightly Recursive Review

After each weekday market close, run the source-of-truth review:

```sh
python3 scripts/nightly_review.py --refresh-browser --send-telegram --print-json
```

The review reads Chrome Discord channel history directly without feeding old messages back into trading. It compares Steve's visible alerts against raw notifications, parsed alerts, Telegram reports, local paper positions/exits, Alpaca paper audits, broker status reports, and health checks.

Reports are written to:

```text
data/nightly_reviews/YYYY-MM-DD.json
data/nightly_reviews/YYYY-MM-DD.md
data/nightly_review_reports.jsonl
```

The nightly LaunchAgent is installed with the monitoring agents and runs weekdays at 5:30 PM local time:

```sh
scripts/install_monitoring_launch_agents.sh
launchctl print gui/$(id -u)/ai.openclaw.nightly-review
```

Use `SKILL.md` as the Codex operating guide when the nightly report recommends code/config/test changes. The allowed automation goal is to make the next paper-trading session better while preserving paper-only broker guards and rollback context.

Uninstall:

```sh
scripts/uninstall_monitoring_launch_agents.sh
scripts/uninstall_launch_agent.sh
```

## Telegram Approval Commands

Non-hedge Steve option alerts are auto-routed to paper trading. The bot writes a local paper position, attempts the Alpaca paper option order when paper submission is enabled, and sends an `AUTO PAPER BUY` Telegram report to the configured approval destinations.

Hedge alerts still require explicit human approval:

```text
skip
buy
buy contracts=1 stop=35% take=80%
buy contracts=1 stop_price=3.80 take_price=6.20
```

`buy` uses the alert contract count, or 1 if the alert has no count, with the default 35% stop and 80% first take-profit.

The current exit behavior and proposed next exit-policy design are documented in `docs/EXIT_STRATEGY.md`.

## Check Runtime State

```sh
tail -n 5 data/steve_approval_cards.jsonl
tail -n 5 data/steve_approval_actions.jsonl
tail -n 5 data/steve_auto_buy_reports.jsonl
tail -n 5 data/steve_close_reports.jsonl
tail -n 5 data/human_paper_positions.jsonl
tail -n 5 data/human_paper_exits.jsonl
tail -n 5 data/orders_paper.jsonl
```

## Daily Summary

```sh
python3 scripts/option_validation.py daily-summary
```

The latest OpenClaw-readable markdown summary is written to:

```text
~/.openclaw/workspace/trading_alerts/latest_steve_options_validation.md
```

## Storage Hygiene

The pipeline keeps trading facts in JSONL, but high-frequency quote noise is reduced:

- `option_tracking_state.json` keeps the latest, high, low, and threshold-hit facts per position.
- `option_quote_snapshots.jsonl` only appends meaningful quote events: first observation, quote status change, stop/take boundary, milestone hit, 5%+ move by default, or 30-minute checkpoint.
- Tune quote-history growth with `OPENCLAW_QUOTE_SNAPSHOT_MIN_MOVE_PCT` and `OPENCLAW_QUOTE_SNAPSHOT_FORCE_INTERVAL_SECONDS`; `option_tracking_state.json` still keeps latest/high/low facts from observed quotes.
- Nightly review includes a storage scorecard and archive-first compaction. Active ledgers are rewritten only when enough space is saved; originals are gzip archived under `data/archive/`.

Manual checks:

```sh
python3 scripts/data_hygiene.py scorecard --print-json
python3 scripts/data_hygiene.py compact --print-json --min-saved-bytes 262144
python3 scripts/data_hygiene.py compact --apply --print-json --min-saved-bytes 262144
```

## Clean Test Session

```sh
scripts/reset_runtime_data.sh
```

This archives current runtime ledgers under `data/archive/` and recreates empty JSONL files.

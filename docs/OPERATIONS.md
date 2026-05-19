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

Uninstall:

```sh
scripts/uninstall_launch_agent.sh
```

## Telegram Approval Commands

```text
skip
buy
buy contracts=1 stop=35% take=80%
buy contracts=1 stop_price=3.80 take_price=6.20
```

`buy` uses the alert contract count, or 1 if the alert has no count, with the default 35% stop and 80% first take-profit.

## Check Runtime State

```sh
tail -n 5 data/steve_approval_cards.jsonl
tail -n 5 data/steve_approval_actions.jsonl
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

## Clean Test Session

```sh
scripts/reset_runtime_data.sh
```

This archives current runtime ledgers under `data/archive/` and recreates empty JSONL files.

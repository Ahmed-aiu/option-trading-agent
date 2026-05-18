# Agent Notes

This repository is a local, paper-only trading alert validation pipeline. Keep changes deterministic and audit-friendly.

## Safety Rules

- Do not commit `.env.local`, `data/*.jsonl`, `logs/*`, or machine-specific `config/watcher.yaml`.
- Do not print secrets from `.env.local` or broker config. Use `.env.example` for variable names.
- Do not add live-trading behavior without an explicit user request and new tests.
- Broker execution is Alpaca paper-only and must keep refusing non-paper endpoints.
- Discord capture must remain local notification/UI reading only. Do not use Discord user tokens or private APIs.

## Main Flow

```text
macOS Discord notification
 -> scripts/notification_watcher.py
 -> data/raw_notifications.jsonl
 -> scripts/run_pipeline_once.py
 -> scripts/parse_alert.py
 -> scripts/option_validation.py
 -> scripts/steve_trade_bot.py
 -> Telegram human approval
 -> local paper ledgers
```

The continuous process is `scripts/run_live_pipeline.py`. On macOS it is installed through `scripts/install_launch_agent.sh`.

## Key Files

- `scripts/parse_alert.py`: deterministic parser for Steve-style option entries and exits.
- `scripts/option_validation.py`: shadow buy-all ledger, quote snapshots, human paper exit rules, daily reports.
- `scripts/steve_trade_bot.py`: Telegram approval cards, reply parsing, human paper entry ledger.
- `scripts/alpaca_options.py`: Alpaca data enrichment and optional paper option order audit.
- `scripts/alpaca_paper_adapter.py`: stock paper order adapter and paper endpoint guard.
- `config/parser_patterns.yaml`: parser feature flags and ambiguous phrase policy.
- `config/risk.yaml`: conservative stock risk policy. Options are handled by manual approval.
- `config/watcher.example.yaml`: sanitized watcher config template. Local `config/watcher.yaml` is ignored.

## Tests

Run before handing off code:

```sh
python3 scripts/test_pipeline.py
python3 scripts/test_full_pipeline.py
python3 scripts/test_steve_options_mvp.py
python3 -m py_compile scripts/*.py
```

## Current Human Exit Policy

For approved option paper trades:

- 1 contract: sell all at +80%.
- More than 1 contract: sell half at +80%, half of the remainder at +120%, rest at +200%.
- Steve close alerts are cumulative catch-up exits. If local paper exits already closed at least as many contracts as Steve has cumulatively closed, do nothing. If Steve is ahead, close enough to catch up.
- Stop loss applies to all remaining contracts until a future exit manager changes it.

## Runtime Data

Append-only ledgers live under `data/` and are intentionally ignored by git. The important ones are:

- `raw_notifications.jsonl`
- `parsed_alerts.jsonl`
- `shadow_option_positions.jsonl`
- `option_quote_snapshots.jsonl`
- `steve_approval_cards.jsonl`
- `steve_approval_actions.jsonl`
- `steve_close_reports.jsonl`
- `human_paper_positions.jsonl`
- `human_paper_exits.jsonl`
- `daily_option_summaries.jsonl`

Use `scripts/reset_runtime_data.sh` to archive and clear local ledgers before a clean test session.

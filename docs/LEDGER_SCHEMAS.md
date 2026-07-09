# Ledger Schemas And Traceability

This project uses append-only JSONL ledgers under `data/` so every alert can be audited without mutating history. The fields below describe the current core records written by the local paper-only pipeline. Runtime ledgers can contain older rows, but new code should preserve these keys instead of adding anonymous records.

Do not copy raw Discord text, Telegram IDs, broker keys, or `.env.local` values into schema reports or docs. Use field names and counts for diagnostics.

## Alert Lookup Path

For one Steve alert, trace records in this order:

1. Browser source of truth: `discord_browser_messages.jsonl`, or nightly `truth_events` in `nightly_reviews/YYYY-MM-DD.json`.
2. Raw capture: `raw_notifications.jsonl` by `dedupe_key`, `notification_timestamp`, `captured_at`, and capture source fields.
3. Parser output: `parsed_alerts.jsonl` by `source_dedupe_key`.
4. Shadow validation: `shadow_option_positions.jsonl` by `canonical_entry_key`, `validation_id`, and `position_id`.
5. Route decision:
   - Guard-passing fresh option buys call auto-entry and write `steve_auto_buy_reports.jsonl` plus `steve_approval_actions.jsonl` with `action=auto_approved`.
   - Guard-blocked, context-incomplete, ambiguous, or uncertain option alerts fall back to owner-DM approval cards in `steve_approval_cards.jsonl`; owner replies are audited in `steve_approval_actions.jsonl`.
   - Stale raw alerts are rejected before route handling and should be traced through `rejected_alerts.jsonl`.
6. Local paper state: `human_paper_positions.jsonl` by `position_id`, `approval_id`, `canonical_entry_key`, and `source_dedupe_key`.
7. Local paper exits: `human_paper_exits.jsonl` by `exit_id`, `position_id`, and `source_dedupe_key`.
8. Broker paper audit: `orders_paper.jsonl` by `position_id`, `source_dedupe_key`, and `payload.client_order_id` or `response.client_order_id`.
9. Terminal broker status: `broker_order_status_reports.jsonl` by `order_id`, `client_order_id`, and `position_id`.
10. Post-market reconciliation: `nightly_review_reports.jsonl`, then the full JSON/Markdown files under `data/nightly_reviews/`.

Auto-entry remains paper-only. The auto-entry guard must pass before `auto_paper_buy` runs. The current auto path builds the same human-paper position shape as owner approval and records broker audit details; the position row is appended when the paper broker audit reaches a submitted state. If the guard fails, the system does not silently trade; it sends an owner-DM approval card instead.

## Core Ledgers

### `raw_notifications.jsonl`

Purpose: normalized local Discord notification or browser-visible Discord message capture.

Required fields for current rows:

- `event_type`: usually `raw_discord_notification` or `raw_discord_ui_backfill`.
- `dedupe_key`: raw capture key. This becomes `source_dedupe_key` in parsed rows.
- `captured_at`: local capture time.
- `notification_timestamp`: Discord or notification source timestamp when available.
- `source_app`: capture source such as `Discord` or `DiscordUI`.
- `bundle_id`: macOS bundle or browser/backfill marker.
- `title`, `subtitle`, `body`: notification/message text fields. `body` is the parser input.
- `raw`: source-specific evidence dictionary.

Join keys:

- `dedupe_key` -> `parsed_alerts.source_dedupe_key`.
- `raw.source`, `subtitle`, `source_app`, `captured_at`, and `notification_timestamp` are evidence for capture-method scoring and latency analysis.

### `parsed_alerts.jsonl`

Purpose: deterministic parser output for stock alerts, option buys, and option exits.

Required fields for current rows:

- `event_type`: `parsed_trade_alert`.
- `source_dedupe_key`: inherited from `raw_notifications.dedupe_key`; multi-alert messages may add `:option:N`.
- `parsed_at`
- `ticker`
- `side`: `buy`, `sell`, or `exit`.
- `instrument_type`: `option` or `stock`.
- `entry_type`
- `entry_price`, `stop_price`, `target_price`
- `time_in_force`
- `confidence`
- `raw_text`
- `parser_version`
- `notification_timestamp`

Option buy fields:

- `option_type`, `expiration_date`, `strike_price`, `contracts`, `tags`, `primary_tag`, `matched_text`.

Option exit fields:

- `exit_action`, `exit_price`, `contracts`, and any parsed context fields such as `expiration_date`, `option_type`, `strike_price`, `context_entry_price`, and `context_contracts`.

Join keys:

- `source_dedupe_key` -> raw capture, approval cards/actions, auto-buy reports, human positions, orders, and nightly matching.
- Option rows derive `canonical_entry_key` and `validation_id` during validation.

### `shadow_option_positions.jsonl`

Purpose: Steve buy-all validation ledger for every parsed option buy that is not filtered as a synthetic test artifact.

Required fields for current rows:

- `event_type`: `shadow_option_position`.
- `position_id`: deterministic `shadow-...` id.
- `validation_id`
- `canonical_entry_key`
- `created_at`
- `opened_at`
- `source_dedupe_key`
- `ticker`
- `contract_symbol`
- `option_type`
- `expiration_date`
- `strike_price`
- `contracts`
- `remaining_contracts`
- `primary_tag`, `tags`
- `alert_entry_price`
- `bot_entry_price`
- `bot_entry_price_source`
- `shadow_models`
- `raw_text`

Join keys:

- `source_dedupe_key` -> parsed alert and route ledgers.
- `canonical_entry_key` -> duplicate prevention across capture sources.
- `validation_id` -> snapshot and auto-entry identity.
- `position_id` -> option quote snapshots and Steve exit matching.

### `human_paper_positions.jsonl`

Purpose: local paper option entries created through owner approval or guard-passing auto-entry.

Required fields for current rows:

- `event_type`: `human_paper_option_position`.
- `position_id`: deterministic `human-...` id.
- `approval_id`: owner approval card id or auto paper id.
- `canonical_entry_key`
- `opened_at`
- `source_dedupe_key`
- `alert_text`
- `alert_time`
- `alert_price`
- `ticker`
- `contract_symbol`
- `option_type`
- `expiration_date`
- `strike_price`
- `contracts`
- `entry_price`
- `entry_price_source`
- `risk_type`
- `stop_percent`, `take_percent`, `stop_price`, `take_price`
- `used_default_contracts`, `used_default_risk`
- `alert_contracts`
- `exit_plan`
- `exit_plan_notes`
- `status`

Join keys:

- `position_id` -> `human_paper_exits`, `orders_paper`, and `broker_order_status_reports`.
- `approval_id` -> `steve_approval_cards` / `steve_approval_actions`, or auto id in `steve_auto_buy_reports`.
- `canonical_entry_key` -> duplicate prevention.
- `source_dedupe_key` -> parsed alert and nightly route checks.

### `human_paper_exits.jsonl`

Purpose: local paper option exits from target tranches, stops, or Steve cumulative catch-up closes.

Required fields for current rows:

- `event_type`: `human_paper_option_exit`.
- `exit_id`
- `position_id`
- `approval_id`
- `recorded_at`
- `source_dedupe_key`
- `ticker`
- `contract_symbol`
- `option_type`
- `expiration_date`
- `strike_price`
- `position_contracts`
- `reason`
- `contracts`
- `exit_price`
- `entry_price`
- `pnl_percent`, `pnl_dollars`
- `remaining_after_exit`
- `broker_status`, `broker_reason`
- `broker_order_id`, `broker_client_order_id`

Join keys:

- `exit_id` -> deterministic exit trigger and broker exit `client_order_id` trigger component.
- `position_id` -> local paper entry and broker status.
- `source_dedupe_key` -> parsed alert and nightly reconciliation.
- `broker_client_order_id` / `broker_order_id` -> broker audit and terminal broker status when available.

### `orders_paper.jsonl`

Purpose: Alpaca paper order attempts, blocked attempts, duplicate skips, and submitted order audits. This ledger is still paper-only and must continue to refuse non-paper endpoints.

Required fields for current rows:

- `event_type`: `alpaca_option_paper_order_audit` for option orders, or `alpaca_paper_order_audit` for stock paper audit rows.
- `action`: examples include `paper_entry_order` and `paper_exit_order`.
- `recorded_at`
- `status`: `submitted`, `blocked`, or `skipped`.
- `reason`
- `source_dedupe_key`
- `ticker`
- `payload`: sanitized order request dictionary.
- `response`: sanitized broker response dictionary.

Option order fields:

- `position_id`
- `contract_symbol`
- `exit_reason` for paper exit orders.

Join keys:

- `position_id` -> human paper position and exits for option orders.
- `source_dedupe_key` -> parsed alert.
- `payload.client_order_id` and `response.client_order_id` -> broker idempotency and status lookup.
- `response.id` -> broker order id consumed by `broker_order_status_reports.jsonl`.

### `broker_order_status_reports.jsonl`

Purpose: terminal broker order status and fill facts for submitted paper option orders.

Required fields for current rows:

- `event_type`: `broker_order_status_report`.
- `recorded_at`
- `order_id`
- `client_order_id`
- `broker_status`
- `action`
- `exit_reason`
- `position_id`
- `source_dedupe_key`
- `contract_symbol`
- `label`
- `side`
- `qty`
- `filled_qty`
- `filled_avg_price`
- `limit_price`
- `submitted_at`
- `filled_at`
- `raw_order`

Join keys:

- `order_id` and `client_order_id` -> `orders_paper.response.id`, `orders_paper.response.client_order_id`, and `orders_paper.payload.client_order_id`.
- `position_id` -> `human_paper_positions` and `human_paper_exits`.
- `source_dedupe_key` -> parsed alert and nightly reconciliation.

### `nightly_review_reports.jsonl`

Purpose: append-only summary index for the full nightly JSON and Markdown reports.

Required fields for current rows:

- `event_type`: `nightly_review_report`.
- `day`
- `created_at`
- `json_path`
- `markdown_path`
- `counts`
- `issue_counts`
- `capture_method_scorecard`
- `all_time_pl`
- `steve_alert_pl`
- `broker_fill_pl`
- `executive_activity`
- `storage_hygiene`
- `recursive_improvement_plan`
- `recommended_next_actions`

Join keys:

- `day` -> full report files under `data/nightly_reviews/YYYY-MM-DD.json` and `.md`.
- `counts`, `issue_counts`, `capture_method_scorecard`, and `recursive_improvement_plan` explain why the next improvement is safe.

## Route Ledgers

These route ledgers are not part of the minimum schema check, but they complete the alert trace.

### `steve_approval_cards.jsonl`

Owner-DM approval card audit for guard-blocked or uncertain option entries. Important keys are `approval_id`, `source_dedupe_key`, the embedded `alert`, the market `snapshot`, and Telegram delivery status. Approval cards must go to the owner DM, not executive groups.

### `steve_approval_actions.jsonl`

Owner reply and auto-approval audit. Important keys are `approval_id`, `action`, `authorization_scope`, `position_id`, `source_dedupe_key`, `broker_status`, and `broker_reason`. Auto-entry uses `action=auto_approved`; owner approval uses approved/skipped reply actions.

### `steve_auto_buy_reports.jsonl`

Operational report for guard-passing auto paper buys. Important keys are `auto_paper_id`, `position_id`, `source_dedupe_key`, `created_at`, `status`, `broker_status`, and `broker_reason`.

## Schema Check

Run the lightweight checker with:

```sh
python3 scripts/ledger_schema_check.py --data-dir data --print-json
```

The checker reads only JSONL files in the chosen data directory and reports counts, missing files, invalid JSON lines, non-object lines, missing required keys, empty join keys, and missing trace-key groups. It does not print raw alert text, order payloads, Telegram IDs, Discord channel contents, or secrets.

To append an audit report, explicitly request it:

```sh
python3 scripts/ledger_schema_check.py --data-dir data --print-json --write-report
```

`--write-report` appends to `ledger_schema_reports.jsonl` in the selected data directory. No report is written without that flag.

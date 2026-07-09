# Recursive Improvement Workplan

This workplan turns the current review findings into implementation tasks. Keep every change paper-only, deterministic, and audit-friendly.

## 1. Machine-Availability Classification

Goal: distinguish true live pipeline latency from startup/browser-history backfills caused by the laptop being asleep, off, logged out, or not running the watcher.

Implementation requirements:

- Add a reusable classifier for alert delay segments:
  - Steve source time -> raw capture time.
  - raw capture time -> paper order/audit time.
  - source time -> paper order/audit time.
- In nightly review, when a buy order is late:
  - If source->capture is high but capture->order is low, emit `machine_unavailable_startup_backfill` instead of only `slow_order_submission`.
  - Keep evidence fields: source time, raw captured time, order time, source-to-capture seconds, capture-to-order seconds, total seconds, source/dedupe keys, and contract identity.
  - Recommend premarket readiness rather than parser or broker tuning.
- Add report-level counts for machine availability findings so later reports can trend them.
- Tests must cover:
  - startup backfill is classified separately.
  - true live order delay remains `slow_order_submission`.
  - missing raw capture time falls back to current behavior.

Suggested files:

- `scripts/nightly_review.py`
- optional helper module under `scripts/`
- a focused test script such as `scripts/test_machine_availability.py`

## 2. Premarket Readiness Check

Goal: fail loudly before market open when the local machine or capture stack is not ready.

Implementation requirements:

- Add `scripts/premarket_readiness.py`.
- Check at least:
  - latest live pipeline heartbeat exists and is fresh.
  - latest browser health exists, is fresh, and has `status=ok`.
  - current disk free space is above a configurable threshold.
  - local watcher config exists without printing channel/private values.
  - Telegram owner bot config is present without printing token or IDs.
  - Alpaca base URL is paper when configured; do not print keys.
- Write:
  - append-only `data/premarket_readiness_reports.jsonl`.
  - latest-state `data/premarket_readiness_latest.json`.
- CLI should support:
  - `--print-json`
  - `--no-telegram`
  - `--max-heartbeat-age-seconds`
  - `--max-browser-health-age-seconds`
  - `--min-free-gb`
- Optional Telegram alerting must go to owner operational channels only, never the executive group.
- Tests must create temp heartbeat/browser/config files and verify pass/fail JSON without touching real secrets.

Suggested files:

- `scripts/premarket_readiness.py`
- focused test script such as `scripts/test_premarket_readiness.py`
- `docs/OPERATIONS.md`

## 3. Ledger Schema And Traceability Checks

Goal: make JSONL ledgers easier for humans and new LLMs to understand and validate.

Implementation requirements:

- Document core ledgers, join keys, required fields, and common lookup path.
- Add a lightweight schema/check command if practical:
  - no external dependencies.
  - no runtime secrets.
  - emit JSON to stdout with counts and missing-key findings.
  - write an append-only report only when explicitly asked or when integrated into nightly later.
- Cover at least:
  - `raw_notifications.jsonl`
  - `parsed_alerts.jsonl`
  - `shadow_option_positions.jsonl`
  - `human_paper_positions.jsonl`
  - `human_paper_exits.jsonl`
  - `orders_paper.jsonl`
  - `broker_order_status_reports.jsonl`
  - `nightly_review_reports.jsonl`

Suggested files:

- `docs/LEDGER_SCHEMAS.md`
- optional `scripts/ledger_schema_check.py`
- focused test script such as `scripts/test_ledger_schema_check.py`

## 4. Log And Storage Hygiene

Goal: prevent local logs and high-frequency telemetry from making the repo/runtime hard to inspect while preserving rollback context.

Implementation requirements:

- Add archive-first log hygiene for files under `logs/`.
- Default behavior must be dry-run/scorecard. `--apply` is required for changes.
- Rotate only logs above a configurable size threshold.
- Archive rotated logs under `logs/archive/` or `data/archive/` with timestamps, preferably gzip.
- Write:
  - append-only `data/log_hygiene_reports.jsonl`.
  - latest-state `data/log_hygiene_latest.json`.
- Do not delete trading ledgers.
- Integrate recommendations with existing storage hygiene docs, not by silently compacting.
- Tests must create temp log files and verify dry-run and apply behavior.

Suggested files:

- `scripts/log_hygiene.py`
- focused test script such as `scripts/test_log_hygiene.py`
- `docs/OPERATIONS.md`

## 5. Test Organization

Goal: make the large Steve options test suite easier for humans and LLMs to extend safely.

Implementation requirements:

- Extract reusable helpers from `scripts/test_steve_options_mvp.py` into a support module.
- Keep `python3 scripts/test_steve_options_mvp.py` as the main compatibility command.
- Avoid changing test behavior during extraction.
- Add a short comment or docstring explaining where new focused test scripts should live.
- Tests must prove the original command still passes.

Suggested files:

- `scripts/steve_options_test_support.py`
- `scripts/test_steve_options_mvp.py`

## 6. Nightly Review Modularization

Goal: make `scripts/nightly_review.py` easier to modify without regressions.

Implementation requirements:

- Do not do a broad rewrite in one patch.
- Start by extracting low-risk pure helpers only after tests cover them.
- Candidate future modules:
  - truth extraction.
  - capture scorecards.
  - issue classification.
  - P/L summary formatting.
  - Telegram report formatting.
- Preserve CLI behavior and report JSON keys.

Suggested first step:

- Add a refactor plan or extract one small pure helper group only if tests remain stable.

## Required Verification

Run the existing suite:

```sh
python3 scripts/test_pipeline.py
python3 scripts/test_full_pipeline.py
python3 scripts/test_steve_options_mvp.py
python3 -m py_compile scripts/*.py
git diff --check
```

Run any new focused test scripts added by this workplan.

## Audit Evidence

New operational checks should write latest-state JSON plus append-only JSONL reports so the nightly loop can compare outcomes across the next few sessions. Never print secrets, Telegram IDs, Discord channel contents, or Alpaca keys.

# LLM Handoff

Use this file when a new coding agent needs to understand the project quickly without reading every runtime ledger.

## Read Order

1. `AGENTS.md`: repository safety rules and required tests.
2. `README.md`: product-level behavior and setup.
3. `docs/ARCHITECTURE.md`: module responsibilities, ledgers, and traceability path.
4. `docs/OPERATIONS.md`: local runbook, LaunchAgents, browser watcher, and nightly review.
5. `SKILL.md`: recursive nightly improvement operating policy.
6. `docs/GITHUB_PUBLISHING.md`: what can be committed and how publishing is gated.

## System Invariants

- The system is paper-only. Never add live trading behavior without an explicit user request and new tests.
- Alpaca execution must keep refusing non-paper endpoints.
- Discord capture is local notification/browser reading only. Do not use Discord user tokens or private Discord APIs.
- Runtime data under `data/`, logs under `logs/`, `.env.local`, and local `config/watcher.yaml` are ignored and must not be committed.
- Browser Discord history is the post-market source of truth; local notifications are a live capture source and backup evidence.
- Local policy P/L, broker-fill P/L, and Steve-alert-price P/L are separate comparison ledgers.

## Alert Lifecycle

For a single Steve alert, trace:

1. Truth: nightly `truth_events` and `data/discord_browser_messages.jsonl`.
2. Capture: `data/raw_notifications.jsonl`.
3. Parse: `data/parsed_alerts.jsonl`.
4. Shadow validation: `data/shadow_option_positions.jsonl`.
5. Route:
   - `data/steve_auto_buy_reports.jsonl` for guard-passing auto paper buys.
   - `data/steve_approval_cards.jsonl` and `data/steve_approval_actions.jsonl` for owner-DM approval fallback.
6. Local paper state: `data/human_paper_positions.jsonl` and `data/human_paper_exits.jsonl`.
7. Broker audit: `data/orders_paper.jsonl` and `data/broker_order_status_reports.jsonl`.
8. Reconciliation: `data/nightly_reviews/YYYY-MM-DD.json`.

Preserve join keys such as `dedupe_key`, `source_dedupe_key`, `canonical_entry_key`, `validation_id`, `position_id`, `approval_id`, `client_order_id`, and `exit_id`.

## Current Routing Model

- Fresh unambiguous option buys, including `#hedge`, can auto-enter local paper when auto-entry guardrails pass.
- Guard-blocked, context-incomplete, stale, or ambiguous alerts must not be silently traded.
- Telegram approvals are owner-DM only. Executive groups receive filled paper buy/sell summaries and one nightly executive summary, not approval cards.
- If the laptop was asleep/off when an alert was posted, browser backfill can recover the alert for audit, but late capture should be classified as startup/backfill latency rather than live pipeline slowness.

## Recursive Improvement Loop

The nightly reviewer is:

```sh
python3 scripts/nightly_review.py --refresh-browser --send-telegram --print-json
```

When improving the system:

- Start from the latest nightly JSON/Markdown report.
- Classify issues by root cause: capture, parse, routing, broker, P/L reconciliation, storage, machine availability, or documentation.
- Prefer observability before behavior changes when evidence is weak.
- Update tests and docs with every behavior change.
- Run the required test set before handoff.

Required checks:

```sh
python3 scripts/test_pipeline.py
python3 scripts/test_full_pipeline.py
python3 scripts/test_steve_options_mvp.py
python3 -m py_compile scripts/*.py
git diff --check
```

## Git Policy

Do not auto-push directly from the trading loop. Publishing should be a separate gated operation after tests pass and a human can inspect the diff scope.

Safe commit candidates are source, tests, sanitized config examples, LaunchAgent templates, and docs. Unsafe commit candidates are runtime ledgers, logs, `.env.local`, local watcher config, screenshots, credentials, and copied private Discord history.

Before any push, run:

```sh
git status -sb
git status --ignored -sb
git diff --stat
```

Then verify that ignored runtime files are not staged:

```sh
git diff --cached --name-only
```

If automated publishing is later added, require an explicit opt-in environment variable, a clean test run, no unsafe staged files, and a generated summary of changed files before `git push`.

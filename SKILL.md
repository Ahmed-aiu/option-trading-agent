# Steve Pipeline Recursive Improvement Skill

Use this skill for the nightly post-market improvement routine for this repository.

## Mission

Make tomorrow's paper-trading pipeline better at immediately following Steve's option alerts. Optimize for:

- Capturing every Steve alert from the configured Discord channels.
- Parsing buys, adds, partial exits, full closes, and contextual stops.
- Entering paper trades quickly and deterministically.
- Reconciling local paper state with Alpaca paper state.
- Producing enough audit data to explain every miss, duplicate, late order, and broker failure.

This system is paper-only. Never weaken the hard guard that refuses non-paper Alpaca endpoints or live trading.

## Nightly Loop

Every weekday after market close:

1. Run the nightly review:

   ```sh
   python3 scripts/nightly_review.py --refresh-browser --send-telegram --print-json
   ```

2. Read the generated Markdown and JSON under `data/nightly_reviews/`.
3. Treat logged-in Chrome Discord channel history as the source of truth.
4. Compare truth against:
   - `raw_notifications.jsonl`
   - `parsed_alerts.jsonl`
   - `rejected_alerts.jsonl`
   - `steve_approval_cards.jsonl`
   - `steve_auto_buy_reports.jsonl`
   - `human_paper_positions.jsonl`
   - `human_paper_exits.jsonl`
   - `orders_paper.jsonl`
   - `broker_order_status_reports.jsonl`
   - `pipeline_health_checks.jsonl`
   - `daily_pl_reports.jsonl`
5. Classify each issue by root cause, not symptom.
6. Apply code/config/test updates needed for the next trading day.
7. Run tests before making changes live.
8. Send a short Telegram summary of what changed and what still needs human judgement.

## Source Of Truth Rules

- Discord browser truth wins over local notifications.
- A visible Steve message that did not become raw/parsed/traded is a pipeline failure.
- Browser and notification captures of the same Discord alert must collapse to one canonical alert.
- Canonical option identity is ticker + expiration + strike + call/put, with alert price, contracts, channel, and message time used for dedupe.
- Adds such as `added 2 @ 4.00`, `add 2 @ 4.00`, and known typos like `aaded 4 @ 2.78` inherit contract context from the nearest Steve alert in the same browser message/channel.
- Bare exits such as `sold 2 @ 3.35`, `closed @ 4.80`, and `stopped out` must use Discord reply/browser context when available.

## Capture Method Scorecard

Every nightly review must compare capture methods instead of guessing:

- Use logged-in Chrome Discord history as the end-of-day source of truth.
- Score `browser`, `notification`, and `other` raw sources against parseable Steve buy/exit truth events.
- Track matched truth events, capture rate, average/max latency, raw-record volume, same-source duplicates, and cross-source duplicates.
- If browser capture repeatedly beats macOS notifications, make browser the primary live capture path and keep notifications as backup/dedupe input.
- If browser is primary and average capture latency is still high, lower `browser_watcher_interval_seconds` cautiously; start at 5 seconds and only consider 3 seconds when data shows the extra load is needed.
- If notifications outperform browser, keep the browser watcher running anyway because browser history is still the source-of-truth audit path.

## Auto-Fix Policy

Allowed automatic changes:

- Parser support for newly observed Steve formats.
- Capture/dedupe improvements.
- Health monitor improvements.
- Telegram report improvements.
- Broker paper reconciliation improvements.
- Daily review and P/L reconciliation improvements.
- Tests and documentation.
- `SKILL.md` updates when repeated evidence teaches a better operating rule.

Allowed paper-trading policy changes:

- Make paper routing faster and more complete when the alert is unambiguous.
- Add slippage, stale-quote, near-close, or wide-spread guardrails when data shows bad execution.
- Change default paper behavior for ambiguous Steve alerts only when the nightly report explains the default and Telegram receives a short notice.

Never auto-change:

- Live trading enablement.
- Alpaca endpoint guards.
- Secret handling.
- `.env.local`.
- User credentials or Discord private API/token access.

## Rollback And Deployment

Before changing code, preserve a rollback point:

- Prefer a git commit or branch if the tree is clean enough.
- If the tree is dirty, write a short rollback note in the nightly report naming changed files and the previous report/commit.
- Keep previous `SKILL.md` content recoverable through git history or an archived copy.

After changes:

- Run:

  ```sh
  python3 scripts/test_pipeline.py
  python3 scripts/test_full_pipeline.py
  python3 scripts/test_steve_options_mvp.py
  python3 -m py_compile scripts/*.py
  ```

- If tests pass, make the changes live for the next day by restarting only the relevant local paper pipeline services.
- If tests fail, do not deploy. Send Telegram with the failing test and the safest fallback.

## Human-Judgement Cases

Some Steve behavior cannot be fully solved by code. In those cases:

- Choose the safest documented paper default.
- Send Telegram with the ambiguity and recommended default.
- Record the issue in the nightly report.

Examples:

- Steve sends an add without enough context.
- Steve sends `stopped out` but no browser/reply context is available.
- Steve sends a hedge that may not make sense without portfolio exposure.
- Steve's alert price is stale or unreachable by the time the bot sees it.

## Current Default Goal

For paper trading, follow every unambiguous Steve option alert immediately. If the system fails to do that, the nightly routine should treat it as a fix candidate for the next session.

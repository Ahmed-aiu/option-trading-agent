# Exit Strategy

## Current Live Paper Process

Entry routing:

- `#hedge` option alerts: send Telegram approval card, no automatic paper buy.
- Non-hedge option alerts such as `#swing`, `#lotto`, or no tag: create a local paper position, attempt Alpaca paper buy when enabled, and send an `AUTO PAPER BUY` Telegram report.

Current local paper exit rules:

- Percent stop: close all remaining contracts at -35%.
- `contracts=1`: sell 1 contract at +80%.
- `contracts>1`: sell `floor(total / 2)` at +80%, `floor(remaining / 2)` at +120%, and the rest at +200%.
- Steve close alerts catch up only when Steve has closed more contracts than the local paper ledger already closed.

Broker-side paper buy orders can be attempted through Alpaca when local paper submission is enabled. Broker-side paper sell orders are not yet wired as the source of truth; local JSONL ledgers are the validation source.

## Key Trading Assumption

Long option swings still lose value from theta. They are not immune to time decay. Longer DTE gives the trade more time to work and reduces same-day gamma risk, but a flat underlying can still reduce the option value.

Because of that, `wait for Steve to close` should not be one global rule. It should depend on DTE, tag, liquidity, and whether the position has already been de-risked.

## Proposed Policy To Test Next

Use separate exit policies instead of one global ladder:

- `fast_ladder`: keep the current +80%, +120%, +200% ladder. Use for 0DTE, 1DTE, lotto, and very short-dated contracts.
- `hybrid_steve_runner`: take partial profit once, then let the rest follow Steve. Use for same-week swings where theta risk is still meaningful.
- `steve_led`: hold for Steve close unless the hard stop or time stop fires. Use only for longer-dated swings with enough DTE and acceptable liquidity.
- `shadow_only`: do not trade, only track outcomes. Use for hedges without portfolio exposure or unsupported edge cases.

Initial DTE split to validate:

- `0-1 DTE`: `fast_ladder`; never rely only on Steve close.
- `2-5 DTE`: `hybrid_steve_runner`; take some profit at +80%, then Steve-led remainder.
- `6-14 DTE`: test `hybrid_steve_runner` versus `steve_led` in shadow results before changing live paper behavior.
- `15+ DTE`: candidate for `steve_led`, still with a hard stop and stale-position review.

## Decision Rule Before Changing Code

Do not replace the current live exit behavior until the daily summaries can compare at least these shadow outcomes:

- current ladder result
- Steve-only exit result
- hybrid partial-profit plus Steve-exit result
- stop-only result
- time-based exit result

The goal is not just highest winner size. The chosen policy should improve expectancy after losses, missed exits, wide spreads, and theta decay are included.

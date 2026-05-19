#!/usr/bin/env python3
"""Process unhandled raw notifications through parse, risk, and paper audit."""

from __future__ import annotations

import argparse
import json
from pathlib import Path
from typing import Any

from alpaca_paper_adapter import AdapterError, audit_record, load_adapter_config, order_payload_from_decision, write_order_audit
from option_validation import handle_option_entry, handle_option_exit, is_option_entry, is_option_exit
from parse_alert import ParseRejected, parse_trade_alert, rejected_record
from pipeline_common import DATA_DIR, append_jsonl, load_seen_keys, now_iso, read_jsonl
from risk_guard import decision_for_alert, existing_decisions


PROCESSED_FILE = DATA_DIR / "processed_notifications.jsonl"


def normalize_parsed(value: dict[str, Any] | list[dict[str, Any]]) -> list[dict[str, Any]]:
    return value if isinstance(value, list) else [value]


def write_openclaw_summary(decision: dict[str, Any]) -> None:
    workspace_dir = Path.home() / ".openclaw/workspace/trading_alerts"
    workspace_dir.mkdir(parents=True, exist_ok=True)
    order = decision.get("would_place_order") or {}
    summary = f"""# Trading Alert Decision

Time: {decision.get('decided_at', '')}
Source: {decision.get('source_dedupe_key', '')}
Ticker: {decision.get('ticker', '')}
Side: {decision.get('side', '')}
Allowed: {decision.get('allowed', False)}
Reason: {decision.get('reason', '')}
Raw Alert: {decision.get('raw_text', '')}
Would-place order: {json.dumps(order, sort_keys=True)}
Risk config: {json.dumps(decision.get('risk_config', {}), sort_keys=True)}
"""
    (workspace_dir / "latest_trade_decision.md").write_text(summary, encoding="utf-8")


def mark_processed(raw: dict[str, Any], status: str, parsed_count: int, decision_count: int) -> None:
    append_jsonl(
        PROCESSED_FILE,
        {
            "event_type": "processed_notification",
            "processed_at": now_iso(),
            "dedupe_key": raw.get("dedupe_key"),
            "status": status,
            "parsed_count": parsed_count,
            "decision_count": decision_count,
        },
    )


def alpaca_dry_run_audit(decision: dict[str, Any]) -> tuple[str, dict[str, Any] | None]:
    if not decision.get("allowed"):
        return "decision_blocked", None
    try:
        config, _env_file = load_adapter_config()
        payload = order_payload_from_decision(decision, config)
        write_order_audit(audit_record("pipeline_dry_run_order", decision, payload, None, "dry_run"))
        return "dry_run_created", payload
    except AdapterError as exc:
        write_order_audit(audit_record("pipeline_dry_run_order", decision, None, None, "blocked", str(exc)))
        return f"dry_run_blocked:{exc}", None


def process_raw_notifications(
    raw_records: list[dict[str, Any]],
    limit: int | None = None,
    dry_run_orders: bool = True,
    prior_decisions_override: list[dict[str, Any]] | None = None,
) -> dict[str, int]:
    seen = load_seen_keys(PROCESSED_FILE, key_name="dedupe_key")
    prior_decisions = existing_decisions() if prior_decisions_override is None else prior_decisions_override
    counts = {
        "raw_seen": len(raw_records),
        "raw_new": 0,
        "parsed": 0,
        "rejected": 0,
        "decisions": 0,
        "allowed": 0,
        "blocked": 0,
        "alpaca_dry_runs": 0,
        "option_shadow_positions": 0,
        "option_approval_cards": 0,
        "option_auto_buys": 0,
        "option_exits": 0,
        "option_validation_errors": 0,
    }
    processed_now = 0
    for raw in raw_records:
        key = raw.get("dedupe_key")
        if not key or key in seen:
            continue
        if limit is not None and processed_now >= limit:
            break
        counts["raw_new"] += 1
        processed_now += 1
        parsed_records: list[dict[str, Any]] = []
        decisions: list[dict[str, Any]] = []
        try:
            parsed_records = normalize_parsed(parse_trade_alert(raw))
            for parsed in parsed_records:
                append_jsonl(DATA_DIR / "parsed_alerts.jsonl", parsed)
                counts["parsed"] += 1
                try:
                    if is_option_entry(parsed):
                        validation_result = handle_option_entry(parsed, send_approval=True)
                        if validation_result.get("shadow_position_created"):
                            counts["option_shadow_positions"] += 1
                        if (validation_result.get("approval_card") or {}).get("approval_id"):
                            counts["option_approval_cards"] += 1
                        if (validation_result.get("auto_buy") or {}).get("created"):
                            counts["option_auto_buys"] += 1
                    elif is_option_exit(parsed):
                        validation_result = handle_option_exit(parsed)
                        if validation_result.get("created"):
                            counts["option_exits"] += 1
                except Exception as exc:  # noqa: BLE001
                    append_jsonl(
                        DATA_DIR / "option_validation_errors.jsonl",
                        {
                            "event_type": "option_validation_error",
                            "recorded_at": now_iso(),
                            "source_dedupe_key": parsed.get("source_dedupe_key"),
                            "ticker": parsed.get("ticker"),
                            "reason": f"{type(exc).__name__}:{exc}",
                            "raw_text": parsed.get("raw_text", ""),
                        },
                    )
                    counts["option_validation_errors"] += 1
                decision = decision_for_alert(parsed, prior_decisions=prior_decisions + decisions)
                append_jsonl(DATA_DIR / "trade_decisions.jsonl", decision)
                write_openclaw_summary(decision)
                decisions.append(decision)
                counts["decisions"] += 1
                if decision.get("allowed"):
                    counts["allowed"] += 1
                else:
                    counts["blocked"] += 1
                if dry_run_orders:
                    status, payload = alpaca_dry_run_audit(decision)
                    if payload:
                        counts["alpaca_dry_runs"] += 1
            prior_decisions.extend(decisions)
            mark_processed(raw, "processed", len(parsed_records), len(decisions))
            seen.add(str(key))
        except ParseRejected as exc:
            append_jsonl(DATA_DIR / "rejected_alerts.jsonl", rejected_record(raw, exc.reason))
            counts["rejected"] += 1
            mark_processed(raw, f"rejected:{exc.reason}", 0, 0)
            seen.add(str(key))
        except Exception as exc:
            append_jsonl(DATA_DIR / "rejected_alerts.jsonl", rejected_record(raw, f"pipeline_error:{type(exc).__name__}:{exc}"))
            counts["rejected"] += 1
            mark_processed(raw, f"error:{type(exc).__name__}", 0, 0)
            seen.add(str(key))
    return counts


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--input", default=str(DATA_DIR / "raw_notifications.jsonl"))
    parser.add_argument("--limit", type=int)
    parser.add_argument("--no-alpaca-dry-run", action="store_true")
    args = parser.parse_args()
    raw_records = read_jsonl(Path(args.input))
    counts = process_raw_notifications(
        raw_records,
        limit=args.limit,
        dry_run_orders=not args.no_alpaca_dry_run,
    )
    for key, value in counts.items():
        print(f"{key}: {value}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())

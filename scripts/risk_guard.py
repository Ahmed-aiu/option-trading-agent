#!/usr/bin/env python3
"""Paper-only risk gate for parsed trade alerts."""

from __future__ import annotations

import argparse
import datetime as dt
import json
import sys
from pathlib import Path
from typing import Any

from pipeline_common import (
    CONFIG_DIR,
    DATA_DIR,
    LOG_DIR,
    append_jsonl,
    load_simple_yaml,
    now_iso,
    parse_datetime,
    read_jsonl,
    setup_logging,
)


def trading_day(value: str | None) -> str:
    parsed = parse_datetime(value) if value else None
    if parsed is None:
        parsed = parse_datetime(now_iso())
    return parsed.date().isoformat()


def existing_decisions() -> list[dict[str, Any]]:
    return read_jsonl(DATA_DIR / "trade_decisions.jsonl")


def decision_for_alert(
    alert: dict[str, Any],
    config: dict[str, Any] | None = None,
    prior_decisions: list[dict[str, Any]] | None = None,
    ignore_age: bool = False,
) -> dict[str, Any]:
    config = config or load_simple_yaml(CONFIG_DIR / "risk.yaml")
    prior_decisions = prior_decisions if prior_decisions is not None else existing_decisions()
    base = {
        "event_type": "trade_decision",
        "decided_at": now_iso(),
        "source_dedupe_key": alert.get("source_dedupe_key", ""),
        "ticker": alert.get("ticker"),
        "side": alert.get("side"),
        "allowed": False,
        "mode": config.get("mode", "paper_only"),
        "reason": "",
    }

    def blocked(reason: str) -> dict[str, Any]:
        result = dict(base)
        result["reason"] = reason
        return result

    if config.get("mode") != "paper_only":
        return blocked("mode_not_paper_only")
    if alert.get("event_type") != "parsed_trade_alert":
        return blocked("not_a_parsed_trade_alert")
    if alert.get("side") == "exit":
        return blocked("exit_candidate_not_new_order")
    if alert.get("instrument_type") == "option" and config.get("option_manual_approval_only", True):
        return blocked("option_requires_manual_approval")
    if alert.get("instrument_type") == "option" and not config.get("allow_options", False):
        return blocked("options_disabled")
    if alert.get("side") == "short" and not config.get("allow_short_selling", False):
        return blocked("short_selling_disabled")
    if config.get("require_stop_loss", True) and alert.get("stop_price") is None:
        return blocked("missing_stop_loss")
    if config.get("require_target", False) and alert.get("target_price") is None:
        return blocked("missing_target")
    if alert.get("entry_price") is None:
        return blocked("missing_entry_price")
    if not ignore_age and alert.get("notification_timestamp"):
        timestamp = parse_datetime(alert.get("notification_timestamp"))
        if timestamp is None:
            return blocked("unparseable_alert_timestamp")
        age = (parse_datetime(now_iso()) - timestamp).total_seconds()
        if age > int(config.get("max_alert_age_seconds", 20)):
            return blocked("alert_too_old")

    today = trading_day(now_iso())
    allowed_today = [
        item
        for item in prior_decisions
        if item.get("allowed") and trading_day(item.get("decided_at")) == today
    ]
    if len(allowed_today) >= int(config.get("max_trades_per_day", 3)):
        return blocked("max_trades_per_day")
    duplicate_window = int(config.get("duplicate_trade_window_minutes", 60))
    now_dt = parse_datetime(now_iso())
    for item in allowed_today:
        if item.get("ticker") != alert.get("ticker") or item.get("side") != alert.get("side"):
            continue
        decided_at = parse_datetime(item.get("decided_at"))
        if decided_at and now_dt and (now_dt - decided_at).total_seconds() <= duplicate_window * 60:
            return blocked("duplicate_trade_window")

    same_ticker_open = [
        item
        for item in allowed_today
        if item.get("ticker") == alert.get("ticker") and item.get("side") != "exit"
    ]
    if len(same_ticker_open) >= int(config.get("max_same_ticker_open_trades", 1)):
        return blocked("max_same_ticker_open_trades")

    order_type = "limit"
    if config.get("allow_market_orders", False) and alert.get("entry_type") == "market":
        order_type = "market"
    result = dict(base)
    result.update(
        {
            "allowed": True,
            "reason": "passed_all_risk_checks",
            "would_place_order": {
                "symbol": alert.get("ticker"),
                "side": "sell" if alert.get("side") == "short" else alert.get("side"),
                "notional": float(config.get("max_dollars_per_trade", 100)),
                "order_type": order_type,
                "limit_price": alert.get("entry_price"),
                "time_in_force": alert.get("time_in_force", "day"),
            },
            "risk_config": {
                "mode": config.get("mode"),
                "max_dollars_per_trade": config.get("max_dollars_per_trade"),
                "max_trades_per_day": config.get("max_trades_per_day"),
                "paper_only": True,
            },
            "raw_text": alert.get("raw_text", ""),
        }
    )
    return result


def decide_alerts(
    alerts: list[dict[str, Any]],
    write: bool = False,
    ignore_age: bool = False,
    prior_decisions: list[dict[str, Any]] | None = None,
) -> list[dict[str, Any]]:
    logger = setup_logging("risk_guard", LOG_DIR / "risk_guard.log")
    config = load_simple_yaml(CONFIG_DIR / "risk.yaml")
    decisions: list[dict[str, Any]] = []
    prior = existing_decisions() if prior_decisions is None else prior_decisions
    for alert in alerts:
        try:
            decision = decision_for_alert(alert, config, prior + decisions, ignore_age=ignore_age)
        except Exception as exc:
            logger.exception("Risk guard failed closed")
            decision = {
                "event_type": "trade_decision",
                "decided_at": now_iso(),
                "source_dedupe_key": alert.get("source_dedupe_key", ""),
                "ticker": alert.get("ticker"),
                "side": alert.get("side"),
                "allowed": False,
                "mode": config.get("mode", "paper_only"),
                "reason": f"risk_guard_error:{type(exc).__name__}",
            }
        decisions.append(decision)
        if write:
            append_jsonl(DATA_DIR / "trade_decisions.jsonl", decision)
    return decisions


def load_input(args: argparse.Namespace) -> list[dict[str, Any]]:
    if args.json:
        value = json.loads(args.json)
        return [value] if isinstance(value, dict) else value
    if args.input:
        return read_jsonl(Path(args.input))
    data = sys.stdin.read().strip()
    if data:
        value = json.loads(data)
        return [value] if isinstance(value, dict) else value
    return []


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--input", help="Parsed alert JSONL file")
    parser.add_argument("--json", help="Single parsed alert JSON object")
    parser.add_argument("--write", action="store_true")
    parser.add_argument("--ignore-age", action="store_true")
    args = parser.parse_args()
    decisions = decide_alerts(load_input(args), write=args.write, ignore_age=args.ignore_age)
    for decision in decisions:
        print(json.dumps(decision, sort_keys=True))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())

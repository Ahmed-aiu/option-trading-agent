#!/usr/bin/env python3
"""Append-only validation ledgers for Steve option alerts."""

from __future__ import annotations

import argparse
import json
from pathlib import Path
from typing import Any

from alpaca_options import enrich_option_alert, option_symbol
from pipeline_common import DATA_DIR, append_jsonl, now_iso, parse_datetime, read_jsonl, stable_hash


SHADOW_POSITIONS_FILE = DATA_DIR / "shadow_option_positions.jsonl"
QUOTE_SNAPSHOTS_FILE = DATA_DIR / "option_quote_snapshots.jsonl"
STEVE_EXITS_FILE = DATA_DIR / "steve_option_exits.jsonl"
HUMAN_POSITIONS_FILE = DATA_DIR / "human_paper_positions.jsonl"
HUMAN_EXITS_FILE = DATA_DIR / "human_paper_exits.jsonl"
DAILY_SUMMARIES_FILE = DATA_DIR / "daily_option_summaries.jsonl"


def is_option_entry(alert: dict[str, Any]) -> bool:
    return (
        alert.get("event_type") == "parsed_trade_alert"
        and alert.get("instrument_type") == "option"
        and alert.get("side") == "buy"
        and alert.get("ticker")
        and alert.get("expiration_date")
        and alert.get("option_type")
        and alert.get("strike_price") is not None
    )


def is_option_exit(alert: dict[str, Any]) -> bool:
    return (
        alert.get("event_type") == "parsed_trade_alert"
        and alert.get("instrument_type") == "option"
        and alert.get("side") == "exit"
        and alert.get("exit_price") is not None
    )


def validation_id(alert: dict[str, Any]) -> str:
    return "val-" + stable_hash(
        [
            alert.get("source_dedupe_key"),
            alert.get("ticker"),
            alert.get("expiration_date"),
            alert.get("option_type"),
            alert.get("strike_price"),
            alert.get("entry_price"),
        ]
    )[:16]


def position_id_for_alert(alert: dict[str, Any]) -> str:
    return "shadow-" + stable_hash([validation_id(alert), "shadow"])[:16]


def existing_values(path: Path, key: str) -> set[str]:
    return {str(row.get(key)) for row in read_jsonl(path) if row.get(key)}


def alert_contract_symbol(alert: dict[str, Any]) -> str:
    return option_symbol(str(alert["ticker"]), str(alert["expiration_date"]), str(alert["option_type"]), alert["strike_price"])


def quote_entry_price(snapshot: dict[str, Any]) -> tuple[float | None, str]:
    quote = snapshot.get("option_quote") or {}
    ask = quote.get("ask")
    mark = quote.get("mark")
    if ask is not None and ask > 0:
        return float(ask), "bot_observed_ask"
    if mark is not None and mark > 0:
        return float(mark), "bot_observed_mark"
    return None, "unavailable"


def append_snapshot(alert: dict[str, Any], snapshot: dict[str, Any], position_id: str | None = None) -> dict[str, Any]:
    record = dict(snapshot)
    record["source_dedupe_key"] = alert.get("source_dedupe_key")
    record["validation_id"] = validation_id(alert)
    record["position_id"] = position_id
    append_jsonl(QUOTE_SNAPSHOTS_FILE, record)
    return record


def create_shadow_position(alert: dict[str, Any], snapshot: dict[str, Any]) -> tuple[dict[str, Any], bool]:
    position_id = position_id_for_alert(alert)
    if position_id in existing_values(SHADOW_POSITIONS_FILE, "position_id"):
        for row in read_jsonl(SHADOW_POSITIONS_FILE):
            if row.get("position_id") == position_id:
                return row, False
    bot_price, bot_price_source = quote_entry_price(snapshot)
    alert_price = float(alert.get("entry_price"))
    position = {
        "event_type": "shadow_option_position",
        "position_id": position_id,
        "validation_id": validation_id(alert),
        "created_at": now_iso(),
        "opened_at": alert.get("notification_timestamp") or alert.get("parsed_at") or now_iso(),
        "source_dedupe_key": alert.get("source_dedupe_key"),
        "ticker": alert.get("ticker"),
        "contract_symbol": snapshot.get("contract_symbol") or alert_contract_symbol(alert),
        "option_type": alert.get("option_type"),
        "expiration_date": alert.get("expiration_date"),
        "strike_price": alert.get("strike_price"),
        "contracts": int(alert.get("contracts") or 1),
        "remaining_contracts": int(alert.get("contracts") or 1),
        "primary_tag": alert.get("primary_tag"),
        "tags": alert.get("tags") or [],
        "alert_entry_price": alert_price,
        "bot_entry_price": bot_price,
        "bot_entry_price_source": bot_price_source,
        "shadow_models": ["steve_exit", "stop_35_take_80", "exit_15m", "exit_30m", "exit_60m", "eod"],
        "raw_text": alert.get("raw_text", ""),
    }
    append_jsonl(SHADOW_POSITIONS_FILE, position)
    return position, True


def handle_option_entry(alert: dict[str, Any], send_approval: bool = True) -> dict[str, Any]:
    snapshot = enrich_option_alert(alert)
    position, created = create_shadow_position(alert, snapshot)
    append_snapshot(alert, snapshot, position.get("position_id"))
    card: dict[str, Any] | None = None
    if send_approval:
        from steve_trade_bot import send_approval_card

        card = send_approval_card(alert, snapshot, position)
    return {
        "event_type": "option_entry_validation_result",
        "validation_id": validation_id(alert),
        "position_id": position.get("position_id"),
        "shadow_position_created": created,
        "approval_card": card or {},
        "snapshot_status": (snapshot.get("option_quote") or {}).get("status"),
    }


def applied_exit_contracts(position_id: str, exit_rows: list[dict[str, Any]]) -> int:
    total = 0
    for row in exit_rows:
        if row.get("matched_shadow_position_id") == position_id:
            total += int(row.get("contracts") or 0)
    return total


def open_shadow_positions(exit_rows: list[dict[str, Any]] | None = None) -> list[dict[str, Any]]:
    exits = exit_rows if exit_rows is not None else read_jsonl(STEVE_EXITS_FILE)
    rows: list[dict[str, Any]] = []
    for position in read_jsonl(SHADOW_POSITIONS_FILE):
        contracts = int(position.get("contracts") or 0)
        remaining = contracts - applied_exit_contracts(str(position.get("position_id")), exits)
        if remaining > 0:
            item = dict(position)
            item["computed_remaining_contracts"] = remaining
            rows.append(item)
    return sorted(rows, key=lambda row: str(row.get("opened_at") or ""))


def shadow_position_by_id(position_id: str | None) -> dict[str, Any] | None:
    if not position_id:
        return None
    for position in read_jsonl(SHADOW_POSITIONS_FILE):
        if str(position.get("position_id")) == str(position_id):
            return position
    return None


def match_shadow_position_for_exit(exit_alert: dict[str, Any]) -> dict[str, Any] | None:
    candidates = open_shadow_positions()
    ticker = exit_alert.get("ticker")
    if ticker:
        candidates = [row for row in candidates if row.get("ticker") == ticker]
    if not candidates:
        return None
    return candidates[-1]


def handle_option_exit(exit_alert: dict[str, Any]) -> dict[str, Any]:
    exit_id = "exit-" + stable_hash(
        [
            exit_alert.get("source_dedupe_key"),
            exit_alert.get("raw_text"),
            exit_alert.get("exit_price"),
            exit_alert.get("parsed_at"),
        ]
    )[:16]
    if exit_id in existing_values(STEVE_EXITS_FILE, "exit_id"):
        return {"event_type": "option_exit_validation_result", "exit_id": exit_id, "created": False}
    matched = match_shadow_position_for_exit(exit_alert)
    requested_contracts = int(exit_alert.get("contracts") or 0)
    if matched and requested_contracts <= 0:
        requested_contracts = int(matched.get("computed_remaining_contracts") or matched.get("contracts") or 0)
    if matched:
        requested_contracts = min(requested_contracts, int(matched.get("computed_remaining_contracts") or requested_contracts))
    record = {
        "event_type": "steve_option_exit",
        "exit_id": exit_id,
        "recorded_at": now_iso(),
        "source_dedupe_key": exit_alert.get("source_dedupe_key"),
        "ticker": exit_alert.get("ticker") or (matched or {}).get("ticker"),
        "exit_price": float(exit_alert.get("exit_price")),
        "contracts": requested_contracts,
        "matched_shadow_position_id": (matched or {}).get("position_id"),
        "match_confidence": "medium" if matched and exit_alert.get("ticker") else ("low" if matched else "none"),
        "raw_text": exit_alert.get("raw_text", ""),
    }
    append_jsonl(STEVE_EXITS_FILE, record)
    human_exits = apply_steve_exit_to_human_positions(record, matched)
    return {
        "event_type": "option_exit_validation_result",
        "exit_id": exit_id,
        "created": True,
        "matched": bool(matched),
        "human_exits": len(human_exits),
    }


def price_from_snapshot(row: dict[str, Any]) -> float | None:
    quote = row.get("option_quote") or {}
    price = quote.get("mark")
    if price is None:
        price = quote.get("ask") or quote.get("bid")
    try:
        return float(price) if price is not None else None
    except (TypeError, ValueError):
        return None


def metrics_for_position(position: dict[str, Any], snapshots: list[dict[str, Any]]) -> dict[str, Any]:
    entry_price = position.get("bot_entry_price") or position.get("alert_entry_price")
    try:
        entry = float(entry_price)
    except (TypeError, ValueError):
        entry = 0.0
    prices = [(row.get("recorded_at"), price_from_snapshot(row)) for row in snapshots if row.get("position_id") == position.get("position_id")]
    prices = [(timestamp, price) for timestamp, price in prices if price is not None]
    if entry <= 0 or not prices:
        return {
            "position_id": position.get("position_id"),
            "status": "insufficient_data",
            "entry_price": entry_price,
        }
    max_timestamp, max_price = max(prices, key=lambda item: item[1])
    min_timestamp, min_price = min(prices, key=lambda item: item[1])

    def first_time_to_gain(target_pct: float) -> str | None:
        target_price = entry * (1 + (target_pct / 100))
        for timestamp, price in prices:
            if price >= target_price:
                return str(timestamp)
        return None

    return {
        "position_id": position.get("position_id"),
        "status": "ok",
        "entry_price": entry,
        "latest_price": prices[-1][1],
        "max_price": max_price,
        "max_price_at": max_timestamp,
        "min_price": min_price,
        "min_price_at": min_timestamp,
        "mfe_pct": ((max_price - entry) / entry) * 100,
        "mae_pct": ((min_price - entry) / entry) * 100,
        "time_to_25pct_gain": first_time_to_gain(25),
        "time_to_50pct_gain": first_time_to_gain(50),
        "time_to_80pct_gain": first_time_to_gain(80),
        "time_to_double": first_time_to_gain(100),
        "fixed_stop_35_hit": min_price <= entry * 0.65,
        "fixed_take_50_hit": max_price >= entry * 1.5,
        "fixed_take_80_hit": max_price >= entry * 1.8,
    }


def safe_float(value: Any) -> float | None:
    if value in (None, ""):
        return None
    try:
        return float(value)
    except (TypeError, ValueError):
        return None


def human_position_exits(position_id: str, exit_rows: list[dict[str, Any]] | None = None) -> list[dict[str, Any]]:
    rows = exit_rows if exit_rows is not None else read_jsonl(HUMAN_EXITS_FILE)
    return [row for row in rows if str(row.get("position_id")) == str(position_id)]


def human_exit_contracts(position_id: str, exit_rows: list[dict[str, Any]] | None = None) -> int:
    return sum(int(row.get("contracts") or 0) for row in human_position_exits(position_id, exit_rows))


def human_remaining_contracts(position: dict[str, Any], exit_rows: list[dict[str, Any]] | None = None) -> int:
    contracts = int(position.get("contracts") or 0)
    return max(0, contracts - human_exit_contracts(str(position.get("position_id")), exit_rows))


def human_positions_for_shadow(shadow_position: dict[str, Any]) -> list[dict[str, Any]]:
    source_key = shadow_position.get("source_dedupe_key")
    contract_symbol = shadow_position.get("contract_symbol")
    positions = []
    for position in read_jsonl(HUMAN_POSITIONS_FILE):
        if source_key and position.get("source_dedupe_key") == source_key:
            positions.append(position)
        elif contract_symbol and position.get("contract_symbol") == contract_symbol:
            positions.append(position)
    return positions


def append_human_exit(
    position: dict[str, Any],
    contracts: int,
    exit_price: float,
    reason: str,
    trigger_key: str,
    extra: dict[str, Any] | None = None,
    exit_rows: list[dict[str, Any]] | None = None,
) -> dict[str, Any] | None:
    existing_ids = existing_values(HUMAN_EXITS_FILE, "exit_id")
    exit_id = "human-exit-" + stable_hash([position.get("position_id"), trigger_key])[:16]
    if exit_id in existing_ids:
        return None
    quantity = min(max(0, int(contracts or 0)), human_remaining_contracts(position, exit_rows))
    if quantity <= 0:
        return None
    entry_price = safe_float(position.get("entry_price"))
    pnl_dollars = None
    pnl_percent = None
    if entry_price and entry_price > 0:
        pnl_percent = ((float(exit_price) - entry_price) / entry_price) * 100
        pnl_dollars = (float(exit_price) - entry_price) * quantity * 100
    record = {
        "event_type": "human_paper_option_exit",
        "exit_id": exit_id,
        "position_id": position.get("position_id"),
        "approval_id": position.get("approval_id"),
        "recorded_at": now_iso(),
        "source_dedupe_key": position.get("source_dedupe_key"),
        "ticker": position.get("ticker"),
        "contract_symbol": position.get("contract_symbol"),
        "reason": reason,
        "contracts": quantity,
        "exit_price": float(exit_price),
        "entry_price": entry_price,
        "pnl_percent": pnl_percent,
        "pnl_dollars": pnl_dollars,
        "remaining_after_exit": human_remaining_contracts(position, exit_rows) - quantity,
    }
    if extra:
        record.update(extra)
    append_jsonl(HUMAN_EXITS_FILE, record)
    return record


def apply_steve_exit_to_human_positions(
    exit_record: dict[str, Any],
    matched_shadow_position: dict[str, Any] | None = None,
) -> list[dict[str, Any]]:
    shadow_position = matched_shadow_position or shadow_position_by_id(exit_record.get("matched_shadow_position_id"))
    if not shadow_position:
        return []
    shadow_position_id = str(shadow_position.get("position_id"))
    steve_exit_rows = read_jsonl(STEVE_EXITS_FILE)
    steve_cumulative_closed = applied_exit_contracts(shadow_position_id, steve_exit_rows)
    human_exit_rows = read_jsonl(HUMAN_EXITS_FILE)
    created: list[dict[str, Any]] = []
    for position in human_positions_for_shadow(shadow_position):
        position_contracts = int(position.get("contracts") or 0)
        target_closed = min(position_contracts, steve_cumulative_closed)
        already_closed = human_exit_contracts(str(position.get("position_id")), human_exit_rows)
        quantity = target_closed - already_closed
        row = append_human_exit(
            position,
            quantity,
            float(exit_record.get("exit_price")),
            "steve_exit_catch_up",
            f"steve_exit:{exit_record.get('exit_id')}",
            {
                "steve_exit_id": exit_record.get("exit_id"),
                "steve_cumulative_closed": steve_cumulative_closed,
                "already_closed_before_exit": already_closed,
                "exit_price_source": "steve_exit_alert",
            },
            human_exit_rows,
        )
        if row:
            created.append(row)
            human_exit_rows.append(row)
    return created


def latest_snapshot_for_human_position(position: dict[str, Any], snapshots: list[dict[str, Any]]) -> dict[str, Any] | None:
    source_key = position.get("source_dedupe_key")
    contract_symbol = position.get("contract_symbol")
    candidates = [
        row
        for row in snapshots
        if (not source_key or row.get("source_dedupe_key") == source_key)
        and ((row.get("contract_symbol") == contract_symbol) or ((row.get("option_quote") or {}).get("symbol") == contract_symbol))
        and parse_datetime(row.get("recorded_at")) is not None
    ]
    if not candidates:
        return None
    return max(candidates, key=lambda row: parse_datetime(row.get("recorded_at")))


def stop_price_for_human_position(position: dict[str, Any]) -> float | None:
    if position.get("risk_type") == "price":
        return safe_float(position.get("stop_price"))
    entry_price = safe_float(position.get("entry_price"))
    stop_percent = safe_float(position.get("stop_percent"))
    if entry_price is None or stop_percent is None:
        return None
    return entry_price * (1 - stop_percent / 100)


def apply_human_exit_rules_once() -> list[dict[str, Any]]:
    snapshots = read_jsonl(QUOTE_SNAPSHOTS_FILE)
    human_exit_rows = read_jsonl(HUMAN_EXITS_FILE)
    created: list[dict[str, Any]] = []
    for position in read_jsonl(HUMAN_POSITIONS_FILE):
        remaining = human_remaining_contracts(position, human_exit_rows)
        if remaining <= 0:
            continue
        snapshot = latest_snapshot_for_human_position(position, snapshots)
        if not snapshot:
            continue
        current_price = price_from_snapshot(snapshot)
        if current_price is None or current_price <= 0:
            continue
        stop_price = stop_price_for_human_position(position)
        if stop_price is not None and current_price <= stop_price:
            row = append_human_exit(
                position,
                remaining,
                current_price,
                "stop_loss",
                "stop_loss",
                {
                    "exit_price_source": "latest_option_snapshot",
                    "snapshot_id": snapshot.get("snapshot_id"),
                    "stop_price": stop_price,
                },
                human_exit_rows,
            )
            if row:
                created.append(row)
                human_exit_rows.append(row)
            continue
        cumulative_target_contracts = 0
        for tranche in sorted(position.get("exit_plan") or [], key=lambda row: float(row.get("take_percent") or 0)):
            tranche_contracts = int(tranche.get("contracts") or 0)
            if tranche_contracts <= 0:
                continue
            cumulative_target_contracts += tranche_contracts
            take_price = safe_float(tranche.get("take_price"))
            if take_price is None:
                entry_price = safe_float(position.get("entry_price"))
                take_percent = safe_float(tranche.get("take_percent"))
                if entry_price is None or take_percent is None:
                    continue
                take_price = entry_price * (1 + take_percent / 100)
            if current_price < take_price:
                continue
            already_closed = human_exit_contracts(str(position.get("position_id")), human_exit_rows)
            quantity = min(cumulative_target_contracts - already_closed, human_remaining_contracts(position, human_exit_rows))
            row = append_human_exit(
                position,
                quantity,
                current_price,
                "take_profit",
                f"take_profit:{tranche.get('take_percent')}",
                {
                    "exit_price_source": "latest_option_snapshot",
                    "snapshot_id": snapshot.get("snapshot_id"),
                    "take_percent": tranche.get("take_percent"),
                    "take_price": take_price,
                    "already_closed_before_exit": already_closed,
                },
                human_exit_rows,
            )
            if row:
                created.append(row)
                human_exit_rows.append(row)
    return created


def date_key(value: Any) -> str:
    parsed = parse_datetime(value)
    if parsed is None:
        parsed = parse_datetime(now_iso())
    return parsed.date().isoformat()


def generate_daily_summary(day: str | None = None) -> dict[str, Any]:
    target_day = day or date_key(now_iso())
    raw = [row for row in read_jsonl(DATA_DIR / "raw_notifications.jsonl") if date_key(row.get("captured_at") or row.get("notification_timestamp")) == target_day]
    parsed = [row for row in read_jsonl(DATA_DIR / "parsed_alerts.jsonl") if date_key(row.get("parsed_at")) == target_day]
    rejected = [row for row in read_jsonl(DATA_DIR / "rejected_alerts.jsonl") if date_key(row.get("rejected_at")) == target_day]
    positions = [row for row in read_jsonl(SHADOW_POSITIONS_FILE) if date_key(row.get("opened_at")) == target_day]
    snapshots = read_jsonl(QUOTE_SNAPSHOTS_FILE)
    exits = [row for row in read_jsonl(STEVE_EXITS_FILE) if date_key(row.get("recorded_at")) == target_day]
    human = [row for row in read_jsonl(HUMAN_POSITIONS_FILE) if date_key(row.get("opened_at")) == target_day]
    human_exits = [row for row in read_jsonl(HUMAN_EXITS_FILE) if date_key(row.get("recorded_at")) == target_day]
    metrics = [metrics_for_position(position, snapshots) for position in positions]
    successful_metrics = [row for row in metrics if row.get("status") == "ok"]
    avg_mfe = sum(float(row.get("mfe_pct") or 0) for row in successful_metrics) / len(successful_metrics) if successful_metrics else None
    avg_mae = sum(float(row.get("mae_pct") or 0) for row in successful_metrics) / len(successful_metrics) if successful_metrics else None
    tags: dict[str, dict[str, Any]] = {}
    for position, metric in zip(positions, metrics):
        tag = str(position.get("primary_tag") or "untagged")
        bucket = tags.setdefault(tag, {"count": 0, "avg_mfe_pct": None, "avg_mae_pct": None, "_mfe": [], "_mae": []})
        bucket["count"] += 1
        if metric.get("status") == "ok":
            bucket["_mfe"].append(metric.get("mfe_pct"))
            bucket["_mae"].append(metric.get("mae_pct"))
    for bucket in tags.values():
        if bucket["_mfe"]:
            bucket["avg_mfe_pct"] = sum(bucket["_mfe"]) / len(bucket["_mfe"])
        if bucket["_mae"]:
            bucket["avg_mae_pct"] = sum(bucket["_mae"]) / len(bucket["_mae"])
        bucket.pop("_mfe", None)
        bucket.pop("_mae", None)
    summary = {
        "event_type": "daily_option_validation_summary",
        "generated_at": now_iso(),
        "day": target_day,
        "raw_alerts": len(raw),
        "parsed_options": len([row for row in parsed if row.get("instrument_type") == "option"]),
        "rejected": len(rejected),
        "shadow_positions": len(positions),
        "human_paper_positions": len(human),
        "human_paper_exits": len(human_exits),
        "steve_exits": len(exits),
        "positions_with_market_data": len(successful_metrics),
        "avg_mfe_pct": avg_mfe,
        "avg_mae_pct": avg_mae,
        "by_tag": tags,
    }
    append_jsonl(DAILY_SUMMARIES_FILE, summary)
    write_openclaw_daily_summary(summary, metrics)
    return summary


def write_openclaw_daily_summary(summary: dict[str, Any], metrics: list[dict[str, Any]]) -> None:
    workspace_dir = Path.home() / ".openclaw/workspace/trading_alerts"
    workspace_dir.mkdir(parents=True, exist_ok=True)
    lines = [
        "# Steve Options Validation Summary",
        "",
        f"Day: {summary.get('day')}",
        f"Generated: {summary.get('generated_at')}",
        "",
        f"Raw alerts: {summary.get('raw_alerts')}",
        f"Parsed options: {summary.get('parsed_options')}",
        f"Rejected: {summary.get('rejected')}",
        f"Shadow positions: {summary.get('shadow_positions')}",
        f"Human paper positions: {summary.get('human_paper_positions')}",
        f"Human paper exits: {summary.get('human_paper_exits')}",
        f"Steve exits: {summary.get('steve_exits')}",
        f"Positions with market data: {summary.get('positions_with_market_data')}",
        f"Average MFE %: {summary.get('avg_mfe_pct')}",
        f"Average MAE %: {summary.get('avg_mae_pct')}",
        "",
        "## Tags",
    ]
    for tag, row in sorted((summary.get("by_tag") or {}).items()):
        lines.append(f"- {tag}: count={row.get('count')} avg_mfe={row.get('avg_mfe_pct')} avg_mae={row.get('avg_mae_pct')}")
    lines.extend(["", "## Positions"])
    for metric in metrics[-20:]:
        lines.append(
            "- {position_id}: status={status} mfe={mfe} mae={mae} latest={latest}".format(
                position_id=metric.get("position_id"),
                status=metric.get("status"),
                mfe=metric.get("mfe_pct"),
                mae=metric.get("mae_pct"),
                latest=metric.get("latest_price"),
            )
        )
    (workspace_dir / "latest_steve_options_validation.md").write_text("\n".join(lines) + "\n", encoding="utf-8")


def track_open_positions_once() -> dict[str, int]:
    counts = {"open_positions": 0, "snapshots": 0, "skipped_not_due": 0, "skipped_stale": 0, "human_exits": 0}
    existing_snapshots = read_jsonl(QUOTE_SNAPSHOTS_FILE)
    now_dt = parse_datetime(now_iso())
    for position in open_shadow_positions():
        counts["open_positions"] += 1
        opened_at = parse_datetime(position.get("opened_at"))
        if opened_at is not None and now_dt is not None:
            age_seconds = (now_dt - opened_at).total_seconds()
            if age_seconds > 90 * 60:
                counts["skipped_stale"] += 1
                continue
            desired_interval = 15 if age_seconds <= 10 * 60 else 60
            position_snapshots = [
                row
                for row in existing_snapshots
                if row.get("position_id") == position.get("position_id") and parse_datetime(row.get("recorded_at")) is not None
            ]
            if position_snapshots:
                last_snapshot_at = max(parse_datetime(row.get("recorded_at")) for row in position_snapshots)
                if last_snapshot_at is not None and (now_dt - last_snapshot_at).total_seconds() < desired_interval:
                    counts["skipped_not_due"] += 1
                    continue
        alert = {
            "source_dedupe_key": position.get("source_dedupe_key"),
            "ticker": position.get("ticker"),
            "expiration_date": position.get("expiration_date"),
            "option_type": position.get("option_type"),
            "strike_price": position.get("strike_price"),
            "parsed_at": now_iso(),
        }
        snapshot = enrich_option_alert(alert)
        append_snapshot(alert, snapshot, position.get("position_id"))
        counts["snapshots"] += 1
    counts["human_exits"] = len(apply_human_exit_rules_once())
    return counts


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    sub = parser.add_subparsers(dest="command", required=True)
    sub.add_parser("track-once")
    summary = sub.add_parser("daily-summary")
    summary.add_argument("--day")
    args = parser.parse_args()
    if args.command == "track-once":
        print(json.dumps(track_open_positions_once(), sort_keys=True))
    elif args.command == "daily-summary":
        print(json.dumps(generate_daily_summary(args.day), sort_keys=True))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())

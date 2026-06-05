#!/usr/bin/env python3
"""Append-only validation ledgers for Steve option alerts."""

from __future__ import annotations

import argparse
import json
import os
import re
from pathlib import Path
from typing import Any

from alpaca_options import enrich_option_alert, option_symbol, submit_option_paper_sell_order
from data_hygiene import (
    compact_market_snapshot,
    option_contract_expired,
    quote_snapshot_signature,
    record_is_synthetic_test_artifact,
    source_value_is_synthetic,
    using_project_runtime_path,
)
from pipeline_common import DATA_DIR, append_jsonl, iter_jsonl, now_iso, parse_datetime, read_jsonl, stable_hash


SHADOW_POSITIONS_FILE = DATA_DIR / "shadow_option_positions.jsonl"
QUOTE_SNAPSHOTS_FILE = DATA_DIR / "option_quote_snapshots.jsonl"
TRACKING_STATE_FILE = DATA_DIR / "option_tracking_state.json"
STEVE_EXITS_FILE = DATA_DIR / "steve_option_exits.jsonl"
HUMAN_POSITIONS_FILE = DATA_DIR / "human_paper_positions.jsonl"
HUMAN_EXITS_FILE = DATA_DIR / "human_paper_exits.jsonl"
DAILY_SUMMARIES_FILE = DATA_DIR / "daily_option_summaries.jsonl"
DAILY_PL_REPORTS_FILE = DATA_DIR / "daily_pl_reports.jsonl"
STEVE_ALERT_PL_REPORTS_FILE = DATA_DIR / "steve_alert_pl_reports.jsonl"
DEFAULT_MAX_AUTO_ENTRY_SLIPPAGE_PCT = 5.0
DEFAULT_TRACKING_SNAPSHOT_MIN_MOVE_PCT = 5.0
DEFAULT_TRACKING_SNAPSHOT_FORCE_INTERVAL_SECONDS = 30 * 60
MAX_AUTO_ENTRY_QUOTE_AGE_SECONDS = 300
TRACKING_MILESTONE_PCTS = (-35, 25, 50, 80, 100, 120, 200)
MIXED_BUY_EXIT_RE = re.compile(r"\b(?:sold|closed?|stopped?\s+out)\b", re.I)


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


def is_hedge_alert(alert: dict[str, Any]) -> bool:
    tags = {str(tag).lower() for tag in (alert.get("tags") or [])}
    primary_tag = str(alert.get("primary_tag") or "").lower()
    return primary_tag == "hedge" or "hedge" in tags


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


def quote_age_seconds(snapshot: dict[str, Any]) -> float | None:
    quote = snapshot.get("option_quote") or {}
    quote_time = parse_datetime(quote.get("timestamp"))
    now_time = parse_datetime(now_iso())
    if quote_time is None or now_time is None:
        return None
    return (now_time - quote_time).total_seconds()


def alert_has_mixed_exit_context(alert: dict[str, Any]) -> bool:
    raw_text = str(alert.get("raw_text") or "")
    matched_text = str(alert.get("matched_text") or "")
    context_text = raw_text.replace(matched_text, "", 1) if matched_text else raw_text
    return bool(MIXED_BUY_EXIT_RE.search(context_text))


def max_auto_entry_slippage_pct() -> float:
    raw = os.environ.get("OPENCLAW_MAX_ENTRY_SLIPPAGE_PCT", "")
    if not raw:
        return DEFAULT_MAX_AUTO_ENTRY_SLIPPAGE_PCT
    try:
        value = float(raw)
    except (TypeError, ValueError):
        return DEFAULT_MAX_AUTO_ENTRY_SLIPPAGE_PCT
    return value if value > 0 else DEFAULT_MAX_AUTO_ENTRY_SLIPPAGE_PCT


def env_float(name: str, default: float, *, minimum: float | None = None) -> float:
    raw = os.environ.get(name, "")
    if not raw:
        return default
    try:
        value = float(raw)
    except (TypeError, ValueError):
        return default
    if minimum is not None and value < minimum:
        return default
    return value


def tracking_snapshot_min_move_pct() -> float:
    return env_float("OPENCLAW_QUOTE_SNAPSHOT_MIN_MOVE_PCT", DEFAULT_TRACKING_SNAPSHOT_MIN_MOVE_PCT, minimum=0.0)


def tracking_snapshot_force_interval_seconds() -> float:
    return env_float(
        "OPENCLAW_QUOTE_SNAPSHOT_FORCE_INTERVAL_SECONDS",
        DEFAULT_TRACKING_SNAPSHOT_FORCE_INTERVAL_SECONDS,
        minimum=60.0,
    )


def auto_entry_guard(alert: dict[str, Any], snapshot: dict[str, Any]) -> dict[str, Any]:
    quote = snapshot.get("option_quote") or {}
    reasons: list[str] = []
    alert_price = safe_float(alert.get("entry_price"))
    observed_price, observed_source = quote_entry_price(snapshot)
    age_seconds = quote_age_seconds(snapshot)
    quote_status = str(quote.get("status") or "")
    slippage_pct: float | None = None
    if quote_status and quote_status != "ok":
        reasons.append("entry_quote_unavailable")
    if age_seconds is None:
        reasons.append("missing_quote_timestamp")
    elif age_seconds < 0 or age_seconds > MAX_AUTO_ENTRY_QUOTE_AGE_SECONDS:
        reasons.append("stale_entry_quote")
    if observed_price is None or observed_price <= 0:
        reasons.append("entry_price_unavailable")
    max_slippage_pct = max_auto_entry_slippage_pct()
    if alert_price and observed_price and observed_price > alert_price:
        slippage_pct = ((observed_price - alert_price) / alert_price) * 100
        if slippage_pct > max_slippage_pct:
            reasons.append("entry_price_above_alert_threshold")
    if alert_has_mixed_exit_context(alert):
        reasons.append("mixed_buy_exit_message")
    return {
        "ok": not reasons,
        "reasons": reasons,
        "max_slippage_pct": max_slippage_pct,
        "max_quote_age_seconds": MAX_AUTO_ENTRY_QUOTE_AGE_SECONDS,
        "alert_entry_price": alert_price,
        "observed_entry_price": observed_price,
        "observed_entry_price_source": observed_source,
        "entry_slippage_pct": slippage_pct,
        "quote_age_seconds": age_seconds,
        "quote_status": quote_status or None,
    }


def auto_entry_guard_message(guard: dict[str, Any]) -> str:
    labels = {
        "entry_quote_unavailable": "quote unavailable",
        "missing_quote_timestamp": "quote timestamp missing",
        "stale_entry_quote": "quote stale",
        "entry_price_unavailable": "entry price unavailable",
        "entry_price_above_alert_threshold": "price moved beyond threshold",
        "mixed_buy_exit_message": "same message includes exit context",
    }
    reasons = ", ".join(labels.get(reason, str(reason)) for reason in guard.get("reasons") or [])
    observed = guard.get("observed_entry_price")
    alert_price = guard.get("alert_entry_price")
    slippage = guard.get("entry_slippage_pct")
    details = []
    if observed is not None and alert_price is not None:
        details.append(f"alert={float(alert_price):.2f}")
        details.append(f"observed={float(observed):.2f}")
    if slippage is not None:
        details.append(f"slippage={float(slippage):.1f}%")
    return "Auto buy held: " + (reasons or "guarded entry") + (f" ({', '.join(details)})" if details else "")


def snapshot_record_for_storage(
    alert: dict[str, Any],
    snapshot: dict[str, Any],
    position_id: str | None = None,
    storage_profile: str = "entry_full_v1",
) -> dict[str, Any]:
    record = compact_market_snapshot(snapshot, profile=storage_profile) if storage_profile != "entry_full_v1" else dict(snapshot)
    record["source_dedupe_key"] = alert.get("source_dedupe_key")
    record["validation_id"] = validation_id(alert)
    record["position_id"] = position_id
    record["storage_profile"] = storage_profile
    return record


def append_snapshot(
    alert: dict[str, Any],
    snapshot: dict[str, Any],
    position_id: str | None = None,
    storage_profile: str = "entry_full_v1",
) -> dict[str, Any]:
    record = snapshot_record_for_storage(alert, snapshot, position_id, storage_profile)
    if record_is_synthetic_test_artifact(record) and using_project_runtime_path(QUOTE_SNAPSHOTS_FILE):
        record["storage_skipped"] = "synthetic_test_artifact"
        return record
    append_jsonl(QUOTE_SNAPSHOTS_FILE, record)
    return record


def enrich_tracking_snapshot(alert: dict[str, Any]) -> dict[str, Any]:
    try:
        return enrich_option_alert(alert, include_context=False)
    except TypeError:
        return enrich_option_alert(alert)


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
    if source_value_is_synthetic(alert.get("source_dedupe_key")) and using_project_runtime_path(SHADOW_POSITIONS_FILE):
        return {
            "event_type": "option_entry_validation_result",
            "validation_id": validation_id(alert),
            "position_id": None,
            "shadow_position_created": False,
            "route": "ignored_test_artifact",
            "route_reason": "synthetic_test_source",
            "auto_entry_guard": {},
            "approval_card": {},
            "auto_buy": {},
            "snapshot_status": None,
        }
    snapshot = enrich_option_alert(alert)
    position, created = create_shadow_position(alert, snapshot)
    append_snapshot(alert, snapshot, position.get("position_id"))
    card: dict[str, Any] | None = None
    auto_buy: dict[str, Any] | None = None
    guard = auto_entry_guard(alert, snapshot)
    if send_approval:
        if is_hedge_alert(alert):
            from steve_trade_bot import send_approval_card

            card = send_approval_card(alert, snapshot, position)
        elif guard["ok"]:
            from steve_trade_bot import auto_paper_buy

            auto_buy = auto_paper_buy(alert, snapshot)
        else:
            from steve_trade_bot import send_approval_card

            guarded_alert = dict(alert)
            guarded_alert["approval_reason"] = auto_entry_guard_message(guard)
            guarded_alert["auto_entry_guard"] = guard
            card = send_approval_card(guarded_alert, snapshot, position)
    return {
        "event_type": "option_entry_validation_result",
        "validation_id": validation_id(alert),
        "position_id": position.get("position_id"),
        "shadow_position_created": created,
        "route": "auto_paper_buy" if auto_buy else "approval_required" if card else "not_sent",
        "route_reason": "hedge" if is_hedge_alert(alert) else "auto_entry_guard" if card and not guard["ok"] else "",
        "auto_entry_guard": guard,
        "approval_card": card or {},
        "auto_buy": auto_buy or {},
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


def option_tracking_market_open(now_dt: Any) -> bool:
    if now_dt is None:
        return True
    if now_dt.weekday() >= 5:
        return False
    market_open = now_dt.replace(hour=9, minute=30, second=0, microsecond=0)
    market_close = now_dt.replace(hour=16, minute=15, second=0, microsecond=0)
    return market_open <= now_dt <= market_close


def latest_snapshot_metadata_by_position() -> dict[str, dict[str, Any]]:
    latest: dict[str, dict[str, Any]] = {}
    for row in iter_jsonl(QUOTE_SNAPSHOTS_FILE):
        position_id = str(row.get("position_id") or "")
        if not position_id:
            continue
        recorded_at = parse_datetime(row.get("recorded_at"))
        if recorded_at is None:
            continue
        previous = latest.get(position_id)
        if previous is None or recorded_at > previous["recorded_at"]:
            latest[position_id] = {
                "recorded_at": recorded_at,
                "signature": quote_snapshot_signature(row),
                "snapshot_id": row.get("snapshot_id"),
            }
    return latest


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

    def keep_if_any(rows: list[dict[str, Any]], key: str, value: Any) -> list[dict[str, Any]]:
        if value in (None, ""):
            return rows
        filtered = [row for row in rows if str(row.get(key)) == str(value)]
        return filtered or rows

    candidates = keep_if_any(candidates, "expiration_date", exit_alert.get("expiration_date"))
    candidates = keep_if_any(candidates, "option_type", exit_alert.get("option_type"))
    strike_price = safe_float(exit_alert.get("strike_price"))
    if strike_price is not None:
        filtered = [row for row in candidates if safe_float(row.get("strike_price")) == strike_price]
        candidates = filtered or candidates
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
    for key in ("expiration_date", "option_type", "strike_price", "context_entry_price", "context_contracts"):
        if exit_alert.get(key) is not None:
            record[key] = exit_alert.get(key)
    if matched and exit_alert.get("expiration_date") and exit_alert.get("strike_price"):
        record["match_confidence"] = "high"
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


def load_tracking_state() -> dict[str, Any]:
    if not TRACKING_STATE_FILE.exists():
        return {"event_type": "option_tracking_state", "storage_profile": "latest_position_state_v1", "positions": {}}
    try:
        state = json.loads(TRACKING_STATE_FILE.read_text(encoding="utf-8"))
    except json.JSONDecodeError:
        return {"event_type": "option_tracking_state", "storage_profile": "latest_position_state_v1", "positions": {}}
    if not isinstance(state, dict):
        return {"event_type": "option_tracking_state", "storage_profile": "latest_position_state_v1", "positions": {}}
    positions = state.get("positions")
    if not isinstance(positions, dict):
        state["positions"] = {}
    return state


def write_tracking_state(state: dict[str, Any]) -> None:
    TRACKING_STATE_FILE.parent.mkdir(parents=True, exist_ok=True)
    state["event_type"] = "option_tracking_state"
    state["storage_profile"] = "latest_position_state_v1"
    state["updated_at"] = now_iso()
    tmp_path = TRACKING_STATE_FILE.with_suffix(TRACKING_STATE_FILE.suffix + ".tmp")
    tmp_path.write_text(json.dumps(state, sort_keys=True, separators=(",", ":")) + "\n", encoding="utf-8")
    tmp_path.replace(TRACKING_STATE_FILE)


def percent_move(previous: Any, current: Any) -> float | None:
    previous_float = safe_float(previous)
    current_float = safe_float(current)
    if previous_float is None or current_float is None or previous_float <= 0:
        return None
    return ((current_float - previous_float) / previous_float) * 100


def position_entry_price(position: dict[str, Any]) -> float | None:
    return safe_float(position.get("bot_entry_price")) or safe_float(position.get("alert_entry_price"))


def position_return_pct(position: dict[str, Any], current_price: float | None) -> float | None:
    entry = position_entry_price(position)
    if entry is None or entry <= 0 or current_price is None:
        return None
    return ((current_price - entry) / entry) * 100


def threshold_key(threshold_pct: int | float) -> str:
    value = int(threshold_pct)
    return f"down_{abs(value)}pct" if value < 0 else f"up_{value}pct"


def threshold_crossed(return_pct: float | None, threshold_pct: int | float) -> bool:
    if return_pct is None:
        return False
    return return_pct <= float(threshold_pct) if float(threshold_pct) < 0 else return_pct >= float(threshold_pct)


def human_exit_boundary_crossed(
    shadow_position: dict[str, Any],
    current_price: float | None,
    human_exit_rows: list[dict[str, Any]],
) -> bool:
    if current_price is None or current_price <= 0:
        return False
    for position in human_positions_for_shadow(shadow_position):
        remaining = human_remaining_contracts(position, human_exit_rows)
        if remaining <= 0:
            continue
        stop_price = stop_price_for_human_position(position)
        if stop_price is not None and current_price <= stop_price:
            return True
        already_closed = human_exit_contracts(str(position.get("position_id")), human_exit_rows)
        cumulative_target_contracts = 0
        for tranche in sorted(position.get("exit_plan") or [], key=lambda row: float(row.get("take_percent") or 0)):
            tranche_contracts = int(tranche.get("contracts") or 0)
            if tranche_contracts <= 0:
                continue
            cumulative_target_contracts += tranche_contracts
            if cumulative_target_contracts <= already_closed:
                continue
            take_price = safe_float(tranche.get("take_price"))
            if take_price is None:
                entry_price = safe_float(position.get("entry_price"))
                take_percent = safe_float(tranche.get("take_percent"))
                if entry_price is None or take_percent is None:
                    continue
                take_price = entry_price * (1 + take_percent / 100)
            if current_price >= take_price:
                return True
    return False


def tracking_decision_for_snapshot(
    position: dict[str, Any],
    record: dict[str, Any],
    position_state: dict[str, Any],
    now_dt: Any,
    human_exit_rows: list[dict[str, Any]],
) -> dict[str, Any]:
    reasons: list[str] = []
    min_move_pct = tracking_snapshot_min_move_pct()
    current_price = price_from_snapshot(record)
    quote = record.get("option_quote") or {}
    quote_status = str(quote.get("status") or "unknown")
    if not position_state.get("last_appended_at"):
        reasons.append("first_observation")
    if quote_status != str(position_state.get("last_quote_status") or ""):
        reasons.append("quote_status_changed")
    if human_exit_boundary_crossed(position, current_price, human_exit_rows):
        reasons.append("human_exit_boundary")
    move_from_appended = percent_move(position_state.get("last_appended_price"), current_price)
    if move_from_appended is not None and abs(move_from_appended) >= min_move_pct:
        reasons.append("significant_move")
    move_from_max = percent_move(position_state.get("max_price"), current_price)
    if move_from_max is not None and move_from_max >= min_move_pct:
        reasons.append("new_mfe_extreme")
    move_from_min = percent_move(position_state.get("min_price"), current_price)
    if move_from_min is not None and move_from_min <= -min_move_pct:
        reasons.append("new_mae_extreme")
    return_pct = position_return_pct(position, current_price)
    threshold_hits = position_state.get("threshold_hits") if isinstance(position_state.get("threshold_hits"), dict) else {}
    for threshold_pct in TRACKING_MILESTONE_PCTS:
        key = threshold_key(threshold_pct)
        if not threshold_hits.get(key) and threshold_crossed(return_pct, threshold_pct):
            reasons.append(f"threshold:{key}")
    last_appended_at = parse_datetime(position_state.get("last_appended_at"))
    if last_appended_at is not None and now_dt is not None:
        if (now_dt - last_appended_at).total_seconds() >= tracking_snapshot_force_interval_seconds():
            reasons.append("forced_checkpoint")
    return {
        "append": bool(reasons),
        "reasons": sorted(set(reasons)),
        "current_price": current_price,
        "return_pct": return_pct,
        "quote_status": quote_status,
    }


def update_tracking_state_for_snapshot(
    state: dict[str, Any],
    position: dict[str, Any],
    record: dict[str, Any],
    decision: dict[str, Any],
) -> dict[str, Any]:
    positions = state.setdefault("positions", {})
    position_id = str(position.get("position_id") or "")
    position_state = dict(positions.get(position_id) or {})
    current_price = decision.get("current_price")
    recorded_at = str(record.get("recorded_at") or now_iso())
    quote = dict(record.get("option_quote") or {})
    position_state.update(
        {
            "position_id": position_id,
            "source_dedupe_key": position.get("source_dedupe_key"),
            "ticker": position.get("ticker"),
            "contract_symbol": position.get("contract_symbol") or record.get("contract_symbol") or quote.get("symbol"),
            "latest_recorded_at": recorded_at,
            "latest_price": current_price,
            "latest_option_quote": quote,
            "latest_snapshot_id": record.get("snapshot_id"),
            "last_quote_status": decision.get("quote_status"),
            "observation_count": int(position_state.get("observation_count") or 0) + 1,
        }
    )
    if current_price is not None:
        if safe_float(position_state.get("max_price")) is None or float(current_price) > float(position_state.get("max_price")):
            position_state["max_price"] = current_price
            position_state["max_price_at"] = recorded_at
        if safe_float(position_state.get("min_price")) is None or float(current_price) < float(position_state.get("min_price")):
            position_state["min_price"] = current_price
            position_state["min_price_at"] = recorded_at
        return_pct = decision.get("return_pct")
        threshold_hits = position_state.get("threshold_hits") if isinstance(position_state.get("threshold_hits"), dict) else {}
        for threshold_pct in TRACKING_MILESTONE_PCTS:
            key = threshold_key(threshold_pct)
            if not threshold_hits.get(key) and threshold_crossed(return_pct, threshold_pct):
                threshold_hits[key] = recorded_at
        position_state["threshold_hits"] = threshold_hits
    if decision.get("append"):
        position_state["last_appended_at"] = recorded_at
        position_state["last_appended_price"] = current_price
        position_state["last_appended_signature"] = quote_snapshot_signature(record)
        position_state["last_append_reasons"] = decision.get("reasons") or []
    else:
        position_state["skipped_uninteresting_count"] = int(position_state.get("skipped_uninteresting_count") or 0) + 1
    positions[position_id] = position_state
    return position_state


def tracking_state_snapshots() -> list[dict[str, Any]]:
    state = load_tracking_state()
    rows: list[dict[str, Any]] = []
    for position_id, position_state in (state.get("positions") or {}).items():
        base = {
            "event_type": "option_market_snapshot",
            "source_dedupe_key": position_state.get("source_dedupe_key"),
            "position_id": position_id,
            "ticker": position_state.get("ticker"),
            "contract_symbol": position_state.get("contract_symbol"),
            "storage_profile": "tracking_state_latest_v1",
        }
        latest_quote = dict(position_state.get("latest_option_quote") or {})
        if position_state.get("latest_recorded_at") and latest_quote:
            rows.append(
                {
                    **base,
                    "snapshot_id": position_state.get("latest_snapshot_id") or f"state-latest-{position_id}",
                    "recorded_at": position_state.get("latest_recorded_at"),
                    "option_quote": latest_quote,
                }
            )
        for label in ("max", "min"):
            price = safe_float(position_state.get(f"{label}_price"))
            timestamp = position_state.get(f"{label}_price_at")
            if price is None or not timestamp:
                continue
            quote = dict(latest_quote)
            quote["mark"] = price
            quote["last"] = price
            quote["status"] = quote.get("status") or "ok"
            rows.append(
                {
                    **base,
                    "snapshot_id": f"state-{label}-{position_id}",
                    "recorded_at": timestamp,
                    "storage_profile": f"tracking_state_{label}_v1",
                    "option_quote": quote,
                }
            )
    return rows


def metrics_for_position(position: dict[str, Any], snapshots: list[dict[str, Any]]) -> dict[str, Any]:
    entry_price = position.get("bot_entry_price") or position.get("alert_entry_price")
    try:
        entry = float(entry_price)
    except (TypeError, ValueError):
        entry = 0.0
    prices = [(row.get("recorded_at"), price_from_snapshot(row)) for row in snapshots if row.get("position_id") == position.get("position_id")]
    prices = [(timestamp, price) for timestamp, price in prices if price is not None]
    prices = sorted(prices, key=lambda item: str(item[0] or ""))
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


def has_open_human_position_for_shadow(shadow_position: dict[str, Any], exit_rows: list[dict[str, Any]] | None = None) -> bool:
    return any(human_remaining_contracts(position, exit_rows) > 0 for position in human_positions_for_shadow(shadow_position))


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
        "option_type": position.get("option_type"),
        "expiration_date": position.get("expiration_date"),
        "strike_price": position.get("strike_price"),
        "position_contracts": int(position.get("contracts") or 0),
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
    broker_audit = submit_option_paper_sell_order(position, quantity, reason, exit_id)
    broker_response = broker_audit.get("response") or {}
    record.update(
        {
            "broker_status": broker_audit.get("status"),
            "broker_reason": broker_audit.get("reason"),
            "broker_order_id": broker_response.get("id"),
            "broker_client_order_id": broker_response.get("client_order_id") or (broker_audit.get("payload") or {}).get("client_order_id"),
        }
    )
    append_jsonl(HUMAN_EXITS_FILE, record)
    try:
        from steve_trade_bot import send_human_exit_report

        send_human_exit_report(record)
    except Exception as exc:  # noqa: BLE001
        append_jsonl(
            DATA_DIR / "option_validation_errors.jsonl",
            {
                "event_type": "option_validation_error",
                "recorded_at": now_iso(),
                "source_dedupe_key": position.get("source_dedupe_key"),
                "ticker": position.get("ticker"),
                "reason": f"close_report_error:{type(exc).__name__}:{exc}",
                "raw_text": "",
            },
        )
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
    snapshots = read_jsonl(QUOTE_SNAPSHOTS_FILE) + tracking_state_snapshots()
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


def strict_date_key(value: Any) -> str:
    parsed = parse_datetime(value)
    return parsed.date().isoformat() if parsed is not None else ""


def generate_daily_summary(day: str | None = None) -> dict[str, Any]:
    target_day = day or date_key(now_iso())
    raw = [row for row in read_jsonl(DATA_DIR / "raw_notifications.jsonl") if date_key(row.get("captured_at") or row.get("notification_timestamp")) == target_day]
    parsed = [row for row in read_jsonl(DATA_DIR / "parsed_alerts.jsonl") if date_key(row.get("parsed_at")) == target_day]
    rejected = [row for row in read_jsonl(DATA_DIR / "rejected_alerts.jsonl") if date_key(row.get("rejected_at")) == target_day]
    positions = [row for row in read_jsonl(SHADOW_POSITIONS_FILE) if date_key(row.get("opened_at")) == target_day]
    snapshots = read_jsonl(QUOTE_SNAPSHOTS_FILE) + tracking_state_snapshots()
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


def compute_daily_pl_summary(day: str | None = None) -> dict[str, Any]:
    target_day = day or date_key(now_iso())
    all_positions = read_jsonl(HUMAN_POSITIONS_FILE)
    all_exits = read_jsonl(HUMAN_EXITS_FILE)
    exits_today = [row for row in all_exits if date_key(row.get("recorded_at")) == target_day]
    snapshots = read_jsonl(QUOTE_SNAPSHOTS_FILE) + tracking_state_snapshots()
    realized_pnl = sum(float(row.get("pnl_dollars") or 0) for row in exits_today)
    open_pnl = 0.0
    open_positions = 0
    marked_positions = 0
    for position in all_positions:
        remaining = human_remaining_contracts(position, all_exits)
        if remaining <= 0:
            continue
        open_positions += 1
        snapshot = latest_snapshot_for_human_position(position, snapshots)
        current_price = price_from_snapshot(snapshot or {})
        entry_price = safe_float(position.get("entry_price"))
        if current_price is None or entry_price is None:
            continue
        marked_positions += 1
        open_pnl += (current_price - entry_price) * remaining * 100
    summary = {
        "event_type": "daily_paper_pl_summary",
        "day": target_day,
        "generated_at": now_iso(),
        "realized_pnl": realized_pnl,
        "open_pnl": open_pnl,
        "total_pnl": realized_pnl + open_pnl,
        "open_positions": open_positions,
        "marked_open_positions": marked_positions,
        "exits_today": len(exits_today),
        "contracts_closed_today": sum(int(row.get("contracts") or 0) for row in exits_today),
    }
    return summary


def latest_snapshot_for_shadow_position(position: dict[str, Any], snapshots: list[dict[str, Any]]) -> dict[str, Any] | None:
    source_key = position.get("source_dedupe_key")
    contract_symbol = position.get("contract_symbol")
    position_id = position.get("position_id")
    candidates = [
        row
        for row in snapshots
        if (
            (position_id and row.get("position_id") == position_id)
            or (source_key and row.get("source_dedupe_key") == source_key)
            or (contract_symbol and row.get("contract_symbol") == contract_symbol)
            or (contract_symbol and (row.get("option_quote") or {}).get("symbol") == contract_symbol)
        )
        and parse_datetime(row.get("recorded_at")) is not None
    ]
    if not candidates:
        return None
    return max(candidates, key=lambda row: parse_datetime(row.get("recorded_at")))


def compute_steve_alert_pl_summary(
    day: str | None = None,
    all_time: bool = False,
    positions: list[dict[str, Any]] | None = None,
    exits: list[dict[str, Any]] | None = None,
    snapshots: list[dict[str, Any]] | None = None,
) -> dict[str, Any]:
    """P/L from Steve's alert buy prices and Steve's explicit sell prices only."""
    target_day = day or date_key(now_iso())
    positions = positions if positions is not None else read_jsonl(SHADOW_POSITIONS_FILE)
    exits = sorted(exits if exits is not None else read_jsonl(STEVE_EXITS_FILE), key=lambda row: str(row.get("recorded_at") or ""))
    snapshots = snapshots if snapshots is not None else read_jsonl(QUOTE_SNAPSHOTS_FILE) + tracking_state_snapshots()
    positions_by_id = {str(row.get("position_id")): row for row in positions if row.get("position_id")}
    closed_by_position: dict[str, int] = {}
    realized_pnl = 0.0
    closed_contracts = 0
    exit_count = 0
    unmatched_exit_contracts = 0
    exit_details: list[dict[str, Any]] = []

    for exit_row in exits:
        exit_day = strict_date_key(exit_row.get("recorded_at"))
        position_id = str(exit_row.get("matched_shadow_position_id") or "")
        position = positions_by_id.get(position_id)
        requested_contracts = int(exit_row.get("contracts") or 0)
        if not position:
            if all_time or exit_day == target_day:
                unmatched_exit_contracts += max(0, requested_contracts)
            continue
        position_contracts = int(position.get("contracts") or 0)
        already_closed = closed_by_position.get(position_id, 0)
        quantity = min(max(0, requested_contracts), max(0, position_contracts - already_closed))
        closed_by_position[position_id] = already_closed + quantity
        if quantity <= 0 or (not all_time and exit_day != target_day):
            continue
        entry_price = safe_float(position.get("alert_entry_price")) or safe_float(position.get("bot_entry_price"))
        exit_price = safe_float(exit_row.get("exit_price"))
        pnl_dollars = None
        pnl_percent = None
        if entry_price is not None and exit_price is not None and entry_price > 0:
            pnl_percent = ((exit_price - entry_price) / entry_price) * 100
            pnl_dollars = (exit_price - entry_price) * quantity * 100
            realized_pnl += pnl_dollars
        closed_contracts += quantity
        exit_count += 1
        exit_details.append(
            {
                "exit_id": exit_row.get("exit_id"),
                "position_id": position_id,
                "ticker": exit_row.get("ticker") or position.get("ticker"),
                "contract_symbol": position.get("contract_symbol"),
                "contracts": quantity,
                "entry_price_source": "steve_buy_alert",
                "entry_price": entry_price,
                "exit_price_source": "steve_sell_alert",
                "exit_price": exit_price,
                "pnl_percent": pnl_percent,
                "pnl_dollars": pnl_dollars,
            }
        )

    open_pnl = 0.0
    open_positions = 0
    open_contracts = 0
    marked_open_positions = 0
    unmarked_open_positions = 0
    for position in positions:
        position_id = str(position.get("position_id") or "")
        remaining = max(0, int(position.get("contracts") or 0) - closed_by_position.get(position_id, 0))
        if remaining <= 0:
            continue
        open_positions += 1
        open_contracts += remaining
        entry_price = safe_float(position.get("alert_entry_price")) or safe_float(position.get("bot_entry_price"))
        snapshot = latest_snapshot_for_shadow_position(position, snapshots)
        current_price = price_from_snapshot(snapshot or {})
        if entry_price is None or current_price is None:
            unmarked_open_positions += 1
            continue
        marked_open_positions += 1
        open_pnl += (current_price - entry_price) * remaining * 100

    summary = {
        "event_type": "steve_alert_pl_summary",
        "period": "all_time" if all_time else "day",
        "day": target_day,
        "generated_at": now_iso(),
        "basis": "steve_buy_alert_and_steve_sell_alert_prices",
        "realized_pnl": realized_pnl,
        "open_pnl": open_pnl,
        "total_pnl": realized_pnl + open_pnl,
        "entries_today": sum(1 for row in positions if strict_date_key(row.get("opened_at")) == target_day),
        "exits": exit_count,
        "contracts_closed": closed_contracts,
        "open_positions": open_positions,
        "open_contracts": open_contracts,
        "marked_open_positions": marked_open_positions,
        "unmarked_open_positions": unmarked_open_positions,
        "unmatched_exit_contracts": unmatched_exit_contracts,
        "exit_details": exit_details[-25:],
    }
    return summary


def daily_pl_summary_exists(day: str) -> bool:
    return any(row.get("day") == day and row.get("status") in {"sent", "partial_sent", "telegram_disabled"} for row in read_jsonl(DAILY_PL_REPORTS_FILE))


def send_daily_pl_summary_once(tz_name: str = "America/Detroit", force: bool = False) -> dict[str, Any]:
    now = parse_datetime(now_iso(tz_name))
    if now is None:
        return {"sent": False, "reason": "clock_unavailable"}
    if now.weekday() >= 5 and not force:
        return {"sent": False, "reason": "weekend"}
    due_at = now.replace(hour=16, minute=10, second=0, microsecond=0)
    if now < due_at and not force:
        return {"sent": False, "reason": "not_due"}
    day = now.date().isoformat()
    if daily_pl_summary_exists(day):
        return {"sent": False, "reason": "already_sent", "day": day}
    summary = compute_daily_pl_summary(day)
    try:
        from steve_trade_bot import send_daily_pl_report

        report = send_daily_pl_report(summary)
    except Exception as exc:  # noqa: BLE001
        report = {
            "event_type": "daily_pl_report",
            "day": day,
            "created_at": now_iso(tz_name),
            "status": "send_failed",
            "reason": f"{type(exc).__name__}:{exc}",
            "summary": summary,
            "telegram_messages": [],
        }
        append_jsonl(DAILY_PL_REPORTS_FILE, report)
    return {"sent": True, "day": day, "report": report}


def track_open_positions_once() -> dict[str, int]:
    counts = {
        "open_positions": 0,
        "snapshots": 0,
        "skipped_not_due": 0,
        "skipped_stale": 0,
        "skipped_synthetic": 0,
        "skipped_expired": 0,
        "skipped_duplicate_quote": 0,
        "skipped_uninteresting_quote": 0,
        "skipped_market_closed": 0,
        "forced_snapshots": 0,
        "milestone_snapshots": 0,
        "human_exits": 0,
    }
    human_exit_rows = read_jsonl(HUMAN_EXITS_FILE)
    now_dt = parse_datetime(now_iso())
    if not option_tracking_market_open(now_dt):
        open_positions = open_shadow_positions()
        counts["open_positions"] = len(open_positions)
        counts["skipped_market_closed"] = len(open_positions)
        counts["human_exits"] = len(apply_human_exit_rules_once())
        return counts
    latest_snapshots = latest_snapshot_metadata_by_position()
    tracking_state = load_tracking_state()
    state_positions = tracking_state.setdefault("positions", {})
    for position in open_shadow_positions():
        counts["open_positions"] += 1
        if record_is_synthetic_test_artifact(position):
            counts["skipped_synthetic"] += 1
            continue
        if option_contract_expired(position, at=now_dt):
            counts["skipped_expired"] += 1
            continue
        opened_at = parse_datetime(position.get("opened_at"))
        if opened_at is not None and now_dt is not None:
            age_seconds = (now_dt - opened_at).total_seconds()
            has_open_human = has_open_human_position_for_shadow(position, human_exit_rows)
            if age_seconds > 90 * 60 and not has_open_human:
                counts["skipped_stale"] += 1
                continue
            desired_interval = 15 if age_seconds <= 10 * 60 else 60
            latest = latest_snapshots.get(str(position.get("position_id") or ""))
            position_state = state_positions.get(str(position.get("position_id") or "")) or {}
            latest_observed_at = parse_datetime(position_state.get("latest_recorded_at"))
            if latest_observed_at is not None and (
                latest is None or latest_observed_at > latest.get("recorded_at")
            ):
                latest = {
                    "recorded_at": latest_observed_at,
                    "signature": position_state.get("last_appended_signature"),
                    "snapshot_id": position_state.get("latest_snapshot_id"),
                }
            if latest:
                last_snapshot_at = latest.get("recorded_at")
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
        snapshot = enrich_tracking_snapshot(alert)
        snapshot["recorded_at"] = now_iso()
        record = snapshot_record_for_storage(alert, snapshot, position.get("position_id"), storage_profile="tracking_core_v1")
        latest = latest_snapshots.get(str(position.get("position_id") or ""))
        signature = quote_snapshot_signature(record)
        if latest and latest.get("signature") == signature:
            counts["skipped_duplicate_quote"] += 1
            latest["recorded_at"] = now_dt
            decision = {"append": False, "reasons": [], "current_price": price_from_snapshot(record), "return_pct": position_return_pct(position, price_from_snapshot(record)), "quote_status": str((record.get("option_quote") or {}).get("status") or "unknown")}
            update_tracking_state_for_snapshot(tracking_state, position, record, decision)
            continue
        position_state = state_positions.get(str(position.get("position_id") or "")) or {}
        decision = tracking_decision_for_snapshot(position, record, position_state, now_dt, human_exit_rows)
        update_tracking_state_for_snapshot(tracking_state, position, record, decision)
        if not decision["append"]:
            counts["skipped_uninteresting_quote"] += 1
            continue
        record["append_reasons"] = decision.get("reasons") or []
        append_jsonl(QUOTE_SNAPSHOTS_FILE, record)
        latest_snapshots[str(position.get("position_id") or "")] = {
            "recorded_at": now_dt,
            "signature": signature,
            "snapshot_id": record.get("snapshot_id"),
        }
        if "forced_checkpoint" in (decision.get("reasons") or []):
            counts["forced_snapshots"] += 1
        if any(str(reason).startswith("threshold:") for reason in (decision.get("reasons") or [])):
            counts["milestone_snapshots"] += 1
        counts["snapshots"] += 1
    write_tracking_state(tracking_state)
    counts["human_exits"] = len(apply_human_exit_rules_once())
    return counts


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    sub = parser.add_subparsers(dest="command", required=True)
    sub.add_parser("track-once")
    summary = sub.add_parser("daily-summary")
    summary.add_argument("--day")
    daily_pl = sub.add_parser("daily-pl")
    daily_pl.add_argument("--day")
    daily_pl.add_argument("--send", action="store_true")
    daily_pl.add_argument("--force", action="store_true")
    args = parser.parse_args()
    if args.command == "track-once":
        print(json.dumps(track_open_positions_once(), sort_keys=True))
    elif args.command == "daily-summary":
        print(json.dumps(generate_daily_summary(args.day), sort_keys=True))
    elif args.command == "daily-pl":
        if args.send:
            print(json.dumps(send_daily_pl_summary_once(force=args.force), sort_keys=True))
        else:
            print(json.dumps(compute_daily_pl_summary(args.day), sort_keys=True))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())

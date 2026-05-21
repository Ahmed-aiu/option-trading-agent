#!/usr/bin/env python3
"""Monitor Alpaca paper order status and report terminal broker outcomes."""

from __future__ import annotations

import argparse
import json
from pathlib import Path
from typing import Any

from alpaca_paper_adapter import alpaca_request, load_adapter_config, require_paper_environment
from alpaca_options import option_exit_order_client_id, submit_option_paper_sell_order
from pipeline_common import DATA_DIR, append_jsonl, now_iso, parse_datetime, read_jsonl


ORDERS_FILE = DATA_DIR / "orders_paper.jsonl"
HUMAN_POSITIONS_FILE = DATA_DIR / "human_paper_positions.jsonl"
HUMAN_EXITS_FILE = DATA_DIR / "human_paper_exits.jsonl"
ORDER_STATUS_FILE = DATA_DIR / "broker_order_status_reports.jsonl"
TERMINAL_STATUSES = {"filled", "canceled", "expired", "rejected", "failed"}


def safe_float(value: Any) -> float | None:
    if value in (None, ""):
        return None
    try:
        return float(value)
    except (TypeError, ValueError):
        return None


def order_age_hours(order: dict[str, Any]) -> float | None:
    recorded_at = parse_datetime(order.get("recorded_at"))
    now = parse_datetime(now_iso())
    if recorded_at is None or now is None:
        return None
    return (now - recorded_at).total_seconds() / 3600


def submitted_order_audits(max_age_hours: float = 8.0) -> list[dict[str, Any]]:
    orders: list[dict[str, Any]] = []
    for row in read_jsonl(ORDERS_FILE):
        if row.get("event_type") != "alpaca_option_paper_order_audit":
            continue
        if row.get("status") != "submitted":
            continue
        order_id = (row.get("response") or {}).get("id")
        if not order_id:
            continue
        age = order_age_hours(row)
        if age is not None and age > max_age_hours:
            continue
        orders.append(row)
    return orders


def reported_keys() -> set[str]:
    return {f"{row.get('order_id')}:{row.get('broker_status')}" for row in read_jsonl(ORDER_STATUS_FILE)}


def load_order_environment() -> dict[str, str]:
    config, env_file = load_adapter_config()
    return require_paper_environment(config, env_file, require_keys=True)


def fetch_order_status(env: dict[str, str], order_id: str) -> dict[str, Any]:
    _status, response, _headers = alpaca_request("GET", f"/v2/orders/{order_id}", env)
    return response


def position_by_id(position_id: str | None) -> dict[str, Any]:
    if not position_id:
        return {}
    for row in read_jsonl(HUMAN_POSITIONS_FILE):
        if str(row.get("position_id")) == str(position_id):
            return row
    return {}


def order_client_ids() -> set[str]:
    ids: set[str] = set()
    for row in read_jsonl(ORDERS_FILE):
        payload = row.get("payload") or {}
        response = row.get("response") or {}
        if payload.get("client_order_id"):
            ids.add(str(payload.get("client_order_id")))
        if response.get("client_order_id"):
            ids.add(str(response.get("client_order_id")))
    return ids


def submit_missing_broker_exit_orders_once(max_age_hours: float = 8.0) -> dict[str, Any]:
    counts = {"submitted": 0, "skipped_existing": 0, "skipped_old": 0, "skipped_missing_position": 0}
    existing_client_ids = order_client_ids()
    for exit_record in read_jsonl(HUMAN_EXITS_FILE):
        if exit_record.get("event_type") != "human_paper_option_exit":
            continue
        if exit_record.get("broker_client_order_id") or exit_record.get("broker_order_id"):
            counts["skipped_existing"] += 1
            continue
        recorded_at = parse_datetime(exit_record.get("recorded_at"))
        now = parse_datetime(now_iso())
        if recorded_at is not None and now is not None and (now - recorded_at).total_seconds() > max_age_hours * 3600:
            counts["skipped_old"] += 1
            continue
        position = position_by_id(exit_record.get("position_id"))
        if not position:
            counts["skipped_missing_position"] += 1
            continue
        contracts = int(exit_record.get("contracts") or 0)
        if contracts <= 0:
            continue
        client_order_id = option_exit_order_client_id(position, contracts, str(exit_record.get("exit_id") or "exit"))
        if client_order_id in existing_client_ids:
            counts["skipped_existing"] += 1
            continue
        audit = submit_option_paper_sell_order(
            position,
            contracts,
            str(exit_record.get("reason") or "local_exit_reconciliation"),
            str(exit_record.get("exit_id") or "exit"),
        )
        payload = audit.get("payload") or {}
        if payload.get("client_order_id"):
            existing_client_ids.add(str(payload.get("client_order_id")))
        if audit.get("status") == "submitted":
            counts["submitted"] += 1
    return counts


def label_for_position(position: dict[str, Any], contract_symbol: str) -> str:
    ticker = str(position.get("ticker") or "").upper()
    expiration = position.get("expiration_date")
    strike = safe_float(position.get("strike_price"))
    option_type = str(position.get("option_type") or "").lower()
    if ticker and expiration and strike is not None and option_type:
        try:
            import datetime as dt

            exp = dt.date.fromisoformat(str(expiration))
            side = "C" if option_type.startswith("call") else "P"
            return f"{ticker} {exp:%b} {exp.day} {strike:g}{side}"
        except (TypeError, ValueError):
            pass
    return contract_symbol


def build_status_report(order_audit: dict[str, Any], broker_order: dict[str, Any]) -> dict[str, Any]:
    payload = order_audit.get("payload") or {}
    position = position_by_id(order_audit.get("position_id"))
    contract_symbol = str(broker_order.get("symbol") or order_audit.get("contract_symbol") or payload.get("symbol") or "")
    return {
        "event_type": "broker_order_status_report",
        "recorded_at": now_iso(),
        "order_id": broker_order.get("id"),
        "client_order_id": broker_order.get("client_order_id") or payload.get("client_order_id"),
        "broker_status": broker_order.get("status"),
        "action": order_audit.get("action"),
        "position_id": order_audit.get("position_id"),
        "source_dedupe_key": order_audit.get("source_dedupe_key"),
        "contract_symbol": contract_symbol,
        "label": label_for_position(position, contract_symbol),
        "side": broker_order.get("side") or payload.get("side"),
        "qty": broker_order.get("qty") or payload.get("qty"),
        "filled_qty": broker_order.get("filled_qty"),
        "filled_avg_price": broker_order.get("filled_avg_price"),
        "limit_price": broker_order.get("limit_price") or payload.get("limit_price"),
        "submitted_at": broker_order.get("submitted_at"),
        "filled_at": broker_order.get("filled_at"),
        "raw_order": broker_order,
    }


def should_report_status(status: str | None) -> bool:
    return str(status or "").lower() in TERMINAL_STATUSES


def check_broker_order_statuses_once(max_age_hours: float = 8.0) -> dict[str, Any]:
    exit_sync_counts = submit_missing_broker_exit_orders_once(max_age_hours=max_age_hours)
    orders = submitted_order_audits(max_age_hours=max_age_hours)
    counts: dict[str, Any] = {"checked": 0, "reported": 0, "skipped_nonterminal": 0, "errors": 0, "exit_sync": exit_sync_counts}
    if not orders:
        return counts
    try:
        env = load_order_environment()
    except Exception as exc:  # noqa: BLE001
        counts["errors"] += 1
        counts["reason"] = str(exc)
        return counts
    existing = reported_keys()
    for order_audit in orders:
        order_id = str((order_audit.get("response") or {}).get("id") or "")
        if not order_id:
            continue
        try:
            broker_order = fetch_order_status(env, order_id)
            counts["checked"] += 1
        except Exception as exc:  # noqa: BLE001
            counts["errors"] += 1
            counts["reason"] = str(exc)
            continue
        status = str(broker_order.get("status") or "")
        if not should_report_status(status):
            counts["skipped_nonterminal"] += 1
            continue
        key = f"{order_id}:{status}"
        if key in existing:
            continue
        report = build_status_report(order_audit, broker_order)
        append_jsonl(ORDER_STATUS_FILE, report)
        existing.add(key)
        counts["reported"] += 1
        try:
            from steve_trade_bot import send_broker_order_report

            send_broker_order_report(report)
        except Exception as exc:  # noqa: BLE001
            counts["errors"] += 1
            counts["reason"] = f"telegram_report_error:{type(exc).__name__}:{exc}"
    return counts


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--once", action="store_true")
    parser.add_argument("--max-age-hours", type=float, default=8.0)
    args = parser.parse_args()
    print(json.dumps(check_broker_order_statuses_once(max_age_hours=args.max_age_hours), sort_keys=True))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())

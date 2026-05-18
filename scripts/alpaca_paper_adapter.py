#!/usr/bin/env python3
"""Paper-only Alpaca adapter for approved trade decisions."""

from __future__ import annotations

import argparse
import json
import os
import sys
import urllib.error
import urllib.request
from decimal import Decimal, ROUND_DOWN
from pathlib import Path
from typing import Any

from pipeline_common import CONFIG_DIR, DATA_DIR, append_jsonl, load_simple_yaml, now_iso, read_jsonl, stable_hash


PAPER_HOST = "https://paper-api.alpaca.markets"


class AdapterError(Exception):
    pass


def load_env_file(path: Path) -> dict[str, str]:
    values: dict[str, str] = {}
    if not path.exists():
        return values
    for raw_line in path.read_text(encoding="utf-8").splitlines():
        line = raw_line.strip()
        if not line or line.startswith("#") or "=" not in line:
            continue
        key, value = line.split("=", 1)
        values[key.strip()] = value.strip().strip('"').strip("'")
    return values


def env_value(name: str, env_file: dict[str, str]) -> str:
    return os.environ.get(name) or env_file.get(name, "")


def normalize_base_url(value: str) -> str:
    value = value.strip().rstrip("/")
    if value.endswith("/v2"):
        value = value[:-3]
    return value


def load_adapter_config() -> tuple[dict[str, Any], dict[str, str]]:
    config = load_simple_yaml(CONFIG_DIR / "broker.yaml")
    env_file = load_env_file(Path(".env.local"))
    return config, env_file


def require_paper_environment(config: dict[str, Any], env_file: dict[str, str], require_keys: bool = True) -> dict[str, str]:
    base_url_name = str(config.get("base_url_env_var", "APCA_API_BASE_URL"))
    key_name = str(config.get("key_id_env_var", "APCA_API_KEY_ID"))
    secret_name = str(config.get("secret_key_env_var", "APCA_API_SECRET_KEY"))
    mode_name = str(config.get("trading_mode_env_var", "OPENCLAW_TRADING_MODE"))
    submit_name = str(config.get("submit_enabled_env_var", "OPENCLAW_ENABLE_PAPER_ORDERS"))
    base_url = normalize_base_url(env_value(base_url_name, env_file) or str(config.get("paper_base_url", PAPER_HOST)))
    mode = env_value(mode_name, env_file) or "paper"
    if base_url != PAPER_HOST:
        raise AdapterError(f"Refusing non-paper Alpaca endpoint: {base_url}")
    if mode != "paper":
        raise AdapterError(f"Refusing non-paper trading mode: {mode}")
    key_id = env_value(key_name, env_file)
    secret_key = env_value(secret_name, env_file)
    if require_keys:
        if not key_id or "your_" in key_id:
            raise AdapterError(f"Missing real paper API key in {key_name}")
        if not secret_key or "your_" in secret_key:
            raise AdapterError(f"Missing real paper API secret in {secret_name}")
    return {
        "base_url": base_url,
        "key_id": key_id,
        "secret_key": secret_key,
        "submit_enabled": (env_value(submit_name, env_file).lower() == "true"),
    }


def alpaca_request(method: str, path: str, env: dict[str, str], body: dict[str, Any] | None = None) -> tuple[int, dict[str, Any], dict[str, str]]:
    url = env["base_url"] + path
    data = None if body is None else json.dumps(body).encode("utf-8")
    request = urllib.request.Request(url, data=data, method=method)
    request.add_header("APCA-API-KEY-ID", env["key_id"])
    request.add_header("APCA-API-SECRET-KEY", env["secret_key"])
    request.add_header("Content-Type", "application/json")
    try:
        with urllib.request.urlopen(request, timeout=15) as response:
            text = response.read().decode("utf-8")
            parsed = json.loads(text) if text else {}
            headers = {key.lower(): value for key, value in response.headers.items()}
            return response.status, parsed, headers
    except urllib.error.HTTPError as exc:
        text = exc.read().decode("utf-8", errors="replace")
        try:
            parsed = json.loads(text)
        except json.JSONDecodeError:
            parsed = {"message": text}
        raise AdapterError(f"Alpaca HTTP {exc.code}: {json.dumps(parsed, sort_keys=True)}") from exc
    except urllib.error.URLError as exc:
        raise AdapterError(f"Alpaca connection failed: {exc}") from exc


def qty_from_notional(notional: Any, limit_price: Any, precision: int) -> str:
    dollars = Decimal(str(notional))
    price = Decimal(str(limit_price))
    if dollars <= 0 or price <= 0:
        raise AdapterError("Cannot calculate qty from non-positive notional/price")
    quantum = Decimal("1").scaleb(-precision)
    qty = (dollars / price).quantize(quantum, rounding=ROUND_DOWN)
    if qty <= 0:
        raise AdapterError("Calculated quantity is zero")
    return format(qty, "f")


def client_order_id(decision: dict[str, Any], config: dict[str, Any]) -> str:
    prefix = str(config.get("client_order_id_prefix", "openclaw-paper"))
    digest = stable_hash(
        [
            decision.get("source_dedupe_key"),
            decision.get("ticker"),
            decision.get("side"),
            decision.get("decided_at"),
        ]
    )[:24]
    return f"{prefix}-{digest}"[:48]


def order_payload_from_decision(decision: dict[str, Any], config: dict[str, Any]) -> dict[str, Any]:
    if decision.get("event_type") != "trade_decision":
        raise AdapterError("Input is not a trade_decision")
    if not decision.get("allowed"):
        raise AdapterError(f"Decision is not allowed: {decision.get('reason')}")
    order = decision.get("would_place_order") or {}
    symbol = order.get("symbol") or decision.get("ticker")
    side = order.get("side") or decision.get("side")
    order_type = order.get("order_type", "limit")
    tif = order.get("time_in_force", "day")
    payload: dict[str, Any] = {
        "symbol": symbol,
        "side": side,
        "type": order_type,
        "time_in_force": tif,
        "client_order_id": client_order_id(decision, config),
    }
    if order_type == "limit":
        limit_price = order.get("limit_price")
        if limit_price is None:
            raise AdapterError("Limit order missing limit_price")
        payload["limit_price"] = str(limit_price)
        if config.get("use_fractional_qty_for_limit_orders", True):
            payload["qty"] = qty_from_notional(order.get("notional"), limit_price, int(config.get("qty_precision", 6)))
        else:
            payload["notional"] = str(order.get("notional"))
    else:
        payload["notional"] = str(order.get("notional"))
    return payload


def audit_record(action: str, decision: dict[str, Any] | None, payload: dict[str, Any] | None, response: dict[str, Any] | None, status: str, reason: str = "") -> dict[str, Any]:
    return {
        "event_type": "alpaca_paper_order_audit",
        "recorded_at": now_iso(),
        "action": action,
        "status": status,
        "reason": reason,
        "source_dedupe_key": (decision or {}).get("source_dedupe_key"),
        "ticker": (decision or {}).get("ticker"),
        "payload": payload or {},
        "response": response or {},
    }


def write_order_audit(record: dict[str, Any]) -> None:
    append_jsonl(DATA_DIR / "orders_paper.jsonl", record)


def load_decision_arg(value: str) -> dict[str, Any]:
    path = Path(value)
    if path.exists():
        text = path.read_text(encoding="utf-8").strip()
        if "\n" in text:
            rows = [json.loads(line) for line in text.splitlines() if line.strip()]
            if not rows:
                raise AdapterError(f"No JSON records in {path}")
            return rows[-1]
        return json.loads(text)
    return json.loads(value)


def cmd_check_account() -> int:
    config, env_file = load_adapter_config()
    env = require_paper_environment(config, env_file, require_keys=True)
    status, account, headers = alpaca_request("GET", "/v2/account", env)
    print("Alpaca paper account reachable")
    print(f"status: {status}")
    print(f"account_status: {account.get('status')}")
    print(f"trading_blocked: {account.get('trading_blocked')}")
    print(f"buying_power: {account.get('buying_power')}")
    if headers.get("x-request-id"):
        print(f"x_request_id: {headers['x-request-id']}")
    return 0


def cmd_order(args: argparse.Namespace, submit: bool) -> int:
    config, env_file = load_adapter_config()
    env = require_paper_environment(config, env_file, require_keys=submit)
    decision = load_decision_arg(args.decision)
    payload = order_payload_from_decision(decision, config)
    if not submit:
        print(json.dumps({"dry_run": True, "paper_endpoint": env["base_url"], "order_payload": payload}, sort_keys=True))
        if args.audit:
            write_order_audit(audit_record("dry_run_order", decision, payload, None, "dry_run"))
        return 0
    if not env["submit_enabled"]:
        raise AdapterError("Paper order submission disabled. Set OPENCLAW_ENABLE_PAPER_ORDERS=true locally to submit.")
    status, response, headers = alpaca_request("POST", "/v2/orders", env, payload)
    response["_http_status"] = status
    if headers.get("x-request-id"):
        response["_x_request_id"] = headers["x-request-id"]
    write_order_audit(audit_record("submit_order", decision, payload, response, "submitted"))
    print(json.dumps(response, sort_keys=True))
    return 0


def cmd_process_latest(args: argparse.Namespace) -> int:
    rows = read_jsonl(Path(args.input))
    allowed = [row for row in rows if row.get("event_type") == "trade_decision" and row.get("allowed")]
    if not allowed:
        raise AdapterError("No allowed trade_decision records found")
    args.decision = json.dumps(allowed[-1])
    return cmd_order(args, submit=args.submit)


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    sub = parser.add_subparsers(dest="command", required=True)
    sub.add_parser("check-account", help="Verify Alpaca paper credentials and account access")
    dry = sub.add_parser("dry-run-order", help="Build order payload from a decision without submitting")
    dry.add_argument("--decision", required=True, help="Decision JSON or path")
    dry.add_argument("--audit", action="store_true", help="Append dry-run audit record")
    submit = sub.add_parser("submit-order", help="Submit a paper order, gated by local env flag")
    submit.add_argument("--decision", required=True, help="Decision JSON or path")
    process = sub.add_parser("process-latest", help="Process latest allowed decision from JSONL")
    process.add_argument("--input", default=str(DATA_DIR / "trade_decisions.jsonl"))
    process.add_argument("--submit", action="store_true")
    process.add_argument("--audit", action="store_true")
    args = parser.parse_args()
    try:
        if args.command == "check-account":
            return cmd_check_account()
        if args.command == "dry-run-order":
            return cmd_order(args, submit=False)
        if args.command == "submit-order":
            return cmd_order(args, submit=True)
        if args.command == "process-latest":
            return cmd_process_latest(args)
    except AdapterError as exc:
        print(f"ERROR: {exc}", file=sys.stderr)
        return 1
    return 2


if __name__ == "__main__":
    raise SystemExit(main())

#!/usr/bin/env python3
"""Alpaca-first helpers for Steve option validation and paper orders."""

from __future__ import annotations

import datetime as dt
import json
import os
import urllib.parse
import urllib.request
from decimal import Decimal, ROUND_HALF_UP
from pathlib import Path
from typing import Any

from alpaca_paper_adapter import AdapterError, alpaca_request, load_adapter_config, require_paper_environment
from pipeline_common import DATA_DIR, append_jsonl, now_iso, parse_datetime, stable_hash


DATA_HOST = "https://data.alpaca.markets"
DEFAULT_STOCK_FEED = "iex"
DEFAULT_OPTION_FEED = "indicative"


class AlpacaDataError(Exception):
    pass


def env_value(name: str, env_file: dict[str, str]) -> str:
    return os.environ.get(name) or env_file.get(name, "")


def normalize_base_url(value: str) -> str:
    return value.strip().rstrip("/")


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


def load_data_env(require_keys: bool = False) -> dict[str, str]:
    config, broker_env_file = load_adapter_config()
    project_env_file = load_env_file(Path(__file__).resolve().parents[1] / ".env.local")
    env_file = {**project_env_file, **broker_env_file}
    key_name = str(config.get("key_id_env_var", "APCA_API_KEY_ID"))
    secret_name = str(config.get("secret_key_env_var", "APCA_API_SECRET_KEY"))
    key_id = env_value(key_name, env_file)
    secret_key = env_value(secret_name, env_file)
    if require_keys and (not key_id or not secret_key or "your_" in key_id or "your_" in secret_key):
        raise AlpacaDataError("missing_alpaca_data_credentials")
    return {
        "base_url": normalize_base_url(env_value("APCA_API_DATA_BASE_URL", env_file) or DATA_HOST),
        "key_id": key_id,
        "secret_key": secret_key,
        "stock_feed": env_value("ALPACA_STOCK_FEED", env_file) or DEFAULT_STOCK_FEED,
        "option_feed": env_value("ALPACA_OPTIONS_FEED", env_file) or DEFAULT_OPTION_FEED,
    }


def data_request(path: str, query: dict[str, Any], require_keys: bool = True) -> dict[str, Any]:
    env = load_data_env(require_keys=require_keys)
    encoded = urllib.parse.urlencode({key: value for key, value in query.items() if value not in (None, "")})
    url = f"{env['base_url']}{path}"
    if encoded:
        url = f"{url}?{encoded}"
    request = urllib.request.Request(url, method="GET")
    if env.get("key_id"):
        request.add_header("APCA-API-KEY-ID", env["key_id"])
    if env.get("secret_key"):
        request.add_header("APCA-API-SECRET-KEY", env["secret_key"])
    try:
        with urllib.request.urlopen(request, timeout=15) as response:
            text = response.read().decode("utf-8")
            return json.loads(text) if text else {}
    except Exception as exc:  # noqa: BLE001
        raise AlpacaDataError(str(exc)) from exc


def option_symbol(ticker: str, expiration_date: str, option_type: str, strike_price: Any) -> str:
    expiration = dt.date.fromisoformat(str(expiration_date))
    side_code = "C" if str(option_type).lower().startswith("call") else "P"
    strike = Decimal(str(strike_price)) * Decimal("1000")
    strike_int = int(strike.quantize(Decimal("1"), rounding=ROUND_HALF_UP))
    return f"{ticker.upper()}{expiration:%y%m%d}{side_code}{strike_int:08d}"


def safe_float(value: Any) -> float | None:
    if value in (None, ""):
        return None
    try:
        return float(value)
    except (TypeError, ValueError):
        return None


def first_present(payload: dict[str, Any], keys: tuple[str, ...]) -> Any:
    for key in keys:
        if key in payload and payload[key] not in (None, ""):
            return payload[key]
    return None


def normalize_quote(symbol: str, payload: dict[str, Any], status: str = "ok", reason: str = "") -> dict[str, Any]:
    bid = safe_float(first_present(payload, ("bp", "bid_price", "bidPrice", "bid")))
    ask = safe_float(first_present(payload, ("ap", "ask_price", "askPrice", "ask")))
    last = safe_float(first_present(payload, ("p", "price", "last_price", "lastPrice", "last")))
    mark = ((bid + ask) / 2) if bid is not None and ask is not None and bid > 0 and ask > 0 else last
    spread = (ask - bid) if bid is not None and ask is not None else None
    spread_pct = ((spread / mark) * 100) if spread is not None and mark and mark > 0 else None
    return {
        "symbol": symbol,
        "status": status,
        "reason": reason,
        "timestamp": first_present(payload, ("t", "timestamp")),
        "bid": bid,
        "ask": ask,
        "last": last,
        "mark": mark,
        "spread": spread,
        "spread_pct": spread_pct,
        "raw": payload,
    }


def latest_option_quote(contract_symbol: str) -> dict[str, Any]:
    try:
        env = load_data_env(require_keys=True)
        payload = data_request(
            "/v1beta1/options/quotes/latest",
            {"symbols": contract_symbol, "feed": env["option_feed"]},
            require_keys=True,
        )
        quote = (payload.get("quotes") or {}).get(contract_symbol) or payload.get("quote") or {}
        if not quote:
            return normalize_quote(contract_symbol, {}, "unavailable", "quote_missing")
        result = normalize_quote(contract_symbol, quote)
        result["feed"] = env["option_feed"]
        return result
    except Exception as exc:  # noqa: BLE001
        return normalize_quote(contract_symbol, {}, "unavailable", str(exc))


def latest_stock_quote(symbol: str) -> dict[str, Any]:
    try:
        env = load_data_env(require_keys=True)
        payload = data_request(
            f"/v2/stocks/{symbol.upper()}/quotes/latest",
            {"feed": env["stock_feed"]},
            require_keys=True,
        )
        quote = payload.get("quote") or {}
        if not quote:
            return normalize_quote(symbol.upper(), {}, "unavailable", "quote_missing")
        result = normalize_quote(symbol.upper(), quote)
        result["feed"] = env["stock_feed"]
        return result
    except Exception as exc:  # noqa: BLE001
        return normalize_quote(symbol.upper(), {}, "unavailable", str(exc))


def stock_bars(symbol: str, minutes: int = 90) -> list[dict[str, Any]]:
    end = dt.datetime.now(dt.timezone.utc)
    start = end - dt.timedelta(minutes=minutes * 3)
    try:
        env = load_data_env(require_keys=True)
        payload = data_request(
            "/v2/stocks/bars",
            {
                "symbols": symbol.upper(),
                "timeframe": "1Min",
                "start": start.isoformat().replace("+00:00", "Z"),
                "end": end.isoformat().replace("+00:00", "Z"),
                "limit": minutes,
                "feed": env["stock_feed"],
            },
            require_keys=True,
        )
        bars = payload.get("bars") or {}
        rows = bars.get(symbol.upper()) if isinstance(bars, dict) else None
        return rows or []
    except Exception:
        return []


def ema(values: list[float], period: int) -> float | None:
    if not values:
        return None
    multiplier = 2 / (period + 1)
    current = values[0]
    for value in values[1:]:
        current = (value * multiplier) + (current * (1 - multiplier))
    return current


def rsi(values: list[float], period: int = 14) -> float | None:
    if len(values) <= period:
        return None
    gains: list[float] = []
    losses: list[float] = []
    for previous, current in zip(values[-period - 1 : -1], values[-period:]):
        change = current - previous
        gains.append(max(change, 0))
        losses.append(abs(min(change, 0)))
    avg_gain = sum(gains) / period
    avg_loss = sum(losses) / period
    if avg_loss == 0:
        return 100.0
    relative_strength = avg_gain / avg_loss
    return 100 - (100 / (1 + relative_strength))


def atr(bars: list[dict[str, Any]], period: int = 14) -> float | None:
    if len(bars) <= period:
        return None
    ranges: list[float] = []
    previous_close = safe_float(bars[-period - 1].get("c"))
    for bar in bars[-period:]:
        high = safe_float(bar.get("h"))
        low = safe_float(bar.get("l"))
        close = safe_float(bar.get("c"))
        if high is None or low is None:
            continue
        candidates = [high - low]
        if previous_close is not None:
            candidates.extend([abs(high - previous_close), abs(low - previous_close)])
        ranges.append(max(candidates))
        previous_close = close
    return (sum(ranges) / len(ranges)) if ranges else None


def indicator_snapshot(symbol: str) -> dict[str, Any]:
    bars = stock_bars(symbol)
    closes = [safe_float(row.get("c")) for row in bars]
    closes = [value for value in closes if value is not None]
    latest = bars[-1] if bars else {}
    latest_close = safe_float(latest.get("c"))
    latest_volume = safe_float(latest.get("v"))
    previous_volumes = [safe_float(row.get("v")) for row in bars[-21:-1]]
    previous_volumes = [value for value in previous_volumes if value is not None]
    avg_volume = (sum(previous_volumes) / len(previous_volumes)) if previous_volumes else None
    vwap_values = [(safe_float(row.get("vw")), safe_float(row.get("v"))) for row in bars if safe_float(row.get("vw")) is not None]
    vwap_numerator = sum((vw or 0) * (volume or 0) for vw, volume in vwap_values)
    vwap_denominator = sum((volume or 0) for _vw, volume in vwap_values)
    vwap = (vwap_numerator / vwap_denominator) if vwap_denominator else safe_float(latest.get("vw"))
    ema_9 = ema(closes[-60:], 9)
    ema_20 = ema(closes[-60:], 20)
    ema_50 = ema(closes[-90:], 50)
    alignment = None
    if latest_close is not None and ema_9 is not None and ema_20 is not None and ema_50 is not None:
        if latest_close >= ema_9 >= ema_20 >= ema_50:
            alignment = "bullish"
        elif latest_close <= ema_9 <= ema_20 <= ema_50:
            alignment = "bearish"
        else:
            alignment = "mixed"
    return {
        "symbol": symbol.upper(),
        "status": "ok" if bars else "unavailable",
        "bar_count": len(bars),
        "latest_close": latest_close,
        "vwap": vwap,
        "price_vs_vwap_pct": (((latest_close - vwap) / vwap) * 100) if latest_close is not None and vwap else None,
        "ema_9": ema_9,
        "ema_20": ema_20,
        "ema_50": ema_50,
        "ema_alignment": alignment,
        "rsi_14": rsi(closes, 14),
        "atr_14": atr(bars, 14),
        "relative_volume": (latest_volume / avg_volume) if latest_volume is not None and avg_volume else None,
        "latest_bar": latest,
    }


def latest_news(symbol: str, limit: int = 3) -> dict[str, Any]:
    try:
        payload = data_request(
            "/v1beta1/news",
            {"symbols": symbol.upper(), "limit": limit, "sort": "desc"},
            require_keys=True,
        )
        articles = payload.get("news") or []
        compact = [
            {
                "headline": item.get("headline"),
                "summary": item.get("summary"),
                "source": item.get("source"),
                "created_at": item.get("created_at"),
                "url": item.get("url"),
            }
            for item in articles[:limit]
            if isinstance(item, dict)
        ]
        return {
            "status": "ok" if compact else "unavailable",
            "reason": "" if compact else "news_missing",
            "articles": compact,
            "sentiment_hint": sentiment_hint(compact),
        }
    except Exception as exc:  # noqa: BLE001
        return {"status": "unavailable", "reason": str(exc), "articles": [], "sentiment_hint": "unknown"}


def sentiment_hint(articles: list[dict[str, Any]]) -> str:
    text = " ".join(str(item.get("headline") or "") + " " + str(item.get("summary") or "") for item in articles).lower()
    positive = sum(1 for word in ("beat", "beats", "upgrade", "surge", "raises", "record", "strong") if word in text)
    negative = sum(1 for word in ("miss", "downgrade", "falls", "lawsuit", "probe", "weak", "cuts") if word in text)
    if positive > negative:
        return "positive"
    if negative > positive:
        return "negative"
    if text.strip():
        return "mixed_or_neutral"
    return "unknown"


def days_to_expiration(alert: dict[str, Any]) -> int | None:
    expiration = alert.get("expiration_date")
    if not expiration:
        return None
    reference_dt = parse_datetime(alert.get("notification_timestamp")) or parse_datetime(alert.get("parsed_at")) or parse_datetime(now_iso())
    try:
        return (dt.date.fromisoformat(str(expiration)) - reference_dt.date()).days
    except ValueError:
        return None


def signal_score(alert: dict[str, Any], snapshot: dict[str, Any]) -> tuple[int, list[str]]:
    score = 50
    warnings: list[str] = []
    quote = snapshot.get("option_quote") or {}
    spread_pct = quote.get("spread_pct")
    if spread_pct is None:
        warnings.append("option_quote_unavailable")
        score -= 10
    elif spread_pct <= 5:
        score += 10
    elif spread_pct >= 15:
        warnings.append("wide_option_spread")
        score -= 20
    dte = snapshot.get("dte")
    if dte == 0:
        warnings.append("zero_dte")
        score -= 10
    underlying = snapshot.get("underlying_indicators") or {}
    alignment = underlying.get("ema_alignment")
    price_vs_vwap = underlying.get("price_vs_vwap_pct")
    option_type = str(alert.get("option_type") or "").lower()
    if option_type == "call":
        if alignment == "bullish":
            score += 10
        if price_vs_vwap is not None and price_vs_vwap >= 0:
            score += 5
    elif option_type == "put":
        if alignment == "bearish":
            score += 10
        if price_vs_vwap is not None and price_vs_vwap <= 0:
            score += 5
    rsi_14 = underlying.get("rsi_14")
    if option_type == "call" and rsi_14 is not None and rsi_14 >= 75:
        warnings.append("call_rsi_overextended")
        score -= 5
    if option_type == "put" and rsi_14 is not None and rsi_14 <= 25:
        warnings.append("put_rsi_overextended")
        score -= 5
    return max(0, min(100, score)), warnings


def enrich_option_alert(alert: dict[str, Any], include_context: bool = True) -> dict[str, Any]:
    contract_symbol = option_symbol(alert["ticker"], alert["expiration_date"], alert["option_type"], alert["strike_price"])
    option_quote = latest_option_quote(contract_symbol)
    underlying_quote = latest_stock_quote(str(alert["ticker"]))
    snapshot = {
        "event_type": "option_market_snapshot",
        "snapshot_id": stable_hash([alert.get("source_dedupe_key"), contract_symbol, now_iso()])[:16],
        "recorded_at": now_iso(),
        "source_dedupe_key": alert.get("source_dedupe_key"),
        "ticker": alert.get("ticker"),
        "contract_symbol": contract_symbol,
        "dte": days_to_expiration(alert),
        "option_quote": option_quote,
        "underlying_quote": underlying_quote,
        "data_provider": "alpaca",
    }
    if include_context:
        snapshot["underlying_indicators"] = indicator_snapshot(str(alert["ticker"]))
        snapshot["spy_indicators"] = indicator_snapshot("SPY")
        snapshot["qqq_indicators"] = indicator_snapshot("QQQ")
        snapshot["recent_news"] = latest_news(str(alert["ticker"]))
    score, warnings = signal_score(alert, snapshot)
    snapshot["signal_score"] = score
    snapshot["signal_warnings"] = warnings
    return snapshot


def option_order_client_id(source_key: str, contract_symbol: str) -> str:
    return f"openclaw-opt-{stable_hash([source_key, contract_symbol])[:24]}"[:48]


def option_exit_order_client_id(position: dict[str, Any], contracts: int, trigger_key: str) -> str:
    return f"openclaw-opt-exit-{stable_hash([position.get('position_id'), position.get('contract_symbol'), contracts, trigger_key])[:19]}"[:48]


def build_option_order_payload(position: dict[str, Any]) -> dict[str, Any]:
    qty = int(position.get("contracts") or 0)
    if qty <= 0:
        raise AdapterError("Option order qty must be a positive whole number")
    limit_price = position.get("entry_price")
    if limit_price is None or float(limit_price) <= 0:
        raise AdapterError("Option order missing positive entry_price")
    return {
        "symbol": position["contract_symbol"],
        "side": "buy",
        "type": "limit",
        "time_in_force": "day",
        "qty": str(qty),
        "limit_price": str(limit_price),
        "client_order_id": option_order_client_id(str(position.get("source_dedupe_key") or position.get("position_id")), position["contract_symbol"]),
    }


def build_option_sell_order_payload(position: dict[str, Any], contracts: int, trigger_key: str) -> dict[str, Any]:
    qty = int(contracts or 0)
    if qty <= 0:
        raise AdapterError("Option sell order qty must be a positive whole number")
    return {
        "symbol": position["contract_symbol"],
        "side": "sell",
        "type": "market",
        "time_in_force": "day",
        "qty": str(qty),
        "client_order_id": option_exit_order_client_id(position, qty, trigger_key),
    }


def options_market_open(env: dict[str, str]) -> tuple[bool, str]:
    try:
        _status, clock, _headers = alpaca_request("GET", "/v2/clock", env)
    except Exception as exc:  # noqa: BLE001
        return False, f"alpaca_clock_unavailable:{exc}"
    if bool(clock.get("is_open")):
        return True, ""
    next_open = str(clock.get("next_open") or "")
    if next_open:
        return False, f"options_market_closed:next_open={next_open}"
    return False, "options_market_closed"


def submit_option_paper_order(position: dict[str, Any]) -> dict[str, Any]:
    payload: dict[str, Any] | None = None
    try:
        config, env_file = load_adapter_config()
        env = require_paper_environment(config, env_file, require_keys=True)
        payload = build_option_order_payload(position)
        is_open, market_reason = options_market_open(env)
        if not is_open:
            raise AdapterError(market_reason)
        if not env.get("submit_enabled"):
            raise AdapterError("paper_order_submission_disabled")
        status, response, headers = alpaca_request("POST", "/v2/orders", env, payload)
        response["_http_status"] = status
        if headers.get("x-request-id"):
            response["_x_request_id"] = headers["x-request-id"]
        audit = option_order_audit(position, payload, response, "submitted")
    except Exception as exc:  # noqa: BLE001
        audit = option_order_audit(position, payload, None, "blocked", str(exc))
    append_jsonl(DATA_DIR / "orders_paper.jsonl", audit)
    return audit


def submit_option_paper_sell_order(position: dict[str, Any], contracts: int, reason: str, trigger_key: str) -> dict[str, Any]:
    payload: dict[str, Any] | None = None
    try:
        config, env_file = load_adapter_config()
        env = require_paper_environment(config, env_file, require_keys=True)
        payload = build_option_sell_order_payload(position, contracts, trigger_key)
        is_open, market_reason = options_market_open(env)
        if not is_open:
            raise AdapterError(market_reason)
        if not env.get("submit_enabled"):
            raise AdapterError("paper_order_submission_disabled")
        status, response, headers = alpaca_request("POST", "/v2/orders", env, payload)
        response["_http_status"] = status
        if headers.get("x-request-id"):
            response["_x_request_id"] = headers["x-request-id"]
        audit = option_order_audit(position, payload, response, "submitted", action="paper_exit_order", exit_reason=reason)
    except Exception as exc:  # noqa: BLE001
        audit = option_order_audit(position, payload, None, "blocked", str(exc), action="paper_exit_order", exit_reason=reason)
    append_jsonl(DATA_DIR / "orders_paper.jsonl", audit)
    return audit


def option_order_audit(
    position: dict[str, Any],
    payload: dict[str, Any] | None,
    response: dict[str, Any] | None,
    status: str,
    reason: str = "",
    action: str = "paper_entry_order",
    exit_reason: str = "",
) -> dict[str, Any]:
    return {
        "event_type": "alpaca_option_paper_order_audit",
        "action": action,
        "recorded_at": now_iso(),
        "status": status,
        "reason": reason,
        "exit_reason": exit_reason,
        "position_id": position.get("position_id"),
        "source_dedupe_key": position.get("source_dedupe_key"),
        "ticker": position.get("ticker"),
        "contract_symbol": position.get("contract_symbol"),
        "payload": payload or {},
        "response": response or {},
    }

#!/usr/bin/env python3
"""Dedicated Telegram approval bot for Steve option paper trades."""

from __future__ import annotations

import argparse
import json
import os
import re
import time
import urllib.parse
import urllib.request
from dataclasses import dataclass
from pathlib import Path
from typing import Any

from alpaca_options import submit_option_paper_order
from option_validation import validation_id
from pipeline_common import DATA_DIR, append_jsonl, now_iso, parse_datetime, read_jsonl, stable_hash


APPROVAL_CARDS_FILE = DATA_DIR / "steve_approval_cards.jsonl"
APPROVAL_ACTIONS_FILE = DATA_DIR / "steve_approval_actions.jsonl"
HUMAN_POSITIONS_FILE = DATA_DIR / "human_paper_positions.jsonl"
BOT_STATE_FILE = DATA_DIR / "steve_trade_bot_state.json"
DEFAULT_STOP_PERCENT = 35.0
DEFAULT_TAKE_PERCENT = 80.0
DEFAULT_RUNNER_TAKE_PERCENTS = (120.0, 200.0)

COMMAND_RE = re.compile(r"^(?P<command>buy|skip)\b(?P<rest>.*)$", re.I | re.S)
KV_RE = re.compile(r"(?P<key>[A-Za-z_]+)=(?P<value>[^\s]+)")
NEWS_TERMS_BY_SYMBOL = {
    "AAPL": ["aapl", "apple"],
    "AMD": ["amd", "advanced micro devices"],
    "AMZN": ["amzn", "amazon"],
    "GOOGL": ["googl", "google", "alphabet"],
    "GOOG": ["goog", "google", "alphabet"],
    "META": ["meta", "facebook"],
    "MSFT": ["msft", "microsoft"],
    "NVDA": ["nvda", "nvidia"],
    "QQQ": ["qqq", "nasdaq"],
    "SPY": ["spy", "s&p", "sp500", "s&p 500"],
    "TSLA": ["tsla", "tesla"],
}


@dataclass(frozen=True)
class BotConfig:
    token: str
    approval_chat_id: str
    owner_chat_id: str
    owner_user_id: str


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


def load_bot_config(required: bool = False) -> BotConfig | None:
    env_file = load_env_file(Path(__file__).resolve().parents[1] / ".env.local")
    legacy_approver_chat_id = env_value("STEVE_TRADE_APPROVER_CHAT_ID", env_file)
    legacy_approver_user_id = env_value("STEVE_TRADE_APPROVER_USER_ID", env_file)
    config = BotConfig(
        token=env_value("STEVE_TRADE_BOT_TOKEN", env_file),
        approval_chat_id=env_value("STEVE_TRADE_APPROVAL_CHAT_ID", env_file) or legacy_approver_chat_id,
        owner_chat_id=env_value("STEVE_TRADE_OWNER_CHAT_ID", env_file) or legacy_approver_chat_id,
        owner_user_id=env_value("STEVE_TRADE_OWNER_USER_ID", env_file) or legacy_approver_user_id,
    )
    if required and (not config.token or not config.approval_chat_id or not config.owner_chat_id or not config.owner_user_id):
        raise RuntimeError(
            "Missing STEVE_TRADE_BOT_TOKEN, STEVE_TRADE_APPROVAL_CHAT_ID, "
            "STEVE_TRADE_OWNER_CHAT_ID, or STEVE_TRADE_OWNER_USER_ID"
        )
    if not config.token or not config.approval_chat_id or not config.owner_chat_id or not config.owner_user_id:
        return None
    return config


def load_bot_token(required: bool = False) -> str:
    env_file = load_env_file(Path(__file__).resolve().parents[1] / ".env.local")
    token = env_value("STEVE_TRADE_BOT_TOKEN", env_file)
    if required and not token:
        raise RuntimeError("Missing STEVE_TRADE_BOT_TOKEN")
    return token


def telegram_request(token: str, method: str, payload: dict[str, Any]) -> dict[str, Any]:
    data = urllib.parse.urlencode(payload).encode("utf-8")
    request = urllib.request.Request(
        f"https://api.telegram.org/bot{token}/{method}",
        data=data,
        headers={"Content-Type": "application/x-www-form-urlencoded"},
        method="POST",
    )
    with urllib.request.urlopen(request, timeout=20) as response:
        return json.loads(response.read().decode("utf-8"))


def send_telegram_message(config: BotConfig, text: str) -> dict[str, Any]:
    return telegram_request(config.token, "sendMessage", {"chat_id": config.approval_chat_id, "text": text})


def load_state() -> dict[str, Any]:
    if not BOT_STATE_FILE.exists():
        return {}
    try:
        return json.loads(BOT_STATE_FILE.read_text(encoding="utf-8"))
    except json.JSONDecodeError:
        return {}


def save_state(state: dict[str, Any]) -> None:
    BOT_STATE_FILE.parent.mkdir(parents=True, exist_ok=True)
    BOT_STATE_FILE.write_text(json.dumps(state, indent=2, sort_keys=True) + "\n", encoding="utf-8")


def approval_id_for_alert(alert: dict[str, Any]) -> str:
    return "approval-" + stable_hash([validation_id(alert), "telegram"])[:12]


def existing_card(approval_id: str) -> dict[str, Any] | None:
    for row in read_jsonl(APPROVAL_CARDS_FILE):
        if row.get("approval_id") == approval_id:
            return row
    return None


def option_quote_line(snapshot: dict[str, Any]) -> str:
    quote = snapshot.get("option_quote") or {}
    if quote.get("status") != "ok":
        return f"quote unavailable: {quote.get('reason') or 'unknown'}"
    freshness = "fresh" if quote_is_fresh(quote) else "stale"
    timestamp = quote.get("timestamp") or "unknown_time"
    return "bid={bid} ask={ask} mark={mark} spread={spread_pct:.1f}% quote={freshness} ts={timestamp}".format(
        bid=quote.get("bid"),
        ask=quote.get("ask"),
        mark=quote.get("mark"),
        spread_pct=float(quote.get("spread_pct") or 0),
        freshness=freshness,
        timestamp=timestamp,
    )


def compact_indicator_line(snapshot: dict[str, Any]) -> str:
    indicators = snapshot.get("underlying_indicators") or {}
    if indicators.get("status") != "ok":
        return "indicators unavailable"
    return "vwap_delta={vwap} ema={ema} rsi={rsi} rel_vol={rel_vol}".format(
        vwap=round(float(indicators.get("price_vs_vwap_pct") or 0), 2),
        ema=indicators.get("ema_alignment"),
        rsi=round(float(indicators.get("rsi_14") or 0), 1) if indicators.get("rsi_14") is not None else None,
        rel_vol=round(float(indicators.get("relative_volume") or 0), 2) if indicators.get("relative_volume") is not None else None,
    )


def compact_news_lines(snapshot: dict[str, Any]) -> list[str]:
    news = snapshot.get("recent_news") or {}
    if news.get("status") != "ok":
        return [f"news unavailable: {news.get('reason') or 'not configured'}"]
    ticker = str(snapshot.get("ticker") or "").upper()
    terms = NEWS_TERMS_BY_SYMBOL.get(ticker, [ticker.lower()] if ticker else [])
    relevant_articles = []
    broad_articles = []
    for article in news.get("articles") or []:
        headline = str(article.get("headline") or "").lower()
        summary = str(article.get("summary") or "").lower()
        if terms and any(term in headline or term in summary for term in terms):
            relevant_articles.append(article)
        else:
            broad_articles.append(article)
    if not relevant_articles:
        return [
            "news: Alpaca returned only broad/low-relevance headlines; no ticker-specific headline shown",
            "news sentiment hint: not_used_low_relevance",
        ]
    lines = [f"news sentiment hint: {news.get('sentiment_hint')} (keyword-only, ticker-filtered)"]
    for article in relevant_articles[:2]:
        headline = str(article.get("headline") or "").replace("\n", " ").strip()
        if headline:
            lines.append(f"- {headline[:160]}")
    if broad_articles:
        lines.append(f"news hidden: {len(broad_articles)} broad/low-relevance headline(s)")
    return lines


def quote_is_fresh(quote: dict[str, Any], max_age_seconds: int = 300) -> bool:
    quote_time = parse_datetime(quote.get("timestamp"))
    now_time = parse_datetime(now_iso())
    if quote_time is None or now_time is None:
        return False
    return 0 <= (now_time - quote_time).total_seconds() <= max_age_seconds


def suggested_entry_price(alert: dict[str, Any], snapshot: dict[str, Any]) -> tuple[float, str]:
    quote = snapshot.get("option_quote") or {}
    if quote.get("status") == "ok" and quote_is_fresh(quote):
        for key, source in (("ask", "current_ask"), ("mark", "current_mark")):
            value = quote.get(key)
            if value is not None and float(value) > 0:
                return float(value), source
    return float(alert.get("entry_price")), "steve_alert_price"


def dynamic_price_command_example(alert: dict[str, Any], snapshot: dict[str, Any]) -> str:
    entry, _source = suggested_entry_price(alert, snapshot)
    stop_price = max(0.01, entry * 0.65)
    take_price = entry * (1 + (DEFAULT_TAKE_PERCENT / 100))
    return f"buy contracts=1 stop_price={stop_price:.2f} take_price={take_price:.2f}"


def exit_plan_for_contracts(
    contracts: int,
    entry_price: float | None = None,
    first_take_percent: float | None = DEFAULT_TAKE_PERCENT,
    first_take_price: float | None = None,
) -> list[dict[str, Any]]:
    contracts = max(1, int(contracts or 1))
    first_target_percent = float(first_take_percent or DEFAULT_TAKE_PERCENT)
    if contracts == 1:
        tranches = [(first_target_percent, 1)]
    else:
        first = contracts // 2
        remaining = contracts - first
        second = remaining // 2
        if second < 1:
            second = remaining
        third = contracts - first - second
        tranches = [
            (first_target_percent, first),
            (DEFAULT_RUNNER_TAKE_PERCENTS[0], second),
        ]
        if third > 0:
            tranches.append((DEFAULT_RUNNER_TAKE_PERCENTS[1], third))
    plan: list[dict[str, Any]] = []
    for index, (take_percent, quantity) in enumerate(tranches):
        row = {
            "action": "sell",
            "contracts": quantity,
            "take_percent": take_percent,
        }
        if index == 0 and first_take_price is not None:
            row["take_price"] = round(float(first_take_price), 2)
            if entry_price is not None and float(entry_price) > 0:
                row["take_percent"] = round(((float(first_take_price) - float(entry_price)) / float(entry_price)) * 100, 2)
        elif entry_price is not None:
            row["take_price"] = round(float(entry_price) * (1 + take_percent / 100), 2)
        plan.append(row)
    return plan


def approval_message(alert: dict[str, Any], snapshot: dict[str, Any], approval_id: str) -> str:
    return "\n".join(
        [
            f"Alert: {alert.get('matched_text') or alert.get('raw_text')}",
            "",
            "Reply:",
            "skip",
            "buy",
            f"buy contracts=1 stop={DEFAULT_STOP_PERCENT:g} take={DEFAULT_TAKE_PERCENT:g}",
            dynamic_price_command_example(alert, snapshot),
        ]
    )


def send_approval_card(alert: dict[str, Any], snapshot: dict[str, Any], shadow_position: dict[str, Any]) -> dict[str, Any]:
    approval_id = approval_id_for_alert(alert)
    existing = existing_card(approval_id)
    if existing:
        return existing
    config = load_bot_config(required=False)
    text = approval_message(alert, snapshot, approval_id)
    card = {
        "event_type": "steve_approval_card",
        "approval_id": approval_id,
        "created_at": now_iso(),
        "status": "telegram_disabled",
        "reason": "missing_steve_trade_bot_env",
        "source_dedupe_key": alert.get("source_dedupe_key"),
        "validation_id": validation_id(alert),
        "shadow_position_id": shadow_position.get("position_id"),
        "telegram_message_id": None,
        "message_text": text,
        "alert": alert,
        "snapshot": snapshot,
        "shadow_position": shadow_position,
    }
    if config is not None:
        try:
            response = send_telegram_message(config, text)
            if not response.get("ok"):
                raise RuntimeError(f"Telegram returned non-ok response: {response}")
            card["status"] = "sent"
            card["reason"] = ""
            card["telegram_message_id"] = response.get("result", {}).get("message_id")
            card["telegram_chat_id"] = response.get("result", {}).get("chat", {}).get("id")
        except Exception as exc:  # noqa: BLE001
            card["status"] = "send_failed"
            card["reason"] = str(exc)
    append_jsonl(APPROVAL_CARDS_FILE, card)
    return card


def parse_number(value: str) -> float:
    return float(value.strip().rstrip("%"))


def parse_approval_command(text: str) -> dict[str, Any]:
    match = COMMAND_RE.match(text.strip())
    if not match:
        return {"ok": False, "reason": "unsupported_command"}
    command = match.group("command").lower()
    if command == "skip":
        return {"ok": True, "command": "skip"}
    kv = {item.group("key").lower(): item.group("value") for item in KV_RE.finditer(match.group("rest") or "")}
    if not kv:
        return {
            "ok": True,
            "command": "buy",
            "contracts": None,
            "stop_percent": DEFAULT_STOP_PERCENT,
            "take_percent": DEFAULT_TAKE_PERCENT,
            "risk_type": "percent",
            "used_default_contracts": True,
            "used_default_risk": True,
        }
    try:
        contracts = int(kv.get("contracts", "0"))
    except ValueError:
        contracts = 0
    if contracts <= 0:
        return {"ok": False, "command": "buy", "reason": "missing_positive_contracts"}
    if "stop" in kv and "take" in kv:
        return {
            "ok": True,
            "command": "buy",
            "contracts": contracts,
            "stop_percent": parse_number(kv["stop"]),
            "take_percent": parse_number(kv["take"]),
            "risk_type": "percent",
        }
    if "stop_price" in kv and "take_price" in kv:
        return {
            "ok": True,
            "command": "buy",
            "contracts": contracts,
            "stop_price": parse_number(kv["stop_price"]),
            "take_price": parse_number(kv["take_price"]),
            "risk_type": "price",
        }
    return {"ok": False, "command": "buy", "reason": "missing_stop_take"}


def actions_for_approval(approval_id: str) -> list[dict[str, Any]]:
    return [row for row in read_jsonl(APPROVAL_ACTIONS_FILE) if row.get("approval_id") == approval_id]


def card_for_message(message: dict[str, Any]) -> dict[str, Any] | None:
    reply = message.get("reply_to_message") or {}
    reply_message_id = reply.get("message_id")
    cards = read_jsonl(APPROVAL_CARDS_FILE)
    if reply_message_id is not None:
        for card in cards:
            if str(card.get("telegram_message_id")) == str(reply_message_id):
                return card
    acted = {row.get("approval_id") for row in read_jsonl(APPROVAL_ACTIONS_FILE) if row.get("action") in {"approved", "skipped"}}
    pending = [card for card in cards if card.get("approval_id") not in acted]
    return pending[-1] if pending else None


def fill_price_from_card(card: dict[str, Any]) -> tuple[float, str]:
    snapshot = card.get("snapshot") or {}
    alert = card.get("alert") or {}
    price, source = suggested_entry_price(alert, snapshot)
    if source == "steve_alert_price":
        return price, source
    quote = snapshot.get("option_quote") or {}
    for key, source in (("ask", "approval_ask"), ("mark", "approval_mark")):
        value = quote.get(key)
        if value is not None and float(value) > 0:
            return float(value), source
    return float(alert.get("entry_price")), "steve_alert_price"


def validate_command_for_card(card: dict[str, Any], command: dict[str, Any]) -> tuple[bool, str]:
    if command.get("command") != "buy":
        return True, ""
    if command.get("risk_type") == "percent":
        if float(command.get("stop_percent") or 0) <= 0 or float(command.get("take_percent") or 0) <= 0:
            return False, "invalid_percent_risk"
        if float(command.get("stop_percent") or 0) >= 100:
            return False, "stop_percent_too_large"
        return True, ""
    if command.get("risk_type") == "price":
        entry_price, _source = fill_price_from_card(card)
        stop_price = float(command.get("stop_price") or 0)
        take_price = float(command.get("take_price") or 0)
        if stop_price <= 0 or take_price <= 0:
            return False, "invalid_price_risk"
        if not (stop_price < entry_price < take_price):
            return False, f"price_risk_must_bracket_entry:{entry_price:.2f}"
        return True, ""
    return False, "missing_risk"


def create_human_position(card: dict[str, Any], command: dict[str, Any]) -> dict[str, Any]:
    approval_id = str(card["approval_id"])
    existing_position_id = "human-" + stable_hash([approval_id, "human"])[:16]
    for row in read_jsonl(HUMAN_POSITIONS_FILE):
        if row.get("position_id") == existing_position_id:
            return row
    alert = card.get("alert") or {}
    snapshot = card.get("snapshot") or {}
    fill_price, fill_source = fill_price_from_card(card)
    contracts = int(command.get("contracts") or alert.get("contracts") or 1)
    first_take_percent = command.get("take_percent") if command.get("risk_type") == "percent" else DEFAULT_TAKE_PERCENT
    first_take_price = command.get("take_price") if command.get("risk_type") == "price" else None
    position = {
        "event_type": "human_paper_option_position",
        "position_id": existing_position_id,
        "approval_id": approval_id,
        "opened_at": now_iso(),
        "source_dedupe_key": alert.get("source_dedupe_key"),
        "ticker": alert.get("ticker"),
        "contract_symbol": snapshot.get("contract_symbol"),
        "option_type": alert.get("option_type"),
        "expiration_date": alert.get("expiration_date"),
        "strike_price": alert.get("strike_price"),
        "contracts": contracts,
        "entry_price": fill_price,
        "entry_price_source": fill_source,
        "risk_type": command.get("risk_type"),
        "stop_percent": command.get("stop_percent"),
        "take_percent": command.get("take_percent"),
        "stop_price": command.get("stop_price"),
        "take_price": command.get("take_price"),
        "used_default_contracts": bool(command.get("used_default_contracts")),
        "used_default_risk": bool(command.get("used_default_risk")),
        "alert_contracts": int(alert.get("contracts") or 1),
        "exit_plan": exit_plan_for_contracts(
            contracts,
            fill_price,
            first_take_percent=first_take_percent,
            first_take_price=first_take_price,
        ),
        "exit_plan_notes": [
            "Steve close/stopped/sold alert closes remaining contracts before later profit tranches.",
            "Default hard stop applies to all open contracts until a later exit manager changes it.",
        ],
        "status": "open",
    }
    append_jsonl(HUMAN_POSITIONS_FILE, position)
    return position


def append_action(row: dict[str, Any]) -> None:
    base = {"event_type": "steve_approval_action", "recorded_at": now_iso()}
    base.update(row)
    append_jsonl(APPROVAL_ACTIONS_FILE, base)


def chat_id_from_message(message: dict[str, Any]) -> str:
    chat = message.get("chat") or {}
    value = chat.get("id")
    return "" if value is None else str(value)


def sender_id_from_message(message: dict[str, Any]) -> str:
    sender = message.get("from") or {}
    value = sender.get("id")
    return "" if value is None else str(value)


def authorization_for_message(message: dict[str, Any], config: BotConfig) -> tuple[bool, str]:
    chat_id = chat_id_from_message(message)
    sender_id = sender_id_from_message(message)
    if chat_id == str(config.approval_chat_id):
        return True, "approval_group"
    if chat_id == str(config.owner_chat_id) and sender_id == str(config.owner_user_id):
        return True, "owner_dm"
    return False, "unauthorized_chat"


def process_approval_message(message: dict[str, Any], config: BotConfig) -> dict[str, Any]:
    sender_id = sender_id_from_message(message)
    chat_id = chat_id_from_message(message)
    text = (message.get("text") or "").strip()
    authorized, authorization_scope = authorization_for_message(message, config)
    if not authorized:
        row = {
            "action": "unauthorized",
            "reason": authorization_scope,
            "telegram_message_id": message.get("message_id"),
            "chat_id": chat_id,
            "sender_id": sender_id,
            "text": text,
        }
        append_action(row)
        return row
    command = parse_approval_command(text)
    if not command.get("ok"):
        row = {
            "action": "rejected_command",
            "reason": command.get("reason"),
            "authorization_scope": authorization_scope,
            "telegram_message_id": message.get("message_id"),
            "chat_id": chat_id,
            "sender_id": sender_id,
            "text": text,
        }
        append_action(row)
        return row
    card = card_for_message(message)
    if not card:
        row = {
            "action": "orphan_command",
            "authorization_scope": authorization_scope,
            "telegram_message_id": message.get("message_id"),
            "chat_id": chat_id,
            "sender_id": sender_id,
            "text": text,
        }
        append_action(row)
        return row
    command_ok, command_reason = validate_command_for_card(card, command)
    if not command_ok:
        row = {
            "action": "rejected_command",
            "reason": command_reason,
            "authorization_scope": authorization_scope,
            "telegram_message_id": message.get("message_id"),
            "chat_id": chat_id,
            "sender_id": sender_id,
            "text": text,
            "approval_id": card.get("approval_id"),
        }
        append_action(row)
        return row
    approval_id = str(card["approval_id"])
    if any(row.get("action") in {"approved", "skipped"} for row in actions_for_approval(approval_id)):
        row = {
            "action": "duplicate_command",
            "approval_id": approval_id,
            "authorization_scope": authorization_scope,
            "telegram_message_id": message.get("message_id"),
            "chat_id": chat_id,
            "sender_id": sender_id,
            "text": text,
        }
        append_action(row)
        return row
    if command["command"] == "skip":
        row = {
            "action": "skipped",
            "approval_id": approval_id,
            "authorization_scope": authorization_scope,
            "telegram_message_id": message.get("message_id"),
            "chat_id": chat_id,
            "sender_id": sender_id,
            "text": text,
        }
        append_action(row)
        return row
    position = create_human_position(card, command)
    broker_audit = submit_option_paper_order(position)
    row = {
        "action": "approved",
        "approval_id": approval_id,
        "authorization_scope": authorization_scope,
        "telegram_message_id": message.get("message_id"),
        "chat_id": chat_id,
        "sender_id": sender_id,
        "text": text,
        "position_id": position.get("position_id"),
        "broker_status": broker_audit.get("status"),
        "broker_reason": broker_audit.get("reason"),
    }
    append_action(row)
    return row


def get_updates(config: BotConfig, offset: int | None = None, timeout: int = 0) -> list[dict[str, Any]]:
    payload: dict[str, Any] = {"timeout": timeout, "allowed_updates": json.dumps(["message"])}
    if offset is not None:
        payload["offset"] = offset
    response = telegram_request(config.token, "getUpdates", payload)
    if not response.get("ok"):
        raise RuntimeError(f"Telegram getUpdates returned non-ok response: {response}")
    return response.get("result") or []


def discover_chats() -> list[dict[str, Any]]:
    token = load_bot_token(required=True)
    response = telegram_request(token, "getUpdates", {"timeout": 0, "allowed_updates": json.dumps(["message"])})
    if not response.get("ok"):
        raise RuntimeError(f"Telegram getUpdates returned non-ok response: {response}")
    rows: list[dict[str, Any]] = []
    for update in response.get("result") or []:
        message = update.get("message") or {}
        chat = message.get("chat") or {}
        sender = message.get("from") or {}
        text = str(message.get("text") or "")
        rows.append(
            {
                "update_id": update.get("update_id"),
                "message_id": message.get("message_id"),
                "chat_id": chat.get("id"),
                "chat_type": chat.get("type"),
                "chat_title": chat.get("title") or chat.get("username") or chat.get("first_name"),
                "sender_user_id": sender.get("id"),
                "sender_username": sender.get("username"),
                "sender_name": " ".join(part for part in [sender.get("first_name"), sender.get("last_name")] if part),
                "text_preview": text[:120],
            }
        )
    return rows


def bot_info() -> dict[str, Any]:
    token = load_bot_token(required=True)
    return telegram_request(token, "getMe", {})


def reset_updates() -> dict[str, Any]:
    token = load_bot_token(required=True)
    response = telegram_request(token, "getUpdates", {"offset": -1, "timeout": 0, "allowed_updates": json.dumps(["message"])})
    save_state({})
    return response


def poll_once(require_config: bool = True) -> dict[str, int]:
    config = load_bot_config(required=require_config)
    if config is None:
        return {"updates": 0, "messages": 0, "actions": 0}
    state = load_state()
    offset = state.get("telegram_update_offset")
    updates = get_updates(config, offset=offset, timeout=0)
    counts = {"updates": len(updates), "messages": 0, "actions": 0}
    for update in updates:
        state["telegram_update_offset"] = int(update["update_id"]) + 1
        message = update.get("message")
        if not message:
            continue
        counts["messages"] += 1
        process_approval_message(message, config)
        counts["actions"] += 1
    save_state(state)
    return counts


def poll_loop(interval: float = 2.0) -> None:
    while True:
        poll_once()
        time.sleep(interval)


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    sub = parser.add_subparsers(dest="command", required=True)
    poll = sub.add_parser("poll")
    poll.add_argument("--once", action="store_true")
    poll.add_argument("--interval", type=float, default=2.0)
    sub.add_parser("discover-chats")
    sub.add_parser("bot-info")
    sub.add_parser("reset-updates")
    args = parser.parse_args()
    if args.command == "poll":
        if args.once:
            print(json.dumps(poll_once(require_config=True), sort_keys=True))
        else:
            poll_loop(args.interval)
    elif args.command == "discover-chats":
        print(json.dumps(discover_chats(), indent=2, sort_keys=True))
    elif args.command == "bot-info":
        print(json.dumps(bot_info(), indent=2, sort_keys=True))
    elif args.command == "reset-updates":
        print(json.dumps(reset_updates(), indent=2, sort_keys=True))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())

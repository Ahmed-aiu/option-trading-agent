#!/usr/bin/env python3
"""Check premarket local pipeline readiness and write audit reports."""

from __future__ import annotations

import argparse
import datetime as dt
import json
import os
import shutil
from dataclasses import dataclass
from pathlib import Path
from typing import Any, Callable, Mapping
from zoneinfo import ZoneInfo

from pipeline_common import CONFIG_DIR, DATA_DIR, DEFAULT_TZ, ROOT, load_simple_yaml, parse_datetime


PAPER_HOST = "https://paper-api.alpaca.markets"
HEARTBEAT_FILE = DATA_DIR / "live_pipeline_heartbeat.json"
BROWSER_HEALTH_LATEST_FILE = DATA_DIR / "discord_browser_health_latest.json"
REPORTS_FILE = DATA_DIR / "premarket_readiness_reports.jsonl"
LATEST_FILE = DATA_DIR / "premarket_readiness_latest.json"
WATCHER_CONFIG_FILE = CONFIG_DIR / "watcher.yaml"
BROKER_CONFIG_FILE = CONFIG_DIR / "broker.yaml"
ENV_FILE = ROOT / ".env.local"


@dataclass(frozen=True)
class ReadinessPaths:
    data_dir: Path
    heartbeat_file: Path
    browser_health_file: Path
    watcher_config_file: Path
    broker_config_file: Path
    env_file: Path
    reports_file: Path
    latest_file: Path


def default_paths() -> ReadinessPaths:
    return ReadinessPaths(
        data_dir=DATA_DIR,
        heartbeat_file=HEARTBEAT_FILE,
        browser_health_file=BROWSER_HEALTH_LATEST_FILE,
        watcher_config_file=WATCHER_CONFIG_FILE,
        broker_config_file=BROKER_CONFIG_FILE,
        env_file=ENV_FILE,
        reports_file=REPORTS_FILE,
        latest_file=LATEST_FILE,
    )


def append_jsonl(path: Path, record: dict[str, Any]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    with path.open("a", encoding="utf-8") as handle:
        handle.write(json.dumps(record, sort_keys=True, separators=(",", ":")) + "\n")


def write_latest_json(path: Path, record: dict[str, Any]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    tmp_path = path.with_suffix(path.suffix + ".tmp")
    tmp_path.write_text(json.dumps(record, sort_keys=True, separators=(",", ":")) + "\n", encoding="utf-8")
    tmp_path.replace(path)


def load_json(path: Path) -> tuple[dict[str, Any] | None, str]:
    if not path.exists():
        return None, "missing"
    try:
        value = json.loads(path.read_text(encoding="utf-8"))
    except json.JSONDecodeError:
        return None, "invalid_json"
    if not isinstance(value, dict):
        return None, "not_object"
    return value, ""


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


def env_value(name: str, env_file: Mapping[str, str], env: Mapping[str, str]) -> str:
    return str(env.get(name) or env_file.get(name, ""))


def normalize_base_url(value: str) -> str:
    value = value.strip().rstrip("/")
    if value.endswith("/v2"):
        value = value[:-3]
    return value


def now_in_tz(tz_name: str) -> dt.datetime:
    return dt.datetime.now(ZoneInfo(tz_name))


def age_seconds(value: Any, now: dt.datetime, tz_name: str) -> float | None:
    parsed = parse_datetime(value, tz_name)
    if parsed is None:
        return None
    return (now - parsed).total_seconds()


def check_record(
    *,
    name: str,
    status: str,
    summary: str,
    evidence: dict[str, Any] | None = None,
    recommendation: str = "",
) -> dict[str, Any]:
    record = {
        "name": name,
        "status": status,
        "summary": summary,
        "evidence": evidence or {},
    }
    if recommendation:
        record["recommendation"] = recommendation
    return record


def check_fresh_json_record(
    name: str,
    path: Path,
    timestamp_key: str,
    max_age_seconds: float,
    now: dt.datetime,
    tz_name: str,
) -> dict[str, Any]:
    record, error = load_json(path)
    if record is None:
        return check_record(
            name=name,
            status="failed",
            summary=f"{name} file is {error}.",
            evidence={"exists": path.exists(), "error": error},
            recommendation=f"Start or repair the component that writes {path.name}.",
        )
    seconds = age_seconds(record.get(timestamp_key), now, tz_name)
    if seconds is None:
        return check_record(
            name=name,
            status="failed",
            summary=f"{name} has no parseable {timestamp_key}.",
            evidence={"exists": True, timestamp_key: record.get(timestamp_key)},
            recommendation=f"Restart the component that writes {path.name} so it refreshes liveness timestamps.",
        )
    evidence = {
        "exists": True,
        timestamp_key: record.get(timestamp_key),
        "age_seconds": round(seconds, 3),
        "max_age_seconds": max_age_seconds,
    }
    if seconds < -300:
        return check_record(
            name=name,
            status="failed",
            summary=f"{name} timestamp is too far in the future.",
            evidence=evidence,
            recommendation="Check the local system clock before market open.",
        )
    if seconds > max_age_seconds:
        return check_record(
            name=name,
            status="failed",
            summary=f"{name} is stale.",
            evidence=evidence,
            recommendation=f"Restart the component that writes {path.name} and verify it updates before open.",
        )
    return check_record(
        name=name,
        status="ok",
        summary=f"{name} is fresh.",
        evidence=evidence,
    )


def check_heartbeat(path: Path, max_age_seconds: float, now: dt.datetime, tz_name: str) -> dict[str, Any]:
    return check_fresh_json_record(
        "live_pipeline_heartbeat",
        path,
        "recorded_at",
        max_age_seconds,
        now,
        tz_name,
    )


def check_browser_health(path: Path, max_age_seconds: float, now: dt.datetime, tz_name: str) -> dict[str, Any]:
    fresh_check = check_fresh_json_record(
        "discord_browser_health_latest",
        path,
        "recorded_at",
        max_age_seconds,
        now,
        tz_name,
    )
    if fresh_check["status"] != "ok":
        return fresh_check
    record, _ = load_json(path)
    browser_status = str((record or {}).get("status") or "")
    evidence = dict(fresh_check["evidence"])
    evidence["browser_status"] = browser_status
    totals = (record or {}).get("totals") or {}
    if isinstance(totals, dict):
        evidence["channels"] = totals.get("channels")
        evidence["channels_ok"] = totals.get("channels_ok")
    if browser_status != "ok":
        return check_record(
            name="discord_browser_health_latest",
            status="failed",
            summary="Browser capture latest health is not ok.",
            evidence=evidence,
            recommendation="Restart the foreground browser watcher and verify Chrome Discord channel reads succeed.",
        )
    return check_record(
        name="discord_browser_health_latest",
        status="ok",
        summary="Browser capture latest health is fresh and ok.",
        evidence=evidence,
    )


def disk_usage_path(data_dir: Path) -> Path:
    candidate = data_dir
    while not candidate.exists() and candidate != candidate.parent:
        candidate = candidate.parent
    return candidate


def check_disk_space(data_dir: Path, min_free_gb: float) -> dict[str, Any]:
    usage = shutil.disk_usage(disk_usage_path(data_dir))
    free_gb = usage.free / (1024**3)
    evidence = {
        "free_gb": round(free_gb, 3),
        "min_free_gb": min_free_gb,
        "total_gb": round(usage.total / (1024**3), 3),
    }
    if free_gb < min_free_gb:
        return check_record(
            name="disk_free_space",
            status="failed",
            summary="Disk free space is below the premarket threshold.",
            evidence=evidence,
            recommendation="Free local disk space or lower the threshold only if the remaining space is operationally acceptable.",
        )
    return check_record(
        name="disk_free_space",
        status="ok",
        summary="Disk free space is above the premarket threshold.",
        evidence=evidence,
    )


def check_watcher_config(path: Path) -> tuple[dict[str, Any], dict[str, Any]]:
    if not path.exists():
        return (
            {},
            check_record(
                name="watcher_config",
                status="failed",
                summary="Local watcher config is missing.",
                evidence={"exists": False},
                recommendation="Create config/watcher.yaml from config/watcher.example.yaml and keep private values local.",
            ),
        )
    try:
        config = load_simple_yaml(path)
    except Exception as exc:  # noqa: BLE001
        return (
            {},
            check_record(
                name="watcher_config",
                status="failed",
                summary="Local watcher config could not be parsed.",
                evidence={"exists": True, "error_type": type(exc).__name__},
                recommendation="Fix config/watcher.yaml syntax without printing or committing private channel values.",
            ),
        )
    return (
        config,
        check_record(
            name="watcher_config",
            status="ok",
            summary="Local watcher config exists and is parseable.",
            evidence={"exists": True, "parseable": True},
        ),
    )


def check_telegram_owner_config(env_file: Mapping[str, str], env: Mapping[str, str]) -> dict[str, Any]:
    token_present = bool(env_value("STEVE_TRADE_BOT_TOKEN", env_file, env))
    legacy_chat_used = not bool(env_value("STEVE_TRADE_OWNER_CHAT_ID", env_file, env)) and bool(
        env_value("STEVE_TRADE_APPROVER_CHAT_ID", env_file, env)
    )
    legacy_user_used = not bool(env_value("STEVE_TRADE_OWNER_USER_ID", env_file, env)) and bool(
        env_value("STEVE_TRADE_APPROVER_USER_ID", env_file, env)
    )
    owner_chat_present = bool(
        env_value("STEVE_TRADE_OWNER_CHAT_ID", env_file, env)
        or env_value("STEVE_TRADE_APPROVER_CHAT_ID", env_file, env)
    )
    owner_user_present = bool(
        env_value("STEVE_TRADE_OWNER_USER_ID", env_file, env)
        or env_value("STEVE_TRADE_APPROVER_USER_ID", env_file, env)
    )
    evidence = {
        "token_present": token_present,
        "owner_chat_id_present": owner_chat_present,
        "owner_user_id_present": owner_user_present,
        "legacy_owner_chat_fallback_used": legacy_chat_used,
        "legacy_owner_user_fallback_used": legacy_user_used,
    }
    missing = []
    if not token_present:
        missing.append("STEVE_TRADE_BOT_TOKEN")
    if not owner_chat_present:
        missing.append("STEVE_TRADE_OWNER_CHAT_ID")
    if not owner_user_present:
        missing.append("STEVE_TRADE_OWNER_USER_ID")
    if missing:
        return check_record(
            name="telegram_owner_config",
            status="failed",
            summary="Telegram owner bot config is incomplete.",
            evidence=evidence | {"missing_names": missing},
            recommendation="Set the owner Telegram bot token, owner chat id, and owner user id in .env.local.",
        )
    return check_record(
        name="telegram_owner_config",
        status="ok",
        summary="Telegram owner bot config is present.",
        evidence=evidence,
    )


def check_alpaca_paper_config(
    broker_config_path: Path,
    env_file: Mapping[str, str],
    env: Mapping[str, str],
) -> dict[str, Any]:
    if not broker_config_path.exists():
        return check_record(
            name="alpaca_paper_config",
            status="failed",
            summary="Broker config is missing.",
            evidence={"broker_config_exists": False},
            recommendation="Restore config/broker.yaml with the paper Alpaca endpoint guard.",
        )
    try:
        config = load_simple_yaml(broker_config_path)
    except Exception as exc:  # noqa: BLE001
        return check_record(
            name="alpaca_paper_config",
            status="failed",
            summary="Broker config could not be parsed.",
            evidence={"broker_config_exists": True, "error_type": type(exc).__name__},
            recommendation="Fix config/broker.yaml before running the paper pipeline.",
        )
    base_url_name = str(config.get("base_url_env_var", "APCA_API_BASE_URL"))
    key_name = str(config.get("key_id_env_var", "APCA_API_KEY_ID"))
    secret_name = str(config.get("secret_key_env_var", "APCA_API_SECRET_KEY"))
    mode_name = str(config.get("trading_mode_env_var", "OPENCLAW_TRADING_MODE"))
    submit_name = str(config.get("submit_enabled_env_var", "OPENCLAW_ENABLE_PAPER_ORDERS"))
    configured_base_url = env_value(base_url_name, env_file, env)
    base_url = normalize_base_url(configured_base_url or str(config.get("paper_base_url", PAPER_HOST)))
    mode = (env_value(mode_name, env_file, env) or "paper").strip().lower()
    submit_enabled = env_value(submit_name, env_file, env).strip().lower() == "true"
    key_present = bool(env_value(key_name, env_file, env))
    secret_present = bool(env_value(secret_name, env_file, env))
    evidence = {
        "base_url_configured": bool(configured_base_url),
        "base_url_kind": "paper" if base_url == PAPER_HOST else "non_paper",
        "trading_mode": mode or "paper",
        "submit_enabled": submit_enabled,
        "credentials_present": key_present and secret_present,
    }
    if base_url != PAPER_HOST:
        return check_record(
            name="alpaca_paper_config",
            status="failed",
            summary="Alpaca base URL is not the paper endpoint.",
            evidence=evidence,
            recommendation="Set APCA_API_BASE_URL to the Alpaca paper endpoint before any paper order audit can run.",
        )
    if mode != "paper":
        return check_record(
            name="alpaca_paper_config",
            status="failed",
            summary="Trading mode is not paper.",
            evidence=evidence,
            recommendation="Set OPENCLAW_TRADING_MODE=paper before market open.",
        )
    if submit_enabled and not (key_present and secret_present):
        return check_record(
            name="alpaca_paper_config",
            status="degraded",
            summary="Alpaca paper order submission is enabled but credentials are incomplete.",
            evidence=evidence,
            recommendation="Provide paper Alpaca credentials or disable OPENCLAW_ENABLE_PAPER_ORDERS for local-only validation.",
        )
    return check_record(
        name="alpaca_paper_config",
        status="ok",
        summary="Alpaca broker config is paper-only.",
        evidence=evidence,
    )


def overall_status(checks: list[dict[str, Any]]) -> str:
    statuses = {str(check.get("status")) for check in checks}
    if "failed" in statuses:
        return "failed"
    if "degraded" in statuses:
        return "degraded"
    return "ok"


def collect_recommendations(checks: list[dict[str, Any]]) -> list[str]:
    recommendations: list[str] = []
    seen: set[str] = set()
    for check in checks:
        recommendation = str(check.get("recommendation") or "")
        if check.get("status") != "ok" and recommendation and recommendation not in seen:
            seen.add(recommendation)
            recommendations.append(recommendation)
    return recommendations


def readiness_message(record: dict[str, Any]) -> str:
    status = str(record.get("status") or "unknown").upper()
    lines = [f"PREMARKET READINESS - {status}", f"Generated {record.get('generated_at')}", ""]
    problem_checks = [check for check in record.get("checks", []) if check.get("status") != "ok"]
    if problem_checks:
        lines.append("Checks needing attention:")
        for check in problem_checks[:8]:
            lines.append(f"- {check.get('name')}: {check.get('summary')}")
    else:
        lines.append("All readiness checks passed.")
    recommendations = list(record.get("recommendations") or [])
    if recommendations:
        lines.extend(["", "Recommendations:"])
        for recommendation in recommendations[:6]:
            lines.append(f"- {recommendation}")
    return "\n".join(lines)


def sanitize_telegram_delivery(status: str, reason: str, messages: list[dict[str, Any]]) -> dict[str, Any]:
    return {
        "audience": "owner_operational",
        "status": status,
        "reason_present": bool(reason),
        "attempted_messages": len(messages),
        "sent_messages": sum(1 for message in messages if message.get("status") == "sent"),
    }


def send_readiness_telegram(
    record: dict[str, Any],
    sender: Callable[[str], tuple[str, str, list[dict[str, Any]]]] | None = None,
) -> dict[str, Any]:
    if sender is None:
        from steve_trade_bot import send_message_to_approval_chats

        sender = send_message_to_approval_chats
    status, reason, messages = sender(readiness_message(record))
    return sanitize_telegram_delivery(status, reason, messages)


def run_readiness(
    *,
    paths: ReadinessPaths | None = None,
    max_heartbeat_age_seconds: float = 120.0,
    max_browser_health_age_seconds: float = 180.0,
    min_free_gb: float = 2.0,
    send_telegram: bool = True,
    env: Mapping[str, str] | None = None,
    now: dt.datetime | None = None,
    telegram_sender: Callable[[str], tuple[str, str, list[dict[str, Any]]]] | None = None,
) -> dict[str, Any]:
    paths = paths or default_paths()
    env_values = env if env is not None else os.environ
    watcher_config, watcher_check = check_watcher_config(paths.watcher_config_file)
    tz_name = str(watcher_config.get("timezone") or DEFAULT_TZ)
    current = now.astimezone(ZoneInfo(tz_name)) if now is not None else now_in_tz(tz_name)
    env_file = load_env_file(paths.env_file)
    checks = [
        check_heartbeat(paths.heartbeat_file, max_heartbeat_age_seconds, current, tz_name),
        check_browser_health(paths.browser_health_file, max_browser_health_age_seconds, current, tz_name),
        check_disk_space(paths.data_dir, min_free_gb),
        watcher_check,
        check_telegram_owner_config(env_file, env_values),
        check_alpaca_paper_config(paths.broker_config_file, env_file, env_values),
    ]
    record = {
        "event_type": "premarket_readiness",
        "generated_at": current.isoformat(timespec="seconds"),
        "status": overall_status(checks),
        "checks": checks,
        "recommendations": collect_recommendations(checks),
    }
    if send_telegram:
        record["telegram"] = send_readiness_telegram(record, telegram_sender)
    else:
        record["telegram"] = {"audience": "owner_operational", "status": "not_sent", "reason": "disabled_by_flag"}
    append_jsonl(paths.reports_file, record)
    write_latest_json(paths.latest_file, record)
    return record


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--print-json", action="store_true")
    telegram_group = parser.add_mutually_exclusive_group()
    telegram_group.add_argument("--send-telegram", dest="send_telegram", action="store_true")
    telegram_group.add_argument("--no-telegram", dest="send_telegram", action="store_false")
    parser.set_defaults(send_telegram=True)
    parser.add_argument("--max-heartbeat-age-seconds", type=float, default=120.0)
    parser.add_argument("--max-browser-health-age-seconds", type=float, default=180.0)
    parser.add_argument("--min-free-gb", type=float, default=2.0)
    args = parser.parse_args()

    record = run_readiness(
        max_heartbeat_age_seconds=args.max_heartbeat_age_seconds,
        max_browser_health_age_seconds=args.max_browser_health_age_seconds,
        min_free_gb=args.min_free_gb,
        send_telegram=args.send_telegram,
    )
    if args.print_json:
        print(json.dumps(record, sort_keys=True))
    else:
        print(f"premarket_readiness status={record['status']} recommendations={len(record['recommendations'])}")
    return 0 if record["status"] == "ok" else 1


if __name__ == "__main__":
    raise SystemExit(main())

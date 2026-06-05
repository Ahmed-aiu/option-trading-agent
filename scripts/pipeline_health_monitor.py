#!/usr/bin/env python3
"""Monitor Steve alert capture stages and report exact failure points."""

from __future__ import annotations

import argparse
import datetime as dt
import json
import os
import signal
import subprocess
import time
from dataclasses import asdict, dataclass
from pathlib import Path
from typing import Any
from zoneinfo import ZoneInfo

from notification_probe import probe_notification_db
from notification_watcher import is_matching_notification, normalize_raw
from pipeline_common import CONFIG_DIR, DATA_DIR, LOG_DIR, append_jsonl, load_seen_keys, load_simple_yaml, now_iso, parse_datetime, read_jsonl, setup_logging
from steve_trade_bot import configured_approval_chat_ids, load_bot_config, send_telegram_message


HEALTH_CHECKS_FILE = DATA_DIR / "pipeline_health_checks.jsonl"
HEALTH_LATEST_FILE = DATA_DIR / "pipeline_health_latest.json"
HEALTH_ALERTS_FILE = DATA_DIR / "pipeline_health_alerts.jsonl"
HEALTH_STATE_FILE = DATA_DIR / "pipeline_health_state.json"
LIVE_HEARTBEAT_FILE = DATA_DIR / "live_pipeline_heartbeat.json"
BROWSER_HEALTH_LATEST_FILE = DATA_DIR / "discord_browser_health_latest.json"
BROWSER_MESSAGES_FILE = DATA_DIR / "discord_browser_messages.jsonl"
RAW_FILE = DATA_DIR / "raw_notifications.jsonl"
PROCESSED_FILE = DATA_DIR / "processed_notifications.jsonl"
PARSED_FILE = DATA_DIR / "parsed_alerts.jsonl"
REJECTED_FILE = DATA_DIR / "rejected_alerts.jsonl"
APPROVAL_CARDS_FILE = DATA_DIR / "steve_approval_cards.jsonl"
AUTO_BUY_REPORTS_FILE = DATA_DIR / "steve_auto_buy_reports.jsonl"
CLOSE_REPORTS_FILE = DATA_DIR / "steve_close_reports.jsonl"
HUMAN_POSITIONS_FILE = DATA_DIR / "human_paper_positions.jsonl"
ORDERS_FILE = DATA_DIR / "orders_paper.jsonl"
STEVE_EXITS_FILE = DATA_DIR / "steve_option_exits.jsonl"

STOP = False


@dataclass(frozen=True)
class Issue:
    stage: str
    code: str
    severity: str
    message: str
    evidence: dict[str, Any]

    @property
    def key(self) -> str:
        parts = [self.stage, self.code]
        for field in ("dedupe_key", "source_dedupe_key", "channel_id", "message_key"):
            value = self.evidence.get(field)
            if value:
                parts.append(str(value))
                break
        return ":".join(parts)


def request_stop(signum: int, frame: object) -> None:
    global STOP
    STOP = True


def write_latest_json(path: Path, record: dict[str, Any]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    tmp_path = path.with_suffix(path.suffix + ".tmp")
    tmp_path.write_text(json.dumps(record, sort_keys=True, separators=(",", ":")) + "\n", encoding="utf-8")
    tmp_path.replace(path)


def load_json(path: Path) -> dict[str, Any]:
    if not path.exists():
        return {}
    try:
        value = json.loads(path.read_text(encoding="utf-8"))
        return value if isinstance(value, dict) else {}
    except json.JSONDecodeError:
        return {}


def current_time(tz_name: str) -> dt.datetime:
    return dt.datetime.now(ZoneInfo(tz_name))


def age_seconds(value: Any, tz_name: str) -> float | None:
    parsed = parse_datetime(value, tz_name)
    if parsed is None:
        return None
    return (current_time(tz_name) - parsed).total_seconds()


def is_recent_record(record: dict[str, Any], keys: list[str], max_age_seconds: float, tz_name: str) -> bool:
    for key in keys:
        seconds = age_seconds(record.get(key), tz_name)
        if seconds is not None:
            return -300 <= seconds <= max_age_seconds
    return False


def launchctl_state(label: str) -> dict[str, Any]:
    result = subprocess.run(
        ["launchctl", "print", f"gui/{os.getuid()}/{label}"],
        text=True,
        capture_output=True,
        timeout=10,
        check=False,
    )
    text = result.stdout + result.stderr
    state = ""
    pid = ""
    for line in text.splitlines():
        stripped = line.strip()
        if stripped.startswith("state =") and not state:
            state = stripped.split("=", 1)[1].strip()
        if stripped.startswith("pid =") and not pid:
            pid = stripped.split("=", 1)[1].strip()
    return {"returncode": result.returncode, "state": state, "pid": pid, "raw": text[-1000:]}


def check_live_watcher(tz_name: str, stale_seconds: int) -> list[Issue]:
    issues: list[Issue] = []
    service = launchctl_state("ai.openclaw.trading-alert-watcher")
    if service["returncode"] != 0 or service.get("state") != "running":
        issues.append(
            Issue(
                stage="live_watcher",
                code="launch_agent_not_running",
                severity="critical",
                message="Live notification pipeline LaunchAgent is not running.",
                evidence={"state": service.get("state"), "pid": service.get("pid")},
            )
        )
    heartbeat = load_json(LIVE_HEARTBEAT_FILE)
    if not heartbeat:
        issues.append(
            Issue(
                stage="live_watcher",
                code="heartbeat_missing",
                severity="critical",
                message="Live pipeline heartbeat file is missing.",
                evidence={"path": str(LIVE_HEARTBEAT_FILE)},
            )
        )
        return issues
    seconds = age_seconds(heartbeat.get("recorded_at"), tz_name)
    if seconds is None or seconds > stale_seconds:
        issues.append(
            Issue(
                stage="live_watcher",
                code="heartbeat_stale",
                severity="critical",
                message="Live pipeline heartbeat is stale.",
                evidence={"recorded_at": heartbeat.get("recorded_at"), "age_seconds": seconds},
            )
        )
    return issues


def check_notification_capture(config: dict[str, Any], last_minutes: int, sla_seconds: int, tz_name: str) -> tuple[list[Issue], dict[str, Any]]:
    app_name = str((config.get("app_names") or ["Discord"])[0])
    db_records, diagnostics = probe_notification_db(app_name, last_minutes=last_minutes, tz_name=tz_name)
    raw_seen = load_seen_keys(RAW_FILE)
    issues: list[Issue] = []
    matching = []
    missing = []
    for record in db_records:
        normalized = normalize_raw(record, config)
        if not is_matching_notification(normalized, config):
            continue
        matching.append(normalized)
        key = str(normalized.get("dedupe_key") or "")
        seconds = age_seconds(normalized.get("notification_timestamp") or normalized.get("captured_at"), tz_name)
        if key and key not in raw_seen and (seconds is None or seconds > sla_seconds):
            missing.append(normalized)
            issues.append(
                Issue(
                    stage="notification_capture",
                    code="notification_db_row_not_raw",
                    severity="critical",
                    message="Discord notification exists in macOS DB but was not captured into raw_notifications.",
                    evidence={
                        "dedupe_key": key,
                        "title": normalized.get("title"),
                        "subtitle": normalized.get("subtitle"),
                        "notification_timestamp": normalized.get("notification_timestamp"),
                        "body_preview": str(normalized.get("body") or "")[:180],
                    },
                )
            )
    summary = {
        "db_records": len(db_records),
        "matching_records": len(matching),
        "missing_raw_records": len(missing),
        "latest_matching_timestamp": max((row.get("notification_timestamp") or "" for row in matching), default=""),
        "diagnostics": diagnostics[-3:],
    }
    return issues, summary


def check_browser_capture(stale_seconds: int, sla_seconds: int, tz_name: str) -> tuple[list[Issue], dict[str, Any]]:
    issues: list[Issue] = []
    latest = load_json(BROWSER_HEALTH_LATEST_FILE)
    if not latest:
        issues.append(
            Issue(
                stage="browser_capture",
                code="browser_health_missing",
                severity="warning",
                message="Browser capture has not written a health record yet.",
                evidence={"path": str(BROWSER_HEALTH_LATEST_FILE)},
            )
        )
    else:
        seconds = age_seconds(latest.get("recorded_at"), tz_name)
        if seconds is None or seconds > stale_seconds:
            issues.append(
                Issue(
                    stage="browser_capture",
                    code="browser_health_stale",
                    severity="critical",
                    message="Browser capture health is stale.",
                    evidence={"recorded_at": latest.get("recorded_at"), "age_seconds": seconds},
                )
            )
        if latest.get("status") not in {"ok", None}:
            issues.append(
                Issue(
                    stage="browser_capture",
                    code="browser_capture_degraded",
                    severity="critical",
                    message="Browser capture reported channel read errors.",
                    evidence={"status": latest.get("status"), "errors": latest.get("errors") or []},
                )
            )

    raw_seen = load_seen_keys(RAW_FILE)
    browser_messages = read_jsonl(BROWSER_MESSAGES_FILE)
    for message in browser_messages:
        if message.get("capture_mode") != "live":
            continue
        if not is_recent_record(message, ["captured_at"], sla_seconds * 10, tz_name):
            continue
        raw_keys = [str(item) for item in message.get("raw_record_keys") or [] if item]
        if raw_keys and not any(key in raw_seen for key in raw_keys):
            seconds = age_seconds(message.get("captured_at"), tz_name)
            if seconds is None or seconds > sla_seconds:
                issues.append(
                    Issue(
                        stage="browser_capture",
                        code="browser_message_not_raw",
                        severity="critical",
                        message="Browser saw a Steve message but no derived raw record is present.",
                        evidence={
                            "message_key": message.get("message_key"),
                            "channel_id": message.get("channel_id"),
                            "raw_record_keys": raw_keys,
                            "text_preview": message.get("text_preview"),
                        },
                    )
                )
    summary = {
        "latest_status": latest.get("status") if latest else "missing",
        "latest_recorded_at": latest.get("recorded_at") if latest else "",
        "latest_totals": latest.get("totals") if latest else {},
    }
    return issues, summary


def check_raw_processing(sla_seconds: int, tz_name: str) -> list[Issue]:
    issues: list[Issue] = []
    processed = load_seen_keys(PROCESSED_FILE, key_name="dedupe_key")
    for raw in read_jsonl(RAW_FILE):
        if not is_recent_record(raw, ["captured_at", "notification_timestamp"], 24 * 60 * 60, tz_name):
            continue
        key = str(raw.get("dedupe_key") or "")
        if not key or key in processed:
            continue
        seconds = age_seconds(raw.get("captured_at") or raw.get("notification_timestamp"), tz_name)
        if seconds is None or seconds > sla_seconds:
            issues.append(
                Issue(
                    stage="raw_processing",
                    code="raw_not_processed",
                    severity="critical",
                    message="Raw alert exists but has not been processed by parser pipeline.",
                    evidence={"dedupe_key": key, "body_preview": str(raw.get("body") or "")[:180]},
                )
            )
    return issues


def check_routing(sla_seconds: int, tz_name: str) -> list[Issue]:
    issues: list[Issue] = []
    raw_keys = load_seen_keys(RAW_FILE)
    cards = load_seen_keys(APPROVAL_CARDS_FILE, key_name="source_dedupe_key")
    auto_reports = load_seen_keys(AUTO_BUY_REPORTS_FILE, key_name="source_dedupe_key")
    human_positions = load_seen_keys(HUMAN_POSITIONS_FILE, key_name="source_dedupe_key")
    exits = load_seen_keys(STEVE_EXITS_FILE, key_name="source_dedupe_key")
    for parsed in read_jsonl(PARSED_FILE):
        if not is_recent_record(parsed, ["parsed_at", "notification_timestamp"], 24 * 60 * 60, tz_name):
            continue
        key = str(parsed.get("source_dedupe_key") or "")
        if not key:
            continue
        if key not in raw_keys:
            continue
        seconds = age_seconds(parsed.get("parsed_at") or parsed.get("notification_timestamp"), tz_name)
        if seconds is not None and seconds <= sla_seconds:
            continue
        if parsed.get("instrument_type") != "option":
            continue
        if parsed.get("side") == "exit":
            if key not in exits:
                issues.append(
                    Issue(
                        stage="routing",
                        code="option_exit_not_recorded",
                        severity="critical",
                        message="Parsed Steve exit did not create a Steve exit record.",
                        evidence={"source_dedupe_key": key, "raw_text": str(parsed.get("raw_text") or "")[:180]},
                    )
                )
            continue
        if parsed.get("side") != "buy":
            continue
        tags = {str(tag).lower() for tag in (parsed.get("tags") or [])}
        if "hedge" in tags:
            if key not in cards:
                issues.append(
                    Issue(
                        stage="routing",
                        code="hedge_missing_approval_card",
                        severity="critical",
                        message="Parsed hedge alert did not create a Telegram approval card.",
                        evidence={"source_dedupe_key": key, "ticker": parsed.get("ticker"), "raw_text": str(parsed.get("raw_text") or "")[:180]},
                    )
                )
        elif key not in auto_reports and key not in human_positions and key not in cards:
            issues.append(
                Issue(
                    stage="routing",
                    code="non_hedge_missing_auto_buy",
                    severity="critical",
                    message="Parsed non-hedge alert did not create auto paper-buy artifacts.",
                    evidence={"source_dedupe_key": key, "ticker": parsed.get("ticker"), "raw_text": str(parsed.get("raw_text") or "")[:180]},
                )
            )
    return issues


def check_telegram_and_broker(tz_name: str) -> list[Issue]:
    issues: list[Issue] = []
    for path, stage, label in [
        (APPROVAL_CARDS_FILE, "telegram", "approval_card"),
        (AUTO_BUY_REPORTS_FILE, "telegram", "auto_buy_report"),
        (CLOSE_REPORTS_FILE, "telegram", "close_report"),
    ]:
        for row in read_jsonl(path):
            if not is_recent_record(row, ["created_at", "recorded_at"], 24 * 60 * 60, tz_name):
                continue
            if row.get("status") in {"sent", "partial_sent", "telegram_disabled"}:
                continue
            issues.append(
                Issue(
                    stage=stage,
                    code=f"{label}_send_failed",
                    severity="critical",
                    message=f"Telegram {label} was not delivered.",
                    evidence={
                        "source_dedupe_key": row.get("source_dedupe_key"),
                        "status": row.get("status"),
                        "reason": row.get("reason"),
                    },
                )
            )
    order_keys = load_seen_keys(ORDERS_FILE, key_name="source_dedupe_key")
    for report in read_jsonl(AUTO_BUY_REPORTS_FILE):
        if not is_recent_record(report, ["created_at"], 24 * 60 * 60, tz_name):
            continue
        key = str(report.get("source_dedupe_key") or "")
        if key and key not in order_keys:
            issues.append(
                Issue(
                    stage="broker",
                    code="auto_buy_missing_order_audit",
                    severity="warning",
                    message="Auto-buy report exists without a broker/order audit record.",
                    evidence={"source_dedupe_key": key, "auto_paper_id": report.get("auto_paper_id")},
                )
            )
    return issues


def health_status(issues: list[Issue]) -> str:
    if any(issue.severity == "critical" for issue in issues):
        return "failed"
    if issues:
        return "degraded"
    return "ok"


def alert_message(record: dict[str, Any], new_issues: list[Issue], recovered: list[str] | None = None) -> str:
    if recovered and not new_issues:
        return "PIPELINE HEALTH RECOVERED\nAll monitored stages are currently OK."
    lines = [f"PIPELINE HEALTH {str(record.get('status') or '').upper()}"]
    if recovered:
        lines.append(f"Recovered: {len(recovered)} issue(s)")
    for issue in new_issues[:6]:
        lines.append(f"{issue.stage}: {issue.code}")
        preview = issue.evidence.get("body_preview") or issue.evidence.get("text_preview") or issue.evidence.get("raw_text")
        if preview:
            lines.append(str(preview)[:120])
    if len(new_issues) > 6:
        lines.append(f"+{len(new_issues) - 6} more issue(s)")
    return "\n".join(lines)


def send_health_alert(record: dict[str, Any], new_issues: list[Issue], recovered: list[str] | None = None) -> None:
    config = load_bot_config(required=False)
    message = alert_message(record, new_issues, recovered)
    alert_record = {
        "event_type": "pipeline_health_alert",
        "recorded_at": now_iso(),
        "status": record.get("status"),
        "message_text": message,
        "telegram_messages": [],
    }
    if config is None:
        alert_record["status"] = "telegram_disabled"
        alert_record["reason"] = "missing_steve_trade_bot_env"
        append_jsonl(HEALTH_ALERTS_FILE, alert_record)
        return
    messages = []
    for chat_id in configured_approval_chat_ids(config):
        try:
            response = send_telegram_message(config, message, chat_id=chat_id)
            result = response.get("result", {}) if response.get("ok") else {}
            messages.append({"chat_id": str(chat_id), "message_id": result.get("message_id"), "status": "sent" if response.get("ok") else "send_failed", "reason": "" if response.get("ok") else str(response)})
        except Exception as exc:  # noqa: BLE001
            messages.append({"chat_id": str(chat_id), "message_id": None, "status": "send_failed", "reason": str(exc)})
    alert_record["telegram_messages"] = messages
    append_jsonl(HEALTH_ALERTS_FILE, alert_record)


def load_state() -> dict[str, Any]:
    return load_json(HEALTH_STATE_FILE) or {"active_issue_keys": []}


def save_state(state: dict[str, Any]) -> None:
    state["updated_at"] = now_iso()
    write_latest_json(HEALTH_STATE_FILE, state)


def run_check(
    config: dict[str, Any],
    notification_last_minutes: int,
    stage_sla_seconds: int,
    heartbeat_stale_seconds: int,
    browser_stale_seconds: int,
    send_alerts: bool,
    send_ok: bool,
) -> dict[str, Any]:
    tz_name = str(config.get("timezone") or "America/Detroit")
    issues: list[Issue] = []
    issues.extend(check_live_watcher(tz_name, heartbeat_stale_seconds))
    notification_issues, notification_summary = check_notification_capture(config, notification_last_minutes, stage_sla_seconds, tz_name)
    browser_issues, browser_summary = check_browser_capture(browser_stale_seconds, stage_sla_seconds, tz_name)
    issues.extend(notification_issues)
    issues.extend(browser_issues)
    issues.extend(check_raw_processing(stage_sla_seconds, tz_name))
    issues.extend(check_routing(stage_sla_seconds, tz_name))
    issues.extend(check_telegram_and_broker(tz_name))
    record = {
        "event_type": "pipeline_health_check",
        "recorded_at": now_iso(tz_name),
        "status": health_status(issues),
        "issue_count": len(issues),
        "issues": [asdict(issue) | {"key": issue.key} for issue in issues],
        "summaries": {
            "notification_capture": notification_summary,
            "browser_capture": browser_summary,
        },
    }
    append_jsonl(HEALTH_CHECKS_FILE, record)
    write_latest_json(HEALTH_LATEST_FILE, record)

    state = load_state()
    old_keys = set(str(item) for item in state.get("active_issue_keys") or [])
    new_keys = {issue.key for issue in issues}
    new_issues = [issue for issue in issues if issue.key not in old_keys]
    recovered = sorted(old_keys - new_keys)
    if send_alerts and (new_issues or recovered or (send_ok and record["status"] == "ok" and not old_keys)):
        send_health_alert(record, new_issues, recovered)
    state["active_issue_keys"] = sorted(new_keys)
    state["last_status"] = record["status"]
    save_state(state)
    return record


def in_market_window(tz_name: str) -> bool:
    now = current_time(tz_name)
    if now.weekday() >= 5:
        return False
    start = now.replace(hour=9, minute=25, second=0, microsecond=0)
    end = now.replace(hour=16, minute=10, second=0, microsecond=0)
    return start <= now <= end


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--config", default=str(CONFIG_DIR / "watcher.yaml"))
    parser.add_argument("--once", action="store_true")
    parser.add_argument("--interval", type=float, default=600.0)
    parser.add_argument("--market-hours-only", action="store_true")
    parser.add_argument("--notification-last-minutes", type=int, default=720)
    parser.add_argument("--stage-sla-seconds", type=int, default=90)
    parser.add_argument("--heartbeat-stale-seconds", type=int, default=120)
    parser.add_argument("--browser-stale-seconds", type=int, default=180)
    parser.add_argument("--no-telegram", action="store_true")
    parser.add_argument("--send-ok", action="store_true")
    args = parser.parse_args()
    config = load_simple_yaml(Path(args.config))
    logger = setup_logging("pipeline_health_monitor", LOG_DIR / "pipeline_health_monitor.log")
    signal.signal(signal.SIGTERM, request_stop)
    signal.signal(signal.SIGINT, request_stop)
    while not STOP:
        if not args.market_hours_only or in_market_window(str(config.get("timezone") or "America/Detroit")):
            record = run_check(
                config,
                notification_last_minutes=args.notification_last_minutes,
                stage_sla_seconds=args.stage_sla_seconds,
                heartbeat_stale_seconds=args.heartbeat_stale_seconds,
                browser_stale_seconds=args.browser_stale_seconds,
                send_alerts=not args.no_telegram,
                send_ok=args.send_ok,
            )
            logger.info("pipeline_health status=%s issue_count=%s", record["status"], record["issue_count"])
            print(json.dumps({"status": record["status"], "issue_count": record["issue_count"]}, sort_keys=True))
        if args.once:
            break
        time.sleep(args.interval)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())

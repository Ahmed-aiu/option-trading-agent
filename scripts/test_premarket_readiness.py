#!/usr/bin/env python3
"""Focused tests for the premarket readiness check."""

from __future__ import annotations

import datetime as dt
import json
import tempfile
from pathlib import Path
from zoneinfo import ZoneInfo

import premarket_readiness


def write_json(path: Path, payload: dict) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(payload, sort_keys=True) + "\n", encoding="utf-8")


def write_text(path: Path, text: str) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(text, encoding="utf-8")


def temp_paths(tmp_path: Path) -> premarket_readiness.ReadinessPaths:
    data_dir = tmp_path / "data"
    config_dir = tmp_path / "config"
    return premarket_readiness.ReadinessPaths(
        data_dir=data_dir,
        heartbeat_file=data_dir / "live_pipeline_heartbeat.json",
        browser_health_file=data_dir / "discord_browser_health_latest.json",
        watcher_config_file=config_dir / "watcher.yaml",
        broker_config_file=config_dir / "broker.yaml",
        env_file=tmp_path / ".env.local",
        reports_file=data_dir / "premarket_readiness_reports.jsonl",
        latest_file=data_dir / "premarket_readiness_latest.json",
    )


def broker_config() -> str:
    return "\n".join(
        [
            "paper_base_url: https://paper-api.alpaca.markets",
            "base_url_env_var: APCA_API_BASE_URL",
            "trading_mode_env_var: OPENCLAW_TRADING_MODE",
            "key_id_env_var: APCA_API_KEY_ID",
            "secret_key_env_var: APCA_API_SECRET_KEY",
            "submit_enabled_env_var: OPENCLAW_ENABLE_PAPER_ORDERS",
            "",
        ]
    )


def env_config(base_url: str = "https://paper-api.alpaca.markets") -> str:
    return "\n".join(
        [
            "STEVE_TRADE_BOT_TOKEN=super-secret-token",
            "STEVE_TRADE_OWNER_CHAT_ID=secret-owner-chat",
            "STEVE_TRADE_OWNER_USER_ID=secret-owner-user",
            f"APCA_API_BASE_URL={base_url}",
            "OPENCLAW_TRADING_MODE=paper",
            "OPENCLAW_ENABLE_PAPER_ORDERS=false",
            "APCA_API_KEY_ID=secret-alpaca-key",
            "APCA_API_SECRET_KEY=secret-alpaca-secret",
            "",
        ]
    )


def read_jsonl(path: Path) -> list[dict]:
    return [json.loads(line) for line in path.read_text(encoding="utf-8").splitlines() if line.strip()]


def test_passing_readiness_writes_temp_reports_without_secrets() -> None:
    now = dt.datetime(2026, 7, 8, 8, 45, tzinfo=ZoneInfo("America/Detroit"))
    sent_messages: list[str] = []

    def fake_sender(message: str) -> tuple[str, str, list[dict]]:
        sent_messages.append(message)
        return "sent", "", [{"chat_id": "secret-owner-chat", "status": "sent"}]

    with tempfile.TemporaryDirectory() as tmp:
        paths = temp_paths(Path(tmp))
        write_text(paths.watcher_config_file, "timezone: America/Detroit\n")
        write_text(paths.broker_config_file, broker_config())
        write_text(paths.env_file, env_config())
        write_json(
            paths.heartbeat_file,
            {
                "event_type": "live_pipeline_heartbeat",
                "recorded_at": (now - dt.timedelta(seconds=30)).isoformat(timespec="seconds"),
            },
        )
        write_json(
            paths.browser_health_file,
            {
                "event_type": "discord_browser_capture_health",
                "recorded_at": (now - dt.timedelta(seconds=20)).isoformat(timespec="seconds"),
                "status": "ok",
                "totals": {"channels": 2, "channels_ok": 2},
            },
        )
        record = premarket_readiness.run_readiness(
            paths=paths,
            max_heartbeat_age_seconds=120,
            max_browser_health_age_seconds=180,
            min_free_gb=0,
            send_telegram=True,
            env={},
            now=now,
            telegram_sender=fake_sender,
        )
        assert record["status"] == "ok"
        assert all(check["status"] == "ok" for check in record["checks"])
        assert record["recommendations"] == []
        assert record["telegram"]["audience"] == "owner_operational"
        assert record["telegram"]["sent_messages"] == 1
        assert sent_messages and sent_messages[0].startswith("PREMARKET READINESS - OK")
        reports = read_jsonl(paths.reports_file)
        latest = json.loads(paths.latest_file.read_text(encoding="utf-8"))
        assert reports == [record]
        assert latest == record
        serialized = json.dumps(record, sort_keys=True)
        for secret in (
            "super-secret-token",
            "secret-owner-chat",
            "secret-owner-user",
            "secret-alpaca-key",
            "secret-alpaca-secret",
        ):
            assert secret not in serialized


def test_failing_readiness_reports_sanitized_failures() -> None:
    now = dt.datetime(2026, 7, 8, 8, 45, tzinfo=ZoneInfo("America/Detroit"))
    with tempfile.TemporaryDirectory() as tmp:
        paths = temp_paths(Path(tmp))
        write_text(paths.broker_config_file, broker_config())
        write_text(paths.env_file, "APCA_API_BASE_URL=https://api.alpaca.markets\n")
        write_json(
            paths.heartbeat_file,
            {
                "event_type": "live_pipeline_heartbeat",
                "recorded_at": (now - dt.timedelta(minutes=15)).isoformat(timespec="seconds"),
            },
        )
        write_json(
            paths.browser_health_file,
            {
                "event_type": "discord_browser_capture_health",
                "recorded_at": (now - dt.timedelta(seconds=10)).isoformat(timespec="seconds"),
                "status": "degraded",
                "errors": ["private channel read failure"],
                "totals": {"channels": 2, "channels_ok": 1},
            },
        )
        record = premarket_readiness.run_readiness(
            paths=paths,
            max_heartbeat_age_seconds=120,
            max_browser_health_age_seconds=180,
            min_free_gb=1_000_000_000,
            send_telegram=False,
            env={},
            now=now,
        )
        assert record["status"] == "failed"
        failed_names = {check["name"] for check in record["checks"] if check["status"] == "failed"}
        assert {
            "live_pipeline_heartbeat",
            "discord_browser_health_latest",
            "disk_free_space",
            "watcher_config",
            "telegram_owner_config",
            "alpaca_paper_config",
        }.issubset(failed_names)
        assert len(record["recommendations"]) >= 5
        assert record["telegram"]["status"] == "not_sent"
        reports = read_jsonl(paths.reports_file)
        latest = json.loads(paths.latest_file.read_text(encoding="utf-8"))
        assert reports == [record]
        assert latest == record
        serialized = json.dumps(record, sort_keys=True)
        assert "https://api.alpaca.markets" not in serialized
        assert "private channel read failure" not in serialized


def main() -> int:
    test_passing_readiness_writes_temp_reports_without_secrets()
    test_failing_readiness_reports_sanitized_failures()
    print("Premarket readiness tests passed")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())

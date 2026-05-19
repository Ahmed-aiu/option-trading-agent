#!/usr/bin/env python3
"""Shared helpers for the local alert capture pipeline."""

from __future__ import annotations

import datetime as dt
import hashlib
import json
import logging
import os
from pathlib import Path
from typing import Any, Iterable
from zoneinfo import ZoneInfo


ROOT = Path(__file__).resolve().parents[1]
CONFIG_DIR = ROOT / "config"
DATA_DIR = ROOT / "data"
LOG_DIR = ROOT / "logs"
TESTS_DIR = ROOT / "tests"
DEFAULT_TZ = "America/Detroit"


def ensure_project_dirs() -> None:
    for path in (CONFIG_DIR, DATA_DIR, LOG_DIR, TESTS_DIR):
        path.mkdir(parents=True, exist_ok=True)


def setup_logging(name: str, log_path: Path) -> logging.Logger:
    ensure_project_dirs()
    logger = logging.getLogger(name)
    logger.setLevel(logging.INFO)
    logger.handlers.clear()
    formatter = logging.Formatter("%(asctime)s %(levelname)s %(name)s %(message)s")
    file_handler = logging.FileHandler(log_path)
    file_handler.setFormatter(formatter)
    stream_handler = logging.StreamHandler()
    stream_handler.setFormatter(formatter)
    logger.addHandler(file_handler)
    logger.addHandler(stream_handler)
    return logger


def parse_scalar(value: str) -> Any:
    value = value.strip()
    if value == "[]":
        return []
    if value == "{}":
        return {}
    lowered = value.lower()
    if lowered == "true":
        return True
    if lowered == "false":
        return False
    if lowered in {"null", "none"}:
        return None
    if (value.startswith('"') and value.endswith('"')) or (
        value.startswith("'") and value.endswith("'")
    ):
        return value[1:-1]
    try:
        if "." in value:
            return float(value)
        return int(value)
    except ValueError:
        return value


def load_simple_yaml(path: Path) -> dict[str, Any]:
    """Load the small YAML subset used by this project without PyYAML."""
    result: dict[str, Any] = {}
    current_key: str | None = None
    for raw_line in path.read_text(encoding="utf-8").splitlines():
        line = raw_line.split("#", 1)[0].rstrip()
        if not line.strip():
            continue
        if line.startswith("  - "):
            if current_key is None:
                raise ValueError(f"List item without key in {path}: {raw_line}")
            result.setdefault(current_key, []).append(parse_scalar(line[4:]))
            continue
        if ":" not in line:
            raise ValueError(f"Unsupported YAML line in {path}: {raw_line}")
        key, value = line.split(":", 1)
        key = key.strip()
        value = value.strip()
        current_key = key
        if value == "":
            result[key] = []
        else:
            result[key] = parse_scalar(value)
            current_key = None
    return result


def now_iso(tz_name: str = DEFAULT_TZ) -> str:
    return dt.datetime.now(ZoneInfo(tz_name)).isoformat(timespec="seconds")


def parse_datetime(value: Any, tz_name: str = DEFAULT_TZ) -> dt.datetime | None:
    if value in (None, ""):
        return None
    tz = ZoneInfo(tz_name)
    if isinstance(value, (int, float)):
        # Notification Center databases have used both Unix and Apple epoch values.
        if value > 1_000_000_000:
            return dt.datetime.fromtimestamp(value, tz)
        return (
            dt.datetime(2001, 1, 1, tzinfo=dt.timezone.utc)
            + dt.timedelta(seconds=float(value))
        ).astimezone(tz)
    if isinstance(value, str):
        candidate = value.strip()
        if not candidate:
            return None
        if candidate.endswith("Z"):
            candidate = candidate[:-1] + "+00:00"
        try:
            parsed = dt.datetime.fromisoformat(candidate)
            if parsed.tzinfo is None:
                parsed = parsed.replace(tzinfo=tz)
            return parsed.astimezone(tz)
        except ValueError:
            return None
    return None


def read_jsonl(path: Path) -> list[dict[str, Any]]:
    if not path.exists():
        return []
    rows: list[dict[str, Any]] = []
    with path.open("r", encoding="utf-8") as handle:
        for line_no, line in enumerate(handle, 1):
            text = line.strip()
            if not text:
                continue
            try:
                value = json.loads(text)
            except json.JSONDecodeError:
                raise ValueError(f"Invalid JSONL in {path} at line {line_no}")
            if isinstance(value, dict):
                rows.append(value)
    return rows


def append_jsonl(path: Path, record: dict[str, Any]) -> None:
    ensure_project_dirs()
    with path.open("a", encoding="utf-8") as handle:
        handle.write(json.dumps(record, sort_keys=True, separators=(",", ":")) + "\n")


def stable_hash(parts: Iterable[Any]) -> str:
    normalized = "|".join("" if part is None else str(part).strip() for part in parts)
    return hashlib.sha256(normalized.encode("utf-8")).hexdigest()


def notification_dedupe_key(record: dict[str, Any]) -> str:
    return stable_hash(
        [
            record.get("bundle_id"),
            record.get("source_app"),
            record.get("notification_timestamp"),
            record.get("title"),
            record.get("subtitle"),
            record.get("body"),
        ]
    )


def load_seen_keys(path: Path, key_name: str = "dedupe_key") -> set[str]:
    seen: set[str] = set()
    for record in read_jsonl(path):
        value = record.get(key_name) or record.get("source_dedupe_key")
        if value:
            seen.add(str(value))
    return seen


def project_path(*parts: str) -> Path:
    return ROOT.joinpath(*parts)


def atomic_touch_jsonl_files() -> None:
    for path in [
        DATA_DIR / "raw_notifications.jsonl",
        DATA_DIR / "parsed_alerts.jsonl",
        DATA_DIR / "rejected_alerts.jsonl",
        DATA_DIR / "trade_decisions.jsonl",
        DATA_DIR / "orders_paper.jsonl",
        DATA_DIR / "shadow_option_positions.jsonl",
        DATA_DIR / "option_quote_snapshots.jsonl",
        DATA_DIR / "steve_option_exits.jsonl",
        DATA_DIR / "steve_approval_cards.jsonl",
        DATA_DIR / "steve_approval_actions.jsonl",
        DATA_DIR / "steve_close_reports.jsonl",
        DATA_DIR / "human_paper_positions.jsonl",
        DATA_DIR / "human_paper_exits.jsonl",
        DATA_DIR / "option_validation_errors.jsonl",
        DATA_DIR / "daily_option_summaries.jsonl",
        DATA_DIR / "live_pipeline_heartbeats.jsonl",
    ]:
        path.parent.mkdir(parents=True, exist_ok=True)
        if not path.exists():
            os.close(os.open(path, os.O_CREAT | os.O_EXCL | os.O_WRONLY, 0o644))

#!/usr/bin/env python3
"""Ledger storage hygiene, scorecards, and archive-first compaction."""

from __future__ import annotations

import argparse
import datetime as dt
import gzip
import json
import shutil
from pathlib import Path
from typing import Any, Callable

from pipeline_common import DATA_DIR, DEFAULT_TZ, append_jsonl, json_record_signature, now_iso, parse_datetime, stable_hash


SYNTHETIC_TEST_SOURCE_PREFIXES = ("full-stock-", "full-option-", "full-add-", "human-test", "shadow-test", "test-")
SYNTHETIC_TEST_VALUES = {
    "closed-source",
    "approval-test",
    "approval-test-2",
    "manual-dm-close-report-test",
    "source-test",
}
HEAVY_SNAPSHOT_KEYS = ("recent_news", "underlying_indicators", "spy_indicators", "qqq_indicators")
CORE_SNAPSHOT_KEYS = (
    "event_type",
    "snapshot_id",
    "recorded_at",
    "source_dedupe_key",
    "validation_id",
    "position_id",
    "ticker",
    "contract_symbol",
    "dte",
    "option_quote",
    "underlying_quote",
    "signal_score",
    "signal_warnings",
    "append_reasons",
    "data_provider",
    "storage_profile",
)
HEALTH_HISTORY_IGNORE_KEYS = ("recorded_at", "loop_seconds", "history_appended")
IMPORTANT_JSONL_FILES = (
    "option_quote_snapshots.jsonl",
    "discord_browser_health.jsonl",
    "live_pipeline_heartbeats.jsonl",
    "shadow_option_positions.jsonl",
    "human_paper_positions.jsonl",
    "raw_notifications.jsonl",
    "parsed_alerts.jsonl",
    "orders_paper.jsonl",
    "broker_order_status_reports.jsonl",
    "human_paper_exits.jsonl",
    "steve_option_exits.jsonl",
)
IMPORTANT_JSON_FILES = ("option_tracking_state.json",)
SYNTHETIC_CLEANUP_JSONL_FILES = (
    "raw_notifications.jsonl",
    "processed_notifications.jsonl",
    "parsed_alerts.jsonl",
    "rejected_alerts.jsonl",
    "trade_decisions.jsonl",
    "orders_paper.jsonl",
    "steve_approval_cards.jsonl",
    "steve_approval_actions.jsonl",
    "steve_auto_buy_reports.jsonl",
    "steve_close_reports.jsonl",
    "steve_broker_order_reports.jsonl",
    "broker_order_status_reports.jsonl",
    "human_paper_exits.jsonl",
    "daily_option_summaries.jsonl",
    "daily_pl_reports.jsonl",
    "steve_alert_pl_reports.jsonl",
    "broker_fill_pl_reports.jsonl",
)


def source_value_is_synthetic(value: Any) -> bool:
    text = str(value or "")
    return text in SYNTHETIC_TEST_VALUES or any(text.startswith(prefix) for prefix in SYNTHETIC_TEST_SOURCE_PREFIXES)


def record_is_synthetic_test_artifact(row: dict[str, Any]) -> bool:
    for key in ("source_dedupe_key", "dedupe_key", "raw_record_key", "approval_id", "position_id", "exit_id"):
        if source_value_is_synthetic(row.get(key)):
            return True
    for key in ("raw_record_keys", "source_dedupe_keys"):
        values = row.get(key) or []
        if isinstance(values, list) and any(source_value_is_synthetic(value) for value in values):
            return True
    return False


def using_project_runtime_path(path: Path) -> bool:
    try:
        return path.resolve().is_relative_to(DATA_DIR.resolve())
    except AttributeError:
        resolved = path.resolve()
        root = DATA_DIR.resolve()
        return root == resolved or root in resolved.parents


def expiration_date(row: dict[str, Any]) -> dt.date | None:
    value = row.get("expiration_date")
    if not value:
        return None
    try:
        return dt.date.fromisoformat(str(value)[:10])
    except ValueError:
        return None


def option_contract_expired(row: dict[str, Any], *, at: Any = None, tz_name: str = DEFAULT_TZ) -> bool:
    expiration = expiration_date(row)
    if expiration is None:
        return False
    if isinstance(at, dt.datetime):
        reference = at
    else:
        reference = parse_datetime(at, tz_name) if at is not None else parse_datetime(now_iso(tz_name), tz_name)
    if reference is None:
        return False
    return expiration < reference.date()


def compact_market_snapshot(row: dict[str, Any], *, profile: str = "tracking_core_v1") -> dict[str, Any]:
    record = {key: row.get(key) for key in CORE_SNAPSHOT_KEYS if key in row}
    record["storage_profile"] = profile
    return record


def quote_snapshot_signature(row: dict[str, Any]) -> str:
    quote = row.get("option_quote") or {}
    day = str(row.get("recorded_at") or "")[:10]
    return stable_hash(
        [
            day,
            row.get("position_id"),
            row.get("source_dedupe_key"),
            row.get("contract_symbol") or quote.get("symbol"),
            quote.get("status"),
            quote.get("bid"),
            quote.get("ask"),
            quote.get("mark"),
            quote.get("last"),
            quote.get("timestamp"),
        ]
    )


def health_history_signature(row: dict[str, Any]) -> str:
    return json_record_signature(row, HEALTH_HISTORY_IGNORE_KEYS)


def heartbeat_is_interesting(row: dict[str, Any]) -> bool:
    if int(row.get("capture_written") or 0) > 0 or int(row.get("duplicate_notifications") or 0) > 0:
        return True
    pipeline = row.get("pipeline") or {}
    if any(int(pipeline.get(key) or 0) > 0 for key in ("raw_new", "parsed", "decisions", "option_validation_errors")):
        return True
    telegram = row.get("telegram") or {}
    if any(int(telegram.get(key) or 0) > 0 for key in ("actions", "messages")):
        return True
    option_tracking = row.get("option_tracking") or {}
    if int(option_tracking.get("human_exits") or 0) > 0:
        return True
    broker_orders = row.get("broker_orders") or {}
    if int(broker_orders.get("reported") or 0) > 0:
        return True
    daily_pl = row.get("daily_pl") or {}
    return bool(daily_pl.get("sent"))


def browser_health_is_interesting(row: dict[str, Any]) -> bool:
    if row.get("status") not in (None, "", "ok"):
        return True
    if row.get("errors"):
        return True
    totals = row.get("totals") or {}
    return any(int(totals.get(key) or 0) > 0 for key in ("messages_new", "raw_backfilled", "raw_processed"))


def file_stats(path: Path) -> dict[str, Any]:
    rows = 0
    if path.exists():
        with path.open("r", encoding="utf-8") as handle:
            for line in handle:
                if line.strip():
                    rows += 1
    return {"path": str(path), "bytes": path.stat().st_size if path.exists() else 0, "rows": rows}


def tracking_state_scorecard(path: Path) -> dict[str, Any]:
    stats = {"path": str(path), "bytes": path.stat().st_size if path.exists() else 0, "positions": 0, "observations": 0, "storage_findings": []}
    if not path.exists():
        stats["storage_findings"].append("tracking_state_missing")
        return stats
    try:
        state = json.loads(path.read_text(encoding="utf-8"))
    except json.JSONDecodeError:
        stats["storage_findings"].append("tracking_state_invalid_json")
        return stats
    positions = state.get("positions") if isinstance(state, dict) else {}
    if not isinstance(positions, dict):
        stats["storage_findings"].append("tracking_state_positions_invalid")
        return stats
    stats["positions"] = len(positions)
    stats["observations"] = sum(int((row or {}).get("observation_count") or 0) for row in positions.values() if isinstance(row, dict))
    skipped = sum(int((row or {}).get("skipped_uninteresting_count") or 0) for row in positions.values() if isinstance(row, dict))
    stats["skipped_uninteresting_quotes"] = skipped
    return stats


def option_snapshot_scorecard(path: Path) -> dict[str, Any]:
    stats = file_stats(path)
    synthetic_rows = 0
    unavailable_rows = 0
    heavy_key_bytes = {key: 0 for key in HEAVY_SNAPSHOT_KEYS}
    signatures: set[str] = set()
    duplicate_rows = 0
    if path.exists():
        with path.open("r", encoding="utf-8") as handle:
            for line in handle:
                text = line.strip()
                if not text:
                    continue
                try:
                    row = json.loads(text)
                except json.JSONDecodeError:
                    continue
                if record_is_synthetic_test_artifact(row):
                    synthetic_rows += 1
                quote = row.get("option_quote") or {}
                if quote.get("status") and quote.get("status") != "ok":
                    unavailable_rows += 1
                signature = quote_snapshot_signature(row)
                if signature in signatures:
                    duplicate_rows += 1
                else:
                    signatures.add(signature)
                for key in HEAVY_SNAPSHOT_KEYS:
                    if key in row:
                        heavy_key_bytes[key] += len(json.dumps(row.get(key), sort_keys=True, separators=(",", ":")))
    stats.update(
        {
            "synthetic_rows": synthetic_rows,
            "duplicate_quote_rows_estimate": duplicate_rows,
            "unavailable_quote_rows": unavailable_rows,
            "heavy_key_bytes": heavy_key_bytes,
            "storage_findings": [],
        }
    )
    if synthetic_rows:
        stats["storage_findings"].append("synthetic_test_artifacts_present")
    if duplicate_rows:
        stats["storage_findings"].append("duplicate_quote_snapshots_present")
    if sum(heavy_key_bytes.values()) > stats["bytes"] * 0.25:
        stats["storage_findings"].append("repeated_heavy_market_context_present")
    return stats


def duplicate_scorecard(path: Path, *, kind: str) -> dict[str, Any]:
    stats = file_stats(path)
    signatures: set[str] = set()
    duplicates = 0
    interesting = 0
    if path.exists():
        with path.open("r", encoding="utf-8") as handle:
            for line in handle:
                text = line.strip()
                if not text:
                    continue
                try:
                    row = json.loads(text)
                except json.JSONDecodeError:
                    continue
                signature = health_history_signature(row)
                if signature in signatures:
                    duplicates += 1
                else:
                    signatures.add(signature)
                if kind == "browser_health" and browser_health_is_interesting(row):
                    interesting += 1
                if kind == "heartbeat" and heartbeat_is_interesting(row):
                    interesting += 1
    stats.update({"duplicate_rows_estimate": duplicates, "interesting_rows": interesting, "storage_findings": []})
    if duplicates:
        stats["storage_findings"].append("unchanged_status_history_present")
    return stats


def data_hygiene_scorecard(data_dir: Path = DATA_DIR) -> dict[str, Any]:
    files: dict[str, Any] = {}
    for name in IMPORTANT_JSONL_FILES:
        path = data_dir / name
        if name == "option_quote_snapshots.jsonl":
            files[name] = option_snapshot_scorecard(path)
        elif name == "discord_browser_health.jsonl":
            files[name] = duplicate_scorecard(path, kind="browser_health")
        elif name == "live_pipeline_heartbeats.jsonl":
            files[name] = duplicate_scorecard(path, kind="heartbeat")
        else:
            files[name] = file_stats(path)
    for name in IMPORTANT_JSON_FILES:
        files[name] = tracking_state_scorecard(data_dir / name)
    total_bytes = sum(int(item.get("bytes") or 0) for item in files.values())
    recommendations: list[str] = []
    snapshot = files.get("option_quote_snapshots.jsonl") or {}
    if int(snapshot.get("synthetic_rows") or 0) > 0:
        recommendations.append("quarantine_synthetic_test_artifacts")
    if int(snapshot.get("duplicate_quote_rows_estimate") or 0) > 0:
        recommendations.append("compact_duplicate_quote_snapshots")
    if "repeated_heavy_market_context_present" in (snapshot.get("storage_findings") or []):
        recommendations.append("store_repeated_news_and_indicators_outside_tracking_snapshots")
    for name in ("discord_browser_health.jsonl", "live_pipeline_heartbeats.jsonl"):
        if int((files.get(name) or {}).get("duplicate_rows_estimate") or 0) > 0:
            recommendations.append(f"append_{name.replace('.jsonl', '')}_history_only_on_change")
    tracking_state = files.get("option_tracking_state.json") or {}
    if "tracking_state_missing" in (tracking_state.get("storage_findings") or []):
        recommendations.append("enable_latest_option_tracking_state")
    return {
        "event_type": "data_hygiene_scorecard",
        "generated_at": now_iso(),
        "total_tracked_bytes": total_bytes,
        "files": files,
        "recommendations": recommendations,
    }


def archive_original(path: Path, archive_dir: Path) -> Path | None:
    if not path.exists():
        return None
    archive_dir.mkdir(parents=True, exist_ok=True)
    archive_path = archive_dir / f"{path.name}.gz"
    with path.open("rb") as source, gzip.open(archive_path, "wb") as target:
        shutil.copyfileobj(source, target)
    return archive_path


def write_jsonl(path: Path, rows: list[dict[str, Any]]) -> None:
    tmp_path = path.with_suffix(path.suffix + ".tmp")
    with tmp_path.open("w", encoding="utf-8") as handle:
        for row in rows:
            handle.write(json.dumps(row, sort_keys=True, separators=(",", ":")) + "\n")
    tmp_path.replace(path)


def compact_option_snapshots(path: Path) -> tuple[list[dict[str, Any]], dict[str, int]]:
    by_signature: dict[str, dict[str, Any]] = {}
    order: list[str] = []
    stats = {"input_rows": 0, "output_rows": 0, "synthetic_rows": 0, "duplicate_rows": 0}
    if path.exists():
        with path.open("r", encoding="utf-8") as handle:
            for line in handle:
                text = line.strip()
                if not text:
                    continue
                stats["input_rows"] += 1
                try:
                    row = json.loads(text)
                except json.JSONDecodeError:
                    continue
                if record_is_synthetic_test_artifact(row):
                    stats["synthetic_rows"] += 1
                    continue
                compacted = compact_market_snapshot(row)
                signature = quote_snapshot_signature(compacted)
                existing = by_signature.get(signature)
                if existing is None:
                    compacted["compacted_first_recorded_at"] = compacted.get("recorded_at")
                    compacted["compacted_last_recorded_at"] = compacted.get("recorded_at")
                    compacted["compacted_duplicate_count"] = 1
                    by_signature[signature] = compacted
                    order.append(signature)
                else:
                    stats["duplicate_rows"] += 1
                    existing["recorded_at"] = compacted.get("recorded_at") or existing.get("recorded_at")
                    existing["compacted_last_recorded_at"] = compacted.get("recorded_at") or existing.get("compacted_last_recorded_at")
                    existing["compacted_duplicate_count"] = int(existing.get("compacted_duplicate_count") or 1) + 1
    rows = [by_signature[key] for key in order]
    stats["output_rows"] = len(rows)
    return rows, stats


def compact_change_history(path: Path, *, interesting_fn: Callable[[dict[str, Any]], bool]) -> tuple[list[dict[str, Any]], dict[str, int]]:
    by_signature: dict[str, dict[str, Any]] = {}
    order: list[str] = []
    stats = {"input_rows": 0, "output_rows": 0, "duplicate_rows": 0}
    if path.exists():
        with path.open("r", encoding="utf-8") as handle:
            for line in handle:
                text = line.strip()
                if not text:
                    continue
                stats["input_rows"] += 1
                try:
                    row = json.loads(text)
                except json.JSONDecodeError:
                    continue
                day = str(row.get("recorded_at") or "")[:10]
                interesting = interesting_fn(row)
                signature = stable_hash([day, interesting, health_history_signature(row)])
                existing = by_signature.get(signature)
                if existing is None:
                    compacted = dict(row)
                    compacted["compacted_first_recorded_at"] = row.get("recorded_at")
                    compacted["compacted_last_recorded_at"] = row.get("recorded_at")
                    compacted["compacted_duplicate_count"] = 1
                    by_signature[signature] = compacted
                    order.append(signature)
                else:
                    stats["duplicate_rows"] += 1
                    existing["recorded_at"] = row.get("recorded_at") or existing.get("recorded_at")
                    existing["compacted_last_recorded_at"] = row.get("recorded_at") or existing.get("compacted_last_recorded_at")
                    existing["compacted_duplicate_count"] = int(existing.get("compacted_duplicate_count") or 1) + 1
    rows = [by_signature[key] for key in order]
    stats["output_rows"] = len(rows)
    return rows, stats


def compact_without_synthetic(path: Path) -> tuple[list[dict[str, Any]], dict[str, int]]:
    rows: list[dict[str, Any]] = []
    stats = {"input_rows": 0, "output_rows": 0, "synthetic_rows": 0}
    if path.exists():
        with path.open("r", encoding="utf-8") as handle:
            for line in handle:
                text = line.strip()
                if not text:
                    continue
                stats["input_rows"] += 1
                try:
                    row = json.loads(text)
                except json.JSONDecodeError:
                    continue
                if record_is_synthetic_test_artifact(row):
                    stats["synthetic_rows"] += 1
                    continue
                rows.append(row)
    stats["output_rows"] = len(rows)
    return rows, stats


def compact_runtime_ledgers(data_dir: Path = DATA_DIR, *, apply: bool = False, min_saved_bytes: int = 1) -> dict[str, Any]:
    archive_dir = data_dir / "archive" / f"data_hygiene_{dt.datetime.now().strftime('%Y%m%d-%H%M%S')}"
    jobs: dict[str, Callable[[Path], tuple[list[dict[str, Any]], dict[str, int]]]] = {
        "option_quote_snapshots.jsonl": compact_option_snapshots,
        "discord_browser_health.jsonl": lambda path: compact_change_history(path, interesting_fn=browser_health_is_interesting),
        "live_pipeline_heartbeats.jsonl": lambda path: compact_change_history(path, interesting_fn=heartbeat_is_interesting),
        "shadow_option_positions.jsonl": compact_without_synthetic,
        "human_paper_positions.jsonl": compact_without_synthetic,
    }
    for name in SYNTHETIC_CLEANUP_JSONL_FILES:
        jobs.setdefault(name, compact_without_synthetic)
    results: dict[str, Any] = {
        "event_type": "data_hygiene_compaction",
        "generated_at": now_iso(),
        "applied": apply,
        "archive_dir": str(archive_dir),
        "files": {},
    }
    for name, compact_fn in jobs.items():
        path = data_dir / name
        before_bytes = path.stat().st_size if path.exists() else 0
        rows, stats = compact_fn(path)
        after_bytes = sum(len(json.dumps(row, sort_keys=True, separators=(",", ":"))) + 1 for row in rows)
        archive_path = None
        should_apply = apply and path.exists() and max(0, before_bytes - after_bytes) >= min_saved_bytes
        if should_apply:
            archive_path = archive_original(path, archive_dir)
            write_jsonl(path, rows)
        results["files"][name] = {
            **stats,
            "before_bytes": before_bytes,
            "estimated_after_bytes": after_bytes,
            "saved_bytes": max(0, before_bytes - after_bytes),
            "applied": should_apply,
            "archive_path": str(archive_path) if archive_path else "",
        }
    if apply:
        append_jsonl(data_dir / "data_hygiene_reports.jsonl", results)
    return results


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    sub = parser.add_subparsers(dest="command", required=True)
    scorecard = sub.add_parser("scorecard")
    scorecard.add_argument("--print-json", action="store_true")
    compact = sub.add_parser("compact")
    compact.add_argument("--apply", action="store_true")
    compact.add_argument("--min-saved-bytes", type=int, default=1)
    compact.add_argument("--print-json", action="store_true")
    args = parser.parse_args()
    if args.command == "scorecard":
        result = data_hygiene_scorecard()
    else:
        result = compact_runtime_ledgers(apply=bool(args.apply), min_saved_bytes=int(args.min_saved_bytes))
    print(json.dumps(result, sort_keys=True) if getattr(args, "print_json", False) else json.dumps(result, indent=2, sort_keys=True))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())

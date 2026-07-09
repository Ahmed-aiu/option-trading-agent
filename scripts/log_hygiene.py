#!/usr/bin/env python3
"""Archive-first hygiene for local log files."""

from __future__ import annotations

import argparse
import datetime as dt
import gzip
import json
import os
import shutil
import time
from pathlib import Path
from typing import Any

from pipeline_common import DATA_DIR, DEFAULT_TZ, LOG_DIR, now_iso


DEFAULT_MIN_SIZE_BYTES = 10 * 1024 * 1024
REPORTS_FILE = "log_hygiene_reports.jsonl"
LATEST_FILE = "log_hygiene_latest.json"


def non_negative_int(value: str) -> int:
    parsed = int(value)
    if parsed < 0:
        raise argparse.ArgumentTypeError("value must be non-negative")
    return parsed


def path_is_within(path: Path, parent: Path) -> bool:
    try:
        path.resolve().relative_to(parent.resolve())
        return True
    except ValueError:
        return False


def json_bytes(record: dict[str, Any]) -> str:
    return json.dumps(record, sort_keys=True, separators=(",", ":"))


def append_jsonl(path: Path, record: dict[str, Any]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    with path.open("a", encoding="utf-8") as handle:
        handle.write(json_bytes(record) + "\n")


def atomic_write_json(path: Path, record: dict[str, Any]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    tmp_path = path.with_name(f".{path.name}.{os.getpid()}.{time.time_ns()}.tmp")
    try:
        tmp_path.write_text(json.dumps(record, indent=2, sort_keys=True) + "\n", encoding="utf-8")
        tmp_path.replace(path)
    finally:
        if tmp_path.exists():
            tmp_path.unlink()


def archive_run_dir(base_archive_dir: Path, generated_at: str) -> Path:
    try:
        timestamp = dt.datetime.fromisoformat(generated_at).strftime("%Y%m%d-%H%M%S")
    except ValueError:
        timestamp = dt.datetime.now().strftime("%Y%m%d-%H%M%S")
    return base_archive_dir / f"log_hygiene_{timestamp}"


def archive_path_for(path: Path, logs_dir: Path, run_archive_dir: Path) -> Path:
    relative = path.relative_to(logs_dir)
    return run_archive_dir / relative.parent / f"{relative.name}.gz"


def iter_active_log_files(logs_dir: Path, archive_dir: Path) -> list[Path]:
    if not logs_dir.exists():
        return []
    files: list[Path] = []
    for path in logs_dir.rglob("*"):
        if path_is_within(path, archive_dir):
            continue
        if not path.is_file() or path.is_symlink():
            continue
        files.append(path)
    return sorted(files, key=lambda item: str(item.relative_to(logs_dir)))


def gzip_archive(source_path: Path, archive_path: Path) -> None:
    archive_path.parent.mkdir(parents=True, exist_ok=True)
    with source_path.open("rb") as source, gzip.open(archive_path, "wb") as target:
        shutil.copyfileobj(source, target)


def truncate_in_place(path: Path) -> None:
    with path.open("r+b") as handle:
        handle.truncate(0)


def file_score(
    path: Path,
    *,
    logs_dir: Path,
    run_archive_dir: Path,
    min_size_bytes: int,
    apply: bool,
) -> dict[str, Any]:
    before_bytes = path.stat().st_size
    eligible = before_bytes >= min_size_bytes
    archive_path = archive_path_for(path, logs_dir, run_archive_dir) if eligible else None
    record: dict[str, Any] = {
        "path": str(path),
        "relative_path": str(path.relative_to(logs_dir)),
        "before_bytes": before_bytes,
        "estimated_after_bytes": 0 if eligible else before_bytes,
        "saved_bytes": before_bytes if eligible else 0,
        "min_size_bytes": min_size_bytes,
        "eligible": eligible,
        "applied": False,
        "archive_path": str(archive_path) if archive_path else "",
        "status": "would_rotate" if eligible else "below_threshold",
    }
    if not apply or not eligible:
        return record

    try:
        gzip_archive(path, archive_path)
        truncate_in_place(path)
    except OSError as exc:
        record["status"] = "error"
        record["error"] = str(exc)
        record["saved_bytes"] = 0
        return record

    after_bytes = path.stat().st_size
    record.update(
        {
            "estimated_after_bytes": after_bytes,
            "saved_bytes": max(0, before_bytes - after_bytes),
            "applied": True,
            "status": "rotated",
        }
    )
    return record


def recommendations_for(result: dict[str, Any]) -> list[str]:
    files = result.get("files") or []
    if "logs_dir_missing" in result.get("findings", []):
        return ["logs_dir_missing"]
    if "logs_dir_not_directory" in result.get("findings", []):
        return ["logs_dir_not_directory"]
    errors = [item for item in files if item.get("status") == "error"]
    if errors:
        return ["review_log_hygiene_errors"]
    candidates = [item for item in files if item.get("eligible")]
    if not candidates:
        return ["no_logs_exceed_min_size"]
    if not result.get("applied"):
        return ["run_with_apply_to_archive_and_truncate_large_logs"]
    return ["inspect_archives_before_removing_old_log_history"]


def run_log_hygiene(
    *,
    logs_dir: Path = LOG_DIR,
    archive_dir: Path | None = None,
    data_dir: Path = DATA_DIR,
    min_size_bytes: int = DEFAULT_MIN_SIZE_BYTES,
    apply: bool = False,
    write_reports: bool = True,
) -> dict[str, Any]:
    logs_dir = logs_dir.expanduser()
    archive_base_dir = (archive_dir or (logs_dir / "archive")).expanduser()
    generated_at = now_iso(DEFAULT_TZ)
    run_archive_dir = archive_run_dir(archive_base_dir, generated_at)
    findings: list[str] = []
    files: list[dict[str, Any]] = []

    if not logs_dir.exists():
        findings.append("logs_dir_missing")
    elif not logs_dir.is_dir():
        findings.append("logs_dir_not_directory")
    else:
        for path in iter_active_log_files(logs_dir, archive_base_dir):
            files.append(
                file_score(
                    path,
                    logs_dir=logs_dir,
                    run_archive_dir=run_archive_dir,
                    min_size_bytes=min_size_bytes,
                    apply=apply,
                )
            )

    result: dict[str, Any] = {
        "event_type": "log_hygiene_report",
        "generated_at": generated_at,
        "applied": apply,
        "logs_dir": str(logs_dir),
        "archive_dir": str(run_archive_dir),
        "min_size_bytes": min_size_bytes,
        "files": files,
        "total_saved_bytes": sum(int(item.get("saved_bytes") or 0) for item in files),
        "findings": findings,
        "recommendations": [],
    }
    result["recommendations"] = recommendations_for(result)

    if write_reports:
        append_jsonl(data_dir / REPORTS_FILE, result)
        atomic_write_json(data_dir / LATEST_FILE, result)
    return result


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--logs-dir", type=Path, default=LOG_DIR)
    parser.add_argument("--archive-dir", type=Path, default=None)
    parser.add_argument("--data-dir", type=Path, default=DATA_DIR)
    parser.add_argument("--min-size-bytes", type=non_negative_int, default=DEFAULT_MIN_SIZE_BYTES)
    parser.add_argument("--apply", action="store_true", help="Archive and truncate eligible logs.")
    parser.add_argument("--print-json", action="store_true")
    args = parser.parse_args()

    result = run_log_hygiene(
        logs_dir=args.logs_dir,
        archive_dir=args.archive_dir,
        data_dir=args.data_dir,
        min_size_bytes=args.min_size_bytes,
        apply=bool(args.apply),
    )
    if args.print_json:
        print(json_bytes(result))
    else:
        print(json.dumps(result, indent=2, sort_keys=True))
    return 1 if any(item.get("status") == "error" for item in result.get("files", [])) else 0


if __name__ == "__main__":
    raise SystemExit(main())

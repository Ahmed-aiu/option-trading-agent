#!/usr/bin/env python3
"""Focused tests for archive-first log hygiene."""

from __future__ import annotations

import gzip
import json
import tempfile
from pathlib import Path

import log_hygiene


def read_jsonl(path: Path) -> list[dict]:
    return [json.loads(line) for line in path.read_text(encoding="utf-8").splitlines() if line.strip()]


def by_relative_path(result: dict) -> dict[str, dict]:
    return {item["relative_path"]: item for item in result["files"]}


def test_dry_run_scores_without_rotating() -> None:
    with tempfile.TemporaryDirectory() as tmp:
        root = Path(tmp)
        logs_dir = root / "logs"
        data_dir = root / "data"
        archive_dir = root / "archive"
        logs_dir.mkdir()
        large_content = "large-log-line\n" * 5
        small_content = "tiny\n"
        large_log = logs_dir / "live_pipeline.log"
        small_log = logs_dir / "parser.log"
        large_log.write_text(large_content, encoding="utf-8")
        small_log.write_text(small_content, encoding="utf-8")

        result = log_hygiene.run_log_hygiene(
            logs_dir=logs_dir,
            archive_dir=archive_dir,
            data_dir=data_dir,
            min_size_bytes=20,
            apply=False,
        )

        files = by_relative_path(result)
        assert result["event_type"] == "log_hygiene_report"
        assert result["applied"] is False
        assert files["live_pipeline.log"]["eligible"] is True
        assert files["live_pipeline.log"]["status"] == "would_rotate"
        assert files["parser.log"]["eligible"] is False
        assert large_log.read_text(encoding="utf-8") == large_content
        assert small_log.read_text(encoding="utf-8") == small_content
        assert not archive_dir.exists()
        assert result["total_saved_bytes"] == len(large_content.encode("utf-8"))
        assert result["recommendations"] == ["run_with_apply_to_archive_and_truncate_large_logs"]
        assert len(read_jsonl(data_dir / log_hygiene.REPORTS_FILE)) == 1
        latest = json.loads((data_dir / log_hygiene.LATEST_FILE).read_text(encoding="utf-8"))
        assert latest == result


def test_apply_archives_then_truncates_active_log() -> None:
    with tempfile.TemporaryDirectory() as tmp:
        root = Path(tmp)
        logs_dir = root / "logs"
        data_dir = root / "data"
        archive_dir = root / "archive"
        nested_dir = logs_dir / "workers"
        nested_dir.mkdir(parents=True)
        large_content = "worker-log-line\n" * 5
        small_content = "small\n"
        large_log = nested_dir / "discord.log"
        small_log = logs_dir / "watcher.log"
        large_log.write_text(large_content, encoding="utf-8")
        small_log.write_text(small_content, encoding="utf-8")

        result = log_hygiene.run_log_hygiene(
            logs_dir=logs_dir,
            archive_dir=archive_dir,
            data_dir=data_dir,
            min_size_bytes=20,
            apply=True,
        )

        files = by_relative_path(result)
        rotated = files["workers/discord.log"]
        skipped = files["watcher.log"]
        archive_path = Path(rotated["archive_path"])
        assert result["applied"] is True
        assert rotated["status"] == "rotated"
        assert rotated["applied"] is True
        assert skipped["status"] == "below_threshold"
        assert large_log.exists()
        assert large_log.read_text(encoding="utf-8") == ""
        assert small_log.read_text(encoding="utf-8") == small_content
        assert archive_path.exists()
        assert archive_path.name == "discord.log.gz"
        assert archive_path.parent.name == "workers"
        assert archive_path.parent.parent.name.startswith("log_hygiene_")
        assert str(archive_path.parent.parent) == result["archive_dir"]
        with gzip.open(archive_path, "rt", encoding="utf-8") as handle:
            assert handle.read() == large_content
        assert result["total_saved_bytes"] == len(large_content.encode("utf-8"))
        assert result["recommendations"] == ["inspect_archives_before_removing_old_log_history"]
        reports = read_jsonl(data_dir / log_hygiene.REPORTS_FILE)
        assert len(reports) == 1
        assert reports[0] == result


def main() -> int:
    test_dry_run_scores_without_rotating()
    test_apply_archives_then_truncates_active_log()
    print("log hygiene tests passed")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())

"""持久化一致性、崩溃恢复与重放幂等审计。

该模块只读生产数据。它验证能够由现有证据证明的事实：raw 与 manifest
计数、归档文件与覆盖登记、SQLite 完整性及跨表关系、heatmap 主文件与
当前 generation 物理块的一致性。没有内容散列或采集端确认序号的链路，
只会标为 ``unproven``，不会把“文件可读”冒充为端到端零丢失。

``probe_recovery`` 只在调用方指定的临时目录中构造故障，不接触生产数据。
"""
from __future__ import annotations

import argparse
import gzip
import zipfile
import hashlib
import json
import os
import sqlite3
import sys
import tempfile
import zlib
from collections import Counter
from collections.abc import Iterator, Mapping, Sequence
from dataclasses import asdict, dataclass, field
from datetime import UTC, datetime
from pathlib import Path
from typing import Literal

from guvolu.data.heatmap_tiles import (
    TILE_PERSISTENCE_VERSION,
)
from guvolu.data.raw_writer import RAW_DURABILITY_VERSION
from guvolu.data.paths import data_root
from guvolu.data.store import DB_FILE_NAME
from guvolu.venues import archive

AuditMode = Literal["quick", "full"]
Severity = Literal["error", "warning", "unproven"]


@dataclass(frozen=True, slots=True)
class AuditIssue:
    """单项审计发现。"""

    severity: Severity
    code: str
    detail: str
    path: str | None = None


@dataclass(slots=True)
class AuditReport:
    """机器可读审计结果。"""

    mode: AuditMode
    started_at: str
    finished_at: str | None = None
    files_checked: int = 0
    bytes_checked: int = 0
    counters: dict[str, int] = field(default_factory=dict)
    issues: list[AuditIssue] = field(default_factory=list)

    @property
    def loss_detected(self) -> bool:
        """是否发现确定的不一致或损坏。"""
        return any(issue.severity == "error" for issue in self.issues)

    @property
    def fully_proven(self) -> bool:
        """是否无错误且不存在证据盲区。"""
        return not self.loss_detected and not any(
            issue.severity == "unproven" for issue in self.issues
        )

    def add(
        self,
        severity: Severity,
        code: str,
        detail: str,
        path: Path | None = None,
    ) -> None:
        """追加规范化发现。"""
        self.issues.append(
            AuditIssue(
                severity,
                code,
                detail,
                None if path is None else path.as_posix(),
            )
        )

    def count(self, key: str, amount: int = 1) -> None:
        """累加审计计数。"""
        self.counters[key] = self.counters.get(key, 0) + amount

    def as_dict(self) -> dict[str, object]:
        """返回稳定 JSON 结构。"""
        return {
            "mode": self.mode,
            "started_at": self.started_at,
            "finished_at": self.finished_at,
            "files_checked": self.files_checked,
            "bytes_checked": self.bytes_checked,
            "loss_detected": self.loss_detected,
            "fully_proven": self.fully_proven,
            "counters": dict(sorted(self.counters.items())),
            "issues": [asdict(issue) for issue in self.issues],
        }


def _now_iso() -> str:
    return datetime.now(UTC).isoformat()


def _check_file(report: AuditReport, path: Path) -> None:
    report.files_checked += 1
    try:
        report.bytes_checked += path.stat().st_size
    except OSError:
        pass


def audit_raw(data_root: Path, report: AuditReport) -> None:
    """核对 raw JSONL 与已完成 manifest 的逐运行计数。"""
    root = data_root / "raw"
    if not root.exists():
        report.add("warning", "raw_root_missing", "raw 目录不存在", root)
        return
    counts: Counter[tuple[str, str]] = Counter()
    seen_runs: set[str] = set()
    legacy_records = 0
    for path in sorted(root.rglob("*.jsonl")):
        _check_file(report, path)
        report.count("raw_files")
        relative = path.relative_to(root)
        if len(relative.parts) < 2:
            report.add(
                "error", "raw_path_invalid", "raw 文件缺日期目录", path
            )
            continue
        name = Path(*relative.parts[1:]).as_posix().removesuffix(".jsonl")
        try:
            if report.mode == "quick":
                with path.open("rb") as handle:
                    first = handle.readline()
                    if first and not first.endswith(b"\n"):
                        raise ValueError("首行缺换行，疑似截断")
                    if first:
                        loaded = json.loads(first)
                        if not isinstance(loaded, Mapping):
                            raise ValueError("首行非 JSON 对象")
                continue
            with path.open("rb") as handle:
                for number, raw_line in enumerate(handle, 1):
                    if not raw_line.strip():
                        continue
                    try:
                        if not raw_line.endswith(b"\n"):
                            raise ValueError("行缺换行，疑似截断")
                        record = json.loads(raw_line)
                        if not isinstance(record, Mapping):
                            raise ValueError("行非 JSON 对象")
                        run_id = record.get("run_id")
                        if not isinstance(run_id, str) or not run_id:
                            raise ValueError("raw 行缺 run_id")
                        if record.get("schema_version") != 1:
                            raise ValueError("raw 行 schema_version 非 1")
                        if (
                            record.get("durability_version")
                            != RAW_DURABILITY_VERSION
                        ):
                            legacy_records += 1
                        counts[(run_id, name)] += 1
                        seen_runs.add(run_id)
                        report.count("raw_records")
                    except (
                        UnicodeError,
                        json.JSONDecodeError,
                        ValueError,
                    ) as exc:
                        report.add(
                            "error",
                            "raw_corrupt",
                            f"第 {number} 行: {exc}",
                            path,
                        )
        except OSError as exc:
            report.add("error", "raw_corrupt", str(exc), path)
    manifests: set[str] = set()
    checkpoints: set[str] = set()
    legacy_manifests = 0
    for path in sorted(
        [*root.rglob("manifest-*.json"), *root.rglob("checkpoint-*.json")]
    ):
        _check_file(report, path)
        report.count("raw_manifests")
        try:
            loaded = json.loads(path.read_text(encoding="utf-8"))
            if not isinstance(loaded, Mapping):
                raise ValueError("manifest 非 JSON 对象")
            run_id = loaded.get("run_id")
            expected = loaded.get("record_counts")
            if not isinstance(run_id, str) or not isinstance(expected, Mapping):
                raise ValueError("manifest 缺 run_id/record_counts")
            if loaded.get("status") == "open" or loaded.get("heartbeat") is True:
                checkpoints.add(run_id)
                report.count("raw_checkpoints")
                continue
            manifests.add(run_id)
            if loaded.get("durability_version") != RAW_DURABILITY_VERSION:
                legacy_manifests += 1
            if report.mode == "full":
                for name, value in expected.items():
                    wanted = int(str(value))
                    key = str(name)
                    actual = counts[(run_id, key)]
                    relative_manifest = path.relative_to(root)
                    prefix = Path(*relative_manifest.parts[1:-1]).as_posix()
                    if actual == 0 and prefix and prefix != ".":
                        actual = counts[(run_id, f"{prefix}/{key}")]
                    if wanted > actual:
                        report.add(
                            "error",
                            "raw_manifest_count_shortfall",
                            f"{run_id}/{name}: manifest={wanted}, raw={actual}",
                            path,
                        )
                    elif wanted < actual:
                        report.add(
                            "warning",
                            "raw_manifest_extra_records",
                            (
                                f"{run_id}/{name}: manifest={wanted}, raw={actual}; "
                                "manifest 后仍有追加或重复，normalized 须幂等"
                            ),
                            path,
                        )
        except (OSError, UnicodeError, json.JSONDecodeError, ValueError) as exc:
            report.add("error", "raw_manifest_corrupt", str(exc), path)
    if report.mode == "full":
        for run_id in sorted(seen_runs - manifests):
            if run_id in checkpoints:
                report.add(
                    "warning",
                    "raw_open_checkpoint",
                    f"run {run_id} 只有运行中检查点，未见终态 manifest",
                )
            else:
                report.add(
                    "warning",
                    "raw_unfinished_run",
                    f"run {run_id} 有 raw 行但无完成 manifest；数据仍在，可重建清单",
                )
    else:
        report.add(
            "unproven",
            "raw_counts_not_scanned",
            "quick 模式未逐行核对 raw 与 manifest；使用 --mode full",
            root,
        )
    if legacy_records or legacy_manifests:
        report.add(
            "unproven",
            "raw_legacy_no_durable_ack",
            (
                f"{legacy_records} 行、{legacy_manifests} 个 manifest 来自 durable "
                "ack 版本登记前；新写入已使用 fsync-per-record-v1"
            ),
            root,
        )


def _coverage_path(
    data_root: Path, venue: str, symbol: str, day: str
) -> Path | None:
    if venue == "bitbank":
        return archive.bitbank_day_path(data_root, symbol, day)
    if venue == "bitflyer":
        return archive.bitflyer_day_path(data_root, symbol, day)
    if venue == "gmo":
        return (
            data_root
            / "archive"
            / "trades"
            / symbol
            / day[:4]
            / day[4:6]
            / f"{day}_{symbol}.csv.gz"
        )
    if venue == "binance":
        return archive.binance_aggtrade_path(data_root, symbol, day)
    return None


def _archive_stats(venue: str, path: Path) -> archive.FileStats:
    if venue == "bitbank":
        return archive.bitbank_file_stats(path)
    if venue == "bitflyer":
        return archive.bitflyer_file_stats(path)
    if venue == "gmo":
        return archive.gmo_csv_stats(path)
    if venue == "binance":
        return archive.binance_aggtrade_file_stats(path)
    raise ValueError(f"无 {venue} 归档统计器")


def _gzip_header_ok(path: Path) -> bool:
    with path.open("rb") as handle:
        return handle.read(2) == b"\x1f\x8b"


def audit_archive_coverage(
    data_root: Path, conn: sqlite3.Connection, report: AuditReport
) -> None:
    """逐覆盖登记核对文件存在性；full 模式核对 gzip CRC 与行数。"""
    # quick 模式批量建立索引。
    # full 模式仍逐文件校验。
    archive_root = data_root / "archive"
    existing = (
        set(archive_root.rglob("*.gz")) | set(archive_root.rglob("*.zip"))
        if archive_root.exists() else set()
    )
    covered_paths: set[Path] = set()
    if report.mode == "quick":
        report.files_checked += len(existing)
        report.count("archive_files_indexed", len(existing))
    query = (
        "SELECT venue_id, venue_symbol, day, rows, first_ts, last_ts, status "
        "FROM archive_coverage WHERE domain='trade' "
        "ORDER BY venue_id, venue_symbol, day"
    )
    for venue, symbol, day, rows, first_ts, last_ts, status in conn.execute(query):
        venue_text = str(venue)
        path = _coverage_path(data_root, venue_text, str(symbol), str(day))
        report.count("coverage_rows")
        if path is None:
            report.add(
                "unproven",
                "coverage_mapper_missing",
                f"{venue_text}/{symbol}/{day} 尚无归档路径映射",
            )
            continue
        covered_paths.add(path)
        if str(status) == archive.STATUS_MISSING:
            present = path in existing
            if present:
                report.add(
                    "warning",
                    "coverage_stale_missing",
                    "登记为 missing 但文件存在，应重算覆盖",
                    path,
                )
            continue
        present = path in existing
        if not present:
            report.add(
                "error",
                "coverage_file_missing",
                f"{venue_text}/{symbol}/{day} 登记 {status} 但文件不存在",
                path,
            )
            continue
        if report.mode == "full":
            _check_file(report, path)
        report.count("archive_files")
        try:
            if report.mode == "quick":
                continue
            actual = _archive_stats(venue_text, path)
            expected_rows = 0 if rows is None else int(rows)
            if actual.rows != expected_rows:
                report.add(
                    "error",
                    "coverage_row_count_mismatch",
                    f"登记={expected_rows}, 文件={actual.rows}",
                    path,
                )
            if actual.first_ts != first_ts or actual.last_ts != last_ts:
                report.add(
                    "error",
                    "coverage_time_range_mismatch",
                    (
                        f"登记=({first_ts},{last_ts}), "
                        f"文件=({actual.first_ts},{actual.last_ts})"
                    ),
                    path,
                )
            expected_status = (
                archive.STATUS_OK if actual.rows else archive.STATUS_EMPTY
            )
            if str(status) != expected_status:
                report.add(
                    "error",
                    "coverage_status_mismatch",
                    f"登记={status}, 文件应为={expected_status}",
                    path,
                )
        except (
            OSError, EOFError, gzip.BadGzipFile, zipfile.BadZipFile,
            UnicodeError, ValueError,
        ) as exc:
            report.add("error", "archive_corrupt", str(exc), path)
    unregistered = sorted(existing - covered_paths)
    if unregistered:
        report.count("archive_files_unregistered", len(unregistered))
        examples = ", ".join(path.as_posix() for path in unregistered[:5])
        report.add(
            "warning",
            "archive_files_unregistered",
            (
                f"{len(unregistered)} 个归档文件未登记 archive_coverage；"
                f"数据仍在但 coverage 查询不可见。示例: {examples}"
            ),
            archive_root,
        )
    if report.mode == "quick":
        report.add(
            "unproven",
            "archive_crc_not_scanned",
            "quick 模式批量检查存在性，未逐文件解压验证 gzip CRC/行数",
            data_root / "archive",
        )
    report.add(
        "unproven",
        "archive_checksum_absent",
        "archive_coverage 未保存文件 SHA-256，无法识别可解析但被等行数篡改的文件",
        data_root / "archive",
    )


def _table_names(conn: sqlite3.Connection) -> set[str]:
    return {
        str(row[0])
        for row in conn.execute(
            "SELECT name FROM sqlite_master WHERE type='table'"
        )
    }


def _audit_analysis(conn: sqlite3.Connection, report: AuditReport) -> None:
    for run_id, status, judgments in conn.execute(
        "SELECT run_id, status, judgments FROM analysis_run"
    ):
        try:
            loaded = json.loads(str(judgments))
            if not isinstance(loaded, list):
                raise ValueError("judgments 非数组")
            met = {
                str(item["kind"])
                for item in loaded
                if isinstance(item, Mapping) and item.get("met") is True
            }
        except (json.JSONDecodeError, KeyError, ValueError) as exc:
            report.add(
                "error",
                "analysis_judgments_corrupt",
                f"run {run_id}: {exc}",
            )
            continue
        linked = {
            str(row[0])
            for row in conn.execute(
                "SELECT kind FROM book_feature WHERE run_id=?", (run_id,)
            )
        }
        if str(status) == "complete" and met != linked:
            report.add(
                "error",
                "analysis_feature_mismatch",
                f"run {run_id}: met={sorted(met)}, linked={sorted(linked)}",
            )


def _audit_backfills(conn: sqlite3.Connection, report: AuditReport) -> None:
    for row in conn.execute(
        "SELECT run_id, planned_parts, ok_parts, missing_parts, empty_parts, "
        "rows, checksum_failures, status FROM backfill_run"
    ):
        run_id, planned, ok, missing, empty, rows, failures, status = row
        values = [int(planned), int(ok), int(missing), int(empty), int(rows), int(failures)]
        if any(value < 0 for value in values):
            report.add(
                "error", "backfill_negative_count", f"run {run_id}: {values}"
            )
        if str(status) == "complete" and int(planned) != (
            int(ok) + int(missing) + int(empty)
        ):
            report.add(
                "error",
                "backfill_partition_mismatch",
                (
                    f"run {run_id}: planned={planned}, "
                    f"terminal={int(ok) + int(missing) + int(empty)}"
                ),
            )


def _audit_stream_health(conn: sqlite3.Connection, report: AuditReport) -> None:
    for row in conn.execute(
        "SELECT venue_id, channel, instrument_id, window_start, frames, "
        "sequence_gaps, sequence_regressions, checksum_failures, "
        "snapshot_mismatches, reconnects FROM stream_health"
    ):
        identity = "/".join(str(cell) for cell in row[:4])
        values = [int(cell) for cell in row[4:]]
        if any(value < 0 for value in values):
            report.add(
                "error", "stream_health_negative_count", f"{identity}: {values}"
            )


def audit_sqlite(data_root: Path, report: AuditReport) -> sqlite3.Connection | None:
    """验证 SQLite、WAL 配置、外键与派生跨表终态。"""
    path = data_root / DB_FILE_NAME
    if not path.exists():
        report.add("error", "sqlite_missing", "SQLite 数据库不存在", path)
        return None
    _check_file(report, path)
    try:
        conn = sqlite3.connect(f"file:{path.as_posix()}?mode=ro", uri=True)
    except sqlite3.Error as exc:
        report.add("error", "sqlite_open_failed", str(exc), path)
        return None
    required = {
        "archive_coverage",
        "trade_tick",
        "book_top",
        "stream_health",
        "backfill_run",
        "analysis_run",
        "book_feature",
        "alert_event",
    }
    missing = sorted(required - _table_names(conn))
    if missing:
        report.add(
            "error", "sqlite_tables_missing", ", ".join(missing), path
        )
        conn.close()
        return None
    check = "integrity_check" if report.mode == "full" else "quick_check"
    try:
        result = [str(row[0]) for row in conn.execute(f"PRAGMA {check}")]
        if result != ["ok"]:
            report.add("error", "sqlite_integrity_failed", repr(result), path)
        foreign = list(conn.execute("PRAGMA foreign_key_check"))
        if foreign:
            report.add(
                "error", "sqlite_foreign_key_failed", repr(foreign[:20]), path
            )
        if str(conn.execute("PRAGMA journal_mode").fetchone()[0]) != "wal":
            report.add("warning", "sqlite_not_wal", "journal_mode 非 WAL", path)
        if int(conn.execute("PRAGMA synchronous").fetchone()[0]) < 2:
            report.add(
                "warning",
                "sqlite_synchronous_weak",
                "synchronous 低于 FULL(2)",
                path,
            )
        for table in sorted(required):
            count = int(conn.execute(f"SELECT COUNT(*) FROM {table}").fetchone()[0])
            report.counters[f"db_{table}"] = count
        coverage_count = report.counters["db_archive_coverage"]
        if coverage_count and report.counters["db_trade_tick"] == 0:
            report.add(
                "unproven",
                "normalized_trade_sink_empty",
                "已有 archive_coverage 但 trade_tick 为 0；归一化写入器尚未接入回补任务",
                path,
            )
        if coverage_count and report.counters["db_backfill_run"] == 0:
            report.add(
                "unproven",
                "backfill_ledger_empty",
                "已有覆盖登记但 backfill_run 为 0；旧任务没有运行级持久化台账",
                path,
            )
        if report.counters["db_stream_health"] == 0:
            report.add(
                "unproven",
                "stream_health_sink_empty",
                "stream_health 为 0；实时采集器尚未持久化健康窗口",
                path,
            )
        _audit_analysis(conn, report)
        _audit_backfills(conn, report)
        _audit_stream_health(conn, report)
        orphan_features = int(
            conn.execute(
                "SELECT COUNT(*) FROM book_feature f LEFT JOIN analysis_run r "
                "ON r.run_id=f.run_id WHERE f.run_id IS NOT NULL AND r.run_id IS NULL"
            ).fetchone()[0]
        )
        if orphan_features:
            report.add(
                "error",
                "feature_analysis_orphan",
                f"{orphan_features} 条 feature 缺 analysis_run",
                path,
            )
    except sqlite3.Error as exc:
        report.add("error", "sqlite_query_failed", str(exc), path)
        conn.close()
        return None
    report.add(
        "warning",
        "sqlite_cross_commit_recoverable",
        "analysis_run、book_feature、alert_event 分三次提交；崩溃可留部分终态，审计可检测且同 request 可幂等补齐",
        path,
    )
    return conn


def _iter_gzip_json_lines(path: Path) -> Iterator[Mapping[str, object]]:
    with gzip.open(path, "rt", encoding="utf-8") as handle:
        for number, line in enumerate(handle, 1):
            if not line.strip():
                continue
            loaded = json.loads(line)
            if not isinstance(loaded, Mapping):
                raise ValueError(f"第 {number} 行非 JSON 对象")
            yield loaded


def _canonical_update(digest: hashlib._Hash, row: Mapping[str, object]) -> None:
    digest.update(
        (json.dumps(row, sort_keys=True, ensure_ascii=False) + "\n").encode(
            "utf-8"
        )
    )


def _active_chunk_files(
    meta_path: Path, date_text: str, generation: str
) -> list[Path]:
    base = meta_path.parent / "chunks" / date_text / generation
    if not base.exists():
        return []

    def start(path: Path) -> int:
        return int(path.name.split(".", 1)[0])

    return sorted(base.glob("*.jsonl.gz"), key=start)


def audit_heatmap(data_root: Path, report: AuditReport) -> None:
    """核对 meta、整日 gzip 与当前 generation 分块。"""
    root = data_root / "derived" / "heatmap_tiles"
    if not root.exists():
        report.add("warning", "heatmap_root_missing", "无 heatmap 制品", root)
        return
    legacy_pointers = 0
    for meta_path in sorted(root.rglob("*.meta.json")):
        _check_file(report, meta_path)
        report.count("heatmap_meta")
        daily_path = meta_path.with_name(
            meta_path.name.removesuffix(".meta.json") + ".jsonl.gz"
        )
        try:
            meta = json.loads(meta_path.read_text(encoding="utf-8"))
            if not isinstance(meta, Mapping):
                raise ValueError("meta 非 JSON 对象")
            if meta.get("persistence_version") != TILE_PERSISTENCE_VERSION:
                legacy_pointers += 1
            if not daily_path.exists():
                raise FileNotFoundError("整日 gzip 缺失")
            _check_file(report, daily_path)
            generation = meta.get("chunk_generation")
            date_text = str(meta.get("date", ""))
            if not isinstance(generation, str) or not generation:
                report.add(
                    "warning",
                    "heatmap_generation_missing",
                    "无物理块 generation，查询会回退整日文件",
                    meta_path,
                )
                continue
            chunks = _active_chunk_files(meta_path, date_text, generation)
            if not chunks:
                raise FileNotFoundError("meta 指向的 generation 无物理块")
            if report.mode == "quick":
                if not _gzip_header_ok(daily_path):
                    raise ValueError("整日文件 gzip magic 不正确")
                for chunk in chunks:
                    _check_file(report, chunk)
                    if not _gzip_header_ok(chunk):
                        raise ValueError(f"chunk gzip magic 不正确: {chunk.name}")
                continue
            daily_digest = hashlib.sha256()
            chunk_digest = hashlib.sha256()
            daily_count = gap_count = carried_count = 0
            daily_epochs: set[int] = set()
            for row in _iter_gzip_json_lines(daily_path):
                _canonical_update(daily_digest, row)
                daily_count += 1
                epoch = row.get("e")
                if isinstance(epoch, int):
                    if epoch in daily_epochs:
                        report.add(
                            "error",
                            "heatmap_duplicate_epoch",
                            f"重复 epoch {epoch}",
                            daily_path,
                        )
                    daily_epochs.add(epoch)
                gap_count += int(row.get("gap") is True)
                carried_count += int(row.get("carried") is True)
            chunk_count = 0
            chunk_epochs: set[int] = set()
            for chunk in chunks:
                _check_file(report, chunk)
                for row in _iter_gzip_json_lines(chunk):
                    _canonical_update(chunk_digest, row)
                    chunk_count += 1
                    epoch = row.get("e")
                    if isinstance(epoch, int):
                        if epoch in chunk_epochs:
                            report.add(
                                "error",
                                "heatmap_chunk_duplicate_epoch",
                                f"重复 epoch {epoch}",
                                chunk,
                            )
                        chunk_epochs.add(epoch)
            expected_columns = int(str(meta.get("columns", -1)))
            expected_gaps = int(str(meta.get("gap_columns", -1)))
            expected_carried = int(str(meta.get("carried_columns", -1)))
            if daily_count != expected_columns:
                report.add(
                    "error",
                    "heatmap_meta_column_mismatch",
                    f"meta={expected_columns}, daily={daily_count}",
                    meta_path,
                )
            if gap_count != expected_gaps or carried_count != expected_carried:
                report.add(
                    "error",
                    "heatmap_meta_flag_mismatch",
                    (
                        f"gap meta/daily={expected_gaps}/{gap_count}, "
                        f"carried={expected_carried}/{carried_count}"
                    ),
                    meta_path,
                )
            if (
                daily_count != chunk_count
                or daily_epochs != chunk_epochs
                or daily_digest.digest() != chunk_digest.digest()
            ):
                report.add(
                    "error",
                    "heatmap_generation_mismatch",
                    (
                        f"daily/chunk columns={daily_count}/{chunk_count}; "
                        "内容或 epoch 集不一致"
                    ),
                    meta_path,
                )
            report.count("heatmap_columns", daily_count)
            report.count("heatmap_chunks", len(chunks))
        except (
            OSError,
            EOFError,
            gzip.BadGzipFile,
            UnicodeError,
            json.JSONDecodeError,
            ValueError,
        ) as exc:
            report.add("error", "heatmap_corrupt", str(exc), meta_path)
    if report.mode == "quick":
        report.add(
            "unproven",
            "heatmap_content_not_scanned",
            "quick 模式未逐列核对 daily/meta/current generation",
            root,
        )
    report.add(
        "unproven",
        "heatmap_source_hash_absent",
        "meta 未保存 raw 内容散列，daily 与 chunk 一致仍不能证明与源文件逐字对应",
        root,
    )
    if legacy_pointers:
        report.add(
            "unproven",
            "heatmap_legacy_pointer",
            (
                f"{legacy_pointers} 个 meta 生成于 atomic-pointer-v1 登记前；"
                "新 meta/tick/cursor 已 fsync 后原子替换"
            ),
            root,
        )


@dataclass(frozen=True, slots=True)
class GzipRecovery:
    """串联 gzip 完整成员边界。"""

    complete_members: int
    valid_bytes: int
    total_bytes: int

    @property
    def intact(self) -> bool:
        return self.valid_bytes == self.total_bytes


def inspect_gzip_members(path: Path) -> GzipRecovery:
    """定位串联 gzip 最后一个完整成员，不修改源文件。"""
    blob = path.read_bytes()
    offset = 0
    members = 0
    while offset < len(blob):
        decoder = zlib.decompressobj(16 + zlib.MAX_WBITS)
        try:
            decoder.decompress(blob[offset:])
        except zlib.error:
            break
        if not decoder.eof:
            break
        consumed = len(blob) - offset - len(decoder.unused_data)
        if consumed <= 0:
            break
        offset += consumed
        members += 1
    return GzipRecovery(members, offset, len(blob))


def recover_gzip_prefix(source: Path, destination: Path) -> GzipRecovery:
    """把完整 gzip 成员复制到新文件；绝不原地截断生产文件。"""
    recovery = inspect_gzip_members(source)
    if recovery.valid_bytes == 0:
        raise ValueError("没有可恢复的完整 gzip 成员")
    destination.parent.mkdir(parents=True, exist_ok=True)
    temp = destination.with_name(destination.name + ".tmp")
    with source.open("rb") as source_handle, temp.open("wb") as target:
        remaining = recovery.valid_bytes
        while remaining:
            block = source_handle.read(min(1024 * 1024, remaining))
            if not block:
                raise OSError("源文件提前结束")
            target.write(block)
            remaining -= len(block)
        target.flush()
        os.fsync(target.fileno())
    os.replace(temp, destination)
    return recovery


def probe_recovery(work_root: Path, records: int = 1000) -> dict[str, object]:
    """在隔离临时目录验证提交、回滚、幂等、原子替换和 gzip 恢复。"""
    work_root.mkdir(parents=True, exist_ok=True)
    with tempfile.TemporaryDirectory(prefix="guvolu-persist-", dir=work_root) as raw:
        root = Path(raw)
        db_path = root / "probe.sqlite3"
        conn = sqlite3.connect(db_path)
        conn.execute("PRAGMA journal_mode=WAL")
        conn.execute("PRAGMA synchronous=FULL")
        conn.execute("CREATE TABLE item (id INTEGER PRIMARY KEY, value TEXT NOT NULL)")
        committed = [(at, f"v-{at}") for at in range(records)]
        conn.executemany("INSERT INTO item VALUES (?,?)", committed)
        conn.commit()
        conn.executemany(
            "INSERT OR IGNORE INTO item VALUES (?,?)", committed
        )
        conn.commit()
        conn.execute("INSERT INTO item VALUES (?,?)", (records, "uncommitted"))
        conn.rollback()
        conn.close()
        reopened = sqlite3.connect(db_path)
        persisted = int(reopened.execute("SELECT COUNT(*) FROM item").fetchone()[0])
        integrity = str(reopened.execute("PRAGMA integrity_check").fetchone()[0])
        reopened.close()

        stable = root / "stable.bin"
        stable.write_bytes(b"committed")
        (root / "stable.bin.tmp").write_bytes(b"torn replacement")
        atomic_preserved = stable.read_bytes() == b"committed"

        members = root / "members.jsonl.gz"
        with members.open("ab") as handle:
            handle.write(gzip.compress(b'{"batch":1}\n'))
            handle.write(gzip.compress(b'{"batch":2}\n'))
        intact = inspect_gzip_members(members)
        torn = root / "members-torn.jsonl.gz"
        blob = members.read_bytes()
        torn.write_bytes(blob[:-5])
        broken = inspect_gzip_members(torn)
        recovered = root / "members-recovered.jsonl.gz"
        recover_gzip_prefix(torn, recovered)
        recovered_rows = 0
        with gzip.open(recovered, "rt", encoding="utf-8") as handle:
            recovered_rows = sum(1 for line in handle if line.strip())
        return {
            "records_requested": records,
            "records_persisted": persisted,
            "sqlite_integrity": integrity,
            "idempotent_replay": persisted == records,
            "uncommitted_rolled_back": persisted == records,
            "atomic_old_value_preserved": atomic_preserved,
            "gzip_members": intact.complete_members,
            "torn_detected": not broken.intact,
            "recoverable_members": broken.complete_members,
            "recovered_rows": recovered_rows,
            "ok": (
                persisted == records
                and integrity == "ok"
                and atomic_preserved
                and intact.complete_members == 2
                and not broken.intact
                and broken.complete_members == 1
                and recovered_rows == 1
            ),
        }


def audit_persistence(data_root: Path, mode: AuditMode = "quick") -> AuditReport:
    """运行生产数据只读审计。"""
    report = AuditReport(mode=mode, started_at=_now_iso())
    audit_raw(data_root, report)
    conn = audit_sqlite(data_root, report)
    if conn is not None:
        audit_archive_coverage(data_root, conn, report)
        conn.close()
    audit_heatmap(data_root, report)
    report.finished_at = _now_iso()
    return report


def _write_report(path: Path, payload: Mapping[str, object]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    temp = path.with_name(path.name + ".tmp")
    text = json.dumps(payload, ensure_ascii=False, indent=2) + "\n"
    with temp.open("w", encoding="utf-8") as handle:
        handle.write(text)
        handle.flush()
        os.fsync(handle.fileno())
    os.replace(temp, path)


def main(argv: Sequence[str] | None = None) -> int:
    """命令行入口；错误返回 2，证据盲区返回 1，完全证明返回 0。"""
    if hasattr(sys.stdout, "reconfigure"):
        sys.stdout.reconfigure(encoding="utf-8", errors="replace")
    parser = argparse.ArgumentParser(description="guvolu 持久化一致性审计")
    parser.add_argument("--data-root", type=Path, default=data_root())
    parser.add_argument("--mode", choices=("quick", "full"), default="quick")
    parser.add_argument("--output", type=Path)
    parser.add_argument("--probe", action="store_true", help="追加隔离恢复压力探针")
    parser.add_argument("--probe-records", type=int, default=1000)
    args = parser.parse_args(argv)
    mode: AuditMode = args.mode
    report = audit_persistence(args.data_root, mode)
    payload = report.as_dict()
    if args.probe:
        payload["recovery_probe"] = probe_recovery(
            Path(tempfile.gettempdir()), args.probe_records
        )
    if args.output is not None:
        _write_report(args.output, payload)
    print(json.dumps(payload, ensure_ascii=False, indent=2), flush=True)
    if report.loss_detected:
        return 2
    return 0 if report.fully_proven else 1


if __name__ == "__main__":
    raise SystemExit(main())

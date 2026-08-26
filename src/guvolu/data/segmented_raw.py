"""不可回补实时流的 run-scoped 分段原件。

运行中片段使用 ``.open`` 后缀；逐条 fsync。封口后先关闭文件，
再原子改名并写包含 SHA-256 的不可变 manifest。物化只接受封口片段。
"""
from __future__ import annotations

import asyncio
import hashlib
import json
import os
import re
import time
from collections.abc import Awaitable, Callable, Mapping
from dataclasses import dataclass
from datetime import UTC, datetime, timedelta
from pathlib import Path
from typing import BinaryIO

from guvolu.data.durable_io import atomic_write_text
from guvolu.domain.ids import new_run_id

SEGMENT_SCHEMA_VERSION = 3
SEGMENT_DURABILITY_VERSION = "fsync-per-record-v1"
_SAFE_COMPONENT = re.compile(r"^[A-Za-z0-9_.-]+$")
_SEGMENT_NAME = re.compile(r"^segment-([0-9]{6})\.jsonl$")
RUN_RECOVERY_MANIFEST = "run.recovered-incomplete.manifest.json"


def _now() -> datetime:
    return datetime.now(UTC)


def _safe(value: str, label: str) -> str:
    if not value or _SAFE_COMPONENT.fullmatch(value) is None:
        raise ValueError(f"{label} 含不安全路径字符: {value!r}")
    return value


def _sha256_file(path: Path) -> str:
    digest = hashlib.sha256()
    with path.open("rb") as handle:
        for block in iter(lambda: handle.read(1024 * 1024), b""):
            digest.update(block)
    return digest.hexdigest()


@dataclass(frozen=True, slots=True)
class SegmentedRunAudit:
    """一个缺正式 run 终态目录的只读、逐段验证结果。"""

    run_storage_path: str
    domain: str
    venue_id: str
    venue_symbol: str
    run_id: str
    checkpoint_storage_path: str
    checkpoint_sha256: str | None
    started_at: str | None
    checkpoint_at: str | None
    checkpoint_records: int | None
    checkpoint_sealed_segments: int | None
    verified_records: int
    verified_segments: int
    sealed_segments: int
    recovered_incomplete_segments: int
    eligible_for_reconciliation: bool
    issues: tuple[str, ...]


@dataclass(frozen=True, slots=True)
class _SegmentedRunInspection:
    audit: SegmentedRunAudit
    receipts: tuple[dict[str, object], ...]


def _audit_integer(
    value: object, label: str, issues: list[str],
) -> int | None:
    if isinstance(value, bool) or not isinstance(value, int) or value < 0:
        issues.append(f"{label}_invalid")
        return None
    return value


def _audit_time(
    value: object, label: str, issues: list[str],
) -> datetime | None:
    if not isinstance(value, str):
        issues.append(f"{label}_invalid")
        return None
    try:
        parsed = datetime.fromisoformat(value.replace("Z", "+00:00"))
    except ValueError:
        issues.append(f"{label}_invalid")
        return None
    if parsed.tzinfo is None or parsed.utcoffset() is None:
        issues.append(f"{label}_naive")
        return None
    return parsed.astimezone(UTC)


def _audit_json_mapping(
    path: Path, label: str, issues: list[str],
) -> tuple[bytes | None, Mapping[str, object] | None]:
    if path.is_symlink() or not path.is_file():
        issues.append(f"{label}_not_ordinary_file")
        return None, None
    try:
        raw = path.read_bytes()
        loaded = json.loads(raw)
    except (OSError, UnicodeError, json.JSONDecodeError):
        issues.append(f"{label}_unreadable_json")
        return None, None
    if not isinstance(loaded, Mapping):
        issues.append(f"{label}_not_mapping")
        return raw, None
    return raw, loaded


def _segment_receipt_for_audit(
    data_root: Path,
    final_path: Path,
    manifest_path: Path,
    checkpoint: Mapping[str, object],
    expected_sequence: int,
    expected_record_sequence: int,
    issues: list[str],
) -> tuple[dict[str, object] | None, int]:
    label = f"segment_{expected_sequence:06d}"
    raw_manifest, manifest = _audit_json_mapping(
        manifest_path, f"{label}_manifest", issues,
    )
    if raw_manifest is None or manifest is None:
        return None, expected_record_sequence
    if final_path.is_symlink() or not final_path.is_file():
        issues.append(f"{label}_data_not_ordinary_file")
        return None, expected_record_sequence
    status = manifest.get("status")
    completion_claim = manifest.get("completion_claim")
    if not (
        (status == "sealed" and completion_claim is True)
        or (status == "recovered_incomplete" and completion_claim is False)
    ):
        issues.append(f"{label}_terminal_claim_invalid")
    identity_fields = (
        "run_id", "venue_id", "venue_symbol", "domain",
        "endpoint_id", "endpoint_revision",
    )
    for field_name in identity_fields:
        if manifest.get(field_name) != checkpoint.get(field_name):
            issues.append(f"{label}_{field_name}_mismatch")
    if manifest.get("segment_sequence") != expected_sequence:
        issues.append(f"{label}_sequence_mismatch")
    expected_storage = final_path.relative_to(data_root).as_posix()
    if manifest.get("storage_path") != expected_storage:
        issues.append(f"{label}_storage_path_mismatch")

    digest = hashlib.sha256()
    row_count = 0
    byte_count = 0
    next_record_sequence = expected_record_sequence
    try:
        with final_path.open("rb") as handle:
            for raw_line in handle:
                digest.update(raw_line)
                byte_count += len(raw_line)
                if not raw_line.endswith(b"\n"):
                    issues.append(f"{label}_partial_line")
                    continue
                try:
                    row = json.loads(raw_line)
                except (UnicodeError, json.JSONDecodeError):
                    issues.append(f"{label}_invalid_json_line")
                    continue
                if not isinstance(row, Mapping):
                    issues.append(f"{label}_row_not_mapping")
                    continue
                row_count += 1
                for field_name in identity_fields:
                    if row.get(field_name) != checkpoint.get(field_name):
                        issues.append(f"{label}_row_{field_name}_mismatch")
                if row.get("segment_sequence") != expected_sequence:
                    issues.append(f"{label}_row_segment_sequence_mismatch")
                if row.get("record_sequence") != next_record_sequence:
                    issues.append(f"{label}_row_record_sequence_mismatch")
                next_record_sequence += 1
                payload = row.get("payload_raw")
                payload_sha = row.get("raw_payload_sha256")
                if isinstance(payload, str) and isinstance(payload_sha, str):
                    if hashlib.sha256(payload.encode("utf-8")).hexdigest() != payload_sha:
                        issues.append(f"{label}_row_payload_hash_mismatch")
    except OSError:
        issues.append(f"{label}_data_read_failure")
        return None, next_record_sequence

    sha256 = digest.hexdigest()
    if manifest.get("sha256") != sha256:
        issues.append(f"{label}_sha256_mismatch")
    if manifest.get("artifact_id") != f"sha256-{sha256}":
        issues.append(f"{label}_artifact_id_mismatch")
    if manifest.get("byte_count") != byte_count:
        issues.append(f"{label}_byte_count_mismatch")
    if manifest.get("record_count") != row_count:
        issues.append(f"{label}_record_count_mismatch")
    receipt = {
        "segment_sequence": expected_sequence,
        "status": status,
        "completion_claim": completion_claim,
        "artifact_id": f"sha256-{sha256}",
        "sha256": sha256,
        "byte_count": byte_count,
        "record_count": row_count,
        "storage_path": expected_storage,
        "manifest_path": manifest_path.relative_to(data_root).as_posix(),
        "manifest_sha256": hashlib.sha256(raw_manifest).hexdigest(),
        "first_ingest_time": manifest.get("first_ingest_time"),
        "last_ingest_time": manifest.get("last_ingest_time"),
    }
    return receipt, next_record_sequence


def _inspect_unfinished_segmented_runs(
    data_root: Path, older_minutes: int, *, domain: str,
) -> tuple[_SegmentedRunInspection, ...]:
    if older_minutes <= 0:
        raise ValueError("older_minutes 必须为正数")
    root = data_root / "raw" / "realtime" / _safe(domain, "domain")
    if not root.is_dir():
        return ()
    cutoff = _now() - timedelta(minutes=older_minutes)
    output: list[_SegmentedRunInspection] = []
    for checkpoint_path in sorted(root.rglob("checkpoint.json")):
        run_directory = checkpoint_path.parent
        if (run_directory / "run.manifest.json").exists():
            continue
        issues: list[str] = []
        raw_checkpoint, checkpoint = _audit_json_mapping(
            checkpoint_path, "checkpoint", issues,
        )
        fallback_parts = {
            part.split("=", 1)[0]: part.split("=", 1)[1]
            for part in run_directory.parts
            if "=" in part
        }
        loaded = checkpoint or {}
        run_id = str(loaded.get("run_id") or fallback_parts.get("run_id", ""))
        venue_id = str(
            loaded.get("venue_id") or fallback_parts.get("venue_id", "")
        )
        venue_symbol = str(
            loaded.get("venue_symbol")
            or fallback_parts.get("venue_symbol", "")
        )
        if loaded.get("status") != "open":
            issues.append("checkpoint_status_not_open")
        if loaded.get("domain") != domain:
            issues.append("checkpoint_domain_mismatch")
        if run_directory.name != f"run_id={run_id}":
            issues.append("checkpoint_run_path_mismatch")
        started = _audit_time(loaded.get("started_at"), "started_at", issues)
        checkpoint_at = _audit_time(
            loaded.get("checkpoint_at"), "checkpoint_at", issues,
        )
        if started is not None and checkpoint_at is not None and checkpoint_at < started:
            issues.append("checkpoint_time_reversed")
        try:
            modified = datetime.fromtimestamp(checkpoint_path.stat().st_mtime, UTC)
        except OSError:
            modified = _now()
            issues.append("checkpoint_stat_failure")
        if max(
            modified,
            checkpoint_at if checkpoint_at is not None else modified,
        ) > cutoff:
            issues.append("checkpoint_not_stale")
        checkpoint_records = _audit_integer(
            loaded.get("records"), "checkpoint_records", issues,
        )
        checkpoint_segments = _audit_integer(
            loaded.get("sealed_segments"), "checkpoint_segments", issues,
        )
        open_paths = sorted(run_directory.glob("segment-*.jsonl.open"))
        if open_paths:
            issues.append(f"active_or_unrecovered_open_segments:{len(open_paths)}")
        if (run_directory / RUN_RECOVERY_MANIFEST).exists():
            issues.append("recovery_manifest_exists")

        finals = sorted(run_directory.glob("segment-*.jsonl"))
        manifests = sorted(run_directory.glob("segment-*.manifest.json"))
        final_names = {path.name.removesuffix(".jsonl") for path in finals}
        manifest_names = {
            path.name.removesuffix(".manifest.json") for path in manifests
        }
        if final_names != manifest_names:
            issues.append("segment_data_manifest_set_mismatch")
        receipts: list[dict[str, object]] = []
        next_record_sequence = 1
        for position, final_path in enumerate(finals, start=1):
            match = _SEGMENT_NAME.fullmatch(final_path.name)
            if match is None or int(match.group(1)) != position:
                issues.append("segment_sequence_not_contiguous")
                continue
            manifest_path = final_path.with_name(
                final_path.name.removesuffix(".jsonl") + ".manifest.json"
            )
            if not manifest_path.exists():
                continue
            receipt, next_record_sequence = _segment_receipt_for_audit(
                data_root, final_path, manifest_path, loaded, position,
                next_record_sequence, issues,
            )
            if receipt is not None:
                receipts.append(receipt)
        verified_records = sum(
            int(str(receipt["record_count"])) for receipt in receipts
        )
        if checkpoint_records is not None and checkpoint_records != verified_records:
            issues.append("checkpoint_record_count_mismatch")
        if checkpoint_segments is not None and not (
            checkpoint_segments <= len(receipts) <= checkpoint_segments + 1
        ):
            issues.append("checkpoint_segment_count_mismatch")
        sealed_segments = sum(
            receipt["status"] == "sealed" for receipt in receipts
        )
        recovered_segments = sum(
            receipt["status"] == "recovered_incomplete" for receipt in receipts
        )
        audit = SegmentedRunAudit(
            run_storage_path=run_directory.relative_to(data_root).as_posix(),
            domain=domain,
            venue_id=venue_id,
            venue_symbol=venue_symbol,
            run_id=run_id,
            checkpoint_storage_path=checkpoint_path.relative_to(data_root).as_posix(),
            checkpoint_sha256=(
                hashlib.sha256(raw_checkpoint).hexdigest()
                if raw_checkpoint is not None else None
            ),
            started_at=(started.isoformat() if started is not None else None),
            checkpoint_at=(
                checkpoint_at.isoformat() if checkpoint_at is not None else None
            ),
            checkpoint_records=checkpoint_records,
            checkpoint_sealed_segments=checkpoint_segments,
            verified_records=verified_records,
            verified_segments=len(receipts),
            sealed_segments=sealed_segments,
            recovered_incomplete_segments=recovered_segments,
            eligible_for_reconciliation=not issues,
            issues=tuple(dict.fromkeys(issues)),
        )
        output.append(_SegmentedRunInspection(audit, tuple(receipts)))
    return tuple(output)


def audit_unfinished_segmented_runs(
    data_root: Path, older_minutes: int = 60, *, domain: str = "book_l2",
) -> tuple[SegmentedRunAudit, ...]:
    """只读验证缺终态 run；不会改 checkpoint、segment 或 manifest。"""
    return tuple(
        inspection.audit
        for inspection in _inspect_unfinished_segmented_runs(
            data_root, older_minutes, domain=domain,
        )
    )


def reconcile_unfinished_segmented_runs(
    data_root: Path, older_minutes: int = 60, *, domain: str = "book_l2",
) -> tuple[Path, ...]:
    """显式追加 recovered-incomplete run 证据；绝不伪造正式 complete。"""
    written: list[Path] = []
    for inspection in _inspect_unfinished_segmented_runs(
        data_root, older_minutes, domain=domain,
    ):
        audit = inspection.audit
        if not audit.eligible_for_reconciliation:
            continue
        run_directory = data_root / audit.run_storage_path
        target = run_directory / RUN_RECOVERY_MANIFEST
        checkpoint_path = data_root / audit.checkpoint_storage_path
        if (
            target.exists()
            or (run_directory / "run.manifest.json").exists()
            or list(run_directory.glob("segment-*.jsonl.open"))
            or _sha256_file(checkpoint_path) != audit.checkpoint_sha256
        ):
            continue
        reconciled_at = _now().isoformat()
        body = {
            "schema_version": SEGMENT_SCHEMA_VERSION,
            "status": "recovered_incomplete",
            "completion_claim": False,
            "recovery_basis": (
                "stale-open-checkpoint-plus-segment-receipts-v1"
            ),
            "run_id": audit.run_id,
            "venue_id": audit.venue_id,
            "venue_symbol": audit.venue_symbol,
            "domain": audit.domain,
            "started_at": audit.started_at,
            "last_checkpoint_at": audit.checkpoint_at,
            "reconciled_at": reconciled_at,
            "checkpoint_path": audit.checkpoint_storage_path,
            "checkpoint_sha256": audit.checkpoint_sha256,
            "record_count": audit.verified_records,
            "segment_count": audit.verified_segments,
            "sealed_segments": audit.sealed_segments,
            "recovered_incomplete_segments": (
                audit.recovered_incomplete_segments
            ),
            "segments": list(inspection.receipts),
            "materialization_scope": "segment_receipts_only",
            "historical_gap_fill_claim": False,
        }
        atomic_write_text(
            target, json.dumps(body, ensure_ascii=False, indent=2) + "\n"
        )
        written.append(target)
    return tuple(written)


async def supervise_capture_tasks(
    recorder: Awaitable[None], checkpoint: Awaitable[None],
    *monitors: Awaitable[None],
) -> None:
    """竞速 recorder/monitors；monitor 失败优先且所有任务均被观察。"""
    recorder_task = asyncio.ensure_future(recorder)
    checkpoint_task = asyncio.ensure_future(checkpoint)
    monitor_tasks = (
        checkpoint_task,
        *(
            asyncio.ensure_future(monitor)
            for monitor in monitors
        ),
    )
    tasks = (recorder_task, *monitor_tasks)
    try:
        done, _ = await asyncio.wait(
            tasks, return_when=asyncio.FIRST_COMPLETED,
        )
    except BaseException as primary:
        for task in tasks:
            if not task.done():
                task.cancel()
        cleanup = await asyncio.gather(*tasks, return_exceptions=True)
        for result in cleanup:
            if isinstance(result, BaseException) and not isinstance(
                result, asyncio.CancelledError,
            ):
                primary.add_note(
                    "capture task cleanup: "
                    f"{type(result).__name__}: {result}"
                )
        raise

    selected = next(
        (task for task in monitor_tasks if task in done), recorder_task,
    )
    primary_error: BaseException | None = None
    if selected in monitor_tasks and not selected.cancelled():
        try:
            selected.result()
        except BaseException as exc:
            primary_error = exc
        else:
            primary_error = RuntimeError("checkpoint task 意外提前结束")
    else:
        try:
            selected.result()
        except BaseException as exc:
            primary_error = exc
    primary_is_monitor = selected in monitor_tasks

    peers = [task for task in tasks if task is not selected]
    for task in peers:
        if not task.done():
            task.cancel()
    cleanup = await asyncio.gather(*peers, return_exceptions=True)
    for peer, result in zip(peers, cleanup, strict=True):
        if isinstance(result, BaseException) and not isinstance(
            result, asyncio.CancelledError,
        ):
            if peer in monitor_tasks and not primary_is_monitor:
                if primary_error is not None:
                    result.add_note(
                        "recorder failure demoted by monitor failure: "
                        f"{type(primary_error).__name__}: {primary_error}"
                    )
                primary_error = result
                primary_is_monitor = True
            elif primary_error is None:
                primary_error = result
            else:
                primary_error.add_note(
                    "capture peer cleanup: "
                    f"{type(result).__name__}: {result}"
                )
    if primary_error is not None:
        raise primary_error


class SegmentedRawWriter:
    """一个 venue/market/domain run 的可封口实时分段写入器。"""

    def __init__(
        self,
        data_root: Path,
        venue_id: str,
        venue_symbol: str,
        domain: str = "book_l2",
        run_id: str | None = None,
        endpoint_id: str | None = None,
        *,
        endpoint_revision: int,
        segment_seconds: float = 300.0,
        segment_max_bytes: int = 128 * 1024 * 1024,
        on_segment_sealed: Callable[[Mapping[str, object]], None] | None = None,
    ) -> None:
        if segment_seconds <= 0:
            raise ValueError("segment_seconds 必须为正数")
        if segment_max_bytes <= 0:
            raise ValueError("segment_max_bytes 必须为正数")
        self.data_root = data_root
        self.venue_id = _safe(venue_id, "venue_id")
        self.venue_symbol = _safe(venue_symbol, "venue_symbol")
        self.domain = _safe(domain, "domain")
        self.run_id = _safe(run_id or new_run_id(), "run_id")
        if endpoint_id is None:
            raise ValueError("raw v3 endpoint_id 必须显式绑定")
        self.endpoint_id = _safe(endpoint_id, "endpoint_id")
        if (
            isinstance(endpoint_revision, bool)
            or not isinstance(endpoint_revision, int)
            or endpoint_revision < 0
        ):
            raise ValueError("endpoint_revision 必须为非负整数")
        self.endpoint_revision = endpoint_revision
        self.segment_seconds = segment_seconds
        self.segment_max_bytes = segment_max_bytes
        self.started_at = _now()
        self._on_segment_sealed = on_segment_sealed
        self._directory = (
            data_root / "raw" / "realtime" / self.domain
            / f"venue_id={self.venue_id}"
            / f"venue_symbol={self.venue_symbol}"
            / f"run_id={self.run_id}"
        )
        self._directory.mkdir(parents=True, exist_ok=False)
        self._segment_sequence = 0
        self._record_sequence = 0
        self._segment_handle: BinaryIO | None = None
        self._segment_open_path: Path | None = None
        self._segment_started_at: datetime | None = None
        self._segment_first_ingest: str | None = None
        self._segment_last_ingest: str | None = None
        self._segment_records = 0
        self._segment_bytes = 0
        self._segments: list[dict[str, object]] = []
        self._finished = False
        self._terminal_manifest: Path | None = None
        self._finish_signature: tuple[str, str] | None = None
        self._write_failure: str | None = None
        self._active_segment_tainted = False

    @property
    def directory(self) -> Path:
        """本 run 的唯一目录。"""
        return self._directory

    @property
    def record_count(self) -> int:
        """已持久化 wire 行数。"""
        return self._record_sequence

    @property
    def segment_count(self) -> int:
        """已封口 segment 数。"""
        return len(self._segments)

    def _open_segment(self, moment: datetime) -> None:
        next_sequence = self._segment_sequence + 1
        open_path = self._directory / (
            f"segment-{next_sequence:06d}.jsonl.open"
        )
        handle = open_path.open("ab", buffering=0)
        self._segment_sequence = next_sequence
        self._segment_started_at = moment
        self._segment_first_ingest = None
        self._segment_last_ingest = None
        self._segment_records = 0
        self._segment_bytes = 0
        self._segment_open_path = open_path
        self._segment_handle = handle

    def _rotation_due(self, moment: datetime) -> bool:
        if self._segment_started_at is None or self._segment_records == 0:
            return False
        age = (moment - self._segment_started_at).total_seconds()
        return age >= self.segment_seconds or self._segment_bytes >= self.segment_max_bytes

    def write_frame(
        self,
        payload_raw: str,
        source_endpoint: str,
        transport: str = "websocket",
        *,
        connection_id: str | None = None,
        channel_id: str | None = None,
        recv_ts_utc: str | None = None,
        recv_ts_mono_ns: int | None = None,
    ) -> None:
        """先于业务解析持久化一帧 wire 文本。"""
        if self._finished:
            raise RuntimeError("已封口的分段写入器不可继续写入")
        if self._write_failure is not None:
            raise RuntimeError(
                f"分段写入器已因持久化失败关闭: {self._write_failure}"
            )
        moment = _now()
        if self._rotation_due(moment):
            self.seal_segment()
        if self._segment_handle is None:
            self._open_segment(moment)
        next_record_sequence = self._record_sequence + 1
        received_utc = moment.isoformat() if recv_ts_utc is None else recv_ts_utc
        received_mono_ns = (
            time.monotonic_ns() if recv_ts_mono_ns is None else recv_ts_mono_ns
        )
        payload_sha256 = hashlib.sha256(payload_raw.encode("utf-8")).hexdigest()
        record = {
            "schema_version": SEGMENT_SCHEMA_VERSION,
            "durability_version": SEGMENT_DURABILITY_VERSION,
            "run_id": self.run_id,
            "segment_sequence": self._segment_sequence,
            "record_sequence": next_record_sequence,
            "venue_id": self.venue_id,
            "venue_symbol": self.venue_symbol,
            "domain": self.domain,
            "endpoint_id": self.endpoint_id,
            "endpoint_revision": self.endpoint_revision,
            "connection_id": connection_id,
            "channel_id": channel_id,
            "source": transport,
            "source_endpoint": source_endpoint,
            "payload_raw": payload_raw,
            "raw_payload_sha256": payload_sha256,
            "recv_ts_utc": received_utc,
            "recv_ts_mono_ns": received_mono_ns,
            # 保留旧字段。
            # 语义为首次可见时间。
            "ingest_time": received_utc,
        }
        encoded = (json.dumps(record, ensure_ascii=False) + "\n").encode("utf-8")
        handle = self._segment_handle
        assert handle is not None
        start_offset = handle.tell()
        try:
            written = handle.write(encoded)
            if written != len(encoded):
                raise OSError(
                    "segment short write: "
                    f"expected={len(encoded)} actual={written!r}"
                )
            handle.flush()
            os.fsync(handle.fileno())
        except BaseException as exc:
            rollback_error: BaseException | None = None
            try:
                handle.truncate(start_offset)
                handle.seek(start_offset)
                handle.flush()
                os.fsync(handle.fileno())
            except BaseException as rollback_exc:
                rollback_error = rollback_exc
                self._active_segment_tainted = True
            detail = f"{type(exc).__name__}: {exc}"
            if rollback_error is not None:
                detail += (
                    "; rollback="
                    f"{type(rollback_error).__name__}: {rollback_error}"
                )
            self._write_failure = detail
            raise
        self._record_sequence = next_record_sequence
        self._segment_records += 1
        self._segment_bytes += len(encoded)
        self._segment_first_ingest = self._segment_first_ingest or received_utc
        self._segment_last_ingest = received_utc

    def _reset_active_segment(self) -> None:
        self._segment_open_path = None
        self._segment_started_at = None
        self._segment_first_ingest = None
        self._segment_last_ingest = None
        self._segment_records = 0
        self._segment_bytes = 0
        self._active_segment_tainted = False

    def seal_segment(self, recovery: Mapping[str, object] | None = None) -> Path | None:
        """封口当前非空片段并写逐片段散列清单。"""
        if self._segment_handle is None:
            return None
        handle = self._segment_handle
        try:
            handle.flush()
            os.fsync(handle.fileno())
            handle.close()
        except BaseException as exc:
            self._write_failure = (
                self._write_failure or f"{type(exc).__name__}: {exc}"
            )
            self._active_segment_tainted = True
            try:
                handle.close()
            except BaseException:
                pass
            self._segment_handle = None
            raise
        self._segment_handle = None
        open_path = self._segment_open_path
        assert open_path is not None
        if self._segment_records == 0:
            # 只有首帧写入失败才会留下
            # 空的 active segment。
            # 它没有可声明的 durable
            # record，必须保持 .open
            # 供审计，而不是产生零行 sealed。
            return None
        final_path = open_path.with_suffix("")
        manifest_path = final_path.with_name(
            final_path.name.removesuffix(".jsonl") + ".manifest.json"
        )
        replaced = False
        try:
            os.replace(open_path, final_path)
            replaced = True
            sha256 = _sha256_file(final_path)
            sealed_at = _now().isoformat()
            body: dict[str, object] = {
                "schema_version": SEGMENT_SCHEMA_VERSION,
                "status": (
                    "sealed" if recovery is None else "recovered_incomplete"
                ),
                "completion_claim": recovery is None,
                "artifact_id": f"sha256-{sha256}",
                "sha256": sha256,
                "byte_count": final_path.stat().st_size,
                "record_count": self._segment_records,
                "run_id": self.run_id,
                "segment_sequence": self._segment_sequence,
                "venue_id": self.venue_id,
                "venue_symbol": self.venue_symbol,
                "domain": self.domain,
                "endpoint_id": self.endpoint_id,
                "endpoint_revision": self.endpoint_revision,
                "started_at": (
                    self._segment_started_at.isoformat()
                    if self._segment_started_at is not None else None
                ),
                "first_ingest_time": self._segment_first_ingest,
                "last_ingest_time": self._segment_last_ingest,
                "sealed_at": sealed_at,
                "storage_path": final_path.relative_to(self.data_root).as_posix(),
                **(dict(recovery) if recovery is not None else {}),
            }
            atomic_write_text(
                manifest_path,
                json.dumps(body, ensure_ascii=False, indent=2) + "\n",
            )
        except BaseException as exc:
            rollback_error: BaseException | None = None
            if replaced and final_path.exists() and not manifest_path.exists():
                try:
                    os.replace(final_path, open_path)
                except BaseException as rollback_exc:
                    rollback_error = rollback_exc
            detail = f"{type(exc).__name__}: {exc}"
            if rollback_error is not None:
                detail += (
                    "; final_to_open_rollback="
                    f"{type(rollback_error).__name__}: {rollback_error}"
                )
            self._write_failure = self._write_failure or detail
            self._active_segment_tainted = True
            raise
        body["manifest_path"] = manifest_path.relative_to(self.data_root).as_posix()
        self._segments.append(body)
        self._reset_active_segment()
        callback = self._on_segment_sealed
        if callback is not None:
            callback(body)
        return final_path

    def checkpoint(self, extra: Mapping[str, object] | None = None) -> Path:
        """写运行中状态；不把当前 `.open` 片段声明为完成。"""
        if self._finished:
            raise RuntimeError("已封口的分段写入器不可写 checkpoint")
        if self._write_failure is not None:
            raise RuntimeError(
                f"持久化失败后不可写 checkpoint: {self._write_failure}"
            )
        moment = _now()
        body = {
            "schema_version": SEGMENT_SCHEMA_VERSION,
            "status": "open",
            "run_id": self.run_id,
            "venue_id": self.venue_id,
            "venue_symbol": self.venue_symbol,
            "domain": self.domain,
            "endpoint_id": self.endpoint_id,
            "endpoint_revision": self.endpoint_revision,
            "started_at": self.started_at.isoformat(),
            "checkpoint_at": moment.isoformat(),
            "sealed_segments": len(self._segments),
            "records": self._record_sequence,
            **(dict(extra) if extra is not None else {}),
        }
        path = self._directory / "checkpoint.json"
        atomic_write_text(path, json.dumps(body, ensure_ascii=False, indent=2) + "\n")
        return path

    def finish(
        self,
        extra: Mapping[str, object] | None = None,
        status: str = "complete",
    ) -> Path:
        """封口最后片段并写唯一 run manifest。"""
        if status not in {"complete", "interrupted", "failed"}:
            raise ValueError(f"未知 run 终态: {status}")
        extra_body = dict(extra) if extra is not None else {}
        if self._write_failure is not None:
            status = "failed"
            extra_body.setdefault("writer_failure_detail", self._write_failure)
        signature = (
            status,
            json.dumps(extra_body, ensure_ascii=False, sort_keys=True),
        )
        if self._finished:
            if signature != self._finish_signature:
                raise RuntimeError("分段写入器已以不同终态封口")
            assert self._terminal_manifest is not None
            return self._terminal_manifest
        seal_error: BaseException | None = None
        recovery: Mapping[str, object] | None = None
        if self._write_failure is not None:
            recovery = {
                "recovery_basis": "writer-failure-rolled-back-v1",
                "writer_failure_detail": self._write_failure,
            }
            if self._active_segment_tainted:
                # 无法证明末尾已回滚到
                # durable record 边界，
                # 保留原路径；
                # 静默恢复扫描会逐行验证，绝不在这里封口。
                recovery = None
        if self._active_segment_tainted and self._segment_handle is not None:
            handle = self._segment_handle
            try:
                handle.close()
            except BaseException as close_error:
                extra_body.setdefault(
                    "writer_close_failure_detail",
                    f"{type(close_error).__name__}: {close_error}",
                )
            finally:
                self._segment_handle = None
        try:
            if not self._active_segment_tainted:
                self.seal_segment(recovery=recovery)
        except BaseException as exc:
            seal_error = exc
            status = "failed"
            extra_body.setdefault(
                "writer_failure_detail", f"{type(exc).__name__}: {exc}",
            )
        finished_at = _now().isoformat()
        body = {
            "schema_version": SEGMENT_SCHEMA_VERSION,
            "status": status,
            "completion_claim": status == "complete",
            "run_id": self.run_id,
            "venue_id": self.venue_id,
            "venue_symbol": self.venue_symbol,
            "domain": self.domain,
            "endpoint_id": self.endpoint_id,
            "endpoint_revision": self.endpoint_revision,
            "started_at": self.started_at.isoformat(),
            "finished_at": finished_at,
            "record_count": self._record_sequence,
            "segment_count": len(self._segments),
            "segments": [
                {
                    key: segment[key]
                    for key in (
                        "segment_sequence", "artifact_id", "sha256",
                        "byte_count", "record_count", "storage_path",
                        "manifest_path", "first_ingest_time", "last_ingest_time",
                    )
                }
                for segment in self._segments
            ],
            **extra_body,
        }
        if status == "complete":
            sealed_records = sum(
                int(str(segment["record_count"])) for segment in self._segments
            )
            if (
                sealed_records != self._record_sequence
                or (self._record_sequence > 0 and not self._segments)
            ):
                raise RuntimeError(
                    "complete run 的 durable record/segment 计数不一致"
                )
        path = self._directory / "run.manifest.json"
        if path.exists():
            raise RuntimeError(f"run manifest 已存在: {path}")
        checkpoint = self._directory / "checkpoint.json"
        if checkpoint.exists():
            checkpoint.unlink()
        atomic_write_text(path, json.dumps(body, ensure_ascii=False, indent=2) + "\n")
        self._finished = True
        self._terminal_manifest = path
        self._finish_signature = (
            status,
            json.dumps(extra_body, ensure_ascii=False, sort_keys=True),
        )
        if seal_error is not None:
            raise seal_error
        return path


def recover_open_segments(
    data_root: Path, older_minutes: int = 60, *, domain: str = "book_l2"
) -> tuple[Path, ...]:
    """恢复静默且逐行完整的 `.open` 或缺 manifest 的 final 片段。"""
    if older_minutes <= 0:
        raise ValueError("older_minutes 必须为正数")
    root = data_root / "raw" / "realtime" / _safe(domain, "domain")
    if not root.is_dir():
        return ()
    cutoff = _now() - timedelta(minutes=older_minutes)
    recovered: list[Path] = []
    candidates = [
        (path, True) for path in root.rglob("segment-*.jsonl.open")
    ]
    candidates.extend(
        (path, False)
        for path in root.rglob("segment-*.jsonl")
        if not path.with_name(
            path.name.removesuffix(".jsonl") + ".manifest.json"
        ).exists()
    )
    for candidate_path, is_open in sorted(
        candidates, key=lambda item: item[0].as_posix(),
    ):
        candidate_final = (
            candidate_path.with_suffix("") if is_open else candidate_path
        )
        counterpart_open = Path(f"{candidate_final}.open")
        if (
            (is_open and candidate_final.exists())
            or (not is_open and counterpart_open.exists())
        ):
            # 同一序号同时存在 open/final
            # 是冲突证据，不能猜测覆盖哪份。
            continue
        try:
            modified = datetime.fromtimestamp(candidate_path.stat().st_mtime, UTC)
        except FileNotFoundError:
            continue
        if modified > cutoff:
            continue
        checkpoint_path = candidate_path.parent / "checkpoint.json"
        if checkpoint_path.exists():
            checkpoint_modified = datetime.fromtimestamp(
                checkpoint_path.stat().st_mtime, UTC
            )
            if checkpoint_modified > cutoff:
                # 稀疏流可能暂时没有业务帧。
                # checkpoint 新鲜，跳过。
                continue
        records: list[Mapping[str, object]] = []
        valid = True
        with candidate_path.open("rb") as handle:
            for raw_line in handle:
                if not raw_line.endswith(b"\n"):
                    valid = False
                    break
                try:
                    loaded = json.loads(raw_line)
                except (UnicodeError, json.JSONDecodeError):
                    valid = False
                    break
                if not isinstance(loaded, Mapping):
                    valid = False
                    break
                records.append(loaded)
        if not valid or not records:
            continue
        first = records[0]
        identity_fields = (
            "schema_version", "run_id", "segment_sequence", "venue_id",
            "venue_symbol", "domain", "endpoint_id", "endpoint_revision",
        )
        if any(
            any(row.get(field) != first.get(field) for field in identity_fields)
            for row in records[1:]
        ):
            continue
        final_path = candidate_final
        if is_open:
            os.replace(candidate_path, final_path)
        sha256 = _sha256_file(final_path)
        body = {
            "schema_version": int(str(first.get("schema_version", 1))),
            "status": "recovered_incomplete",
            "completion_claim": False,
            "recovery_basis": (
                "complete-json-lines-after-silence-v1"
                if is_open
                else "orphan-final-without-manifest-after-silence-v1"
            ),
            "artifact_id": f"sha256-{sha256}",
            "sha256": sha256,
            "byte_count": final_path.stat().st_size,
            "record_count": len(records),
            "run_id": first.get("run_id"),
            "segment_sequence": first.get("segment_sequence"),
            "venue_id": first.get("venue_id"),
            "venue_symbol": first.get("venue_symbol"),
            "domain": first.get("domain"),
            "endpoint_id": first.get("endpoint_id"),
            "endpoint_revision": first.get("endpoint_revision"),
            "first_ingest_time": first.get("ingest_time"),
            "last_ingest_time": records[-1].get("ingest_time"),
            "sealed_at": _now().isoformat(),
            "storage_path": final_path.relative_to(data_root).as_posix(),
        }
        manifest_path = final_path.with_name(
            final_path.name.removesuffix(".jsonl") + ".manifest.json"
        )
        if manifest_path.exists():
            continue
        atomic_write_text(
            manifest_path, json.dumps(body, ensure_ascii=False, indent=2) + "\n"
        )
        recovered.append(manifest_path)
    return tuple(recovered)

"""不可回补实时流的 run-scoped 分段原件。

运行中片段使用 ``.open`` 后缀；逐条 fsync。封口后先关闭文件，
再原子改名并写包含 SHA-256 的不可变 manifest。物化只接受封口片段。
"""
from __future__ import annotations

import hashlib
import json
import os
import re
import time
from collections.abc import Callable, Mapping
from datetime import UTC, datetime, timedelta
from pathlib import Path
from typing import BinaryIO

from guvolu.data.durable_io import atomic_write_text
from guvolu.domain.ids import new_run_id

SEGMENT_SCHEMA_VERSION = 3
SEGMENT_DURABILITY_VERSION = "fsync-per-record-v1"
_SAFE_COMPONENT = re.compile(r"^[A-Za-z0-9_.-]+$")


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
        self._segment_sequence += 1
        self._segment_started_at = moment
        self._segment_first_ingest = None
        self._segment_last_ingest = None
        self._segment_records = 0
        self._segment_bytes = 0
        self._segment_open_path = self._directory / (
            f"segment-{self._segment_sequence:06d}.jsonl.open"
        )
        self._segment_handle = self._segment_open_path.open("ab", buffering=0)

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
        moment = _now()
        if self._rotation_due(moment):
            self.seal_segment()
        if self._segment_handle is None:
            self._open_segment(moment)
        self._record_sequence += 1
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
            "record_sequence": self._record_sequence,
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
        handle.write(encoded)
        handle.flush()
        os.fsync(handle.fileno())
        self._segment_records += 1
        self._segment_bytes += len(encoded)
        self._segment_first_ingest = self._segment_first_ingest or received_utc
        self._segment_last_ingest = received_utc

    def seal_segment(self, recovery: Mapping[str, object] | None = None) -> Path | None:
        """封口当前非空片段并写逐片段散列清单。"""
        if self._segment_handle is None:
            return None
        handle = self._segment_handle
        handle.flush()
        os.fsync(handle.fileno())
        handle.close()
        self._segment_handle = None
        open_path = self._segment_open_path
        assert open_path is not None
        final_path = open_path.with_suffix("")
        os.replace(open_path, final_path)
        sha256 = _sha256_file(final_path)
        sealed_at = _now().isoformat()
        body: dict[str, object] = {
            "schema_version": SEGMENT_SCHEMA_VERSION,
            "status": "sealed" if recovery is None else "recovered_incomplete",
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
        manifest_path = final_path.with_name(
            final_path.name.removesuffix(".jsonl") + ".manifest.json"
        )
        atomic_write_text(
            manifest_path, json.dumps(body, ensure_ascii=False, indent=2) + "\n"
        )
        body["manifest_path"] = manifest_path.relative_to(self.data_root).as_posix()
        self._segments.append(body)
        if self._on_segment_sealed is not None:
            self._on_segment_sealed(body)
        self._segment_open_path = None
        self._segment_started_at = None
        self._segment_records = 0
        self._segment_bytes = 0
        return final_path

    def checkpoint(self, extra: Mapping[str, object] | None = None) -> Path:
        """写运行中状态；不把当前 `.open` 片段声明为完成。"""
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
        if self._finished:
            raise RuntimeError("分段写入器已经封口")
        self.seal_segment()
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
            **(dict(extra) if extra is not None else {}),
        }
        path = self._directory / "run.manifest.json"
        if path.exists():
            raise RuntimeError(f"run manifest 已存在: {path}")
        atomic_write_text(path, json.dumps(body, ensure_ascii=False, indent=2) + "\n")
        checkpoint = self._directory / "checkpoint.json"
        if checkpoint.exists():
            checkpoint.unlink()
        self._finished = True
        return path


def recover_open_segments(
    data_root: Path, older_minutes: int = 60, *, domain: str = "book_l2"
) -> tuple[Path, ...]:
    """只封口静默且逐行 JSON 完整的崩溃片段。"""
    if older_minutes <= 0:
        raise ValueError("older_minutes 必须为正数")
    root = data_root / "raw" / "realtime" / _safe(domain, "domain")
    if not root.is_dir():
        return ()
    cutoff = _now() - timedelta(minutes=older_minutes)
    recovered: list[Path] = []
    for open_path in sorted(root.rglob("segment-*.jsonl.open")):
        modified = datetime.fromtimestamp(open_path.stat().st_mtime, UTC)
        if modified > cutoff:
            continue
        checkpoint_path = open_path.parent / "checkpoint.json"
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
        with open_path.open("rb") as handle:
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
        final_path = open_path.with_suffix("")
        os.replace(open_path, final_path)
        sha256 = _sha256_file(final_path)
        body = {
            "schema_version": int(str(first.get("schema_version", 1))),
            "status": "recovered_incomplete",
            "completion_claim": False,
            "recovery_basis": "complete-json-lines-after-silence-v1",
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
        atomic_write_text(
            manifest_path, json.dumps(body, ensure_ascii=False, indent=2) + "\n"
        )
        recovered.append(manifest_path)
    return tuple(recovered)

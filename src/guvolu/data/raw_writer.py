"""raw 层写入：行格式与运行清单（storage-design 第 3、4 节）。

行只追加、错误照常落盘、不去重（D-02）；
每次运行一份 manifest，计数与时段可审计（D-09）。
"""
from __future__ import annotations

import json
import threading
from collections import Counter
from collections.abc import Mapping
from datetime import UTC, datetime
from pathlib import Path

from guvolu.data.durable_io import atomic_write_text, durable_append_bytes
from guvolu.domain.ids import new_run_id

RAW_SCHEMA_VERSION = 1
RAW_DURABILITY_VERSION = "fsync-per-record-v1"


def _now() -> datetime:
    return datetime.now(UTC)


class RawWriter:
    """按日期目录与端点文件追加 raw 行。"""

    def __init__(self, data_root: Path, run_id: str | None = None) -> None:
        self.data_root = data_root
        self.run_id = run_id if run_id is not None else new_run_id()
        self.started_at = _now()
        self._counts: Counter[str] = Counter()
        self._lock = threading.Lock()
        self._finished = False
        self._checkpoint_sequence = 0

    def _write(self, name: str, record: dict[str, object]) -> None:
        if self._finished:
            raise RuntimeError("已封口的 RawWriter 不可继续写入")
        moment = _now()
        record = {
            "schema_version": RAW_SCHEMA_VERSION,
            "durability_version": RAW_DURABILITY_VERSION,
            "run_id": self.run_id,
            **record,
            "ingest_time": moment.isoformat(),
        }
        directory = self.data_root / "raw" / moment.strftime("%Y-%m-%d")
        line = json.dumps(record, ensure_ascii=False, default=str)
        with self._lock:
            directory.mkdir(parents=True, exist_ok=True)
            target = directory / f"{name}.jsonl"
            target.parent.mkdir(parents=True, exist_ok=True)
            # fsync 成功后才推进计数。
            # 盘口流不可由来源回补。
            durable_append_bytes(
                target, (line + "\n").encode("utf-8")
            )
            self._counts[name] += 1

    def rest(
        self,
        name: str,
        source: str,
        method: str,
        path: str,
        params: dict[str, object] | None,
        http_status: int | None,
        payload: object,
        latency_ms: float,
        network_error: str | None = None,
    ) -> None:
        """记录一次 REST 请求与响应包络原文。"""
        self._write(
            name,
            {
                "source": source,
                "method": method,
                "path": path,
                "params": params,
                "latency_ms": round(latency_ms, 1),
                "http_status": http_status,
                "payload": payload,
                "network_error": network_error,
            },
        )

    def ws(self, name: str, channel: str, symbol: str | None, payload: object) -> None:
        """记录已解析 WS 报文；保留给重放、测试与旧调用方。"""
        self._write(
            name,
            {"source": "ws_public", "channel": channel, "symbol": symbol, "payload": payload},
        )

    def ws_frame(self, name: str, payload_raw: str) -> None:
        """在任何业务解析前持久化交易所 WS 原始文本。"""
        self._write(
            name,
            {"source": "ws_public", "payload_raw": payload_raw},
        )

    def checkpoint(self, extra: dict[str, object] | None = None) -> Path:
        """写不可变运行中检查点；它绝不是终态 manifest。"""
        if self._finished:
            raise RuntimeError("已封口的 RawWriter 不可创建检查点")
        self._checkpoint_sequence += 1
        moment = _now()
        checkpoint = {
            "schema_version": RAW_SCHEMA_VERSION,
            "durability_version": RAW_DURABILITY_VERSION,
            "status": "open",
            "checkpoint_sequence": self._checkpoint_sequence,
            "run_id": self.run_id,
            "started_at": self.started_at.isoformat(),
            "checkpoint_at": moment.isoformat(),
            "record_counts": dict(self._counts),
            **(extra or {}),
        }
        directory = self.data_root / "raw" / moment.strftime("%Y-%m-%d")
        directory.mkdir(parents=True, exist_ok=True)
        path = directory / (
            f"checkpoint-{self.run_id}-{self._checkpoint_sequence:06d}.json"
        )
        atomic_write_text(path, json.dumps(checkpoint, ensure_ascii=False, indent=2) + "\n")
        return path

    def finish(self, extra: dict[str, object] | None = None) -> Path:
        """封口并写唯一终态 manifest，之后拒绝继续写入。"""
        if self._finished:
            raise RuntimeError("RawWriter 已封口")
        moment = _now()
        manifest = {
            "schema_version": RAW_SCHEMA_VERSION,
            "durability_version": RAW_DURABILITY_VERSION,
            "status": "complete",
            "run_id": self.run_id,
            "started_at": self.started_at.isoformat(),
            "finished_at": moment.isoformat(),
            "record_counts": dict(self._counts),
            **(extra or {}),
        }
        directory = self.data_root / "raw" / moment.strftime("%Y-%m-%d")
        directory.mkdir(parents=True, exist_ok=True)
        path = directory / f"manifest-{self.run_id}.json"
        if path.exists():
            raise RuntimeError(f"终态 manifest 已存在: {path}")
        atomic_write_text(
            path, json.dumps(manifest, ensure_ascii=False, indent=2) + "\n"
        )
        self._finished = True
        return path


def reconcile_unfinished_runs(
    data_root: Path, older_minutes: int = 60
) -> tuple[Path, ...]:
    """为已停止但缺终态的 run 追加恢复清单。"""
    if older_minutes <= 0:
        raise ValueError("最小静默分钟数必须为正")
    root = data_root / "raw"
    if not root.is_dir():
        return ()
    terminal: set[str] = set()
    for path in [*root.rglob("manifest-*.json"), *root.rglob("checkpoint-*.json")]:
        try:
            loaded = json.loads(path.read_text(encoding="utf-8"))
        except (OSError, UnicodeError, json.JSONDecodeError):
            continue
        if not isinstance(loaded, Mapping):
            continue
        run_id = loaded.get("run_id")
        if not isinstance(run_id, str) or not run_id:
            continue
        if loaded.get("status") == "open" or loaded.get("heartbeat") is True:
            continue
        terminal.add(run_id)

    counts: dict[str, Counter[str]] = {}
    first_seen: dict[str, str] = {}
    last_seen: dict[str, str] = {}
    last_directory: dict[str, Path] = {}
    durable: dict[str, bool] = {}
    for path in sorted(root.rglob("*.jsonl")):
        relative = path.relative_to(root)
        if len(relative.parts) < 2:
            continue
        name = Path(*relative.parts[1:]).as_posix().removesuffix(".jsonl")
        with path.open("rb") as handle:
            for raw_line in handle:
                try:
                    record = json.loads(raw_line)
                except (UnicodeError, json.JSONDecodeError):
                    continue
                if not isinstance(record, Mapping):
                    continue
                run_id = record.get("run_id")
                ingest_time = record.get("ingest_time")
                if not isinstance(run_id, str) or not isinstance(ingest_time, str):
                    continue
                counts.setdefault(run_id, Counter())[name] += 1
                first_seen[run_id] = min(first_seen.get(run_id, ingest_time), ingest_time)
                if ingest_time >= last_seen.get(run_id, ""):
                    last_seen[run_id] = ingest_time
                    last_directory[run_id] = path.parent
                is_durable = record.get("durability_version") == RAW_DURABILITY_VERSION
                durable[run_id] = durable.get(run_id, True) and is_durable

    cutoff = _now().timestamp() - older_minutes * 60
    written: list[Path] = []
    for run_id in sorted(counts):
        if run_id in terminal:
            continue
        finished_at = datetime.fromisoformat(last_seen[run_id])
        if finished_at.timestamp() > cutoff:
            continue
        directory = last_directory[run_id]
        target = directory / f"manifest-{run_id}.json"
        if target.exists():
            target = directory / f"manifest-{run_id}-reconciled.json"
        if target.exists():
            raise RuntimeError(f"恢复清单已存在但未被识别: {target}")
        body = {
            "schema_version": RAW_SCHEMA_VERSION,
            "durability_version": (
                RAW_DURABILITY_VERSION if durable[run_id] else "legacy-unproven"
            ),
            "status": "recovered_incomplete",
            "completion_claim": False,
            "recovery_basis": "valid-raw-line-reconciliation-v1",
            "run_id": run_id,
            "started_at": first_seen[run_id],
            "finished_at": last_seen[run_id],
            "reconciled_at": _now().isoformat(),
            "record_counts": dict(sorted(counts[run_id].items())),
        }
        atomic_write_text(
            target, json.dumps(body, ensure_ascii=False, indent=2) + "\n"
        )
        written.append(target)
    return tuple(written)

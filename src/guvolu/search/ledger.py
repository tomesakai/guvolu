"""试验台账：每候选一行追加式 JSONL，内容寻址制品，不写 SQLite（G-07）。"""
from __future__ import annotations

import json
from collections.abc import Iterable, Mapping, Sequence
from dataclasses import dataclass
from pathlib import Path

from guvolu.data.durable_io import durable_append_bytes
from guvolu.search.identity import canonical_json, sha256_bytes

LEDGER_SCHEMA_VERSION = 1
LEDGER_METHOD_VERSION = "searchfast-trial-ledger-v1"
STAGE_F0_REJECTED = "F0_rejected"
STAGE_F1_SCREENED = "F1_screened"
STAGE_F3_EXACT = "F3_exact"
STAGES = (STAGE_F0_REJECTED, STAGE_F1_SCREENED, STAGE_F3_EXACT)
PARTIAL_NAME = "trial-ledger.partial.jsonl"


@dataclass(frozen=True)
class LedgerRow:
    """一条候选评估事实。"""

    evaluation_id: str
    candidate_id: str
    family: str
    bundle_id: str
    stage: str
    device: str
    precision: str
    metrics: Mapping[str, float | int] | None
    parity: Mapping[str, object] | None
    screen_passed: bool | None
    promotable: bool
    reason: str | None = None
    resample: Mapping[str, object] | None = None

    def payload(self) -> Mapping[str, object]:
        """生成台账行。"""
        if self.stage not in STAGES:
            raise ValueError(f"台账阶段不受支持: {self.stage}")
        row: dict[str, object] = {
            "record_type": "search_trial",
            "evaluation_id": self.evaluation_id,
            "candidate_id": self.candidate_id,
            "family": self.family,
            "bundle_id": self.bundle_id,
            "stage": self.stage,
            "device": self.device,
            "precision": self.precision,
            "metrics": None if self.metrics is None else dict(self.metrics),
            "parity": None if self.parity is None else dict(self.parity),
            "screen_passed": self.screen_passed,
            "promotable": self.promotable,
            "reason": self.reason,
        }
        if self.resample is not None:
            row["resample"] = dict(self.resample)
        return row


def ledger_header(
    bundle_id: str,
    identity: Mapping[str, object],
    runtime: Mapping[str, object],
    extra: Mapping[str, object] | None = None,
) -> Mapping[str, object]:
    """生成台账首行。"""
    header: dict[str, object] = {
        "record_type": "search_trial_ledger_header",
        "schema_version": LEDGER_SCHEMA_VERSION,
        "ledger_method_version": LEDGER_METHOD_VERSION,
        "bundle_id": bundle_id,
        "search_bundle_identity": dict(identity),
        "runtime": dict(runtime),
    }
    if extra:
        header.update(dict(extra))
    return header


class TrialLedgerWriter:
    """追加式 JSONL 写入器，完成后按内容散列命名。"""

    def __init__(self, directory: Path) -> None:
        self.directory = directory
        self.partial_path = directory / PARTIAL_NAME
        self.rows_written = 0
        if self.partial_path.exists():
            raise FileExistsError(f"台账已存在未完成文件: {self.partial_path}")

    def append_header(self, header: Mapping[str, object]) -> None:
        """写入首行。"""
        self._append([header])

    def append_rows(self, rows: Iterable[LedgerRow]) -> None:
        """追加候选行。"""
        payloads = [row.payload() for row in rows]
        self._append(payloads)
        self.rows_written += len(payloads)

    def _append(self, payloads: Sequence[Mapping[str, object]]) -> None:
        """持久追加多行。"""
        if not payloads:
            return
        body = "".join(canonical_json(item) + "\n" for item in payloads)
        durable_append_bytes(self.partial_path, body.encode("utf-8"))

    def finalize(self) -> tuple[Path, str]:
        """按内容散列重命名，返回路径与散列。"""
        body = self.partial_path.read_bytes()
        digest = sha256_bytes(body)
        final_path = self.directory / f"trial-ledger-{digest}.jsonl"
        if final_path.exists():
            self.partial_path.unlink()
        else:
            self.partial_path.replace(final_path)
        return final_path, digest


def read_ledger(path: Path) -> tuple[Mapping[str, object], tuple[Mapping[str, object], ...]]:
    """读取台账首行与候选行，并校验文件名散列。"""
    body = path.read_bytes()
    expected = path.name.removeprefix("trial-ledger-").removesuffix(".jsonl")
    if sha256_bytes(body) != expected:
        raise ValueError("台账文件散列与文件名不一致")
    lines = [line for line in body.decode("utf-8").splitlines() if line]
    if not lines:
        raise ValueError("台账为空")
    header = json.loads(lines[0])
    if not isinstance(header, dict) or header.get(
        "record_type",
    ) != "search_trial_ledger_header":
        raise ValueError("台账首行非法")
    rows: list[Mapping[str, object]] = []
    for line in lines[1:]:
        row = json.loads(line)
        if not isinstance(row, dict) or row.get("record_type") != "search_trial":
            raise ValueError("台账行非法")
        rows.append(row)
    return header, tuple(rows)

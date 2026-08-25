"""管理 research-only 经济观测、PIT 语境与搜索提案。"""
from __future__ import annotations

import argparse
import json
import os
from collections.abc import Mapping, Sequence
from datetime import UTC, datetime
from pathlib import Path

from guvolu.research.economic_agent import (
    append_economic_observations,
    build_economic_context,
    load_content_addressed_artifact,
    load_economic_observation_snapshot,
    load_economic_observations,
    load_economic_policy,
    run_economic_research_agent,
    verify_economic_agent_ledger,
    write_content_addressed_artifact,
)
from guvolu.research.provenance import canonical_json


_MAX_INGEST_BATCH_RECORDS = 10_000
_MAX_INGEST_BATCH_BYTES = 16 * 1024 * 1024
_MAX_PROPOSAL_BYTES_PER_RECORD = 64 * 1024
_MAX_PROPOSAL_BATCH_BYTES = 8 * 1024 * 1024
_PROPOSAL_BATCH_OVERHEAD_BYTES = 4096


def _root_path(root: Path, path: Path, name: str) -> Path:
    candidate = path if path.is_absolute() else root / path
    resolved = candidate.resolve(strict=False)
    try:
        resolved.relative_to(root)
    except ValueError as error:
        raise ValueError(f"{name} 必须位于 --root 内") from error
    return resolved


def _write_path(
    root: Path,
    path: Path,
    allowed: Path,
    name: str,
) -> Path:
    candidate = path if path.is_absolute() else root / path
    normalized = Path(os.path.normpath(str(candidate)))
    if os.path.normcase(str(candidate)) != os.path.normcase(str(normalized)):
        raise ValueError(f"{name} 不得使用 . 或 .. 路径别名")
    lexical = Path(os.path.abspath(candidate))
    resolved = candidate.resolve(strict=False)
    if os.path.normcase(str(lexical)) != os.path.normcase(str(resolved)):
        raise ValueError(f"{name} 不得使用符号链接或 junction 别名")
    allowed_lexical = Path(os.path.abspath(root / allowed))
    allowed_resolved = (root / allowed).resolve(strict=False)
    if os.path.normcase(str(allowed_lexical)) != os.path.normcase(
        str(allowed_resolved),
    ):
        raise ValueError("允许写入根不得使用符号链接或 junction")
    try:
        resolved.relative_to(root)
    except ValueError as error:
        raise ValueError(f"{name} 必须位于 --root 内") from error
    try:
        resolved.relative_to(allowed_lexical)
    except ValueError as error:
        raise ValueError(f"{name} 越出允许写入目录 {allowed.as_posix()}") from error
    return resolved


def _ledger_path(
    root: Path,
    path: Path,
    name: str,
    *,
    write: bool,
) -> Path:
    candidate = path if path.is_absolute() else root / path
    normalized = Path(os.path.normpath(str(candidate)))
    if os.path.normcase(str(candidate)) != os.path.normcase(str(normalized)):
        raise ValueError(f"{name} 不得使用 . 或 .. 路径别名")
    resolved = candidate.resolve(strict=False)
    if os.path.normcase(str(candidate)) != os.path.normcase(str(resolved)):
        raise ValueError(f"{name} 不得使用符号链接或 junction 别名")
    if write:
        return _write_path(
            root,
            resolved,
            Path("data/research/economic"),
            name,
        )
    return _root_path(root, resolved, name)


def _time(value: str) -> datetime:
    parsed = datetime.fromisoformat(value.replace("Z", "+00:00"))
    if parsed.tzinfo is None or parsed.utcoffset() is None:
        raise ValueError("时间必须携带时区")
    return parsed.astimezone(UTC)


def _mapping(value: object, name: str) -> Mapping[str, object]:
    if not isinstance(value, dict):
        raise ValueError(f"{name} 必须为 JSON 对象")
    return value


def _records(
    path: Path,
    name: str,
    *,
    max_records: int,
    max_bytes: int,
) -> tuple[Mapping[str, object], ...]:
    """Read one strictly bounded JSON/JSONL batch before any persistence."""
    if max_records <= 0 or max_bytes <= 0:
        raise ValueError(f"{name} 读取配额非法")
    try:
        declared_size = path.stat().st_size
        if declared_size > max_bytes:
            raise ValueError(f"{name} 超出 {max_bytes} bytes 输入上限")
        with path.open("rb") as handle:
            body = handle.read(max_bytes + 1)
            if len(body) > max_bytes or handle.read(1):
                raise ValueError(f"{name} 超出 {max_bytes} bytes 输入上限")
        text = body.decode("utf-8")
    except (OSError, UnicodeDecodeError) as error:
        raise ValueError(f"{name} 无法读取为有界 UTF-8 输入") from error

    def checked(rows: Sequence[Mapping[str, object]]) -> tuple[Mapping[str, object], ...]:
        if len(rows) > max_records:
            raise ValueError(f"{name} 超出 {max_records} records 输入上限")
        return tuple(rows)

    try:
        loaded: object = json.loads(text)
    except json.JSONDecodeError:
        rows: list[Mapping[str, object]] = []
        for line_number, line in enumerate(text.splitlines(), start=1):
            if not line:
                continue
            try:
                row: object = json.loads(line)
            except json.JSONDecodeError as error:
                raise ValueError(f"{name} 第 {line_number} 行非法") from error
            rows.append(_mapping(row, f"{name}[{line_number}]"))
            if len(rows) > max_records:
                raise ValueError(f"{name} 超出 {max_records} records 输入上限")
        return checked(rows)
    if isinstance(loaded, list):
        if len(loaded) > max_records:
            raise ValueError(f"{name} 超出 {max_records} records 输入上限")
        return checked([_mapping(item, f"{name}[]") for item in loaded])
    return checked((_mapping(loaded, name),))


def _parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        description="research-only 经济研究代理（无网络/密钥/交易权限）",
    )
    parser.add_argument("--root", type=Path, default=Path.cwd())
    subparsers = parser.add_subparsers(dest="command", required=True)

    ingest = subparsers.add_parser("ingest", help="追加经济观测")
    ingest.add_argument("--input", type=Path, required=True)
    ingest.add_argument(
        "--ledger",
        type=Path,
        default=Path("data/research/economic/observations.jsonl"),
    )

    context = subparsers.add_parser("context", help="生成内容寻址 PIT 语境")
    context.add_argument(
        "--ledger",
        type=Path,
        default=Path("data/research/economic/observations.jsonl"),
    )
    context.add_argument("--policy", type=Path, required=True)
    context.add_argument("--decision-time", required=True)
    context.add_argument(
        "--output",
        type=Path,
        default=Path("reports/economic-research/contexts"),
    )

    propose = subparsers.add_parser("propose", help="审计并输出 proposal-only 制品")
    propose.add_argument("--context", type=Path, required=True)
    propose.add_argument("--proposals", type=Path, required=True)
    propose.add_argument("--policy", type=Path, required=True)
    propose.add_argument(
        "--observation-ledger",
        type=Path,
        default=Path("data/research/economic/observations.jsonl"),
    )
    propose.add_argument(
        "--ledger",
        type=Path,
        default=Path("data/research/economic/agent-runs.jsonl"),
    )
    propose.add_argument(
        "--output",
        type=Path,
        default=Path("reports/economic-research"),
    )

    verify = subparsers.add_parser("verify", help="校验观测与运行台账")
    verify.add_argument(
        "--observation-ledger",
        type=Path,
        default=Path("data/research/economic/observations.jsonl"),
    )
    verify.add_argument(
        "--agent-ledger",
        type=Path,
        default=Path("data/research/economic/agent-runs.jsonl"),
    )
    verify.add_argument("--policy", type=Path, required=True)
    verify.add_argument(
        "--output",
        type=Path,
        default=Path("reports/economic-research"),
    )
    return parser


def main(argv: Sequence[str] | None = None) -> int:
    """执行经济研究代理的显式子命令。"""
    arguments = _parser().parse_args(argv)
    root = arguments.root.resolve()
    if not root.is_dir():
        raise ValueError(f"--root 不是目录: {root}")
    command = str(arguments.command)
    if command == "ingest":
        source = _root_path(root, arguments.input, "ingest input")
        ledger = _ledger_path(
            root,
            arguments.ledger,
            "observation ledger",
            write=True,
        )
        observations = append_economic_observations(
            ledger,
            _records(
                source,
                "economic observations",
                max_records=_MAX_INGEST_BATCH_RECORDS,
                max_bytes=_MAX_INGEST_BATCH_BYTES,
            ),
        )
        print(canonical_json({
            "command": command,
            "ledger": ledger.as_posix(),
            "appended": len(observations),
            "observation_ids": [item.observation_id for item in observations],
            "research_only": True,
        }))
        return 0
    if command == "context":
        ledger = _ledger_path(
            root,
            arguments.ledger,
            "observation ledger",
            write=False,
        )
        policy = load_economic_policy(_root_path(root, arguments.policy, "policy"))
        artifact = build_economic_context(
            load_economic_observation_snapshot(ledger),
            _time(arguments.decision_time),
            policy,
        )
        path = write_content_addressed_artifact(
            _write_path(
                root,
                arguments.output,
                Path("reports/economic-research"),
                "context output",
            ),
            artifact,
            "economic-context",
        )
        print(canonical_json({
            "command": command,
            "artifact_id": artifact["artifact_id"],
            "path": path.as_posix(),
            "research_only": True,
        }))
        return 0
    if command == "propose":
        context_path = _root_path(root, arguments.context, "context")
        policy = load_economic_policy(_root_path(root, arguments.policy, "policy"))
        proposal_rows = _records(
            _root_path(root, arguments.proposals, "research proposals"),
            "research proposals",
            max_records=policy.proposal_gate.max_proposals_per_run,
            max_bytes=min(
                _MAX_PROPOSAL_BATCH_BYTES,
                _PROPOSAL_BATCH_OVERHEAD_BYTES
                + policy.proposal_gate.max_proposals_per_run
                * _MAX_PROPOSAL_BYTES_PER_RECORD,
            ),
        )
        result = run_economic_research_agent(
            context=load_content_addressed_artifact(
                context_path, "economic-context",
            ),
            proposals=proposal_rows,
            policy=policy,
            observation_ledger_path=_ledger_path(
                root,
                arguments.observation_ledger,
                "observation ledger",
                write=False,
            ),
            output=_write_path(
                root,
                arguments.output,
                Path("reports/economic-research"),
                "proposal output",
            ),
            ledger_path=_ledger_path(
                root,
                arguments.ledger,
                "agent ledger",
                write=True,
            ),
        )
        print(canonical_json({
            "command": command,
            "run_id": result.run_id,
            "ledger": result.ledger_path.as_posix(),
            "receipt_storage": "embedded_in_ledger",
            "proposal_paths": [path.as_posix() for path in result.proposal_paths],
            "accepted_proposal_ids": list(result.accepted_proposal_ids),
            "rejected_count": result.rejected_count,
            "research_only": True,
        }))
        return 0
    observation_ledger = _ledger_path(
        root,
        arguments.observation_ledger,
        "observation ledger",
        write=False,
    )
    agent_ledger = _ledger_path(
        root,
        arguments.agent_ledger,
        "agent ledger",
        write=False,
    )
    policy = load_economic_policy(_root_path(root, arguments.policy, "policy"))
    output = _root_path(root, arguments.output, "economic agent output")
    observations = load_economic_observations(observation_ledger)
    runs = verify_economic_agent_ledger(
        agent_ledger,
        observation_ledger_path=observation_ledger,
        output=output,
        policy=policy,
    )
    print(canonical_json({
        "command": "verify",
        "observation_count": len(observations),
        "agent_run_count": len(runs),
        "research_only": True,
    }))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())

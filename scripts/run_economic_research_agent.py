"""管理 research-only 经济观测、PIT 语境与搜索提案。"""
from __future__ import annotations

import argparse
import json
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


def _resolve(root: Path, path: Path) -> Path:
    return path if path.is_absolute() else root / path


def _time(value: str) -> datetime:
    parsed = datetime.fromisoformat(value.replace("Z", "+00:00"))
    if parsed.tzinfo is None or parsed.utcoffset() is None:
        raise ValueError("时间必须携带时区")
    return parsed.astimezone(UTC)


def _mapping(value: object, name: str) -> Mapping[str, object]:
    if not isinstance(value, dict):
        raise ValueError(f"{name} 必须为 JSON 对象")
    return value


def _records(path: Path, name: str) -> tuple[Mapping[str, object], ...]:
    text = path.read_text(encoding="utf-8")
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
        return tuple(rows)
    if isinstance(loaded, list):
        return tuple(_mapping(item, f"{name}[]") for item in loaded)
    return (_mapping(loaded, name),)


def _optional_mapping(path: Path | None, name: str) -> Mapping[str, object] | None:
    if path is None:
        return None
    loaded: object = json.loads(path.read_text(encoding="utf-8"))
    return _mapping(loaded, name)


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
    propose.add_argument("--inference-identity", type=Path)
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
    return parser


def main(argv: Sequence[str] | None = None) -> int:
    """执行经济研究代理的显式子命令。"""
    arguments = _parser().parse_args(argv)
    root = arguments.root.resolve()
    command = str(arguments.command)
    if command == "ingest":
        source = _resolve(root, arguments.input)
        ledger = _resolve(root, arguments.ledger)
        observations = append_economic_observations(
            ledger, _records(source, "economic observations"),
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
        ledger = _resolve(root, arguments.ledger)
        policy = load_economic_policy(_resolve(root, arguments.policy))
        artifact = build_economic_context(
            load_economic_observation_snapshot(ledger),
            _time(arguments.decision_time),
            policy,
        )
        path = write_content_addressed_artifact(
            _resolve(root, arguments.output), artifact, "economic-context",
        )
        print(canonical_json({
            "command": command,
            "artifact_id": artifact["artifact_id"],
            "path": path.as_posix(),
            "research_only": True,
        }))
        return 0
    if command == "propose":
        context_path = _resolve(root, arguments.context)
        policy = load_economic_policy(_resolve(root, arguments.policy))
        inference_path = (
            None
            if arguments.inference_identity is None
            else _resolve(root, arguments.inference_identity)
        )
        result = run_economic_research_agent(
            context=load_content_addressed_artifact(
                context_path, "economic-context",
            ),
            proposals=_records(
                _resolve(root, arguments.proposals), "research proposals",
            ),
            policy=policy,
            observation_ledger_path=_resolve(root, arguments.observation_ledger),
            output=_resolve(root, arguments.output),
            ledger_path=_resolve(root, arguments.ledger),
            inference_identity=_optional_mapping(
                inference_path, "inference identity",
            ),
        )
        print(canonical_json({
            "command": command,
            "run_id": result.run_id,
            "receipt": result.receipt_path.as_posix(),
            "proposal_paths": [path.as_posix() for path in result.proposal_paths],
            "accepted_proposal_ids": list(result.accepted_proposal_ids),
            "rejected_count": result.rejected_count,
            "research_only": True,
        }))
        return 0
    observation_ledger = _resolve(root, arguments.observation_ledger)
    agent_ledger = _resolve(root, arguments.agent_ledger)
    observations = load_economic_observations(observation_ledger)
    runs = verify_economic_agent_ledger(agent_ledger)
    print(canonical_json({
        "command": "verify",
        "observation_count": len(observations),
        "agent_run_count": len(runs),
        "research_only": True,
    }))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())

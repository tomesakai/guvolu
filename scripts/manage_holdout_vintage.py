"""管理一次性封存研究数据段。"""
from __future__ import annotations

import argparse
import json
from dataclasses import asdict
from pathlib import Path
from typing import Sequence

from guvolu.research.governance import (
    consume_holdout_vintage,
    finalize_holdout_evaluation,
    list_holdout_vintages,
    seal_holdout_vintage,
)
from guvolu.research.panel import parse_time


def _payload(value: object) -> object:
    """把 dataclass 中的时间转换为 JSON 文本。"""
    if isinstance(value, tuple):
        return [_payload(item) for item in value]
    raw = asdict(value)  # type: ignore[call-overload]
    return {
        key: item.isoformat() if hasattr(item, "isoformat") else item
        for key, item in raw.items()
    }


def main(argv: Sequence[str] | None = None) -> int:
    """执行封存、消费、原子终结或只读列表。"""
    parser = argparse.ArgumentParser(description="管理 G-08 一次性封存段")
    parser.add_argument(
        "--registry",
        type=Path,
        default=Path("data/research/governance.sqlite3"),
    )
    subparsers = parser.add_subparsers(dest="action", required=True)
    seal = subparsers.add_parser("seal", help="封存从未被自适应研究读取的区间")
    seal.add_argument("market_id")
    seal.add_argument("start_time")
    seal.add_argument("end_time")
    consume = subparsers.add_parser("consume", help="原子且永久地消费一次封存段")
    consume.add_argument("vintage_id")
    consume.add_argument("candidate_set_hash")
    consume.add_argument("evaluation_id")
    finalize = subparsers.add_parser(
        "finalize",
        help="在同一事务内登记结论并完成评估尝试",
    )
    finalize.add_argument("vintage_id")
    finalize.add_argument("evaluation_id")
    finalize.add_argument("verdict")
    finalize.add_argument("result_manifest_path")
    finalize.add_argument("result_manifest_sha256")
    subparsers.add_parser("list", help="列出封存与已消费历史")
    arguments = parser.parse_args(argv)
    registry = arguments.registry.resolve()
    if arguments.action == "seal":
        result: object = seal_holdout_vintage(
            registry,
            arguments.market_id,
            parse_time(arguments.start_time, "start_time"),
            parse_time(arguments.end_time, "end_time"),
        )
    elif arguments.action == "consume":
        result = consume_holdout_vintage(
            registry,
            arguments.vintage_id,
            arguments.candidate_set_hash,
            arguments.evaluation_id,
        )
    elif arguments.action == "finalize":
        result = finalize_holdout_evaluation(
            registry,
            arguments.vintage_id,
            arguments.evaluation_id,
            arguments.verdict,
            arguments.result_manifest_path,
            arguments.result_manifest_sha256,
        )
    else:
        result = list_holdout_vintages(registry)
    print(json.dumps(_payload(result), ensure_ascii=False, sort_keys=True))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())

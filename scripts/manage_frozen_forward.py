"""冻结前向计划并按新数据追加不可变预测。"""
from __future__ import annotations

import argparse
import json
from dataclasses import asdict
from pathlib import Path
from typing import Sequence

from guvolu.research.frozen_forward import (
    FrozenForwardVerification,
    FrozenPlanResult,
    FrozenPredictionResult,
    freeze_forward_plan,
    run_frozen_forward_prediction,
    verify_frozen_forward,
)

_Result = FrozenPlanResult | FrozenPredictionResult | FrozenForwardVerification


def _payload(value: _Result) -> dict[str, object]:
    raw = asdict(value)
    return {
        key: item.isoformat() if hasattr(item, "isoformat") else str(item)
        if isinstance(item, Path) else item
        for key, item in raw.items()
    }


def main(argv: Sequence[str] | None = None) -> int:
    """执行 plan 或 predict 子命令。"""
    parser = argparse.ArgumentParser(description="管理 FROZEN_FORWARD 前向预测")
    parser.add_argument("--root", type=Path, default=Path.cwd())
    subparsers = parser.add_subparsers(dest="action", required=True)
    plan = subparsers.add_parser("plan", help="在 vintage 开始前冻结候选和资金权重")
    plan.add_argument("vintage_id")
    plan.add_argument("source_summary", type=Path)
    plan.add_argument(
        "--config", type=Path, default=Path("config/strategy_research.json"),
    )
    plan.add_argument(
        "--missing-policy",
        choices=("burn", "zero_exposure"),
        default="burn",
        help="缺预测处置政策：burn 烧毁 vintage，zero_exposure 缺柱记零暴露",
    )
    predict = subparsers.add_parser("predict", help="为最新完整决策柱追加不可变预测")
    predict.add_argument("plan_id")
    predict.add_argument(
        "--registry", type=Path,
        default=Path("data/research/governance.sqlite3"),
    )
    verify = subparsers.add_parser("verify", help="复核计划及全部不可变预测")
    verify.add_argument("plan_id")
    verify.add_argument(
        "--registry", type=Path,
        default=Path("data/research/governance.sqlite3"),
    )
    arguments = parser.parse_args(argv)
    root = arguments.root.resolve()
    result: _Result
    if arguments.action == "plan":
        result = freeze_forward_plan(
            root,
            arguments.config,
            arguments.source_summary,
            arguments.vintage_id,
            missing_policy=arguments.missing_policy,
        )
    elif arguments.action == "predict":
        result = run_frozen_forward_prediction(
            root,
            arguments.plan_id,
            registry_path=arguments.registry,
        )
    else:
        result = verify_frozen_forward(
            root,
            arguments.plan_id,
            registry_path=arguments.registry,
        )
    print(json.dumps(_payload(result), ensure_ascii=False, sort_keys=True))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())

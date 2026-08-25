"""只读输出行业级策略准入检查结果。"""
from __future__ import annotations

import argparse
import json
import os
from pathlib import Path
from typing import Sequence

from guvolu.research.industry_readiness import industry_strategy_readiness
from guvolu.research.provenance import canonical_json

_TRADE_ENVIRONMENT_NAMES = (
    "GMO_COIN_TRADE_API_KEY",
    "GMO_COIN_TRADE_API_SECRET",
    "BITFLYER_TRADE_API_KEY",
    "BITFLYER_TRADE_API_SECRET",
)


def _failure_report(code: str, detail: str) -> dict[str, object]:
    """配置或输入合同异常时生成失败关闭结果。"""
    return {
        "schema_version": 1,
        "method_version": "industry-strategy-readiness-v4",
        "verdict": "NOT_READY",
        "technically_ready_for_external_live_approval": False,
        "live_authorized": False,
        "automated_promotion_performed": False,
        "read_only": True,
        "network_used": False,
        "writes_performed": [],
        "blocking_reason_codes": [code],
        "gates": [{
            "gate_id": "input_contract",
            "passed": False,
            "blocking": True,
            "reason_codes": [code],
            "facts": {"detail": detail},
        }, {
            "gate_id": "external_live_approval",
            "passed": False,
            "blocking": False,
            "reason_codes": ["LIVE_APPROVAL_REMAINS_EXTERNAL"],
            "facts": {
                "required": True,
                "satisfied_by_checker": False,
                "authority": "human_only",
            },
        }],
    }


def main(argv: Sequence[str] | None = None) -> int:
    """运行检查器；NOT_READY 以退出码 2 表达。"""
    parser = argparse.ArgumentParser(
        description="只读检查策略是否达到项目准入政策",
    )
    parser.add_argument("--root", type=Path, default=Path.cwd())
    parser.add_argument(
        "--policy",
        type=Path,
        default=Path("config/industry_strategy_readiness.json"),
    )
    parser.add_argument("--manifest", type=Path)
    parser.add_argument("--governance-registry", type=Path)
    parser.add_argument("--governance-root", type=Path)
    parser.add_argument("--execution-root", type=Path)
    arguments = parser.parse_args(argv)
    root = arguments.root.resolve()

    def resolve(path: Path | None, *, base: Path = root) -> Path | None:
        """按给定基准解析命令行路径。"""
        if path is None:
            return None
        return path.resolve() if path.is_absolute() else (base / path).resolve()

    inherited = tuple(
        name for name in _TRADE_ENVIRONMENT_NAMES if name in os.environ
    )
    # 立即移除且不读取值（T-01、T-13）
    for name in _TRADE_ENVIRONMENT_NAMES:
        os.environ.pop(name, None)
    try:
        result = industry_strategy_readiness(
            root,
            resolve(arguments.policy) or root / arguments.policy,
            manifest_path=resolve(arguments.manifest),
            governance_registry_path=resolve(arguments.governance_registry),
            governance_artifact_root=resolve(arguments.governance_root),
            execution_root=resolve(arguments.execution_root),
            inherited_trade_environment_names=inherited,
        )
    except (OSError, ValueError, json.JSONDecodeError) as error:
        result = _failure_report(
            "READINESS_INPUT_CONTRACT_INVALID",
            f"{type(error).__name__}: {error}",
        )
    print(canonical_json(result))
    return 0 if result.get("verdict") == "READY_FOR_EXTERNAL_LIVE_APPROVAL" else 2


if __name__ == "__main__":
    raise SystemExit(main())

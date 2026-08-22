"""为跨节拍冻结计划生成一个共同栅格预测。"""
from __future__ import annotations

import argparse
from dataclasses import asdict
from pathlib import Path

from guvolu.research.interval_suite_prediction import (
    run_interval_suite_forward_prediction,
)
from guvolu.research.provenance import canonical_json


def main() -> None:
    parser = argparse.ArgumentParser(description="生成多节拍冻结前向预测")
    parser.add_argument("plan_id")
    parser.add_argument("--root", type=Path, default=Path.cwd())
    parser.add_argument(
        "--registry",
        type=Path,
        default=Path("data/research/governance.sqlite3"),
    )
    arguments = parser.parse_args()
    result = run_interval_suite_forward_prediction(
        arguments.root.resolve(),
        arguments.plan_id,
        registry_path=arguments.registry,
    )
    payload = asdict(result)
    payload["prediction_path"] = result.prediction_path.as_posix()
    payload["decision_time"] = result.decision_time.isoformat()
    print(canonical_json(payload))


if __name__ == "__main__":
    main()

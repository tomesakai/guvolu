"""把冻结前向预测封装为执行目标快照。"""
from __future__ import annotations

import argparse
import hashlib
import json
import math
import os
from datetime import datetime
from pathlib import Path
from typing import Mapping, Sequence

from guvolu.execution.dry_run_executor import TARGET_UNIT


ADAPTER_SCHEMA_VERSION = 1
ADAPTER_METHOD_VERSION = "frozen-forward-operational-target-v1"
REQUIRED_QUALITY_FLAGS = frozenset({
    "clock", "coverage", "eligible", "freshness",
    "integrity", "lineage", "pit",
})


class FrozenTargetError(ValueError):
    """表示冻结预测不能进入执行目标层。"""


def _canonical_bytes(value: object) -> bytes:
    """生成稳定 JSON 字节。"""
    return (
        json.dumps(value, ensure_ascii=False, separators=(",", ":"), sort_keys=True)
        + "\n"
    ).encode("utf-8")


def _required_text(payload: Mapping[str, object], key: str) -> str:
    """读取非空文本字段。"""
    value = payload.get(key)
    if not isinstance(value, str) or not value:
        raise FrozenTargetError(f"冻结预测字段缺失: {key}")
    return value


def _aware_time(value: str) -> None:
    """确认时间带有明确时区。"""
    try:
        parsed = datetime.fromisoformat(value)
    except ValueError as exc:
        raise FrozenTargetError("冻结预测 decision_time 非法") from exc
    if parsed.tzinfo is None or parsed.utcoffset() is None:
        raise FrozenTargetError("冻结预测 decision_time 缺少时区")


def load_frozen_prediction(path: Path) -> tuple[dict[str, object], str]:
    """读取并验证冻结预测与质量合同。"""
    try:
        source_bytes = path.read_bytes()
        raw: object = json.loads(source_bytes)
    except FileNotFoundError as exc:
        raise FrozenTargetError(f"冻结预测不存在: {path}") from exc
    except json.JSONDecodeError as exc:
        raise FrozenTargetError("冻结预测不是合法 JSON") from exc
    if not isinstance(raw, dict):
        raise FrozenTargetError("冻结预测根必须为对象")
    if raw.get("schema_version") != 1 or raw.get("scope") != "FROZEN_FORWARD":
        raise FrozenTargetError("冻结预测结构或范围不受支持")
    if raw.get("unit") != TARGET_UNIT:
        raise FrozenTargetError("冻结预测目标口径不受支持")
    decision_time = _required_text(raw, "decision_time")
    _aware_time(decision_time)
    _required_text(raw, "prediction_id")
    _required_text(raw, "plan_id")
    _required_text(raw, "input_head_generation")
    target = raw.get("aggregate_target")
    if isinstance(target, bool) or not isinstance(target, (int, float)):
        raise FrozenTargetError("冻结预测 aggregate_target 非数值")
    if not math.isfinite(float(target)) or abs(float(target)) > 1:
        raise FrozenTargetError("冻结预测 aggregate_target 越界")
    quality = raw.get("quality")
    if not isinstance(quality, dict):
        raise FrozenTargetError("冻结预测缺少质量合同")
    missing = sorted(REQUIRED_QUALITY_FLAGS - set(quality))
    failed = sorted(
        key for key in REQUIRED_QUALITY_FLAGS if quality.get(key) is not True
    )
    reasons = quality.get("reasons")
    if missing or failed or reasons not in ([], None):
        raise FrozenTargetError(
            f"冻结预测质量未通过: missing={missing} failed={failed}"
        )
    families = raw.get("families")
    if not isinstance(families, list):
        raise FrozenTargetError("冻结预测缺少 family 贡献")
    return raw, hashlib.sha256(source_bytes).hexdigest()


def build_operational_target(
    prediction_path: Path,
    *,
    market_id: str,
) -> dict[str, object]:
    """构造只供执行边界消费的目标快照。"""
    if not market_id:
        raise FrozenTargetError("market_id 不能为空")
    prediction, source_sha = load_frozen_prediction(prediction_path)
    prediction_id = _required_text(prediction, "prediction_id")
    return {
        "artifact_kind": "operational_target_snapshot",
        "decision_time": _required_text(prediction, "decision_time"),
        "lineage": {
            "input_head_generation": _required_text(
                prediction, "input_head_generation",
            ),
            "plan_id": _required_text(prediction, "plan_id"),
            "prediction_id": prediction_id,
            "source_prediction_path": str(prediction_path),
            "source_prediction_sha256": source_sha,
        },
        "market_id": market_id,
        "method_version": ADAPTER_METHOD_VERSION,
        "operational_target_contract": {
            "aggregate_target": prediction["aggregate_target"],
            "families": prediction["families"],
            "reserve": prediction.get("reserve"),
            "unit": TARGET_UNIT,
        },
        "quality": prediction["quality"],
        "run_id": prediction_id,
        "schema_version": ADAPTER_SCHEMA_VERSION,
    }


def persist_operational_target(
    prediction_path: Path,
    output_directory: Path,
    *,
    market_id: str,
) -> tuple[Path, str]:
    """以内容散列文件名原子保存执行目标。"""
    payload = _canonical_bytes(build_operational_target(
        prediction_path, market_id=market_id,
    ))
    digest = hashlib.sha256(payload).hexdigest()
    output_directory.mkdir(parents=True, exist_ok=True)
    target = output_directory / f"target-{digest}.json"
    if target.is_file():
        if target.read_bytes() != payload:
            raise FrozenTargetError("执行目标文件名发生散列冲突")
        return target, digest
    temporary = target.with_name(f".{target.name}.{os.getpid()}.tmp")
    with temporary.open("wb", buffering=0) as stream:
        stream.write(payload)
        stream.flush()
        os.fsync(stream.fileno())
    os.replace(temporary, target)
    return target, digest


def main(argv: Sequence[str] | None = None) -> int:
    """命令行生成执行目标快照。"""
    parser = argparse.ArgumentParser(description="冻结预测转执行目标")
    parser.add_argument("--prediction", type=Path, required=True)
    parser.add_argument("--output-directory", type=Path, required=True)
    parser.add_argument("--market-id", required=True)
    args = parser.parse_args(argv)
    path, digest = persist_operational_target(
        args.prediction,
        args.output_directory,
        market_id=str(args.market_id),
    )
    print(json.dumps({
        "path": str(path),
        "sha256": digest,
        "status": "ready_for_dry_run",
    }, ensure_ascii=False, sort_keys=True))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())

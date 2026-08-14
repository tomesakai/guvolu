"""已发布研究运行的内容与安全不变量复核。"""
from __future__ import annotations

import json
from collections.abc import Mapping
from dataclasses import dataclass
from pathlib import Path

from guvolu.research.governance import get_research_exposure
from guvolu.research.provenance import sha256_file


@dataclass(frozen=True)
class VerificationResult:
    """一次研究运行复核的结果。"""

    run_id: str
    manifest_path: Path
    manifest_sha256: str
    checked_artifacts: tuple[str, ...]


def _object(value: object, name: str) -> Mapping[str, object]:
    """验证 JSON 对象。"""
    if not isinstance(value, Mapping):
        raise ValueError(f"{name} 必须为对象")
    return {str(key): item for key, item in value.items()}


def _text(value: object, name: str) -> str:
    """验证非空字符串。"""
    if not isinstance(value, str) or not value:
        raise ValueError(f"{name} 必须为非空字符串")
    return value


def _number(value: object, name: str) -> float:
    """验证 JSON 数值。"""
    if not isinstance(value, (int, float)) or isinstance(value, bool):
        raise ValueError(f"{name} 必须为数值")
    return float(value)


def _read_json(path: Path) -> Mapping[str, object]:
    """读取并验证 JSON 对象。"""
    try:
        value = json.loads(path.read_text(encoding="utf-8"))
    except (OSError, json.JSONDecodeError) as error:
        raise ValueError(f"无法读取 JSON: {path}") from error
    return _object(value, path.as_posix())


def _resolve_manifest(root: Path, manifest_path: Path | None) -> tuple[Path, str | None]:
    """解析显式 manifest 或活动运行指针。"""
    if manifest_path is not None:
        resolved = manifest_path if manifest_path.is_absolute() else root / manifest_path
        return resolved.resolve(), None
    latest_path = root / "reports" / "strategy-research" / "latest.json"
    latest = _read_json(latest_path)
    relative = _text(latest.get("manifest"), "latest.manifest")
    expected = _text(latest.get("manifest_sha256"), "latest.manifest_sha256")
    return (root / relative).resolve(), expected


def _artifact_path(root: Path, record: Mapping[str, object], name: str) -> Path:
    """解析并限制制品位于项目目录内。"""
    relative = _text(record.get("path"), f"artifacts.{name}.path")
    path = (root / relative).resolve()
    try:
        path.relative_to(root.resolve())
    except ValueError as error:
        raise ValueError(f"制品路径越出项目目录: {name}") from error
    return path


def _verify_operational_gate(summary: Mapping[str, object]) -> None:
    """质量失败时运行仓位必须全零。"""
    quality = _object(summary.get("operational_quality"), "operational_quality")
    position = _object(summary.get("operational_position"), "operational_position")
    weights = _object(position.get("weights"), "operational_position.weights")
    eligible = quality.get("eligible")
    if not isinstance(eligible, bool):
        raise ValueError("operational_quality.eligible 必须为布尔值")
    numeric_weights: list[float] = []
    for family, value in weights.items():
        if not isinstance(value, (int, float)) or isinstance(value, bool):
            raise ValueError(f"运行仓位不是数值: {family}")
        numeric_weights.append(float(value))
    if not eligible and any(abs(value) > 1e-12 for value in numeric_weights):
        raise ValueError("运行质量失败但存在非零仓位")
    decision_grade = summary.get("decision_grade")
    if not isinstance(decision_grade, bool):
        raise ValueError("decision_grade 必须为布尔值")
    if not decision_grade and any(abs(value) > 1e-12 for value in numeric_weights):
        raise ValueError("代码身份非决策级但存在非零仓位")
    contract = _object(
        summary.get("operational_target_contract"),
        "operational_target_contract",
    )
    aggregate = _number(
        contract.get("aggregate_target"),
        "operational_target_contract.aggregate_target",
    )
    families = contract.get("families")
    if not isinstance(families, list):
        raise ValueError("operational_target_contract.families 必须为数组")
    contribution_total = 0.0
    for index, raw_family in enumerate(families):
        record = _object(raw_family, f"operational_target_contract.families.{index}")
        name = _text(record.get("family"), "target family")
        target_value = _number(record.get("family_target"), f"{name}.family_target")
        weight_value = _number(
            record.get("allocation_weight"), f"{name}.allocation_weight",
        )
        contribution_value = _number(
            record.get("portfolio_target_contribution"),
            f"{name}.portfolio_target_contribution",
        )
        if abs(contribution_value - target_value * weight_value) > 1e-12:
            raise ValueError(f"运行目标贡献计算不一致: {name}")
        if name not in weights or abs(
            weight_value - _number(weights.get(name), f"weights.{name}")
        ) > 1e-12:
            raise ValueError(f"运行目标权重与分配器不一致: {name}")
        contribution_total += contribution_value
    if abs(contribution_total - aggregate) > 1e-12:
        raise ValueError("运行目标合同聚合值不一致")
    if not eligible and abs(aggregate) > 1e-12:
        raise ValueError("运行质量失败但组合目标非零")
    if not decision_grade and abs(aggregate) > 1e-12:
        raise ValueError("代码身份非决策级但组合目标非零")


def _verify_data_governance(root: Path, summary: Mapping[str, object]) -> None:
    """复核 v8 开发运行绑定的不可变数据暴露。"""
    if summary.get("pipeline_method_version") not in (
        "strategy-research-pipeline-v8",
        "strategy-research-pipeline-v9",
        "strategy-research-pipeline-v10",
    ):
        return
    governance = _object(summary.get("data_governance"), "data_governance")
    if governance.get("scope") != "DEV_ADAPTIVE":
        raise ValueError("普通研究运行的数据范围必须为 DEV_ADAPTIVE")
    relative = _text(governance.get("registry"), "data_governance.registry")
    registry = (root / relative).resolve()
    try:
        registry.relative_to(root)
    except ValueError as error:
        raise ValueError("研究治理注册表越出项目目录") from error
    exposure_id = _text(
        governance.get("exposure_id"),
        "data_governance.exposure_id",
    )
    exposure = get_research_exposure(registry, exposure_id)
    if exposure.research_identity != summary.get("research_identity"):
        raise ValueError("研究暴露与 summary 的 research_identity 不一致")
    if exposure.market_id != summary.get("market_id"):
        raise ValueError("研究暴露与 summary 的 market_id 不一致")
    panel = _object(summary.get("panel"), "panel")
    if exposure.start_time.isoformat() != governance.get("from_time"):
        raise ValueError("研究暴露起点不一致")
    if exposure.end_time.isoformat() != governance.get("to_time"):
        raise ValueError("研究暴露终点不一致")
    panel_from = _text(panel.get("from_time"), "panel.from_time")
    panel_to = _text(panel.get("to_time"), "panel.to_time")
    if panel_from < exposure.start_time.isoformat():
        raise ValueError("研究面板早于已登记暴露区间")
    if panel_to > exposure.end_time.isoformat():
        raise ValueError("研究面板晚于已登记暴露区间")


def verify_research_run(
    root: Path,
    manifest_path: Path | None = None,
) -> VerificationResult:
    """复核 manifest、全部制品散列和运行质量硬门禁。"""
    resolved_root = root.resolve()
    resolved_manifest, expected_manifest_hash = _resolve_manifest(
        resolved_root,
        manifest_path,
    )
    manifest_hash = sha256_file(resolved_manifest)
    if expected_manifest_hash is not None and manifest_hash != expected_manifest_hash:
        raise ValueError("latest 指针中的 manifest 散列不匹配")
    manifest = _read_json(resolved_manifest)
    run_id = _text(manifest.get("run_id"), "manifest.run_id")
    artifacts = _object(manifest.get("artifacts"), "manifest.artifacts")
    checked: list[str] = []
    summary: Mapping[str, object] | None = None
    for name, raw_record in sorted(artifacts.items()):
        record = _object(raw_record, f"artifacts.{name}")
        path = _artifact_path(resolved_root, record, name)
        if not path.is_file():
            raise ValueError(f"制品不存在: {name}")
        expected_hash = _text(record.get("sha256"), f"artifacts.{name}.sha256")
        if sha256_file(path) != expected_hash:
            raise ValueError(f"制品散列不匹配: {name}")
        expected_bytes = record.get("bytes")
        if not isinstance(expected_bytes, int) or isinstance(expected_bytes, bool):
            raise ValueError(f"制品字节数非法: {name}")
        if path.stat().st_size != expected_bytes:
            raise ValueError(f"制品字节数不匹配: {name}")
        if name == "summary_json":
            summary = _read_json(path)
        checked.append(name)
    if summary is None:
        raise ValueError("manifest 缺少 summary_json 制品")
    if summary.get("run_id") != run_id:
        raise ValueError("summary 与 manifest 的 run_id 不一致")
    _verify_operational_gate(summary)
    _verify_data_governance(resolved_root, summary)
    return VerificationResult(
        run_id=run_id,
        manifest_path=resolved_manifest,
        manifest_sha256=manifest_hash,
        checked_artifacts=tuple(checked),
    )

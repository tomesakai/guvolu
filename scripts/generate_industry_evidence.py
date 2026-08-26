"""独立审计入口：对研究运行重算行业稳健性证据并留痕。

只读上游受完整性保护的研究运行与活动 head L2 事实，
只写证据制品与试验台账；不改研究配置，不做晋级，不写治理库。
本入口不用于准入：它在研究运行封版之后才执行，
产出证据的 `generated_at` 必然超出 manifest 的注册窗口，
检查器会报 `INDUSTRY_EVIDENCE_GENERATION_CUTOFF_INVALID`
与 `INDUSTRY_EVIDENCE_GENERATOR_CUTOFF_INVALID` 而不接受。
准入用证据由研究管线在窗口内生成并登记进 manifest。
"""
from __future__ import annotations

import argparse
import hashlib
import json
from collections.abc import Mapping, Sequence
from datetime import UTC, datetime
from pathlib import Path

from guvolu.data.durable_io import atomic_write_bytes
from guvolu.research.industry_evidence import (
    GENERATOR_ID,
    CandidatePath,
    RunIdentity,
    generator_code_sha256,
    read_candidate_paths,
    read_run_identity,
)
from guvolu.research.industry_evidence_run import generate_run_evidence
from guvolu.research.panel_limit import (
    reject_sealed_conflict,
    resolve_panel_to_time,
)
from guvolu.research.provenance import canonical_json
from guvolu.research.verification import verify_research_run


def _object(value: object, name: str) -> Mapping[str, object]:
    """收窄为字符串键映射。"""
    if not isinstance(value, Mapping):
        raise ValueError(f"{name} 必须为对象")
    return {str(key): item for key, item in value.items()}


def _text(value: object, name: str) -> str:
    """读取非空文本。"""
    if not isinstance(value, str) or not value.strip():
        raise ValueError(f"{name} 必须为非空文本")
    return value


def _snapshot(
    root: Path,
    manifest: Mapping[str, object],
    name: str,
) -> tuple[Path, bytes]:
    """按 manifest 身份读取受保护制品字节。"""
    artifacts = _object(manifest.get("artifacts"), "manifest.artifacts")
    record = _object(artifacts.get(name), f"artifacts.{name}")
    path = (root / _text(record.get("path"), f"{name}.path")).resolve()
    path.relative_to(root)
    body = path.read_bytes()
    if hashlib.sha256(body).hexdigest() != record.get("sha256"):
        raise ValueError(f"复验后制品字节发生变化: {name}")
    return path, body


def _parse_arguments(argv: Sequence[str] | None) -> argparse.Namespace:
    """解析命令行参数。"""
    parser = argparse.ArgumentParser(
        description="独立审计入口：对研究运行重算行业稳健性证据",
        epilog=(
            "本入口只用于复核。它在研究运行封版之后执行，"
            "产出证据的 generated_at 超出 manifest 的注册窗口，"
            "检查器会报 INDUSTRY_EVIDENCE_GENERATION_CUTOFF_INVALID "
            "与 INDUSTRY_EVIDENCE_GENERATOR_CUTOFF_INVALID 而不接受。"
            "准入用证据由研究管线在窗口内生成并登记进 manifest。"
        ),
    )
    parser.add_argument("--source-summary", type=Path, required=True)
    parser.add_argument(
        "--config", type=Path, default=Path("config/industry_evidence.json"),
    )
    parser.add_argument("--data-root", type=Path, default=Path("data"))
    parser.add_argument("--to-time", type=str, default=None)
    parser.add_argument("--output-dir", type=Path, default=None)
    parser.add_argument("--root", type=Path, default=Path.cwd())
    parser.add_argument("--evidence-root", type=Path, default=None)
    return parser.parse_args(argv)


def _resolve(root: Path, value: Path) -> Path:
    """把相对路径解析到项目根。"""
    return value if value.is_absolute() else (root / value)


def _identity_and_paths(
    root: Path,
    manifest_path: Path,
    cli_to_time: datetime | None,
    registry_path: Path,
) -> tuple[
    RunIdentity, tuple[CandidatePath, ...], bytes, Mapping[str, object],
]:
    """复验研究运行并读取样本外目标路径。"""
    manifest_body = manifest_path.read_bytes()
    manifest = _object(json.loads(manifest_body), "manifest")
    _summary_path, summary_body = _snapshot(root, manifest, "summary_json")
    _config_path, config_body = _snapshot(root, manifest, "config")
    _replay_path, replay_body = _snapshot(root, manifest, "label_cost_replay")
    _feature_path, feature_body = _snapshot(root, manifest, "features")
    summary = _object(json.loads(summary_body), "summary")
    config = _object(json.loads(config_body), "config")
    identity = read_run_identity(manifest, summary, config)
    governance = _object(
        config.get("data_governance"), "config.data_governance",
    )
    panel = _object(summary.get("panel"), "summary.panel")
    from_time = datetime.fromisoformat(
        _text(panel.get("from_time"), "panel.from_time")
    ).astimezone(UTC)
    panel_to_time = datetime.fromisoformat(
        _text(panel.get("to_time"), "panel.to_time")
    ).astimezone(UTC)
    override = cli_to_time if cli_to_time is not None else panel_to_time
    limit = resolve_panel_to_time(governance, override, from_time)
    effective = limit.effective_to_time(panel_to_time)
    reject_sealed_conflict(
        registry_path, identity.market_id, from_time, effective, limit,
    )
    paths = read_candidate_paths(replay_body, summary, effective)
    return identity, paths, feature_body, {
        "panel_to_time": limit.payload(effective, panel_to_time),
        "registration_cutoff": _text(
            manifest.get("execution_evaluated_at"),
            "manifest.execution_evaluated_at",
        ),
    }


def main(argv: Sequence[str] | None = None) -> int:
    """入口：复验上游、重算四类证据并写出复核报告。"""
    arguments = _parse_arguments(argv)
    root = arguments.root.resolve()
    summary_path = _resolve(root, arguments.source_summary).resolve()
    manifest_path = (summary_path.parent / "manifest.json").resolve()
    verify_research_run(root, manifest_path)
    settings = _object(
        json.loads(
            _resolve(root, arguments.config).read_text(encoding="utf-8")
        ),
        "industry evidence config",
    )
    data_root = _resolve(root, arguments.data_root).resolve()
    registry_path = data_root / "research" / "governance.sqlite3"
    cli_to_time = (
        None if arguments.to_time is None
        else datetime.fromisoformat(
            arguments.to_time.replace("Z", "+00:00")
        ).astimezone(UTC)
    )
    identity, paths, feature_body, context = _identity_and_paths(
        root, manifest_path, cli_to_time, registry_path,
    )
    evidence_root = (
        root if arguments.evidence_root is None
        else arguments.evidence_root.resolve()
    )
    output = (
        _resolve(evidence_root, arguments.output_dir)
        if arguments.output_dir is not None
        else evidence_root / "reports" / "strategy-research"
        / "industry-evidence" / identity.run_id
    ).resolve()
    output.relative_to(evidence_root)
    output.mkdir(parents=True, exist_ok=True)
    evidence = generate_run_evidence(
        settings,
        identity,
        paths,
        feature_body,
        data_root,
        evidence_root,
        output,
    )
    registration_cutoff = datetime.fromisoformat(
        str(context.get("registration_cutoff")).replace("Z", "+00:00")
    ).astimezone(UTC)
    report = {
        "schema_version": 1,
        "entry_point": "independent_audit_recompute_not_admission",
        "run_id": identity.run_id,
        "research_identity": identity.research_identity,
        "config_hash": identity.config_hash,
        "generator_id": GENERATOR_ID,
        "generator_code_sha256": generator_code_sha256(),
        "generation_within_registration_window": (
            identity.decision_time <= evidence.generated_at
            <= registration_cutoff
        ),
        "registration_cutoff": registration_cutoff.isoformat(),
        "panel_to_time": context.get("panel_to_time"),
        **evidence.report(),
    }
    report_body = (canonical_json(report) + "\n").encode("utf-8")
    atomic_write_bytes(output / "generation-report.json", report_body)
    print(canonical_json(report))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())

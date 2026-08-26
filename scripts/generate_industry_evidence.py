"""生成行业稳健性证据制品：成本、尾部、压力与容量四类。

只读上游受完整性保护的研究运行与活动 head L2 事实，
只写证据制品与试验台账；不改研究配置，不做晋级，不写治理库。
"""
from __future__ import annotations

import argparse
import hashlib
import json
from collections.abc import Mapping, Sequence
from datetime import UTC, datetime
from decimal import Decimal
from pathlib import Path

from guvolu.data.durable_io import atomic_write_bytes
from guvolu.research.industry_evidence import (
    SOURCE_ARTIFACT_NAMES,
    CandidatePath,
    RunIdentity,
    SourceArtifact,
    build_capacity_evidence,
    build_cost_evidence,
    build_generator_attestation,
    build_industry_evidence,
    build_stress_evidence,
    build_tail_evidence,
    content_sha256,
    depth_observation,
    evidence_bytes,
    generator_code_sha256,
    ledger_bytes,
    ledger_rows,
    notional_key,
    read_candidate_paths,
    read_pit_volume_scores,
    read_run_identity,
    sample_decision_times,
    source_artifact_bytes,
)
from guvolu.research.panel_limit import (
    reject_sealed_conflict,
    resolve_panel_to_time,
)
from guvolu.research.provenance import canonical_json
from guvolu.research.verification import verify_research_run
from guvolu.ui.materialized_query import MaterializedQuery

_ARTIFACT_FILE_STEMS: Mapping[str, str] = {
    "tail": "tail-risk-evidence",
    "stress": "stress-scenario-evidence",
    "cost": "fixed-target-cost-replay",
    "capacity": "l2-depth-capacity-evidence",
}


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


def _items(value: object, name: str) -> Sequence[object]:
    """收窄为非文本序列。"""
    if not isinstance(value, Sequence) or isinstance(value, (str, bytes)):
        raise ValueError(f"{name} 必须为数组")
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


def _venue_depth_facts(
    query: MaterializedQuery,
    market_ids: Sequence[str],
    notionals: Sequence[float],
    horizon_seconds: int,
    sample_count: int,
    depth_levels: int,
    limit: datetime,
) -> tuple[Mapping[str, object], ...]:
    """在活动 head 上只读采样三所 L2 深度事实。"""
    facts: list[Mapping[str, object]] = []
    for market_id in market_ids:
        snapshot = query.catalog.active_outputs(
            market_id,
            domains=("book_l2",),
            datasets=("book_l2_frame", "book_l2_level"),
        )
        events = [
            output.max_event_time for output in snapshot.outputs
            if output.max_event_time is not None
        ]
        starts = [
            output.min_event_time for output in snapshot.outputs
            if output.min_event_time is not None
        ]
        if not events or not starts:
            facts.append({
                "market_id": market_id,
                "sufficient": False,
                "insufficient_reason": "no_active_l2_head",
            })
            continue
        available_through = min(max(events), limit)
        times = sample_decision_times(
            available_through, horizon_seconds, sample_count,
        )
        samples: dict[str, object] = {
            notional_key(value): {"observations": []} for value in notionals
        }
        resolved = 0
        for decision_time in times:
            if decision_time < min(starts):
                continue
            payload, _tag = query.latest_l2_from_snapshot(
                snapshot, depth_levels, decision_time=decision_time,
            )
            hit = False
            for value in notionals:
                observation = depth_observation(payload, Decimal(str(value)))
                if observation is None:
                    continue
                entry = _object(
                    samples[notional_key(value)], "notional samples",
                )
                observations = entry["observations"]
                if isinstance(observations, list):
                    observations.append({
                        **observation,
                        "decision_time": decision_time.isoformat(),
                    })
                hit = True
            resolved += int(hit)
        facts.append({
            "market_id": market_id,
            "head_generation": snapshot.head_generation,
            "sufficient": resolved > 0,
            "from_time": min(times).isoformat(),
            "to_time": available_through.isoformat(),
            "available_through": available_through.isoformat(),
            "observation_rows": sum(
                output.row_count for output in snapshot.outputs
            ),
            "distinct_days": len({value.date() for value in times}),
            "expected_samples": len(times),
            "resolved_samples": resolved,
            "coverage_ratio": resolved / len(times) if times else 0.0,
            "samples": samples,
        })
    return tuple(facts)


def _write(directory: Path, stem: str, body: bytes, suffix: str) -> Path:
    """按内容身份原子写出制品。"""
    digest = hashlib.sha256(body).hexdigest()
    path = directory / f"{stem}-sha256-{digest}{suffix}"
    atomic_write_bytes(path, body)
    return path


def _source_artifact(
    evidence_root: Path,
    kind: str,
    path: Path,
    body: bytes,
) -> SourceArtifact:
    """把已落盘来源制品转为身份引用。"""
    return SourceArtifact(
        name=SOURCE_ARTIFACT_NAMES[kind],
        kind=SOURCE_ARTIFACT_NAMES[kind],
        path=path.resolve().relative_to(evidence_root).as_posix(),
        sha256=content_sha256(body),
        bytes_count=len(body),
    )


def _parse_arguments(argv: Sequence[str] | None) -> argparse.Namespace:
    """解析命令行参数。"""
    parser = argparse.ArgumentParser(
        description="生成行业稳健性证据制品",
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
        "summary": summary,
    }


def main(argv: Sequence[str] | None = None) -> int:
    """入口：复验上游、生成四类证据并写出汇总与台账。"""
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
    generated_at = datetime.now(UTC)
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

    cost_settings = _object(settings.get("cost"), "config.cost")
    tail_settings = _object(settings.get("tail"), "config.tail")
    stress_settings = _object(settings.get("stress"), "config.stress")
    capacity_settings = _object(settings.get("capacity"), "config.capacity")

    query = MaterializedQuery(data_root)
    notionals = [
        float(str(value)) for value in _items(
            capacity_settings.get("notional_quote_grid"), "notional_quote_grid",
        )
    ]
    venue_facts = _venue_depth_facts(
        query,
        [
            str(value) for value in _items(
                capacity_settings.get("venue_market_ids"), "venue_market_ids",
            )
        ],
        notionals,
        int(str(capacity_settings.get("depth_horizon_seconds"))),
        int(str(capacity_settings.get("depth_sample_count"))),
        int(str(capacity_settings.get("depth_levels"))),
        identity.decision_time,
    )
    volume_scores = read_pit_volume_scores(
        feature_body, paths[0].decision_times,
    )
    evidences = {
        "cost": build_cost_evidence(
            identity,
            paths,
            _object(
                cost_settings.get("tier_multipliers"), "tier_multipliers",
            ),
            float(str(cost_settings.get("minimum_step_bps"))),
            generated_at,
        ),
        "tail": build_tail_evidence(
            identity,
            paths,
            [
                float(str(value)) for value in _items(
                    tail_settings.get("probabilities"), "probabilities",
                )
            ],
            int(str(tail_settings.get("bootstrap_samples"))),
            generated_at,
        ),
        "stress": build_stress_evidence(
            identity,
            paths,
            volume_scores,
            [None] * len(paths[0].decision_times),
            stress_settings,
            {
                "expected_market_ids": [
                    str(value) for value in _items(
                        stress_settings.get("cross_venue_market_ids"),
                        "cross_venue_market_ids",
                    )
                ],
                "decision_aligned_series_available": False,
                "reason": (
                    "研究运行的输入身份不含跨所决策对齐价格序列"
                ),
            },
            generated_at,
        ),
        "capacity": build_capacity_evidence(
            identity,
            paths,
            venue_facts,
            str(capacity_settings.get("execution_market_id")),
            capacity_settings,
            generated_at,
        ),
    }
    sources: dict[str, SourceArtifact] = {}
    for kind, payload in evidences.items():
        body = source_artifact_bytes(payload)
        path = _write(output, _ARTIFACT_FILE_STEMS[kind], body, ".json")
        sources[kind] = _source_artifact(evidence_root, kind, path, body)
    evidence_payload = build_industry_evidence(
        identity, paths, evidences, sources, generated_at,
    )
    evidence_body = evidence_bytes(evidence_payload)
    evidence_path = _write(
        output, "industry-evidence", evidence_body, ".json",
    )
    attested_at = datetime.now(UTC)
    attestation = build_generator_attestation(
        identity,
        evidence_payload,
        content_sha256(evidence_body),
        sources,
        generated_at,
        attested_at,
    )
    attestation_body = evidence_bytes(attestation)
    attestation_path = _write(
        output,
        "industry-evidence-generator-attestation",
        attestation_body,
        ".json",
    )
    ledger_body = ledger_bytes(
        ledger_rows(identity, evidence_payload, generated_at)
    )
    ledger_path = _write(
        output, "industry-evidence-ledger", ledger_body, ".jsonl",
    )
    report = {
        "schema_version": 1,
        "run_id": identity.run_id,
        "research_identity": identity.research_identity,
        "config_hash": identity.config_hash,
        "generator_id": "guvolu-industry-evidence-generator-v1",
        "generator_code_sha256": generator_code_sha256(),
        "generated_at": generated_at.isoformat(),
        "attested_at": attested_at.isoformat(),
        "generation_within_registration_window": (
            identity.decision_time <= generated_at
            <= identity.execution_evaluated_at
        ),
        "registration_cutoff": identity.execution_evaluated_at.isoformat(),
        "panel_to_time": context.get("panel_to_time"),
        "artifacts": {
            **{
                SOURCE_ARTIFACT_NAMES[kind]: {
                    "path": source.path,
                    "sha256": source.sha256,
                    "bytes": source.bytes_count,
                }
                for kind, source in sources.items()
            },
            "industry_evidence": {
                "path": evidence_path.relative_to(evidence_root).as_posix(),
                "sha256": content_sha256(evidence_body),
                "bytes": len(evidence_body),
            },
            "industry_evidence_generator_attestation": {
                "path": (
                    attestation_path.relative_to(evidence_root).as_posix()
                ),
                "sha256": content_sha256(attestation_body),
                "bytes": len(attestation_body),
            },
            "industry_evidence_ledger": {
                "path": ledger_path.relative_to(evidence_root).as_posix(),
                "sha256": content_sha256(ledger_body),
                "bytes": len(ledger_body),
            },
        },
        "venue_l2_coverage": [
            {
                "market_id": fact.get("market_id"),
                "sufficient": fact.get("sufficient"),
                "resolved_samples": fact.get("resolved_samples"),
                "expected_samples": fact.get("expected_samples"),
            }
            for fact in venue_facts
        ],
        "scenario_counts": {
            str(item.get("candidate_id")): {
                name: len(_items(item.get(name), name))
                for name in (
                    "tail_scenarios", "stress_scenarios",
                    "cost_scenarios", "capacity_scenarios",
                )
            }
            for item in [
                _object(raw, "candidate")
                for raw in _items(
                    evidence_payload.get("candidate_evidence"),
                    "candidate_evidence",
                )
            ]
        },
    }
    report_body = (canonical_json(report) + "\n").encode("utf-8")
    atomic_write_bytes(output / "generation-report.json", report_body)
    print(canonical_json(report))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())

"""把行业稳健性证据的生成收敛为一次运行内的统一装配。

研究管线在注册窗口内调用它，独立审计入口对历史运行复算也调用它，
两条路径共用同一段构造代码与同一份版本化阈值，避免数值与身份漂移。
它只读上游受完整性保护的制品与活动 head L2 事实，
不重新冻结输入、不重选候选、不做晋级，也不写治理库。
L2 覆盖不足按既有约定标注 `insufficient_l2_coverage` 并让容量证据不达标，
绝不外推；覆盖缺失是有效结果，与生成器故障区分开。
"""
from __future__ import annotations

from collections.abc import Mapping, Sequence
from dataclasses import dataclass
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
    ledger_bytes,
    ledger_rows,
    notional_key,
    read_pit_volume_scores,
    sample_decision_times,
    source_artifact_bytes,
)
from guvolu.ui.materialized_query import MaterializedQuery

EVIDENCE_ARTIFACT = "industry_evidence"
ATTESTATION_ARTIFACT = "industry_evidence_generator_attestation"
LEDGER_ARTIFACT = "industry_evidence_ledger"
NO_PAPER_ELIGIBLE_CANDIDATE = "no_paper_eligible_candidate"
NO_ACTIVE_L2_HEAD = "no_active_l2_head"
ARTIFACT_FILE_STEMS: Mapping[str, str] = {
    "tail": "tail-risk-evidence",
    "stress": "stress-scenario-evidence",
    "cost": "fixed-target-cost-replay",
    "capacity": "l2-depth-capacity-evidence",
}
_SCENARIO_COLLECTIONS: tuple[str, ...] = (
    "tail_scenarios", "stress_scenarios", "cost_scenarios",
    "capacity_scenarios",
)


def _object(value: object, name: str) -> Mapping[str, object]:
    """收窄为字符串键映射。"""
    if not isinstance(value, Mapping):
        raise ValueError(f"{name} 必须为对象")
    return {str(key): item for key, item in value.items()}


def _items(value: object, name: str) -> Sequence[object]:
    """收窄为非文本序列。"""
    if not isinstance(value, Sequence) or isinstance(value, (str, bytes)):
        raise ValueError(f"{name} 必须为数组")
    return value


def venue_depth_facts(
    query: MaterializedQuery,
    market_ids: Sequence[str],
    notionals: Sequence[float],
    horizon_seconds: int,
    sample_count: int,
    depth_levels: int,
    limit: datetime,
) -> tuple[Mapping[str, object], ...]:
    """在活动 head 上只读采样各所 L2 深度事实。"""
    facts: list[Mapping[str, object]] = []
    for market_id in market_ids:
        try:
            snapshot = query.catalog.active_outputs(
                market_id,
                domains=("book_l2",),
                datasets=("book_l2_frame", "book_l2_level"),
            )
        except (LookupError, OSError, ValueError) as error:
            # 目录不可读按覆盖缺失
            facts.append({
                "market_id": market_id,
                "sufficient": False,
                "insufficient_reason": NO_ACTIVE_L2_HEAD,
                "catalog_error": f"{type(error).__name__}: {error}",
            })
            continue
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
                "insufficient_reason": NO_ACTIVE_L2_HEAD,
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


@dataclass(frozen=True)
class RunEvidence:
    """一次运行内生成的行业证据制品与覆盖事实。"""

    generated_at: datetime
    attested_at: datetime
    evidence_sha256: str
    artifacts: Mapping[str, Mapping[str, object]]
    venue_l2_coverage: tuple[Mapping[str, object], ...]
    scenario_counts: Mapping[str, Mapping[str, int]]

    def report(self) -> Mapping[str, object]:
        """面向人工复核的生成事实摘要。"""
        return {
            "generated_at": self.generated_at.isoformat(),
            "attested_at": self.attested_at.isoformat(),
            "industry_evidence_sha256": self.evidence_sha256,
            "artifacts": {
                name: dict(record)
                for name, record in sorted(self.artifacts.items())
            },
            "venue_l2_coverage": [
                dict(fact) for fact in self.venue_l2_coverage
            ],
            "scenario_counts": {
                candidate_id: dict(counts)
                for candidate_id, counts in sorted(
                    self.scenario_counts.items()
                )
            },
        }


def paper_deployment_candidates(
    summary: Mapping[str, object],
) -> Mapping[str, str]:
    """读取 summary 中 paper 可用的部署候选。"""
    result: dict[str, str] = {}
    for raw in _items(
        summary.get("family_evaluations"), "summary.family_evaluations",
    ):
        evaluation = _object(raw, "family_evaluation")
        if evaluation.get("eligible") is not True:
            continue
        if evaluation.get("mode") != "paper":
            continue
        family = evaluation.get("family")
        candidate_id = evaluation.get("deployment_candidate_id")
        if isinstance(family, str) and isinstance(candidate_id, str):
            result[family] = candidate_id
    return result


def _write(directory: Path, stem: str, body: bytes, suffix: str) -> Path:
    """按内容身份原子写出制品。"""
    directory.mkdir(parents=True, exist_ok=True)
    path = directory / f"{stem}-sha256-{content_sha256(body)}{suffix}"
    atomic_write_bytes(path, body)
    return path


def _record(root: Path, kind: str, path: Path, body: bytes) -> Mapping[
    str, object
]:
    """生成 manifest 制品登记记录。"""
    return {
        "kind": kind,
        "path": path.resolve().relative_to(root.resolve()).as_posix(),
        "sha256": content_sha256(body),
        "bytes": len(body),
    }


def generate_run_evidence(
    settings: Mapping[str, object],
    identity: RunIdentity,
    paths: Sequence[CandidatePath],
    feature_body: bytes,
    data_root: Path,
    root: Path,
    output_directory: Path,
) -> RunEvidence:
    """生成四类证据、汇总制品、attestation 与台账。"""
    if not paths:
        raise ValueError("行业证据生成缺少部署候选路径")
    cost_settings = _object(settings.get("cost"), "config.cost")
    tail_settings = _object(settings.get("tail"), "config.tail")
    stress_settings = _object(settings.get("stress"), "config.stress")
    capacity_settings = _object(settings.get("capacity"), "config.capacity")
    notionals = [
        float(str(value)) for value in _items(
            capacity_settings.get("notional_quote_grid"),
            "notional_quote_grid",
        )
    ]
    generated_at = datetime.now(UTC)
    venue_facts = venue_depth_facts(
        MaterializedQuery(data_root),
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
    volume_scores = read_pit_volume_scores(feature_body, paths[0].decision_times)
    evidences = {
        "cost": build_cost_evidence(
            identity,
            paths,
            _object(cost_settings.get("tier_multipliers"), "tier_multipliers"),
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
                "reason": "研究运行的输入身份不含跨所决策对齐价格序列",
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
    artifacts: dict[str, Mapping[str, object]] = {}
    for kind, payload in evidences.items():
        name = SOURCE_ARTIFACT_NAMES[kind]
        body = source_artifact_bytes(payload)
        path = _write(output_directory, ARTIFACT_FILE_STEMS[kind], body, ".json")
        record = _record(root, name, path, body)
        artifacts[name] = record
        sources[kind] = SourceArtifact(
            name=name,
            kind=name,
            path=str(record["path"]),
            sha256=content_sha256(body),
            bytes_count=len(body),
        )
    evidence_payload = build_industry_evidence(
        identity, paths, evidences, sources, generated_at,
    )
    evidence_body = evidence_bytes(evidence_payload)
    evidence_path = _write(
        output_directory, "industry-evidence", evidence_body, ".json",
    )
    artifacts[EVIDENCE_ARTIFACT] = _record(
        root, EVIDENCE_ARTIFACT, evidence_path, evidence_body,
    )
    attested_at = datetime.now(UTC)
    attestation_body = evidence_bytes(build_generator_attestation(
        identity,
        evidence_payload,
        content_sha256(evidence_body),
        sources,
        generated_at,
        attested_at,
    ))
    attestation_path = _write(
        output_directory,
        "industry-evidence-generator-attestation",
        attestation_body,
        ".json",
    )
    artifacts[ATTESTATION_ARTIFACT] = _record(
        root, ATTESTATION_ARTIFACT, attestation_path, attestation_body,
    )
    ledger_body = ledger_bytes(
        ledger_rows(identity, evidence_payload, generated_at)
    )
    ledger_path = _write(
        output_directory, "industry-evidence-ledger", ledger_body, ".jsonl",
    )
    artifacts[LEDGER_ARTIFACT] = _record(
        root, LEDGER_ARTIFACT, ledger_path, ledger_body,
    )
    return RunEvidence(
        generated_at=generated_at,
        attested_at=attested_at,
        evidence_sha256=content_sha256(evidence_body),
        artifacts=artifacts,
        venue_l2_coverage=tuple(
            {
                "market_id": fact.get("market_id"),
                "sufficient": fact.get("sufficient"),
                "insufficient_reason": fact.get("insufficient_reason"),
                "resolved_samples": fact.get("resolved_samples"),
                "expected_samples": fact.get("expected_samples"),
            }
            for fact in venue_facts
        ),
        scenario_counts={
            str(_object(raw, "candidate").get("candidate_id")): {
                name: len(_items(_object(raw, "candidate").get(name), name))
                for name in _SCENARIO_COLLECTIONS
            }
            for raw in _items(
                evidence_payload.get("candidate_evidence"),
                "candidate_evidence",
            )
        },
    )

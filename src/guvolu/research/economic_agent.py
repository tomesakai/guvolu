"""Research-only 经济研究代理与时点正确的宏观语境制品。

本模块只管理观测、语境和搜索提案。它不联网、不读取密钥、不修改
策略配置或候选注册表，也没有任何执行或 TRADE 权限。
"""
from __future__ import annotations

import json
import math
import os
import re
from collections.abc import Mapping, Sequence
from dataclasses import dataclass
from datetime import UTC, datetime
from pathlib import Path

from guvolu.data.durable_io import atomic_write_text, exclusive_path_lock
from guvolu.research import clock
from guvolu.research.provenance import (
    canonical_json,
    sha256_text,
    stable_identifier,
)

ECONOMIC_OBSERVATION_SCHEMA_VERSION = 1
ECONOMIC_CONTEXT_SCHEMA_VERSION = 1
ECONOMIC_PROPOSAL_SCHEMA_VERSION = 1
ECONOMIC_AGENT_LEDGER_SCHEMA_VERSION = 1
ECONOMIC_CONTEXT_METHOD_VERSION = "economic-context-v1"
ECONOMIC_PROPOSAL_METHOD_VERSION = "economic-search-plan-proposal-v1"
ECONOMIC_AGENT_METHOD_VERSION = "economic-research-agent-v1"

DIMENSIONS = ("growth", "inflation", "rates", "liquidity", "fx", "risk")
_REGIME_LABELS: Mapping[str, tuple[str, str, str]] = {
    "growth": ("strong", "weak", "balanced"),
    "inflation": ("hot", "cool", "balanced"),
    "rates": ("restrictive", "accommodative", "balanced"),
    "liquidity": ("abundant", "scarce", "balanced"),
    "fx": ("supportive", "adverse", "balanced"),
    "risk": ("risk_on", "risk_off", "balanced"),
}
_IDENTIFIER = re.compile(r"[a-zA-Z0-9][a-zA-Z0-9_.:-]{0,127}")
_SHA256 = re.compile(r"[0-9a-f]{64}")
_ZERO_HASH = "0" * 64
_SOURCE_RECEIPT_KEYS = frozenset({"source_id", "receipt_sha256", "locator"})


def _object(value: object, name: str) -> Mapping[str, object]:
    if not isinstance(value, dict):
        raise ValueError(f"{name} 必须为对象")
    return value


def _text(value: object, name: str, *, maximum: int = 4096) -> str:
    if not isinstance(value, str) or not value or len(value) > maximum:
        raise ValueError(f"{name} 必须为非空文本，且长度不超过 {maximum}")
    return value


def _identifier(value: object, name: str) -> str:
    text = _text(value, name, maximum=128)
    if _IDENTIFIER.fullmatch(text) is None:
        raise ValueError(f"{name} 不是规范标识")
    return text


def _sha256(value: object, name: str) -> str:
    text = _text(value, name, maximum=64)
    if _SHA256.fullmatch(text) is None:
        raise ValueError(f"{name} 必须为 64 位小写 SHA-256")
    return text


def _number(value: object, name: str) -> float:
    if isinstance(value, bool) or not isinstance(value, (int, float)):
        raise ValueError(f"{name} 必须为有限数值")
    result = float(value)
    if not math.isfinite(result):
        raise ValueError(f"{name} 必须为有限数值")
    return result


def _positive_integer(value: object, name: str) -> int:
    if isinstance(value, bool) or not isinstance(value, int) or value <= 0:
        raise ValueError(f"{name} 必须为正整数")
    return value


def _utc_time(value: object, name: str) -> datetime:
    text = _text(value, name, maximum=64)
    try:
        parsed = datetime.fromisoformat(text.replace("Z", "+00:00"))
    except ValueError as error:
        raise ValueError(f"{name} 不是合法 ISO-8601 时间") from error
    if parsed.tzinfo is None or parsed.utcoffset() is None:
        raise ValueError(f"{name} 必须携带时区")
    return parsed.astimezone(UTC)


def _time_text(value: datetime) -> str:
    if value.tzinfo is None or value.utcoffset() is None:
        raise ValueError("时间必须携带时区")
    return value.astimezone(UTC).isoformat(timespec="microseconds")


def _load_json(path: Path, name: str) -> Mapping[str, object]:
    try:
        loaded: object = json.loads(path.read_text(encoding="utf-8"))
    except (OSError, UnicodeDecodeError, json.JSONDecodeError) as error:
        raise ValueError(f"{name} 无法读取") from error
    return _object(loaded, name)


def _validate_exact_keys(
    value: Mapping[str, object],
    expected: frozenset[str],
    name: str,
) -> None:
    unexpected = sorted(set(value) - expected)
    missing = sorted(expected - set(value))
    if missing or unexpected:
        raise ValueError(
            f"{name} 字段不合同: missing={missing}, unexpected={unexpected}",
        )


@dataclass(frozen=True)
class EconomicObservation:
    """一条带修订链和三个源时点的经济观测。"""

    observation_id: str
    series_id: str
    value: float
    unit: str
    event_time: datetime
    available_time: datetime
    ingest_time: datetime
    revision_id: str
    supersedes_revision_id: str | None
    source_receipt: tuple[tuple[str, str], ...]

    def payload(self) -> Mapping[str, object]:
        """输出可散列的规范观测。"""
        return {
            "schema_version": ECONOMIC_OBSERVATION_SCHEMA_VERSION,
            "observation_id": self.observation_id,
            "series_id": self.series_id,
            "value": self.value,
            "unit": self.unit,
            "event_time": _time_text(self.event_time),
            "available_time": _time_text(self.available_time),
            "ingest_time": _time_text(self.ingest_time),
            "revision_id": self.revision_id,
            "supersedes_revision_id": self.supersedes_revision_id,
            "source_receipt": dict(self.source_receipt),
        }


@dataclass(frozen=True)
class EconomicObservationSnapshot:
    """绑定观测台账前缀与哈希链头的不可变快照。"""

    observations: tuple[EconomicObservation, ...]
    ledger_sequence: int
    ledger_head_sha256: str


def _source_receipt(value: object) -> tuple[tuple[str, str], ...]:
    receipt = _object(value, "source_receipt")
    unexpected = sorted(set(receipt) - _SOURCE_RECEIPT_KEYS)
    if unexpected:
        raise ValueError(f"source_receipt 含非合同字段: {unexpected}")
    source_id = _identifier(receipt.get("source_id"), "source_receipt.source_id")
    digest = _sha256(
        receipt.get("receipt_sha256"), "source_receipt.receipt_sha256",
    )
    normalized = {"source_id": source_id, "receipt_sha256": digest}
    locator = receipt.get("locator")
    if locator is not None:
        normalized["locator"] = _text(locator, "source_receipt.locator", maximum=1024)
    return tuple(sorted(normalized.items()))


def _observation_identities(
    *,
    series_id: str,
    value: float,
    unit: str,
    event_time: datetime,
    available_time: datetime,
    ingest_time: datetime,
    supersedes_revision_id: str | None,
    source_receipt: tuple[tuple[str, str], ...],
) -> tuple[str, str]:
    revision_body = {
        "series_id": series_id,
        "value": value,
        "unit": unit,
        "event_time": _time_text(event_time),
        "available_time": _time_text(available_time),
        "supersedes_revision_id": supersedes_revision_id,
        "source_receipt": dict(source_receipt),
    }
    revision_id = stable_identifier("economic-revision", revision_body)
    observation_id = stable_identifier("economic-observation", {
        **revision_body,
        "revision_id": revision_id,
        "ingest_time": _time_text(ingest_time),
    })
    return observation_id, revision_id


def parse_economic_observation(value: Mapping[str, object]) -> EconomicObservation:
    """规范化并验证一条观测；缺省标识由内容生成。"""
    schema = value.get("schema_version", ECONOMIC_OBSERVATION_SCHEMA_VERSION)
    if schema != ECONOMIC_OBSERVATION_SCHEMA_VERSION:
        raise ValueError("经济观测 schema_version 不受支持")
    series_id = _identifier(value.get("series_id"), "series_id")
    unit = _identifier(value.get("unit"), "unit")
    number = _number(value.get("value"), "value")
    event = _utc_time(value.get("event_time"), "event_time")
    available = _utc_time(value.get("available_time"), "available_time")
    ingested = _utc_time(value.get("ingest_time"), "ingest_time")
    if event > available:
        raise ValueError("available_time 不得早于 event_time")
    raw_supersedes = value.get("supersedes_revision_id")
    supersedes = (
        None
        if raw_supersedes is None
        else _text(raw_supersedes, "supersedes_revision_id", maximum=96)
    )
    receipt = _source_receipt(value.get("source_receipt"))
    expected_observation, expected_revision = _observation_identities(
        series_id=series_id,
        value=number,
        unit=unit,
        event_time=event,
        available_time=available,
        ingest_time=ingested,
        supersedes_revision_id=supersedes,
        source_receipt=receipt,
    )
    supplied_revision = value.get("revision_id")
    if supplied_revision is not None and supplied_revision != expected_revision:
        raise ValueError("revision_id 与规范内容不一致")
    supplied_observation = value.get("observation_id")
    if supplied_observation is not None and supplied_observation != expected_observation:
        raise ValueError("observation_id 与规范内容不一致")
    return EconomicObservation(
        observation_id=expected_observation,
        series_id=series_id,
        value=number,
        unit=unit,
        event_time=event,
        available_time=available,
        ingest_time=ingested,
        revision_id=expected_revision,
        supersedes_revision_id=supersedes,
        source_receipt=receipt,
    )


def _read_chain(path: Path, record_type: str) -> tuple[Mapping[str, object], ...]:
    if not path.exists():
        return ()
    try:
        body = path.read_bytes()
        text = body.decode("utf-8")
    except (OSError, UnicodeDecodeError) as error:
        raise ValueError(f"{record_type} 台账无法读取") from error
    if body and not body.endswith(b"\n"):
        raise ValueError(f"{record_type} 台账存在未完成行")
    lines = text.splitlines()
    previous = _ZERO_HASH
    rows: list[Mapping[str, object]] = []
    for sequence, line in enumerate(lines, start=1):
        try:
            loaded: object = json.loads(line)
        except json.JSONDecodeError as error:
            raise ValueError(f"{record_type} 台账第 {sequence} 行非法") from error
        row = _object(loaded, f"{record_type} 台账第 {sequence} 行")
        if canonical_json(row) != line:
            raise ValueError(f"{record_type} 台账第 {sequence} 行不是 canonical JSON")
        if row.get("record_type") != record_type or row.get("sequence") != sequence:
            raise ValueError(f"{record_type} 台账顺序或类型非法")
        if row.get("previous_record_sha256") != previous:
            raise ValueError(f"{record_type} 台账哈希链断裂")
        supplied_hash = _sha256(row.get("record_sha256"), "record_sha256")
        hash_body = dict(row)
        del hash_body["record_sha256"]
        expected_hash = sha256_text(canonical_json(hash_body))
        if supplied_hash != expected_hash:
            raise ValueError(f"{record_type} 台账记录散列不匹配")
        previous = supplied_hash
        rows.append(row)
    return tuple(rows)


def _chain_rows(
    record_type: str,
    existing: Sequence[Mapping[str, object]],
    payloads: Sequence[Mapping[str, object]],
) -> tuple[Mapping[str, object], ...]:
    previous = (
        _ZERO_HASH
        if not existing
        else _sha256(existing[-1].get("record_sha256"), "record_sha256")
    )
    rows: list[Mapping[str, object]] = []
    for offset, payload in enumerate(payloads, start=1):
        body: dict[str, object] = {
            "record_type": record_type,
            "sequence": len(existing) + offset,
            "previous_record_sha256": previous,
            **dict(payload),
        }
        digest = sha256_text(canonical_json(body))
        row = {**body, "record_sha256": digest}
        rows.append(row)
        previous = digest
    return tuple(rows)


def _append_chain_unlocked(
    path: Path,
    record_type: str,
    existing: Sequence[Mapping[str, object]],
    payloads: Sequence[Mapping[str, object]],
) -> tuple[Mapping[str, object], ...]:
    rows = _chain_rows(record_type, existing, payloads)
    if not rows:
        return ()
    path.parent.mkdir(parents=True, exist_ok=True)
    encoded = "".join(canonical_json(row) + "\n" for row in rows).encode("utf-8")
    with path.open("ab") as handle:
        handle.write(encoded)
        handle.flush()
        os.fsync(handle.fileno())
    return rows


def _validate_revision_chain(observations: Sequence[EconomicObservation]) -> None:
    observation_ids: set[str] = set()
    revision_ids: set[str] = set()
    latest: dict[tuple[str, datetime], EconomicObservation] = {}
    for observation in observations:
        if observation.observation_id in observation_ids:
            raise ValueError(f"重复 observation_id: {observation.observation_id}")
        if observation.revision_id in revision_ids:
            raise ValueError(f"重复 revision_id: {observation.revision_id}")
        key = (observation.series_id, observation.event_time)
        previous = latest.get(key)
        if previous is None:
            if observation.supersedes_revision_id is not None:
                raise ValueError("首版观测不得声称 supersedes_revision_id")
        else:
            if observation.supersedes_revision_id != previous.revision_id:
                raise ValueError("修订必须精确指向同期前一 revision_id")
            if observation.available_time <= previous.available_time:
                raise ValueError("修订 available_time 必须严格递增")
        observation_ids.add(observation.observation_id)
        revision_ids.add(observation.revision_id)
        latest[key] = observation


def _observation_snapshot(
    rows: Sequence[Mapping[str, object]],
) -> EconomicObservationSnapshot:
    observations = tuple(parse_economic_observation(row) for row in rows)
    _validate_revision_chain(observations)
    head = (
        _ZERO_HASH
        if not rows
        else _sha256(rows[-1].get("record_sha256"), "record_sha256")
    )
    return EconomicObservationSnapshot(observations, len(rows), head)


def load_economic_observation_snapshot(path: Path) -> EconomicObservationSnapshot:
    """在共享路径锁内读取并校验完整观测台账。"""
    with exclusive_path_lock(path):
        if not path.is_file():
            raise ValueError(f"经济观测台账不存在: {path}")
        rows = _read_chain(path, "economic_observation")
    return _observation_snapshot(rows)


def load_economic_observations(path: Path) -> tuple[EconomicObservation, ...]:
    """读取并全链校验追加式观测台账。"""
    return load_economic_observation_snapshot(path).observations


def append_economic_observations(
    path: Path,
    values: Sequence[Mapping[str, object]],
) -> tuple[EconomicObservation, ...]:
    """原子验证一批观测后追加；任一非法则整批不落盘。"""
    parsed = tuple(parse_economic_observation(value) for value in values)
    if not parsed:
        raise ValueError("观测批次不得为空")
    with exclusive_path_lock(path):
        rows = _read_chain(path, "economic_observation")
        existing = tuple(parse_economic_observation(row) for row in rows)
        _validate_revision_chain((*existing, *parsed))
        _append_chain_unlocked(
            path,
            "economic_observation",
            rows,
            tuple(observation.payload() for observation in parsed),
        )
    return parsed


@dataclass(frozen=True)
class EconomicSeriesPolicy:
    """一个经济序列的语义、归一化与新鲜度合同。"""

    series_id: str
    dimension: str
    unit: str
    neutral_value: float
    scale: float
    direction: str
    weight: float
    max_age_seconds: int

    def payload(self) -> Mapping[str, object]:
        return {
            "dimension": self.dimension,
            "unit": self.unit,
            "neutral_value": self.neutral_value,
            "scale": self.scale,
            "direction": self.direction,
            "weight": self.weight,
            "max_age_seconds": self.max_age_seconds,
        }


@dataclass(frozen=True)
class ProposalGatePolicy:
    """搜索提案配额、模板白名单和 holdout 隔离合同。"""

    allowed_templates: tuple[tuple[str, tuple[str, ...]], ...]
    template_parameters: tuple[tuple[str, tuple[str, ...]], ...]
    max_proposals_per_run: int
    max_trial_budget_per_proposal: int
    max_total_trial_budget: int
    max_parameter_count: int
    max_regime_count: int
    max_horizon: int
    holdout_start_time: datetime | None

    def templates_for(self, family: str) -> tuple[str, ...]:
        return dict(self.allowed_templates).get(family, ())

    def parameters_for(self, template: str) -> tuple[str, ...]:
        return dict(self.template_parameters).get(template, ())

    def payload(self) -> Mapping[str, object]:
        return {
            "allowed_templates": {
                family: list(templates) for family, templates in self.allowed_templates
            },
            "template_parameters": {
                template: list(parameters)
                for template, parameters in self.template_parameters
            },
            "max_proposals_per_run": self.max_proposals_per_run,
            "max_trial_budget_per_proposal": self.max_trial_budget_per_proposal,
            "max_total_trial_budget": self.max_total_trial_budget,
            "max_parameter_count": self.max_parameter_count,
            "max_regime_count": self.max_regime_count,
            "max_horizon": self.max_horizon,
            "holdout_start_time": (
                None
                if self.holdout_start_time is None
                else _time_text(self.holdout_start_time)
            ),
        }


@dataclass(frozen=True)
class EconomicAgentPolicy:
    """完整、可内容寻址的经济代理政策。"""

    series: tuple[EconomicSeriesPolicy, ...]
    regime_threshold: float
    proposal_gate: ProposalGatePolicy

    def payload(self) -> Mapping[str, object]:
        return {
            "schema_version": 1,
            "series": {
                item.series_id: dict(item.payload())
                for item in sorted(self.series, key=lambda item: item.series_id)
            },
            "regime_threshold": self.regime_threshold,
            "proposal_gate": dict(self.proposal_gate.payload()),
        }

    @property
    def policy_id(self) -> str:
        return stable_identifier("economic-policy", self.payload())


def _identifier_list(value: object, name: str) -> tuple[str, ...]:
    if not isinstance(value, list) or not value:
        raise ValueError(f"{name} 必须为非空列表")
    items = tuple(_identifier(item, f"{name}[]") for item in value)
    if len(set(items)) != len(items):
        raise ValueError(f"{name} 不得重复")
    return tuple(sorted(items))


def parse_economic_policy(value: Mapping[str, object]) -> EconomicAgentPolicy:
    """校验并规范化经济语境与提案门禁政策。"""
    _validate_exact_keys(
        value,
        frozenset({"schema_version", "series", "regime_threshold", "proposal_gate"}),
        "economic policy",
    )
    if value.get("schema_version") != 1:
        raise ValueError("economic policy schema_version 不受支持")
    raw_series = _object(value.get("series"), "series")
    series: list[EconomicSeriesPolicy] = []
    for raw_id, raw_spec in sorted(raw_series.items()):
        series_id = _identifier(raw_id, "series_id")
        spec = _object(raw_spec, f"series.{series_id}")
        _validate_exact_keys(
            spec,
            frozenset({
                "dimension", "unit", "neutral_value", "scale", "direction",
                "weight", "max_age_seconds",
            }),
            f"series.{series_id}",
        )
        dimension = _identifier(spec.get("dimension"), "dimension")
        if dimension not in DIMENSIONS:
            raise ValueError(f"不受支持的经济维度: {dimension}")
        direction = _identifier(spec.get("direction"), "direction")
        if direction not in {"higher", "lower"}:
            raise ValueError("direction 必须为 higher 或 lower")
        scale = _number(spec.get("scale"), "scale")
        weight = _number(spec.get("weight"), "weight")
        if scale <= 0.0 or weight <= 0.0:
            raise ValueError("scale 与 weight 必须大于零")
        series.append(EconomicSeriesPolicy(
            series_id=series_id,
            dimension=dimension,
            unit=_identifier(spec.get("unit"), "unit"),
            neutral_value=_number(spec.get("neutral_value"), "neutral_value"),
            scale=scale,
            direction=direction,
            weight=weight,
            max_age_seconds=_positive_integer(
                spec.get("max_age_seconds"), "max_age_seconds",
            ),
        ))
    threshold = _number(value.get("regime_threshold"), "regime_threshold")
    if threshold <= 0.0 or threshold > 3.0:
        raise ValueError("regime_threshold 必须在 (0, 3] 内")
    gate = _object(value.get("proposal_gate"), "proposal_gate")
    _validate_exact_keys(
        gate,
        frozenset({
            "allowed_templates", "template_parameters", "max_proposals_per_run",
            "max_trial_budget_per_proposal", "max_total_trial_budget",
            "max_parameter_count", "max_regime_count", "max_horizon",
            "holdout_start_time",
        }),
        "proposal_gate",
    )
    raw_allowed = _object(gate.get("allowed_templates"), "allowed_templates")
    allowed: list[tuple[str, tuple[str, ...]]] = []
    all_templates: set[str] = set()
    for raw_family, raw_templates in sorted(raw_allowed.items()):
        family = _identifier(raw_family, "family")
        templates = _identifier_list(raw_templates, f"allowed_templates.{family}")
        allowed.append((family, templates))
        all_templates.update(templates)
    raw_parameters = _object(
        gate.get("template_parameters"), "template_parameters",
    )
    parameters: list[tuple[str, tuple[str, ...]]] = []
    for raw_template, raw_names in sorted(raw_parameters.items()):
        template = _identifier(raw_template, "template")
        parameters.append((
            template,
            _identifier_list(raw_names, f"template_parameters.{template}"),
        ))
    if set(raw_parameters) != all_templates:
        raise ValueError("template_parameters 必须精确覆盖所有允许模板")
    raw_holdout = gate.get("holdout_start_time")
    holdout = (
        None
        if raw_holdout is None
        else _utc_time(raw_holdout, "holdout_start_time")
    )
    proposal_gate = ProposalGatePolicy(
        allowed_templates=tuple(allowed),
        template_parameters=tuple(parameters),
        max_proposals_per_run=_positive_integer(
            gate.get("max_proposals_per_run"), "max_proposals_per_run",
        ),
        max_trial_budget_per_proposal=_positive_integer(
            gate.get("max_trial_budget_per_proposal"),
            "max_trial_budget_per_proposal",
        ),
        max_total_trial_budget=_positive_integer(
            gate.get("max_total_trial_budget"), "max_total_trial_budget",
        ),
        max_parameter_count=_positive_integer(
            gate.get("max_parameter_count"), "max_parameter_count",
        ),
        max_regime_count=_positive_integer(
            gate.get("max_regime_count"), "max_regime_count",
        ),
        max_horizon=_positive_integer(gate.get("max_horizon"), "max_horizon"),
        holdout_start_time=holdout,
    )
    if (
        proposal_gate.max_total_trial_budget
        < proposal_gate.max_trial_budget_per_proposal
    ):
        raise ValueError("max_total_trial_budget 不得小于单提案上限")
    return EconomicAgentPolicy(tuple(series), threshold, proposal_gate)


def load_economic_policy(path: Path) -> EconomicAgentPolicy:
    """从 JSON 文件读取经济代理政策。"""
    return parse_economic_policy(_load_json(path, "economic policy"))


def _artifact(kind: str, body: Mapping[str, object]) -> Mapping[str, object]:
    artifact_id = stable_identifier(kind, body)
    return {**dict(body), "artifact_id": artifact_id}


def _verify_artifact(value: Mapping[str, object], kind: str) -> str:
    artifact_id = _text(value.get("artifact_id"), "artifact_id", maximum=96)
    body = dict(value)
    del body["artifact_id"]
    if artifact_id != stable_identifier(kind, body):
        raise ValueError(f"{kind} 制品散列不匹配")
    return artifact_id


def write_content_addressed_artifact(
    output: Path,
    value: Mapping[str, object],
    kind: str,
) -> Path:
    """以制品标识命名并且绝不改写既有不同内容。"""
    artifact_id = _verify_artifact(value, kind)
    content = canonical_json(value) + "\n"
    path = output / f"{artifact_id}.json"
    if path.exists():
        if path.read_text(encoding="utf-8") != content:
            raise ValueError(f"已有同名制品内容不同: {path}")
        return path
    atomic_write_text(path, content)
    return path


def load_content_addressed_artifact(path: Path, kind: str) -> Mapping[str, object]:
    """读取并校验内容寻址制品。"""
    value = _load_json(path, kind)
    artifact_id = _verify_artifact(value, kind)
    if path.stem != artifact_id:
        raise ValueError(f"{kind} 制品文件名与内容标识不一致")
    if path.read_text(encoding="utf-8") != canonical_json(value) + "\n":
        raise ValueError(f"{kind} 制品不是 canonical JSON")
    return value


def build_economic_context(
    snapshot: EconomicObservationSnapshot,
    decision_time: datetime,
    policy: EconomicAgentPolicy,
) -> Mapping[str, object]:
    """按第四时点 decision_time 回放可知修订并生成确定性语境。"""
    decision = _utc_time(_time_text(decision_time), "decision_time")
    observations = snapshot.observations
    _validate_revision_chain(observations)
    # D-04 防未来。
    # 只按可用时点判定。
    # ingest_time 只记录落盘。
    # 它可以晚于 decision_time。
    # 不得据此抹去公开事实。
    eligible = tuple(sorted(
        (
            observation
            for observation in observations
            if observation.available_time <= decision
        ),
        key=lambda item: (
            item.series_id,
            item.event_time,
            item.available_time,
            item.ingest_time,
            item.revision_id,
        ),
    ))
    latest_revision: dict[tuple[str, datetime], EconomicObservation] = {}
    for observation in eligible:
        latest_revision[(observation.series_id, observation.event_time)] = observation
    latest_series: dict[str, EconomicObservation] = {}
    for observation in latest_revision.values():
        current = latest_series.get(observation.series_id)
        if current is None or (
            observation.event_time,
            observation.available_time,
            observation.ingest_time,
        ) > (current.event_time, current.available_time, current.ingest_time):
            latest_series[observation.series_id] = observation

    evidence: dict[str, Mapping[str, object]] = {}
    dimensions: dict[str, object] = {}
    missing_series: list[str] = []
    stale_series: list[str] = []
    for dimension in DIMENSIONS:
        configured = tuple(
            item for item in policy.series if item.dimension == dimension
        )
        series_states: list[Mapping[str, object]] = []
        score_parts: list[tuple[float, float]] = []
        for spec in configured:
            selected = latest_series.get(spec.series_id)
            if selected is None:
                missing_series.append(spec.series_id)
                series_states.append({
                    "series_id": spec.series_id,
                    "status": "missing",
                    "observation_id": None,
                    "revision_id": None,
                    "age_seconds": None,
                    "normalized_score": None,
                })
                continue
            if selected.unit != spec.unit:
                raise ValueError(
                    f"序列 {spec.series_id} unit={selected.unit} 与政策不一致",
                )
            age = (decision - selected.available_time).total_seconds()
            normalized = (selected.value - spec.neutral_value) / spec.scale
            if spec.direction == "lower":
                normalized = -normalized
            normalized = round(max(-3.0, min(3.0, normalized)), 12)
            stale = age > spec.max_age_seconds
            status = "stale" if stale else "fresh"
            if stale:
                stale_series.append(spec.series_id)
            else:
                score_parts.append((normalized, spec.weight))
                evidence[selected.observation_id] = selected.payload()
            series_states.append({
                "series_id": spec.series_id,
                "status": status,
                "observation_id": selected.observation_id,
                "revision_id": selected.revision_id,
                "event_time": _time_text(selected.event_time),
                "available_time": _time_text(selected.available_time),
                "ingest_time": _time_text(selected.ingest_time),
                "value": selected.value,
                "unit": selected.unit,
                "age_seconds": round(age, 6),
                "normalized_score": normalized,
            })
        fresh_count = len(score_parts)
        if not configured or not series_states or all(
            state["status"] == "missing" for state in series_states
        ):
            data_status = "missing"
        elif fresh_count == 0:
            data_status = "stale"
        elif fresh_count < len(configured):
            data_status = "partial"
        else:
            data_status = "fresh"
        score = (
            None
            if not score_parts
            else round(
                sum(item * weight for item, weight in score_parts)
                / sum(weight for _item, weight in score_parts),
                12,
            )
        )
        positive, negative, neutral = _REGIME_LABELS[dimension]
        regime = (
            "unknown"
            if score is None
            else positive
            if score >= policy.regime_threshold
            else negative
            if score <= -policy.regime_threshold
            else neutral
        )
        dimensions[dimension] = {
            "data_status": data_status,
            "score": score,
            "regime": regime,
            "configured_series_count": len(configured),
            "fresh_series_count": fresh_count,
            "series": series_states,
        }
    eligible_payloads = [observation.payload() for observation in eligible]
    body: Mapping[str, object] = {
        "schema_version": ECONOMIC_CONTEXT_SCHEMA_VERSION,
        "artifact_type": "economic_context",
        "method_version": ECONOMIC_CONTEXT_METHOD_VERSION,
        "decision_time": _time_text(decision),
        "policy_id": policy.policy_id,
        "policy": policy.payload(),
        "observation_ledger": {
            "sequence": snapshot.ledger_sequence,
            "head_sha256": snapshot.ledger_head_sha256,
        },
        "as_of_input_sha256": sha256_text(canonical_json(eligible_payloads)),
        "eligible_observation_ids": [
            observation.observation_id for observation in eligible
        ],
        "selected_observation_ids": sorted(evidence),
        "selected_revision_ids": sorted(
            str(item["revision_id"]) for item in evidence.values()
        ),
        "evidence": [evidence[item] for item in sorted(evidence)],
        "dimensions": dimensions,
        "quality": {
            "pit": True,
            "pit_basis": "available_time_lte_decision_time",
            "four_clocks_separated": True,
            "missing_series": sorted(missing_series),
            "stale_series": sorted(stale_series),
            "missing_dimensions": [
                dimension for dimension in DIMENSIONS
                if _object(dimensions[dimension], dimension)["data_status"]
                == "missing"
            ],
            "stale_dimensions": [
                dimension for dimension in DIMENSIONS
                if _object(dimensions[dimension], dimension)["data_status"]
                == "stale"
            ],
        },
        "authority": {
            "research_only": True,
            "network": False,
            "secrets": False,
            "trade": False,
            "execution": False,
            "config_mutation": False,
            "registry_mutation": False,
            "promotion": False,
            "holdout_governance_bound": False,
        },
    }
    return _artifact("economic-context", body)


def verify_economic_context(
    context: Mapping[str, object],
    observation_ledger_path: Path,
    policy: EconomicAgentPolicy,
) -> str:
    """由台账已绑定前缀重建 context，并逐字节比对规范制品。"""
    context_id = _verify_artifact(context, "economic-context")
    ledger_identity = _object(
        context.get("observation_ledger"), "context.observation_ledger",
    )
    raw_sequence = ledger_identity.get("sequence")
    if (
        isinstance(raw_sequence, bool)
        or not isinstance(raw_sequence, int)
        or raw_sequence < 0
    ):
        raise ValueError("context.observation_ledger.sequence 非法")
    expected_head = _sha256(
        ledger_identity.get("head_sha256"),
        "context.observation_ledger.head_sha256",
    )
    with exclusive_path_lock(observation_ledger_path):
        if not observation_ledger_path.is_file():
            raise ValueError(f"经济观测台账不存在: {observation_ledger_path}")
        rows = _read_chain(observation_ledger_path, "economic_observation")
    if len(rows) < raw_sequence:
        raise ValueError("context 绑定的观测台账前缀已缺失")
    prefix = rows[:raw_sequence]
    snapshot = _observation_snapshot(prefix)
    if snapshot.ledger_head_sha256 != expected_head:
        raise ValueError("context 绑定的观测台账链头不匹配")
    decision = _utc_time(context.get("decision_time"), "context.decision_time")
    expected = build_economic_context(snapshot, decision, policy)
    if canonical_json(expected) != canonical_json(context):
        raise ValueError("economic-context 不能由已绑定观测台账重建")
    return context_id


def _normalize_bounds(value: object, name: str) -> Mapping[str, object]:
    bounds = _object(value, name)
    unexpected = sorted(set(bounds) - {"minimum", "maximum", "step"})
    if unexpected or "minimum" not in bounds or "maximum" not in bounds:
        raise ValueError(f"{name} 范围字段非法")
    minimum = _number(bounds.get("minimum"), f"{name}.minimum")
    maximum = _number(bounds.get("maximum"), f"{name}.maximum")
    if minimum > maximum:
        raise ValueError(f"{name}.minimum 不得大于 maximum")
    result: dict[str, object] = {"minimum": minimum, "maximum": maximum}
    if "step" in bounds:
        step = _number(bounds.get("step"), f"{name}.step")
        if step <= 0.0:
            raise ValueError(f"{name}.step 必须大于零")
        result["step"] = step
    return result


def _normalize_proposal(
    value: Mapping[str, object],
    policy: ProposalGatePolicy,
) -> Mapping[str, object]:
    required = frozenset({
        "hypothesis", "evidence_ids", "family", "template", "parameter_bounds",
        "regimes", "horizon", "falsification", "trial_budget",
    })
    unexpected = sorted(set(value) - (required | {"proposal_id"}))
    missing = sorted(required - set(value))
    if unexpected or missing:
        raise ValueError(
            f"ResearchProposal 字段不合同: missing={missing}, "
            f"unexpected={unexpected}",
        )
    family = _identifier(value.get("family"), "family")
    template = _identifier(value.get("template"), "template")
    if template not in policy.templates_for(family):
        raise ValueError("提案 family/template 不在白名单")
    raw_bounds = _object(value.get("parameter_bounds"), "parameter_bounds")
    if not raw_bounds or len(raw_bounds) > policy.max_parameter_count:
        raise ValueError("parameter_bounds 为空或超出参数配额")
    allowed_parameters = set(policy.parameters_for(template))
    if not set(raw_bounds).issubset(allowed_parameters):
        raise ValueError("parameter_bounds 含模板未登记参数")
    parameter_bounds = {
        _identifier(name, "parameter name"): _normalize_bounds(
            bounds, f"parameter_bounds.{name}",
        )
        for name, bounds in sorted(raw_bounds.items())
    }
    evidence_ids = _identifier_list(value.get("evidence_ids"), "evidence_ids")
    regimes = _identifier_list(value.get("regimes"), "regimes")
    if len(regimes) > policy.max_regime_count:
        raise ValueError("regimes 超出配额")
    for regime in regimes:
        parts = regime.split(":", maxsplit=1)
        if len(parts) != 2 or parts[0] not in DIMENSIONS:
            raise ValueError(f"非法 regime: {regime}")
    horizon = _object(value.get("horizon"), "horizon")
    _validate_exact_keys(
        horizon, frozenset({"unit", "minimum", "maximum"}), "horizon",
    )
    unit = _identifier(horizon.get("unit"), "horizon.unit")
    if unit not in {"bars", "hours", "days"}:
        raise ValueError("horizon.unit 只能为 bars/hours/days")
    horizon_min = _positive_integer(horizon.get("minimum"), "horizon.minimum")
    horizon_max = _positive_integer(horizon.get("maximum"), "horizon.maximum")
    if horizon_min > horizon_max or horizon_max > policy.max_horizon:
        raise ValueError("horizon 顺序非法或超出上限")
    trial_budget = _positive_integer(value.get("trial_budget"), "trial_budget")
    if trial_budget > policy.max_trial_budget_per_proposal:
        raise ValueError("trial_budget 超出单提案上限")
    body: Mapping[str, object] = {
        "schema_version": ECONOMIC_PROPOSAL_SCHEMA_VERSION,
        "hypothesis": _text(value.get("hypothesis"), "hypothesis"),
        "evidence_ids": list(evidence_ids),
        "family": family,
        "template": template,
        "parameter_bounds": parameter_bounds,
        "regimes": list(regimes),
        "horizon": {
            "unit": unit,
            "minimum": horizon_min,
            "maximum": horizon_max,
        },
        "falsification": _text(value.get("falsification"), "falsification"),
        "trial_budget": trial_budget,
    }
    proposal_id = stable_identifier("research-proposal", body)
    supplied = value.get("proposal_id")
    if supplied is not None and supplied != proposal_id:
        raise ValueError("proposal_id 与规范提案不一致")
    return {**dict(body), "proposal_id": proposal_id}


def _inference_identity(value: Mapping[str, object] | None) -> Mapping[str, object]:
    if value is None:
        empty = sha256_text("")
        return {
            "provider": "none",
            "model_id": "deterministic-rules-v1",
            "model_parameters_sha256": sha256_text(canonical_json({})),
            "prompt_template_id": "none",
            "prompt_sha256": empty,
            "model_input_sha256": empty,
            "model_output_sha256": empty,
        }
    expected = frozenset({
        "provider", "model_id", "model_parameters_sha256", "prompt_template_id",
        "prompt_sha256", "model_input_sha256", "model_output_sha256",
    })
    _validate_exact_keys(value, expected, "inference_identity")
    return {
        "provider": _identifier(value.get("provider"), "provider"),
        "model_id": _identifier(value.get("model_id"), "model_id"),
        "model_parameters_sha256": _sha256(
            value.get("model_parameters_sha256"), "model_parameters_sha256",
        ),
        "prompt_template_id": _identifier(
            value.get("prompt_template_id"), "prompt_template_id",
        ),
        "prompt_sha256": _sha256(value.get("prompt_sha256"), "prompt_sha256"),
        "model_input_sha256": _sha256(
            value.get("model_input_sha256"), "model_input_sha256",
        ),
        "model_output_sha256": _sha256(
            value.get("model_output_sha256"), "model_output_sha256",
        ),
    }


@dataclass(frozen=True)
class EconomicAgentRunResult:
    """一次提案门禁的内容寻址输出。"""

    run_id: str
    receipt_path: Path
    proposal_paths: tuple[Path, ...]
    accepted_proposal_ids: tuple[str, ...]
    rejected_count: int


def _accepted_proposal_ids(rows: Sequence[Mapping[str, object]]) -> set[str]:
    accepted: set[str] = set()
    for row in rows:
        attempts = row.get("attempts")
        if not isinstance(attempts, list):
            raise ValueError("economic_agent_run 台账缺少 attempts")
        for raw_attempt in attempts:
            attempt = _object(raw_attempt, "attempt")
            proposal_id = attempt.get("proposal_id")
            if attempt.get("status") == "accepted" and isinstance(proposal_id, str):
                accepted.add(proposal_id)
    return accepted


def _proposal_context_reasons(
    proposal: Mapping[str, object],
    context: Mapping[str, object],
    evidence: Mapping[str, Mapping[str, object]],
    gate: ProposalGatePolicy,
    evaluated_at: datetime,
) -> list[str]:
    reasons: list[str] = []
    evidence_ids = proposal.get("evidence_ids")
    assert isinstance(evidence_ids, list)
    unknown_evidence = sorted(set(evidence_ids) - set(evidence))
    if unknown_evidence:
        reasons.append("evidence_not_fresh_or_not_in_context")
    dimensions = _object(context.get("dimensions"), "context.dimensions")
    regimes = proposal.get("regimes")
    assert isinstance(regimes, list)
    for raw_regime in regimes:
        dimension, expected = str(raw_regime).split(":", maxsplit=1)
        state = _object(dimensions.get(dimension), f"dimensions.{dimension}")
        if state.get("regime") != expected or state.get("data_status") not in {
            "fresh", "partial",
        }:
            reasons.append("regime_not_supported_by_current_context")
            break
    boundary = gate.holdout_start_time
    context_time = _utc_time(context.get("decision_time"), "context.decision_time")
    if evaluated_at < context_time:
        reasons.append("evaluation_precedes_context")
    if boundary is not None:
        reasons.append("holdout_governance_unbound")
        if context_time >= boundary or evaluated_at >= boundary:
            reasons.append("holdout_boundary_reached")
        for evidence_id in evidence_ids:
            item = evidence.get(str(evidence_id))
            if item is not None and _utc_time(
                item.get("available_time"), "evidence.available_time",
            ) >= boundary:
                reasons.append("holdout_evidence_leakage")
                break
    return sorted(set(reasons))


def _attempt_index(value: Mapping[str, object]) -> int:
    raw = value.get("input_index")
    if isinstance(raw, bool) or not isinstance(raw, int):
        raise ValueError("提案尝试缺少合法 input_index")
    return raw


def run_economic_research_agent(
    *,
    context: Mapping[str, object],
    proposals: Sequence[Mapping[str, object]],
    policy: EconomicAgentPolicy,
    observation_ledger_path: Path,
    output: Path,
    ledger_path: Path,
    inference_identity: Mapping[str, object] | None = None,
) -> EconomicAgentRunResult:
    """审计全部提案尝试，仅写 proposal-only 制品和追加台账。"""
    if not proposals:
        raise ValueError("ResearchProposal 批次不得为空")
    context_id = verify_economic_context(
        context, observation_ledger_path, policy,
    )
    evaluated = _utc_time(_time_text(clock.utc_now()), "evaluated_at")
    holdout_bound = False
    holdout_reason = (
        "holdout_governance_unbound"
        if policy.proposal_gate.holdout_start_time is not None
        else None
    )
    evidence_rows = context.get("evidence")
    if not isinstance(evidence_rows, list):
        raise ValueError("经济语境缺少 evidence")
    evidence: dict[str, Mapping[str, object]] = {}
    for raw in evidence_rows:
        item = _object(raw, "context.evidence[]")
        observation = parse_economic_observation(item)
        evidence[observation.observation_id] = observation.payload()
    batch_sha256 = sha256_text(canonical_json(list(proposals)))
    inference = _inference_identity(inference_identity)
    with exclusive_path_lock(ledger_path):
        ledger_rows = _read_chain(ledger_path, "economic_agent_run")
        historical = _accepted_proposal_ids(ledger_rows)
        normalized: list[tuple[int, Mapping[str, object]]] = []
        attempts: list[Mapping[str, object]] = []
        for index, proposal in enumerate(proposals):
            try:
                normalized.append((index, _normalize_proposal(proposal, policy.proposal_gate)))
            except ValueError as error:
                raw_hash = sha256_text(canonical_json(proposal))
                attempts.append({
                    "input_index": index,
                    "input_sha256": raw_hash,
                    "proposal_id": None,
                    "status": "rejected",
                    "reasons": [f"contract_invalid:{error}"],
                })
        accepted_artifacts: list[Mapping[str, object]] = []
        accepted_ids: set[str] = set()
        total_budget = 0
        for index, proposal in sorted(
            normalized, key=lambda item: (str(item[1]["proposal_id"]), item[0]),
        ):
            proposal_id = str(proposal["proposal_id"])
            reasons = _proposal_context_reasons(
                proposal,
                context,
                evidence,
                policy.proposal_gate,
                evaluated,
            )
            if proposal_id in historical or proposal_id in accepted_ids:
                reasons.append("duplicate_proposal")
            if len(accepted_artifacts) >= policy.proposal_gate.max_proposals_per_run:
                reasons.append("proposal_quota_exceeded")
            budget = _positive_integer(proposal.get("trial_budget"), "trial_budget")
            if total_budget + budget > policy.proposal_gate.max_total_trial_budget:
                reasons.append("total_trial_budget_exceeded")
            reasons = sorted(set(reasons))
            if reasons:
                attempts.append({
                    "input_index": index,
                    "input_sha256": sha256_text(canonical_json(proposal)),
                    "proposal_id": proposal_id,
                    "status": "rejected",
                    "reasons": reasons,
                })
                continue
            artifact_body: Mapping[str, object] = {
                "schema_version": ECONOMIC_PROPOSAL_SCHEMA_VERSION,
                "artifact_type": "economic_search_plan_proposal",
                "method_version": ECONOMIC_PROPOSAL_METHOD_VERSION,
                "source_context_artifact_id": context_id,
                "policy_id": policy.policy_id,
                "proposal": proposal,
                "search_plan_interface": {
                    "contract": "proposal_only",
                    "consumer": "SearchPlan",
                    "family": proposal["family"],
                    "template": proposal["template"],
                    "parameter_bounds": proposal["parameter_bounds"],
                    "trial_budget": proposal["trial_budget"],
                    "requires_explicit_review": True,
                    "requires_candidate_registration": True,
                    "holdout_governance_bound": holdout_bound,
                    "may_write_config": False,
                    "may_write_registry": False,
                    "may_promote": False,
                },
                "authority": {
                    "research_only": True,
                    "network": False,
                    "secrets": False,
                    "trade": False,
                    "execution": False,
                    "holdout_governance_bound": holdout_bound,
                },
            }
            proposal_artifact = _artifact(
                "economic-search-plan-proposal", artifact_body,
            )
            accepted_artifacts.append(proposal_artifact)
            accepted_ids.add(proposal_id)
            total_budget += budget
            attempts.append({
                "input_index": index,
                "input_sha256": sha256_text(canonical_json(proposal)),
                "proposal_id": proposal_id,
                "status": "accepted",
                "reasons": [],
                "output_artifact_id": proposal_artifact["artifact_id"],
            })
        attempts.sort(key=_attempt_index)
        output_identity = {
            "accepted_artifact_ids": sorted(
                str(item["artifact_id"]) for item in accepted_artifacts
            ),
            "accepted_proposal_ids": sorted(accepted_ids),
            "attempts_sha256": sha256_text(canonical_json(attempts)),
            "total_trial_budget": total_budget,
        }
        input_identity = {
            "context_artifact_id": context_id,
            "policy_id": policy.policy_id,
            "proposal_batch_sha256": batch_sha256,
            "proposal_count": len(proposals),
            "observation_ledger": context["observation_ledger"],
            "holdout_governance": {
                "bound": holdout_bound,
                "binding": None,
                "reason": holdout_reason,
            },
        }
        receipt_body: Mapping[str, object] = {
            "schema_version": ECONOMIC_AGENT_LEDGER_SCHEMA_VERSION,
            "artifact_type": "economic_agent_run_receipt",
            "method_version": ECONOMIC_AGENT_METHOD_VERSION,
            "evaluated_at": _time_text(evaluated),
            "model_identity": inference,
            "prompt_identity": {
                "prompt_template_id": inference["prompt_template_id"],
                "prompt_sha256": inference["prompt_sha256"],
            },
            "input_identity": input_identity,
            "output_identity": output_identity,
            "attempts": attempts,
            "authority": {
                "research_only": True,
                "network": False,
                "secrets": False,
                "trade": False,
                "execution": False,
                "config_mutation": False,
                "registry_mutation": False,
                "promotion": False,
                "holdout_governance_bound": holdout_bound,
            },
        }
        receipt = _artifact("economic-agent-run", receipt_body)
        run_id = str(receipt["artifact_id"])
        if any(row.get("run_id") == run_id for row in ledger_rows):
            raise ValueError("相同 economic agent run 已入账")
        proposal_paths = tuple(
            write_content_addressed_artifact(
                output / "proposals", item, "economic-search-plan-proposal",
            )
            for item in accepted_artifacts
        )
        receipt_path = write_content_addressed_artifact(
            output / "runs", receipt, "economic-agent-run",
        )
        ledger_payload: Mapping[str, object] = {
            "schema_version": ECONOMIC_AGENT_LEDGER_SCHEMA_VERSION,
            "method_version": ECONOMIC_AGENT_METHOD_VERSION,
            "run_id": run_id,
            "evaluated_at": _time_text(evaluated),
            "receipt_artifact_id": run_id,
            "receipt_sha256": sha256_text(canonical_json(receipt) + "\n"),
            "model_identity": inference,
            "prompt_identity": receipt["prompt_identity"],
            "input_identity": input_identity,
            "output_identity": output_identity,
            "attempts": attempts,
            "research_only": True,
        }
        _append_chain_unlocked(
            ledger_path,
            "economic_agent_run",
            ledger_rows,
            (ledger_payload,),
        )
    return EconomicAgentRunResult(
        run_id=run_id,
        receipt_path=receipt_path,
        proposal_paths=proposal_paths,
        accepted_proposal_ids=tuple(sorted(accepted_ids)),
        rejected_count=sum(item["status"] == "rejected" for item in attempts),
    )


def verify_economic_agent_ledger(path: Path) -> tuple[Mapping[str, object], ...]:
    """校验运行台账哈希链与必需身份字段。"""
    with exclusive_path_lock(path):
        if not path.is_file():
            raise ValueError(f"经济代理运行台账不存在: {path}")
        rows = _read_chain(path, "economic_agent_run")
    _accepted_proposal_ids(rows)
    for row in rows:
        if row.get("research_only") is not True:
            raise ValueError("economic agent 台账不是 research_only")
        for field in (
            "model_identity", "prompt_identity", "input_identity", "output_identity",
        ):
            _object(row.get(field), field)
    return rows

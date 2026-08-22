"""活动物化 head 的只读市场目录。

查询层只暴露已登记的 market、能力修订和活动输出摘要；不扫描目录，
也不把未激活 attempt 混入前端可见覆盖。
"""
from __future__ import annotations

import hashlib
import json
import sqlite3
from collections import defaultdict
from dataclasses import dataclass
from datetime import UTC, datetime
from pathlib import Path
from typing import Any, Iterable

from guvolu.data.store import connect_readonly
from guvolu.data.storage_paths import storage_resolver

CATALOG_SCHEMA_VERSION = 1
L2_QUALITY_SCHEMA_VERSION = 1
L2_QUALITY_VERSION = "l2-quality-v1"
L2_MATERIALIZED_FRESHNESS_THRESHOLD_SECONDS = 12 * 60
L2_FRESHNESS_BASIS = "latest_materialized_observation_time"


@dataclass(frozen=True)
class ActiveOutput:
    """一个活动分区 head 对应的不可变 Parquet 输出。"""

    domain: str
    partition_key: str
    normalization_version: str
    attempt_id: str
    dataset: str
    artifact_id: str
    path: Path
    row_count: int
    min_event_time: datetime | None
    max_event_time: datetime | None


@dataclass(frozen=True)
class ActiveOutputSnapshot:
    """一次 SQLite 读事务内冻结的市场输出清单。"""

    market: dict[str, Any]
    outputs: tuple[ActiveOutput, ...]
    head_generation: str


@dataclass(frozen=True)
class MultiMarketOutputSnapshot:
    """同一 SQLite 读事务内冻结的多市场输出与质量窗。"""

    decision_time: datetime
    markets: tuple[ActiveOutputSnapshot, ...]
    qualities: tuple[tuple[str, dict[str, Any]], ...]
    head_generation: str

    def quality_for(self, market_id: str) -> dict[str, Any]:
        """按市场返回冻结质量；调用方不得修改返回字典。"""
        for candidate, quality in self.qualities:
            if candidate == market_id:
                return quality
        return QueryCatalog._unknown_l2_quality(market_id)


@dataclass(frozen=True)
class AttemptLineage:
    """一个物化 attempt 在 SQLite 中可证明的精确输入血缘。"""

    input_set_hash: str
    upstream_attempt_ids: frozenset[str]
    input_artifact_ids: frozenset[str]


MAX_L2_CHECKPOINT_SOURCE_ATTEMPTS = 12


def materialization_input_set_hash(outputs: Iterable[ActiveOutput]) -> str:
    """复现 checkpoint 物化器的制品级输入集合散列。"""
    body = json.dumps(sorted(
        (row.dataset, row.artifact_id, row.attempt_id) for row in outputs
    ), separators=(",", ":"))
    return hashlib.sha256(body.encode()).hexdigest()


def select_l2_checkpoint_inputs(
    snapshot: ActiveOutputSnapshot,
    *,
    max_attempts: int = MAX_L2_CHECKPOINT_SOURCE_ATTEMPTS,
) -> ActiveOutputSnapshot:
    """按 checkpoint 写入端规则选择最近且 frame/level 成对的 L2 输入。"""
    if max_attempts <= 0:
        raise ValueError("L2 checkpoint 输入 attempt 上限必须为正数")
    grouped: dict[str, set[str]] = {}
    for row in snapshot.outputs:
        grouped.setdefault(row.attempt_id, set()).add(row.dataset)
    incomplete = [
        attempt_id for attempt_id, datasets in grouped.items()
        if datasets != {"book_l2_frame", "book_l2_level"}
    ]
    if incomplete:
        raise ValueError(
            f"L2 活动 attempt 缺少 frame/level 配对: {incomplete[0]}"
        )
    floor_time = datetime.min.replace(tzinfo=UTC)
    frame_outputs = sorted(
        (row for row in snapshot.outputs if row.dataset == "book_l2_frame"),
        key=lambda row: (
            row.max_event_time or floor_time,
            row.partition_key,
            row.attempt_id,
        ),
        reverse=True,
    )
    selected_attempts = {
        row.attempt_id for row in frame_outputs[:max_attempts]
    }
    return ActiveOutputSnapshot(
        market=snapshot.market,
        outputs=tuple(
            row for row in snapshot.outputs
            if row.attempt_id in selected_attempts
        ),
        head_generation=snapshot.head_generation,
    )


def _timestamp(value: object) -> datetime | None:
    if value is None:
        return None
    try:
        parsed = datetime.fromisoformat(str(value).replace("Z", "+00:00"))
    except ValueError:
        return None
    if parsed.tzinfo is None:
        parsed = parsed.replace(tzinfo=UTC)
    return parsed.astimezone(UTC)


def _iso(value: datetime | None) -> str | None:
    return None if value is None else value.isoformat()


def _generation(heads: Iterable[tuple[str, ...]]) -> str:
    body = json.dumps(sorted(heads), separators=(",", ":"), ensure_ascii=False)
    return "sha256-" + hashlib.sha256(body.encode("utf-8")).hexdigest()


def _min_time(current: datetime | None, candidate: datetime) -> datetime:
    return candidate if current is None else min(current, candidate)


def _max_time(current: datetime | None, candidate: datetime) -> datetime:
    return candidate if current is None else max(current, candidate)


class QueryCatalog:
    """从 SQLite 活动头生成市场、能力和覆盖目录。"""

    def __init__(self, data_root: Path) -> None:
        self.data_root = data_root

    def list_markets(self) -> list[dict[str, Any]]:
        conn = connect_readonly(self.data_root)
        if conn is None:
            return []
        conn.row_factory = sqlite3.Row
        try:
            return self._list_markets(conn)
        finally:
            conn.close()

    def snapshot(self) -> dict[str, list[dict[str, Any]]]:
        """返回去重后的市场与来源能力目录。"""
        conn = connect_readonly(self.data_root)
        if conn is None:
            return {"markets": [], "venues": []}
        conn.row_factory = sqlite3.Row
        try:
            return {
                "markets": self._list_markets(conn),
                "venues": self._list_venues(conn),
            }
        finally:
            conn.close()

    def latest_l2_quality(self, market_id: str) -> dict[str, Any]:
        """读取最新 L2 物化质量窗；旧库与空窗明确返回未知。"""
        unknown = self._unknown_l2_quality(market_id)
        conn = connect_readonly(self.data_root)
        if conn is None:
            return unknown
        conn.row_factory = sqlite3.Row
        try:
            try:
                row = conn.execute(
                    "SELECT * FROM l2_quality_window WHERE market_id=? "
                    "AND quality_version=? ORDER BY window_start DESC LIMIT 1",
                    (market_id, L2_QUALITY_VERSION),
                ).fetchone()
            except sqlite3.OperationalError as exc:
                if "no such table" not in str(exc).lower():
                    raise
                return unknown
        finally:
            conn.close()
        if row is None:
            return unknown
        return self._l2_quality_payload(market_id, row)

    @staticmethod
    def _l2_quality_payload(
        market_id: str, row: sqlite3.Row,
    ) -> dict[str, Any]:
        """把控制面行解码为稳定的只读质量契约。"""
        reasons: list[str]
        try:
            decoded = json.loads(str(row["reasons"]))
            reasons = (
                [str(item) for item in decoded]
                if isinstance(decoded, list) else ["quality_reasons_invalid"]
            )
        except json.JSONDecodeError:
            reasons = ["quality_reasons_invalid"]
        try:
            decoded_attempts = json.loads(str(row["source_attempt_ids"]))
            source_attempt_ids = (
                sorted(str(item) for item in decoded_attempts)
                if isinstance(decoded_attempts, list) else []
            )
        except json.JSONDecodeError:
            source_attempt_ids = []
            reasons.append("quality_source_attempt_ids_invalid")
        try:
            decoded_versions = json.loads(
                str(row["source_normalization_versions"])
            )
            source_normalization_versions = (
                sorted(str(item) for item in decoded_versions)
                if isinstance(decoded_versions, list) else []
            )
        except json.JSONDecodeError:
            source_normalization_versions = []
            reasons.append("quality_source_normalization_versions_invalid")
        return {
            "schema_version": L2_QUALITY_SCHEMA_VERSION,
            "market_id": market_id,
            "quality_version": str(row["quality_version"]),
            "source_head_generation": str(row["source_head_generation"]),
            "source_attempt_ids": source_attempt_ids,
            "source_attempt_count": int(row["source_attempt_count"]),
            "source_normalization_versions": source_normalization_versions,
            "status": str(row["status"]),
            "reasons": reasons,
            "window_start": str(row["window_start"]),
            "window_end": str(row["window_end"]),
            "window_clock_basis": str(row["window_clock_basis"]),
            "frames": int(row["frames"]),
            "snapshot_frames": int(row["snapshot_frames"]),
            "delta_frames": int(row["delta_frames"]),
            "checksum_status": str(row["checksum_status"]),
            "checksum_observed_frames": int(row["checksum_observed_frames"]),
            "checksum_checked_frames": row["checksum_checked_frames"],
            "checksum_failures": row["checksum_failures"],
            "unanchored_before_snapshot_frames": (
                row["unanchored_before_snapshot_frames"]
            ),
            "anchor_unknown_frames": int(row["anchor_unknown_frames"]),
            "sequence_duplicates": row["sequence_duplicates"],
            "sequence_regressions": row["sequence_regressions"],
            "predecessor_unknown_frames": row["predecessor_unknown_frames"],
            "latency_status": str(row["latency_status"]),
            "recv_source_offset_samples": int(row["recv_source_offset_samples"]),
            "recv_source_offset_p50_ms": row["recv_source_offset_p50_ms"],
            "recv_source_offset_p95_ms": row["recv_source_offset_p95_ms"],
            "latest_materialized_observation_time": (
                row["latest_materialized_observation_time"]
            ),
            "materialized_freshness_seconds": row["materialized_freshness_seconds"],
            "materialized_freshness_status": (
                str(row["materialized_freshness_status"])
            ),
            "freshness_basis": L2_FRESHNESS_BASIS,
            "freshness_threshold_seconds": (
                L2_MATERIALIZED_FRESHNESS_THRESHOLD_SECONDS
            ),
            "freshness_scope": "materialized_only",
            "wire_freshness_included": False,
            "checkpoint_freshness_included": False,
            "computed_at": str(row["computed_at"]),
        }

    @staticmethod
    def _unknown_l2_quality(market_id: str) -> dict[str, Any]:
        """构造不伪造实时健康的固定未知响应。"""
        return {
            "schema_version": L2_QUALITY_SCHEMA_VERSION,
            "market_id": market_id,
            "quality_version": L2_QUALITY_VERSION,
            "source_head_generation": None,
            "source_attempt_ids": [],
            "source_attempt_count": 0,
            "source_normalization_versions": [],
            "status": "unknown",
            "reasons": ["l2_quality_window_unavailable"],
            "window_start": None, "window_end": None,
            "window_clock_basis": None,
            "frames": None, "snapshot_frames": None, "delta_frames": None,
            "checksum_status": "unknown", "checksum_observed_frames": None,
            "checksum_checked_frames": None, "checksum_failures": None,
            "unanchored_before_snapshot_frames": None,
            "anchor_unknown_frames": None,
            "sequence_duplicates": None, "sequence_regressions": None,
            "predecessor_unknown_frames": None,
            "latency_status": "unknown",
            "recv_source_offset_samples": None,
            "recv_source_offset_p50_ms": None,
            "recv_source_offset_p95_ms": None,
            "latest_materialized_observation_time": None,
            "materialized_freshness_seconds": None,
            "materialized_freshness_status": "unknown",
            "freshness_basis": L2_FRESHNESS_BASIS,
            "freshness_threshold_seconds": (
                L2_MATERIALIZED_FRESHNESS_THRESHOLD_SECONDS
            ),
            "freshness_scope": "materialized_only",
            "wire_freshness_included": False,
            "checkpoint_freshness_included": False,
            "computed_at": None,
        }

    def active_outputs_many(
        self,
        market_ids: Iterable[str],
        *,
        domains: Iterable[str],
        datasets: Iterable[str],
        decision_time: datetime,
        from_time: datetime | None = None,
        to_time: datetime | None = None,
    ) -> MultiMarketOutputSnapshot:
        """在一个读事务中冻结多个市场的活动 head 与 PIT 质量窗。"""
        if decision_time.tzinfo is None:
            raise ValueError("decision_time 必须带时区")
        decision_time = decision_time.astimezone(UTC)
        selected_markets = tuple(dict.fromkeys(str(item) for item in market_ids))
        selected_domains = tuple(sorted(set(domains)))
        selected_datasets = tuple(sorted(set(datasets)))
        if not selected_markets:
            raise ValueError("多市场查询至少需要一个 market_id")
        if not selected_domains or not selected_datasets:
            raise ValueError("活动输出查询必须指定 domain 与 dataset")
        conn = connect_readonly(self.data_root)
        if conn is None:
            raise LookupError("无本地数据目录")
        conn.row_factory = sqlite3.Row
        market_marks = ",".join("?" for _ in selected_markets)
        domain_marks = ",".join("?" for _ in selected_domains)
        dataset_marks = ",".join("?" for _ in selected_datasets)
        try:
            conn.execute("BEGIN")
            market_rows = conn.execute(
                "SELECT m.market_id,m.venue_id,m.venue_symbol,m.instrument_id,"
                "m.mapping_revision,m.market_kind,i.base,i.quote,i.kind,"
                "im.tick_size,im.size_step,im.min_size "
                "FROM market m JOIN instrument i ON i.instrument_id=m.instrument_id "
                "LEFT JOIN instrument_map im ON im.venue_id=m.venue_id "
                "AND im.venue_symbol=m.venue_symbol "
                "AND im.revision_id=m.mapping_revision "
                f"WHERE m.market_id IN ({market_marks}) ORDER BY m.market_id",
                selected_markets,
            ).fetchall()
            found = {str(row["market_id"]) for row in market_rows}
            missing = [item for item in selected_markets if item not in found]
            if missing:
                raise LookupError(f"市场不存在: {missing[0]}")
            output_rows = conn.execute(
                "SELECT h.market_id,h.domain,h.partition_key,"
                "h.normalization_version,h.attempt_id,o.dataset,o.artifact_id,"
                "a.storage_path,o.row_count,o.min_event_time,o.max_event_time "
                "FROM materialization_partition_head h "
                "JOIN materialization_output o ON o.attempt_id=h.attempt_id "
                "JOIN artifact a ON a.artifact_id=o.artifact_id "
                f"WHERE h.market_id IN ({market_marks}) "
                f"AND h.domain IN ({domain_marks}) "
                f"AND o.dataset IN ({dataset_marks}) "
                "ORDER BY h.market_id,h.domain,h.partition_key,"
                "o.dataset,o.artifact_id",
                (*selected_markets, *selected_domains, *selected_datasets),
            ).fetchall()
            try:
                quality_rows = conn.execute(
                    "SELECT * FROM (SELECT q.*,row_number() OVER ("
                    "PARTITION BY q.market_id ORDER BY q.computed_at DESC,"
                    "q.window_start DESC) AS selected FROM l2_quality_window q "
                    f"WHERE q.market_id IN ({market_marks}) "
                    "AND q.quality_version=? "
                    "AND julianday(q.computed_at)<=julianday(?)) "
                    "WHERE selected=1 ORDER BY market_id",
                    (*selected_markets, L2_QUALITY_VERSION,
                     decision_time.isoformat()),
                ).fetchall()
            except sqlite3.OperationalError as exc:
                if "no such table" not in str(exc).lower():
                    raise
                quality_rows = []
        finally:
            conn.close()

        resolver = storage_resolver(self.data_root)
        rows_by_market: dict[str, list[ActiveOutput]] = defaultdict(list)
        heads_by_market: dict[str, set[tuple[str, str, str, str]]] = defaultdict(set)
        for row in output_rows:
            low = _timestamp(row["min_event_time"])
            high = _timestamp(row["max_event_time"])
            if from_time is not None and high is not None and high < from_time:
                continue
            if to_time is not None and low is not None and low >= to_time:
                continue
            path = resolver.resolve(str(row["storage_path"]))
            if path.suffix.lower() != ".parquet":
                raise ValueError(
                    f"活动输出路径越界或类型非法: {row['storage_path']}"
                )
            if not path.is_file():
                raise FileNotFoundError(f"活动输出缺失: {row['storage_path']}")
            market_id = str(row["market_id"])
            heads_by_market[market_id].add((
                str(row["domain"]), str(row["partition_key"]),
                str(row["attempt_id"]), str(row["normalization_version"]),
            ))
            rows_by_market[market_id].append(ActiveOutput(
                domain=str(row["domain"]),
                partition_key=str(row["partition_key"]),
                normalization_version=str(row["normalization_version"]),
                attempt_id=str(row["attempt_id"]),
                dataset=str(row["dataset"]),
                artifact_id=str(row["artifact_id"]),
                path=path,
                row_count=int(row["row_count"]),
                min_event_time=low,
                max_event_time=high,
            ))
        market_by_id = {str(row["market_id"]): row for row in market_rows}
        snapshots: list[ActiveOutputSnapshot] = []
        combined_heads: list[tuple[str, ...]] = []
        for market_id in selected_markets:
            row = market_by_id[market_id]
            market = {
                "market_id": market_id,
                "venue_id": str(row["venue_id"]),
                "venue_symbol": str(row["venue_symbol"]),
                "instrument_id": str(row["instrument_id"]),
                "mapping_revision": int(row["mapping_revision"]),
                "market_kind": str(row["market_kind"]),
                "base_currency": str(row["base"]),
                "quote_currency": str(row["quote"]),
                "instrument_kind": str(row["kind"]),
                "tick_size": row["tick_size"],
                "size_step": row["size_step"],
                "min_size": row["min_size"],
            }
            snapshots.append(ActiveOutputSnapshot(
                market=market,
                outputs=tuple(rows_by_market[market_id]),
                head_generation=_generation(heads_by_market[market_id]),
            ))
            combined_heads.extend(
                (market_id, *head) for head in heads_by_market[market_id]
            )
        quality_by_id = {
            str(row["market_id"]): self._l2_quality_payload(
                str(row["market_id"]), row,
            )
            for row in quality_rows
        }
        qualities = tuple(
            (market_id, quality_by_id.get(
                market_id, self._unknown_l2_quality(market_id),
            ))
            for market_id in selected_markets
        )
        quality_heads = [
            (
                market_id,
                str(quality.get("quality_version")),
                str(quality.get("window_start")),
                str(quality.get("computed_at")),
                ",".join(quality.get("source_attempt_ids", [])),
            )
            for market_id, quality in qualities
        ]
        return MultiMarketOutputSnapshot(
            decision_time=decision_time,
            markets=tuple(snapshots),
            qualities=qualities,
            head_generation=_generation([*combined_heads, *quality_heads]),
        )

    def attempt_lineage(self, attempt_id: str) -> AttemptLineage | None:
        """读取 attempt 的散列、上游 attempt 与输入制品三重血缘证据。"""
        conn = connect_readonly(self.data_root)
        if conn is None:
            return None
        try:
            row = conn.execute(
                "SELECT input_set_hash FROM partition_attempt "
                "WHERE attempt_id=? AND status IN "
                "('complete','complete_with_rejections')",
                (attempt_id,),
            ).fetchone()
            if row is None:
                return None
            dependencies = frozenset(
                str(item[0]) for item in conn.execute(
                    "SELECT upstream_attempt_id FROM materialization_dependency "
                    "WHERE attempt_id=?", (attempt_id,),
                )
            )
            artifacts = frozenset(
                str(item[0]) for item in conn.execute(
                    "SELECT artifact_id FROM partition_input WHERE attempt_id=?",
                    (attempt_id,),
                )
            )
            return AttemptLineage(str(row[0]), dependencies, artifacts)
        finally:
            conn.close()

    def active_outputs(
        self,
        market_id: str,
        *,
        domains: Iterable[str],
        datasets: Iterable[str],
        from_time: datetime | None = None,
        to_time: datetime | None = None,
    ) -> ActiveOutputSnapshot:
        """解析活动 head；只返回与查询窗口相交的登记输出。

        文件路径必须仍位于数据根目录且实际存在。查询层永不 glob 目录，
        因此失败、旧版本和未激活尝试不会混入一次响应。
        """
        conn = connect_readonly(self.data_root)
        if conn is None:
            raise LookupError("无本地数据目录")
        conn.row_factory = sqlite3.Row
        selected_domains = tuple(sorted(set(domains)))
        selected_datasets = tuple(sorted(set(datasets)))
        if not selected_domains or not selected_datasets:
            raise ValueError("活动输出查询必须指定 domain 与 dataset")
        try:
            conn.execute("BEGIN")
            market_row = conn.execute(
                "SELECT m.market_id,m.venue_id,m.venue_symbol,m.instrument_id,"
                "m.mapping_revision,m.market_kind,i.base,i.quote,i.kind,"
                "im.tick_size,im.size_step,im.min_size "
                "FROM market m JOIN instrument i ON i.instrument_id=m.instrument_id "
                "LEFT JOIN instrument_map im ON im.venue_id=m.venue_id "
                "AND im.venue_symbol=m.venue_symbol "
                "AND im.revision_id=m.mapping_revision WHERE m.market_id=?",
                (market_id,),
            ).fetchone()
            if market_row is None:
                raise LookupError(f"市场不存在: {market_id}")
            domain_marks = ",".join("?" for _ in selected_domains)
            dataset_marks = ",".join("?" for _ in selected_datasets)
            rows = conn.execute(
                "SELECT h.domain,h.partition_key,h.normalization_version,"
                "h.attempt_id,o.dataset,o.artifact_id,a.storage_path,"
                "o.row_count,o.min_event_time,o.max_event_time "
                "FROM materialization_partition_head h "
                "JOIN materialization_output o ON o.attempt_id=h.attempt_id "
                "JOIN artifact a ON a.artifact_id=o.artifact_id "
                f"WHERE h.market_id=? AND h.domain IN ({domain_marks}) "
                f"AND o.dataset IN ({dataset_marks}) "
                "ORDER BY h.domain,h.partition_key,o.dataset,o.artifact_id",
                (market_id, *selected_domains, *selected_datasets),
            ).fetchall()
        finally:
            conn.close()

        resolver = storage_resolver(self.data_root)
        outputs: list[ActiveOutput] = []
        heads: set[tuple[str, str, str, str]] = set()
        for row in rows:
            low = _timestamp(row["min_event_time"])
            high = _timestamp(row["max_event_time"])
            if from_time is not None and high is not None and high < from_time:
                continue
            if to_time is not None and low is not None and low >= to_time:
                continue
            path = resolver.resolve(str(row["storage_path"]))
            if path.suffix.lower() != ".parquet":
                raise ValueError(f"活动输出路径越界或类型非法: {row['storage_path']}")
            if not path.is_file():
                raise FileNotFoundError(f"活动输出缺失: {row['storage_path']}")
            heads.add((
                str(row["domain"]), str(row["partition_key"]),
                str(row["attempt_id"]), str(row["normalization_version"]),
            ))
            outputs.append(ActiveOutput(
                domain=str(row["domain"]),
                partition_key=str(row["partition_key"]),
                normalization_version=str(row["normalization_version"]),
                attempt_id=str(row["attempt_id"]),
                dataset=str(row["dataset"]),
                artifact_id=str(row["artifact_id"]),
                path=path,
                row_count=int(row["row_count"]),
                min_event_time=low,
                max_event_time=high,
            ))
        market = {
            "market_id": str(market_row["market_id"]),
            "venue_id": str(market_row["venue_id"]),
            "venue_symbol": str(market_row["venue_symbol"]),
            "instrument_id": str(market_row["instrument_id"]),
            "mapping_revision": int(market_row["mapping_revision"]),
            "market_kind": str(market_row["market_kind"]),
            "base_currency": str(market_row["base"]),
            "quote_currency": str(market_row["quote"]),
            "instrument_kind": str(market_row["kind"]),
            "tick_size": market_row["tick_size"],
            "size_step": market_row["size_step"],
            "min_size": market_row["min_size"],
        }
        return ActiveOutputSnapshot(
            market=market,
            outputs=tuple(outputs),
            head_generation=_generation(heads),
        )

    @staticmethod
    def _list_markets(conn: sqlite3.Connection) -> list[dict[str, Any]]:
        markets = conn.execute(
            "SELECT m.market_id,m.venue_id,m.venue_symbol,m.instrument_id,"
            "m.mapping_revision,m.market_kind,i.base,i.quote,i.kind,"
            "im.tick_size,im.size_step,im.min_size "
            "FROM market m JOIN instrument i ON i.instrument_id=m.instrument_id "
            "LEFT JOIN instrument_map im ON im.venue_id=m.venue_id "
            "AND im.venue_symbol=m.venue_symbol "
            "AND im.revision_id=m.mapping_revision "
            "ORDER BY i.base,i.quote,m.venue_id,m.venue_symbol"
        ).fetchall()
        head_rows = conn.execute(
            "SELECT h.market_id,h.domain,h.partition_key,h.normalization_version,"
            "h.attempt_id,h.activated_at,o.dataset,o.row_count,"
            "o.min_event_time,o.max_event_time "
            "FROM materialization_partition_head h "
            "JOIN materialization_output o ON o.attempt_id=h.attempt_id "
            "ORDER BY h.market_id,h.domain,h.partition_key,o.dataset"
        ).fetchall()
        domains: dict[str, dict[str, dict[str, Any]]] = defaultdict(dict)
        domain_heads: dict[tuple[str, str], set[tuple[str, str, str]]] = defaultdict(set)
        domain_partitions: dict[tuple[str, str], set[str]] = defaultdict(set)
        domain_versions: dict[tuple[str, str], set[str]] = defaultdict(set)
        for row in head_rows:
            key = (str(row["market_id"]), str(row["domain"]))
            domain_heads[key].add((
                str(row["partition_key"]), str(row["attempt_id"]),
                str(row["normalization_version"]),
            ))
            domain_partitions[key].add(str(row["partition_key"]))
            domain_versions[key].add(str(row["normalization_version"]))
            state = domains[key[0]].setdefault(key[1], {
                "datasets": {}, "coverage_from_value": None,
                "coverage_to_value": None, "activated_at_value": None,
            })
            dataset = str(row["dataset"])
            output = state["datasets"].setdefault(dataset, {
                "files": 0, "rows": 0,
                "coverage_from_value": None, "coverage_to_value": None,
            })
            output["files"] += 1
            output["rows"] += int(row["row_count"])
            low = _timestamp(row["min_event_time"])
            high = _timestamp(row["max_event_time"])
            activated = _timestamp(row["activated_at"])
            if low is not None:
                output["coverage_from_value"] = _min_time(
                    output["coverage_from_value"], low
                )
                state["coverage_from_value"] = _min_time(
                    state["coverage_from_value"], low
                )
            if high is not None:
                output["coverage_to_value"] = _max_time(
                    output["coverage_to_value"], high
                )
                state["coverage_to_value"] = _max_time(
                    state["coverage_to_value"], high
                )
            if activated is not None:
                state["activated_at_value"] = _max_time(
                    state["activated_at_value"], activated
                )

        result: list[dict[str, Any]] = []
        for market in markets:
            market_id = str(market["market_id"])
            clean_domains: dict[str, Any] = {}
            for domain, state in sorted(domains.get(market_id, {}).items()):
                key = (market_id, domain)
                datasets: dict[str, Any] = {}
                for dataset, output in sorted(state["datasets"].items()):
                    datasets[dataset] = {
                        "files": output["files"], "rows": output["rows"],
                        "coverage_from": _iso(output["coverage_from_value"]),
                        "coverage_to": _iso(output["coverage_to_value"]),
                    }
                clean_domains[domain] = {
                    "coverage_state": (
                        "available"
                        if any(item["rows"] > 0 for item in datasets.values())
                        else "empty"
                    ),
                    "coverage_from": _iso(state["coverage_from_value"]),
                    "coverage_to": _iso(state["coverage_to_value"]),
                    "partition_count": len(domain_partitions[key]),
                    "normalization_versions": sorted(domain_versions[key]),
                    "head_generation": _generation(domain_heads[key]),
                    "activated_at": _iso(state["activated_at_value"]),
                    "datasets": datasets,
                }
            result.append({
                "market_id": market_id,
                "venue_id": market["venue_id"],
                "venue_symbol": market["venue_symbol"],
                "instrument_id": market["instrument_id"],
                "mapping_revision": int(market["mapping_revision"]),
                "market_kind": market["market_kind"],
                "base_currency": market["base"],
                "quote_currency": market["quote"],
                "instrument_kind": market["kind"],
                "tick_size": market["tick_size"],
                "size_step": market["size_step"],
                "min_size": market["min_size"],
                "domains": clean_domains,
            })
        return result

    @staticmethod
    def _list_venues(conn: sqlite3.Connection) -> list[dict[str, Any]]:
        rows = conn.execute(
            "SELECT venue_id,domain,endpoint,revision_id,available,access_mode,"
            "backfill_mode,replay_fidelity,integrity,timestamp_unit,"
            "evidence_level,implementation_status,surveyed_at,valid_until "
            "FROM venue_capability_revision "
            "ORDER BY venue_id,domain,endpoint,revision_id"
        ).fetchall()
        latest: dict[tuple[str, str, str], sqlite3.Row] = {}
        for row in rows:
            latest[(
                str(row["venue_id"]), str(row["domain"]), str(row["endpoint"]),
            )] = row
        grouped: dict[str, list[dict[str, Any]]] = defaultdict(list)
        for (venue_id, _domain, _endpoint), row in latest.items():
            grouped[venue_id].append({
                "domain": row["domain"], "endpoint": row["endpoint"],
                "revision_id": int(row["revision_id"]),
                "available": bool(row["available"]),
                "access_mode": row["access_mode"],
                "backfill_mode": row["backfill_mode"],
                "replay_fidelity": row["replay_fidelity"],
                "integrity": row["integrity"],
                "timestamp_unit": row["timestamp_unit"],
                "evidence_level": row["evidence_level"],
                "implementation_status": row["implementation_status"],
                "surveyed_at": row["surveyed_at"],
                "valid_until": row["valid_until"],
            })
        return [
            {"venue_id": venue_id, "capabilities": sorted(
                capabilities,
                key=lambda item: (str(item["domain"]), str(item["endpoint"])),
            )}
            for venue_id, capabilities in sorted(grouped.items())
        ]

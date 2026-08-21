"""从活动成交事实构造紧凑 PIT 面板。"""
from __future__ import annotations

import hashlib
import json
import os
import sqlite3
import tempfile
from dataclasses import replace
from datetime import UTC, datetime
from decimal import Decimal
from pathlib import Path
from typing import Any, Mapping

import duckdb

from guvolu.data.trade_economics import (
    TRADE_FLOW_INPUT_METHOD_VERSION,
    economic_trade_qualification_sql,
)
from guvolu.data.store import connect_readonly
from guvolu.data.durable_io import atomic_write_text
from guvolu.research.contracts import (
    FrozenPanelInputs,
    FrozenPanelPartition,
    PanelSnapshot,
)
from guvolu.research.provenance import sha256_file
from guvolu.strategy.contracts import ResearchBar
from guvolu.ui.query_catalog import QueryCatalog

PANEL_SCHEMA_VERSION = 2
PANEL_METHOD_VERSION = "trade-bars-pit-v2"
TRADE_INPUT_RECEIPT_METHOD_VERSION = "active-trade-head-receipt-v2"
LEGACY_TRADE_INPUT_RECEIPT_METHOD_VERSION = "active-trade-head-receipt-v1"
PANEL_DUCKDB_MEMORY_LIMIT = "4GB"
PANEL_DUCKDB_THREADS = 2
_INTERVAL_SQL = {
    "5min": "5 minutes",
    "15min": "15 minutes",
    "1hour": "1 hour",
    "4hour": "4 hours",
}


def _utc(value: datetime) -> datetime:
    """把时间统一为 UTC。"""
    if value.tzinfo is None:
        return value.replace(tzinfo=UTC)
    return value.astimezone(UTC)


def _quote(value: str) -> str:
    """转义 DuckDB 字符串。"""
    return value.replace("'", "''")


def _path_list(paths: tuple[Path, ...]) -> str:
    """生成只含已冻结文件的 SQL 数组。"""
    return "[" + ",".join(
        f"'{_quote(str(path.resolve()))}'" for path in paths
    ) + "]"


def _freeze_trade_snapshot(
    data_root: Path,
    market_id: str,
) -> tuple[FrozenPanelInputs, tuple[Any, ...]]:
    """在同一只读事务快照中返回成交输入和完整活动输出。"""
    snapshot = QueryCatalog(data_root).active_outputs(
        market_id,
        domains=("trade", "trade_realtime"),
        datasets=("trade_observation",),
    )
    outputs = tuple(
        row for row in snapshot.outputs if row.dataset == "trade_observation"
    )
    if not outputs:
        raise LookupError(f"市场没有活动成交输出: {market_id}")
    maximum_event_times = [
        row.max_event_time for row in outputs if row.max_event_time is not None
    ]
    if not maximum_event_times:
        raise ValueError(f"活动成交输出没有事件覆盖: {market_id}")
    inputs = FrozenPanelInputs(
        market=snapshot.market,
        paths=tuple(row.path for row in outputs),
        head_generation=snapshot.head_generation,
        attempt_ids=tuple(sorted({row.attempt_id for row in outputs})),
        artifact_ids=tuple(sorted({row.artifact_id for row in outputs})),
        normalization_versions=tuple(sorted({
            row.normalization_version for row in outputs
        })),
        maximum_event_time=max(maximum_event_times),
        partitions=tuple(
            FrozenPanelPartition(
                path=row.path,
                row_count=row.row_count,
                min_event_time=row.min_event_time,
                max_event_time=row.max_event_time,
                domain=row.domain,
                normalization_version=row.normalization_version,
            )
            for row in outputs
        ),
    )
    return inputs, outputs


def freeze_trade_inputs(data_root: Path, market_id: str) -> FrozenPanelInputs:
    """在只读事务中冻结成交活动 head。"""
    inputs, outputs = _freeze_trade_snapshot(data_root, market_id)
    source = 0
    economic = 0
    unqualified = 0
    venue_id = str(inputs.market.get("venue_id") or "")
    summaries = _trade_qualification_summaries(
        tuple(output.path.resolve() for output in outputs),
        str(inputs.market.get("market_id") or ""),
        venue_id,
        {
            output.path.resolve(): (
                str(output.domain), str(output.normalization_version),
            )
            for output in outputs
        },
    )
    for output in outputs:
        summary = summaries[output.path.resolve()]
        if summary[0] != int(output.row_count):
            raise ValueError("活动成交控制面行数与物理文件不一致")
        source += summary[0]
        economic += summary[1]
        unqualified += summary[2]
    return replace(
        inputs,
        trade_flow_input_method_version=TRADE_FLOW_INPUT_METHOD_VERSION,
        source_trade_rows=source,
        economic_trade_rows=economic,
        unqualified_trade_rows=unqualified,
        volume_qualified=unqualified == 0,
    )


def _trade_receipt_payload(
    data_root: Path,
    inputs: FrozenPanelInputs,
    outputs: tuple[Any, ...],
) -> Mapping[str, object]:
    """把一次完整活动 head 快照编码为可内容寻址的输入收据。"""
    root = data_root.resolve()
    entries: list[Mapping[str, object]] = []
    source_trade_rows = 0
    economic_trade_rows = 0
    unqualified_trade_rows = 0
    qualifications = _trade_qualification_summaries(
        tuple(output.path.resolve() for output in outputs),
        str(inputs.market.get("market_id") or ""),
        str(inputs.market.get("venue_id") or ""),
        {
            output.path.resolve(): (
                str(output.domain), str(output.normalization_version),
            )
            for output in outputs
        },
    )
    for output in outputs:
        path = output.path.resolve()
        try:
            relative = path.relative_to(root).as_posix()
        except ValueError as error:
            raise ValueError("活动成交输出越出数据目录") from error
        digest = sha256_file(path)
        if output.artifact_id != f"sha256-{digest}":
            raise ValueError("活动成交 artifact_id 与物理文件 SHA-256 不一致")
        qualification = qualifications[path]
        if qualification[0] != int(output.row_count):
            raise ValueError("活动成交控制面行数与物理文件不一致")
        source_trade_rows += qualification[0]
        economic_trade_rows += qualification[1]
        unqualified_trade_rows += qualification[2]
        entries.append({
            "domain": output.domain,
            "partition_key": output.partition_key,
            "normalization_version": output.normalization_version,
            "attempt_id": output.attempt_id,
            "dataset": output.dataset,
            "artifact_id": output.artifact_id,
            "storage_path": relative,
            "sha256": digest,
            "bytes": path.stat().st_size,
            "row_count": output.row_count,
            "min_event_time": (
                None if output.min_event_time is None
                else output.min_event_time.isoformat()
            ),
            "max_event_time": (
                None if output.max_event_time is None
                else output.max_event_time.isoformat()
            ),
            "trade_flow_input_method_version": (
                TRADE_FLOW_INPUT_METHOD_VERSION
            ),
            "source_trade_rows": qualification[0],
            "economic_trade_rows": qualification[1],
            "unqualified_trade_rows": qualification[2],
            "volume_qualified": qualification[3],
        })
    return {
        "schema_version": 1,
        "method_version": TRADE_INPUT_RECEIPT_METHOD_VERSION,
        "market_id": str(inputs.market["market_id"]),
        "head_generation": inputs.head_generation,
        "attempt_ids": list(inputs.attempt_ids),
        "artifact_ids": list(inputs.artifact_ids),
        "normalization_versions": list(inputs.normalization_versions),
        "maximum_event_time": inputs.maximum_event_time.isoformat(),
        "trade_flow_input_method_version": TRADE_FLOW_INPUT_METHOD_VERSION,
        "source_trade_rows": source_trade_rows,
        "economic_trade_rows": economic_trade_rows,
        "unqualified_trade_rows": unqualified_trade_rows,
        "volume_qualified": unqualified_trade_rows == 0,
        "entries": sorted(
            entries,
            key=lambda item: (
                str(item["domain"]), str(item["partition_key"]),
                str(item["dataset"]), str(item["artifact_id"]),
            ),
        ),
    }


def _trade_qualification_summaries(
    paths: tuple[Path, ...],
    market_id: str,
    venue_id: str,
    control_contracts: Mapping[Path, tuple[str, str]] | None = None,
) -> Mapping[Path, tuple[int, int, int, bool]]:
    """一次扫描一组 Parquet，并按物理文件返回资格摘要。"""
    resolved = tuple(path.resolve() for path in paths)
    columns = _parquet_columns(resolved)
    if "market_id" not in columns:
        predicate = "FALSE"
    elif venue_id == "gmo" and control_contracts is not None:
        market_literal = _quote(market_id)
        predicates = tuple(
            "(filename='" + _quote(str(path)) + "' AND "
            + f"market_id='{market_literal}' AND "
            + (
                "FALSE"
                if control_contracts.get(path) is None
                else economic_trade_qualification_sql(
                    venue_id, columns, control_contracts[path],
                )
            ) + ")"
            for path in resolved
        )
        predicate = "(" + " OR ".join(predicates) + ")"
    else:
        predicate = (
            f"(market_id='{_quote(market_id)}' AND "
            + economic_trade_qualification_sql(venue_id, columns) + ")"
        )
    db: Any = duckdb.connect()
    try:
        rows = db.execute(
            "SELECT filename,COUNT(*),COUNT(*) FILTER (WHERE " + predicate + "),"
            "COUNT(*) FILTER (WHERE NOT (" + predicate + ")) "
            f"FROM read_parquet({_path_list(resolved)},union_by_name=true,"
            "filename=true) GROUP BY filename"
        ).fetchall()
    finally:
        db.close()
    result: dict[Path, tuple[int, int, int, bool]] = {}
    for row in rows:
        path = Path(str(row[0])).resolve()
        source = int(row[1])
        economic = int(row[2])
        unqualified = int(row[3])
        if source != economic + unqualified:
            raise ValueError("成交资格摘要不守恒")
        result[path] = (source, economic, unqualified, unqualified == 0)
    extra = set(result).difference(resolved)
    if extra:
        raise ValueError("成交资格摘要包含未冻结物理文件")
    for path in resolved:
        result.setdefault(path, (0, 0, 0, True))
    return result


def capture_trade_input_receipt(
    data_root: Path,
    market_id: str,
    receipt_directory: Path,
) -> FrozenPanelInputs:
    """冻结完整活动 head，并发布内容寻址的不可变输入收据。"""
    inputs, outputs = _freeze_trade_snapshot(data_root, market_id)
    payload = _trade_receipt_payload(data_root, inputs, outputs)
    content = json.dumps(
        payload, ensure_ascii=False, sort_keys=True, separators=(",", ":"),
        allow_nan=False,
    ) + "\n"
    digest = hashlib.sha256(content.encode("utf-8")).hexdigest()
    path = receipt_directory / f"trade-input-receipt-sha256-{digest}.json"
    if path.exists():
        if sha256_file(path) != digest:
            raise ValueError("既有活动成交输入收据内容损坏")
    else:
        atomic_write_text(path, content)
    return replace(
        inputs,
        receipt_path=path,
        receipt_sha256=digest,
        trade_flow_input_method_version=TRADE_FLOW_INPUT_METHOD_VERSION,
        source_trade_rows=int(str(payload["source_trade_rows"])),
        economic_trade_rows=int(str(payload["economic_trade_rows"])),
        unqualified_trade_rows=int(str(payload["unqualified_trade_rows"])),
        volume_qualified=bool(payload["volume_qualified"]),
    )


def attest_trade_input_receipt(
    data_root: Path,
    receipt_path: Path,
    *,
    require_current_head: bool,
) -> FrozenPanelInputs:
    """复核收据、历史注册输出和可选的当前完整活动 head。"""
    try:
        raw = json.loads(receipt_path.read_text(encoding="utf-8"))
    except (OSError, UnicodeDecodeError, json.JSONDecodeError) as error:
        raise ValueError("活动成交输入收据不可读") from error
    if not isinstance(raw, dict):
        raise ValueError("活动成交输入收据必须为对象")
    receipt = {str(key): value for key, value in raw.items()}
    receipt_method = receipt.get("method_version")
    legacy_receipt = (
        receipt.get("schema_version") == 1
        and receipt_method == LEGACY_TRADE_INPUT_RECEIPT_METHOD_VERSION
    )
    current_receipt = (
        receipt.get("schema_version") == 1
        and receipt_method == TRADE_INPUT_RECEIPT_METHOD_VERSION
    )
    if not legacy_receipt and not current_receipt:
        raise ValueError("活动成交输入收据版本不受支持")
    if legacy_receipt and require_current_head:
        raise ValueError("旧版活动成交输入收据不能证明当前 head")
    market_id = str(receipt.get("market_id") or "")
    raw_attempts = receipt.get("attempt_ids")
    raw_artifacts = receipt.get("artifact_ids")
    raw_normalizations = receipt.get("normalization_versions")
    if (
        not isinstance(raw_attempts, list)
        or not isinstance(raw_artifacts, list)
        or not isinstance(raw_normalizations, list)
    ):
        raise ValueError("活动成交输入收据缺少输入集合")
    attempt_ids = tuple(str(value) for value in raw_attempts)
    artifact_ids = tuple(str(value) for value in raw_artifacts)
    normalizations = tuple(str(value) for value in raw_normalizations)
    entries = receipt.get("entries")
    if not isinstance(entries, list) or not entries:
        raise ValueError("活动成交输入收据缺少 partition 映射")
    if (
        attempt_ids != tuple(sorted(set(attempt_ids)))
        or artifact_ids != tuple(sorted(set(artifact_ids)))
        or normalizations != tuple(sorted(set(normalizations)))
    ):
        raise ValueError("活动成交输入收据集合必须有序且不重复")
    connection = connect_readonly(data_root)
    if connection is None:
        raise LookupError("无本地数据目录")
    connection.row_factory = sqlite3.Row
    marks = ",".join("?" for _ in artifact_ids)
    attempt_marks = ",".join("?" for _ in attempt_ids)
    try:
        market = connection.execute(
            "SELECT m.market_id,m.venue_id,m.venue_symbol,m.instrument_id,"
            "m.mapping_revision,m.market_kind,i.base,i.quote,i.kind,"
            "im.tick_size,im.size_step,im.min_size FROM market m "
            "JOIN instrument i ON i.instrument_id=m.instrument_id "
            "LEFT JOIN instrument_map im ON im.venue_id=m.venue_id "
            "AND im.venue_symbol=m.venue_symbol "
            "AND im.revision_id=m.mapping_revision WHERE m.market_id=?",
            (market_id,),
        ).fetchone()
        output_rows = connection.execute(
            "SELECT p.market_id,p.domain,p.partition_key,"
            "p.normalization_version,p.status,o.attempt_id,o.dataset,"
            "o.artifact_id,o.row_count,"
            "o.min_event_time,o.max_event_time,a.storage_path,a.sha256,"
            "a.byte_count FROM materialization_output o "
            "JOIN partition_attempt p ON p.attempt_id=o.attempt_id "
            "JOIN artifact a ON a.artifact_id=o.artifact_id "
            f"WHERE o.artifact_id IN ({marks}) "
            f"AND o.attempt_id IN ({attempt_marks})",
            (*artifact_ids, *attempt_ids),
        ).fetchall()
    finally:
        connection.close()
    if market is None:
        raise LookupError(f"市场不存在: {market_id}")
    outputs_by_identity = {
        (str(row["attempt_id"]), str(row["artifact_id"]), str(row["dataset"])): row
        for row in output_rows
    }
    recorded_paths: list[Path] = []
    recorded_partitions: list[FrozenPanelPartition] = []
    root = data_root.resolve()
    recorded_attempts: set[str] = set()
    recorded_artifacts: set[str] = set()
    recorded_normalizations: set[str] = set()
    source_trade_rows = 0
    economic_trade_rows = 0
    unqualified_trade_rows = 0
    qualification_expectations: list[
        tuple[int, Path, dict[str, object]]
    ] = []
    for index, raw_entry in enumerate(entries):
        if not isinstance(raw_entry, dict):
            raise ValueError("活动成交输入收据 partition 映射无效")
        entry = {str(key): value for key, value in raw_entry.items()}
        identity_key = (
            str(entry.get("attempt_id") or ""),
            str(entry.get("artifact_id") or ""),
            str(entry.get("dataset") or ""),
        )
        output = outputs_by_identity.get(identity_key)
        if output is None:
            raise ValueError("活动成交输入收据不能由历史控制面输出重建")
        if (
            str(output["market_id"]) != market_id
            or str(output["domain"]) != str(entry.get("domain") or "")
            or str(output["domain"]) not in {"trade", "trade_realtime"}
            or str(output["partition_key"])
            != str(entry.get("partition_key") or "")
            or str(output["normalization_version"])
            != str(entry.get("normalization_version") or "")
            or str(output["status"])
            not in {"complete", "complete_with_rejections"}
        ):
            raise ValueError(
                f"活动成交输入收据控制面字段不匹配: {index}"
            )
        path = (root / str(entry.get("storage_path") or "")).resolve()
        if not path.is_relative_to(root) or not path.is_file():
            raise ValueError("活动成交输入收据物理路径无效")
        if path.stat().st_size != entry.get("bytes"):
            raise ValueError(f"活动成交输入收据字节数不匹配: {index}")
        digest = sha256_file(path)
        if (
            digest != entry.get("sha256")
            or entry.get("artifact_id") != f"sha256-{digest}"
            or str(output["storage_path"]) != entry.get("storage_path")
            or str(output["sha256"]) != digest
            or int(output["byte_count"]) != path.stat().st_size
            or int(output["row_count"]) != entry.get("row_count")
            or output["min_event_time"] != entry.get("min_event_time")
            or output["max_event_time"] != entry.get("max_event_time")
        ):
            raise ValueError(f"活动成交输入收据散列不匹配: {index}")
        if current_receipt:
            qualification_expectations.append((index, path, entry))
        recorded_paths.append(path)
        recorded_partitions.append(FrozenPanelPartition(
            path=path,
            row_count=int(output["row_count"]),
            min_event_time=(
                None if output["min_event_time"] is None
                else parse_time(output["min_event_time"], "min_event_time")
            ),
            max_event_time=(
                None if output["max_event_time"] is None
                else parse_time(output["max_event_time"], "max_event_time")
            ),
            domain=str(entry.get("domain") or ""),
            normalization_version=str(
                entry.get("normalization_version") or ""
            ),
        ))
        recorded_attempts.add(identity_key[0])
        recorded_artifacts.add(identity_key[1])
        recorded_normalizations.add(str(entry.get("normalization_version") or ""))
    if current_receipt:
        qualifications = _trade_qualification_summaries(
            tuple(recorded_paths), market_id, str(market["venue_id"]),
            {
                path: (
                    str(entry.get("domain") or ""),
                    str(entry.get("normalization_version") or ""),
                )
                for _index, path, entry in qualification_expectations
            },
        )
        for index, path, entry in qualification_expectations:
            qualification = qualifications[path]
            if (
                entry.get("trade_flow_input_method_version")
                != TRADE_FLOW_INPUT_METHOD_VERSION
                or entry.get("source_trade_rows") != qualification[0]
                or entry.get("economic_trade_rows") != qualification[1]
                or entry.get("unqualified_trade_rows") != qualification[2]
                or entry.get("volume_qualified") is not qualification[3]
            ):
                raise ValueError(
                    f"活动成交输入收据经济成交资格不匹配: {index}"
                )
            source_trade_rows += qualification[0]
            economic_trade_rows += qualification[1]
            unqualified_trade_rows += qualification[2]
    if (
        recorded_attempts != set(attempt_ids)
        or recorded_artifacts != set(artifact_ids)
        or recorded_normalizations != set(normalizations)
        or len(recorded_paths) != len(output_rows)
    ):
        raise ValueError("活动成交输入收据没有精确覆盖注册输出")
    maximum_event_time = parse_time(
        receipt.get("maximum_event_time"), "maximum_event_time",
    )
    if current_receipt and (
        receipt.get("trade_flow_input_method_version")
        != TRADE_FLOW_INPUT_METHOD_VERSION
        or receipt.get("source_trade_rows") != source_trade_rows
        or receipt.get("economic_trade_rows") != economic_trade_rows
        or receipt.get("unqualified_trade_rows") != unqualified_trade_rows
        or receipt.get("volume_qualified") is not (unqualified_trade_rows == 0)
    ):
        raise ValueError("活动成交输入收据资格汇总不匹配")
    registered = FrozenPanelInputs(
        market={
            "market_id": str(market["market_id"]),
            "venue_id": str(market["venue_id"]),
            "venue_symbol": str(market["venue_symbol"]),
            "instrument_id": str(market["instrument_id"]),
            "mapping_revision": int(market["mapping_revision"]),
            "market_kind": str(market["market_kind"]),
            "base_currency": str(market["base"]),
            "quote_currency": str(market["quote"]),
            "instrument_kind": str(market["kind"]),
            "tick_size": market["tick_size"],
            "size_step": market["size_step"],
            "min_size": market["min_size"],
        },
        paths=tuple(recorded_paths),
        head_generation=str(receipt.get("head_generation") or ""),
        attempt_ids=attempt_ids,
        artifact_ids=artifact_ids,
        normalization_versions=normalizations,
        maximum_event_time=maximum_event_time,
        partitions=tuple(recorded_partitions),
        trade_flow_input_method_version=(
            TRADE_FLOW_INPUT_METHOD_VERSION if current_receipt else None
        ),
        source_trade_rows=source_trade_rows,
        economic_trade_rows=economic_trade_rows,
        unqualified_trade_rows=unqualified_trade_rows,
        volume_qualified=(current_receipt and unqualified_trade_rows == 0),
    )
    if require_current_head:
        current, current_outputs = _freeze_trade_snapshot(data_root, market_id)
        current_payload = _trade_receipt_payload(
            data_root, current, current_outputs,
        )
        if json.dumps(
            current_payload, sort_keys=True, separators=(",", ":"),
            ensure_ascii=False,
        ) != json.dumps(
            receipt, sort_keys=True, separators=(",", ":"), ensure_ascii=False,
        ):
            raise ValueError("活动成交输入收据不是登记时的完整当前 head")
    return replace(
        registered,
        receipt_path=receipt_path,
        receipt_sha256=sha256_file(receipt_path),
    )


def registered_trade_inputs(
    data_root: Path,
    market_id: str,
    artifact_ids: tuple[str, ...],
    attempt_ids: tuple[str, ...],
) -> FrozenPanelInputs:
    """从控制面注册表重建一个历史成交输入集合并复核物理内容。"""
    expected = tuple(sorted(set(artifact_ids)))
    expected_attempts = tuple(sorted(set(attempt_ids)))
    if not expected or len(expected) != len(artifact_ids):
        raise ValueError("panel 输入 artifact_ids 必须非空且不重复")
    if not expected_attempts or len(expected_attempts) != len(attempt_ids):
        raise ValueError("panel 输入 attempt_ids 必须非空且不重复")
    connection = connect_readonly(data_root)
    if connection is None:
        raise LookupError("无本地数据目录")
    connection.row_factory = sqlite3.Row
    marks = ",".join("?" for _ in expected)
    attempt_marks = ",".join("?" for _ in expected_attempts)
    try:
        market = connection.execute(
            "SELECT m.market_id,m.venue_id,m.venue_symbol,m.instrument_id,"
            "m.mapping_revision,m.market_kind,i.base,i.quote,i.kind,"
            "im.tick_size,im.size_step,im.min_size FROM market m "
            "JOIN instrument i ON i.instrument_id=m.instrument_id "
            "LEFT JOIN instrument_map im ON im.venue_id=m.venue_id "
            "AND im.venue_symbol=m.venue_symbol "
            "AND im.revision_id=m.mapping_revision WHERE m.market_id=?",
            (market_id,),
        ).fetchone()
        rows = connection.execute(
            "SELECT p.market_id,p.domain,p.partition_key,"
            "p.normalization_version,p.attempt_id,p.status,o.dataset,"
            "o.artifact_id,o.row_count,o.min_event_time,o.max_event_time,"
            "a.storage_path,a.sha256,a.byte_count FROM materialization_output o "
            "JOIN partition_attempt p ON p.attempt_id=o.attempt_id "
            "JOIN artifact a ON a.artifact_id=o.artifact_id "
            f"WHERE o.artifact_id IN ({marks}) "
            f"AND p.attempt_id IN ({attempt_marks}) "
            "ORDER BY o.artifact_id,p.attempt_id",
            (*expected, *expected_attempts),
        ).fetchall()
    finally:
        connection.close()
    if market is None:
        raise LookupError(f"市场不存在: {market_id}")
    found = {str(row["artifact_id"]) for row in rows}
    found_attempts = {str(row["attempt_id"]) for row in rows}
    if found != set(expected) or found_attempts != set(expected_attempts):
        raise ValueError("panel 输入 artifact_ids 不能由控制面完整重建")
    root = data_root.resolve()
    paths: list[Path] = []
    partitions: list[FrozenPanelPartition] = []
    heads: set[tuple[str, str, str, str]] = set()
    maximum_event_times: list[datetime] = []
    registered_attempts: set[str] = set()
    registered_normalizations: set[str] = set()
    for row in rows:
        if (
            str(row["market_id"]) != market_id
            or str(row["domain"]) not in {"trade", "trade_realtime"}
            or str(row["dataset"]) != "trade_observation"
            or str(row["status"]) not in {"complete", "complete_with_rejections"}
        ):
            raise ValueError("panel 输入包含不受支持的控制面输出")
        path = (root / str(row["storage_path"])).resolve()
        if not path.is_relative_to(root) or path.suffix.lower() != ".parquet":
            raise ValueError("panel 输入物理路径越出数据根或类型错误")
        if not path.is_file():
            raise FileNotFoundError(f"panel 输入物理文件缺失: {path}")
        if path.stat().st_size != int(row["byte_count"]):
            raise ValueError("panel 输入物理文件字节数与控制面不一致")
        digest = sha256_file(path)
        if digest != str(row["sha256"]) or str(row["artifact_id"]) != f"sha256-{digest}":
            raise ValueError("panel 输入物理文件散列与控制面不一致")
        if row["max_event_time"] is not None:
            maximum_event_times.append(
                parse_time(row["max_event_time"], "max_event_time")
            )
        attempt_id = str(row["attempt_id"])
        normalization = str(row["normalization_version"])
        registered_attempts.add(attempt_id)
        registered_normalizations.add(normalization)
        heads.add((
            str(row["domain"]),
            str(row["partition_key"]),
            attempt_id,
            normalization,
        ))
        paths.append(path)
        partitions.append(FrozenPanelPartition(
            path=path,
            row_count=int(row["row_count"]),
            min_event_time=(
                None if row["min_event_time"] is None
                else parse_time(row["min_event_time"], "min_event_time")
            ),
            max_event_time=(
                None if row["max_event_time"] is None
                else parse_time(row["max_event_time"], "max_event_time")
            ),
            domain=str(row["domain"]),
            normalization_version=normalization,
        ))
    generation_body = json.dumps(
        sorted(heads), separators=(",", ":"), ensure_ascii=False,
    )
    head_generation = "sha256-" + hashlib.sha256(
        generation_body.encode("utf-8")
    ).hexdigest()
    if not maximum_event_times:
        raise ValueError("panel 输入控制面没有事件覆盖")
    return FrozenPanelInputs(
        market={
            "market_id": str(market["market_id"]),
            "venue_id": str(market["venue_id"]),
            "venue_symbol": str(market["venue_symbol"]),
            "instrument_id": str(market["instrument_id"]),
            "mapping_revision": int(market["mapping_revision"]),
            "market_kind": str(market["market_kind"]),
            "base_currency": str(market["base"]),
            "quote_currency": str(market["quote"]),
            "instrument_kind": str(market["kind"]),
            "tick_size": market["tick_size"],
            "size_step": market["size_step"],
            "min_size": market["min_size"],
        },
        paths=tuple(paths),
        head_generation=head_generation,
        attempt_ids=tuple(sorted(registered_attempts)),
        artifact_ids=expected,
        normalization_versions=tuple(sorted(registered_normalizations)),
        maximum_event_time=max(maximum_event_times),
        partitions=tuple(partitions),
    )
def _panel_contract(
    inputs: FrozenPanelInputs,
    interval: str,
    notional_scale: int,
) -> tuple[str, str, str, int]:
    """验证面板市场合同并返回 SQL 所需标量。"""
    interval_sql = _INTERVAL_SQL.get(interval)
    if interval_sql is None:
        raise ValueError(f"不支持的研究柱周期: {interval}")
    market_id = str(inputs.market["market_id"])
    tick_size = inputs.market.get("tick_size")
    size_step = inputs.market.get("size_step")
    mapping_revision = int(str(inputs.market["mapping_revision"]))
    if tick_size is None or size_step is None:
        raise ValueError(f"研究市场缺少 tick 或 lot: {market_id}")
    if Decimal(str(tick_size)) <= 0 or Decimal(str(size_step)) <= 0:
        raise ValueError("tick 与 lot 必须为正数")
    if notional_scale <= 0:
        raise ValueError("名义金额缩放必须为正数")
    return market_id, str(tick_size), str(size_step), mapping_revision


def _panel_path_groups(
    inputs: FrozenPanelInputs,
    from_time: datetime,
    to_time: datetime,
) -> tuple[tuple[Path, ...], ...]:
    """把事件覆盖相交的文件归组，使去重内存只随最大重叠组增长。"""
    if not inputs.partitions:
        return (inputs.paths,)
    partition_paths = tuple(item.path.resolve() for item in inputs.partitions)
    input_paths = tuple(path.resolve() for path in inputs.paths)
    if (
        len(set(partition_paths)) != len(partition_paths)
        or set(partition_paths) != set(input_paths)
    ):
        raise ValueError("冻结输入的文件覆盖与控制面分区不一致")
    low = _utc(from_time)
    high = _utc(to_time)
    ranges: list[tuple[datetime, datetime, Path]] = []
    for partition in inputs.partitions:
        if partition.row_count < 0:
            raise ValueError("冻结输入分区行数不能为负")
        if partition.row_count == 0:
            if (
                partition.min_event_time is not None
                or partition.max_event_time is not None
            ):
                raise ValueError("空冻结输入分区不得声明事件覆盖")
            continue
        if (
            partition.min_event_time is None
            or partition.max_event_time is None
        ):
            raise ValueError("非空冻结输入分区缺少事件覆盖")
        minimum = _utc(partition.min_event_time)
        maximum = _utc(partition.max_event_time)
        if minimum > maximum:
            raise ValueError("冻结输入分区事件覆盖倒置")
        if maximum < low or minimum >= high:
            continue
        ranges.append((minimum, maximum, partition.path.resolve()))
    ranges.sort(key=lambda item: (item[0], item[1], item[2].as_posix()))
    groups: list[list[Path]] = []
    group_maximum: datetime | None = None
    for minimum, maximum, path in ranges:
        if group_maximum is None or minimum > group_maximum:
            groups.append([path])
            group_maximum = maximum
        else:
            groups[-1].append(path)
            group_maximum = max(group_maximum, maximum)
    if not groups:
        raise ValueError("冻结输入在研究窗口内没有事件覆盖")
    return tuple(tuple(group) for group in groups)


def _bar_fragment_query(
    paths: tuple[Path, ...],
    interval_sql: str,
    venue_id: str,
    columns: set[str],
    control_contracts: Mapping[Path, tuple[str, str]],
) -> str:
    """生成一个事件覆盖重叠组的局部去重小时片段。"""
    files = _path_list(paths)
    if venue_id == "gmo":
        predicates = tuple(
            "(filename='" + _quote(str(path.resolve())) + "' AND "
            + (
                "FALSE"
                if control_contracts.get(path.resolve()) is None
                else economic_trade_qualification_sql(
                    venue_id,
                    columns,
                    control_contracts[path.resolve()],
                )
            ) + ")"
            for path in paths
        )
        economic = "(" + " OR ".join(predicates) + ")"
    else:
        economic = economic_trade_qualification_sql(venue_id, columns)
    return f"""
        WITH source AS (
          SELECT observation_id,event_time,available_time,side,
                 source_side_basis,price,size,
                 ({economic}) AS economic_trade_qualified,
                 ROW_NUMBER() OVER (
                   PARTITION BY observation_id
                   ORDER BY ingest_time,source_artifact_id,source_row_index
                 ) AS duplicate_ordinal
          FROM read_parquet({files}, union_by_name=true,filename=true)
          WHERE market_id=? AND event_time>=? AND event_time<?
            AND available_time<=?
        ), typed AS (
          SELECT observation_id,event_time,available_time,side,
                 source_side_basis,economic_trade_qualified,
                 TRY_CAST(price AS DECIMAL(38,12)) AS price_decimal,
                 TRY_CAST(size AS DECIMAL(38,12)) AS size_decimal,
                 time_bucket(INTERVAL '{interval_sql}',event_time) AS bucket_start
          FROM source WHERE duplicate_ordinal=1
        ), eligible AS (
          SELECT * FROM typed
          WHERE price_decimal>0 AND size_decimal>0
            AND available_time<=bucket_start+INTERVAL '{interval_sql}'
            AND bucket_start+INTERVAL '{interval_sql}'<=?
        )
        SELECT bucket_start,MAX(available_time) AS latest_available_time,
               FIRST(event_time ORDER BY event_time,observation_id)
                 AS open_event_time,
               FIRST(observation_id ORDER BY event_time,observation_id)
                 AS open_observation_id,
               FIRST(price_decimal ORDER BY event_time,observation_id)
                 AS open_price,
               MAX(price_decimal) AS high_price,
               MIN(price_decimal) AS low_price,
               LAST(event_time ORDER BY event_time,observation_id)
                 AS close_event_time,
               LAST(observation_id ORDER BY event_time,observation_id)
                 AS close_observation_id,
               LAST(price_decimal ORDER BY event_time,observation_id)
                 AS close_price,
               SUM(CASE WHEN economic_trade_qualified THEN size_decimal ELSE 0 END)
                 AS base_volume,
               SUM(CASE WHEN economic_trade_qualified
                        THEN price_decimal*size_decimal ELSE 0 END)
                 AS quote_volume,
               SUM(CASE
                     WHEN economic_trade_qualified AND side='buy'
                       THEN size_decimal
                     WHEN economic_trade_qualified AND side='sell'
                       THEN -size_decimal
                     ELSE 0
                   END) AS signed_base_volume,
               COUNT(*) FILTER (WHERE economic_trade_qualified) AS trade_count,
               COUNT(*) AS source_trade_count,
               COUNT(*) FILTER (WHERE NOT economic_trade_qualified)
                 AS unqualified_trade_count,
               BOOL_AND(economic_trade_qualified) AS volume_qualified
        FROM eligible GROUP BY bucket_start
    """


def _parquet_columns(paths: tuple[Path, ...]) -> set[str]:
    """读取一个重叠组实际存在的物理列，缺列按不合格处理。"""
    db: Any = duckdb.connect()
    try:
        rows = db.execute(
            f"DESCRIBE SELECT * FROM read_parquet({_path_list(paths)}, "
            "union_by_name=true)"
        ).fetchall()
    finally:
        db.close()
    return {str(row[0]) for row in rows}


def _panel_output_query(
    market_id: str,
    interval: str,
    interval_sql: str,
    tick_size: str,
    size_step: str,
    mapping_revision: int,
    notional_scale: int,
) -> tuple[str, tuple[object, ...]]:
    """把局部片段确定性归并为最终面板。"""
    query = f"""
        WITH bars AS (
          SELECT bucket_start,
                 bucket_start+INTERVAL '{interval_sql}' AS decision_time,
                 MAX(latest_available_time) AS latest_available_time,
                 FIRST(open_price ORDER BY open_event_time,open_observation_id)
                   AS open_price,
                 MAX(high_price) AS high_price,
                 MIN(low_price) AS low_price,
                 LAST(close_price ORDER BY close_event_time,close_observation_id)
                   AS close_price,
                 SUM(base_volume) AS base_volume,
                 SUM(quote_volume) AS quote_volume,
                 SUM(signed_base_volume) AS signed_base_volume,
                 CAST(SUM(trade_count) AS BIGINT) AS trade_count,
                 CAST(SUM(source_trade_count) AS BIGINT) AS source_trade_count,
                 CAST(SUM(unqualified_trade_count) AS BIGINT)
                   AS unqualified_trade_count,
                 BOOL_AND(volume_qualified) AS volume_qualified
          FROM bar_fragments GROUP BY bucket_start
        )
        SELECT ? AS market_id,? AS bar_interval,bucket_start AS open_time,
               decision_time,latest_available_time,
               open_price AS open,high_price AS high,low_price AS low,
               close_price AS close,base_volume,quote_volume,signed_base_volume,
               trade_count,source_trade_count,unqualified_trade_count,
               volume_qualified,
               CAST(ROUND(open_price/CAST(? AS DECIMAL(38,12))) AS BIGINT)
                 AS open_ticks,
               CAST(ROUND(high_price/CAST(? AS DECIMAL(38,12))) AS BIGINT)
                 AS high_ticks,
               CAST(ROUND(low_price/CAST(? AS DECIMAL(38,12))) AS BIGINT)
                 AS low_ticks,
               CAST(ROUND(close_price/CAST(? AS DECIMAL(38,12))) AS BIGINT)
                 AS close_ticks,
               CAST(ROUND(base_volume/CAST(? AS DECIMAL(38,12))) AS HUGEINT)
                 AS base_volume_lots,
               CAST(ROUND(
                 CAST(quote_volume AS DECIMAL(28,8))*CAST(? AS DECIMAL(9,0))
               ) AS HUGEINT) AS notional_atoms,
               ? AS tick_size,? AS size_step,? AS mapping_revision,
               ? AS notional_scale,? AS panel_method_version,
               ? AS schema_version
        FROM bars ORDER BY bucket_start
    """
    parameters: tuple[object, ...] = (
        market_id,
        interval,
        str(tick_size),
        str(tick_size),
        str(tick_size),
        str(tick_size),
        str(size_step),
        notional_scale,
        str(tick_size),
        str(size_step),
        mapping_revision,
        notional_scale,
        PANEL_METHOD_VERSION,
        PANEL_SCHEMA_VERSION,
    )
    return query, parameters


def compact_trade_panel(
    inputs: FrozenPanelInputs,
    output_directory: Path,
    interval: str,
    from_time: datetime,
    to_time: datetime,
    notional_scale: int,
) -> tuple[Path, str]:
    """生成内容寻址的紧凑 Parquet 面板。"""
    output_directory.mkdir(parents=True, exist_ok=True)
    temporary = output_directory / f".research-panel.{os.getpid()}.tmp.parquet"
    if temporary.exists():
        temporary.unlink()
    market_id, tick_size, size_step, mapping_revision = _panel_contract(
        inputs, interval, notional_scale,
    )
    if str(inputs.market.get("venue_id") or "") == "gmo":
        input_paths = {path.resolve() for path in inputs.paths}
        partition_paths = {
            partition.path.resolve() for partition in inputs.partitions
        }
        if input_paths != partition_paths or len(inputs.partitions) != len(
            input_paths
        ):
            raise ValueError("GMO panel 输入缺少逐文件控制合同")
    interval_sql = _INTERVAL_SQL[interval]
    groups = _panel_path_groups(inputs, from_time, to_time)
    control_contracts = {
        partition.path.resolve(): (
            str(partition.domain or ""),
            str(partition.normalization_version or ""),
        )
        for partition in inputs.partitions
    }
    query, parameters = _panel_output_query(
        market_id,
        interval,
        interval_sql,
        tick_size,
        size_step,
        mapping_revision,
        notional_scale,
    )
    try:
        with tempfile.TemporaryDirectory(
            prefix=".duckdb-panel-spill.", dir=output_directory,
        ) as spill_directory:
            db: Any = duckdb.connect(config={
                "memory_limit": PANEL_DUCKDB_MEMORY_LIMIT,
                "preserve_insertion_order": False,
                "temp_directory": spill_directory,
                "threads": PANEL_DUCKDB_THREADS,
            })
            try:
                db.execute("SET TimeZone='UTC'")
                db.execute("""
                    CREATE TEMP TABLE bar_fragments(
                      bucket_start TIMESTAMPTZ,
                      latest_available_time TIMESTAMPTZ,
                      open_event_time TIMESTAMPTZ,
                      open_observation_id VARCHAR,
                      open_price DECIMAL(38,12),
                      high_price DECIMAL(38,12),
                      low_price DECIMAL(38,12),
                      close_event_time TIMESTAMPTZ,
                      close_observation_id VARCHAR,
                      close_price DECIMAL(38,12),
                      base_volume DECIMAL(38,12),
                      quote_volume DECIMAL(38,24),
                      signed_base_volume DECIMAL(38,12),
                      trade_count BIGINT,
                      source_trade_count BIGINT,
                      unqualified_trade_count BIGINT,
                      volume_qualified BOOLEAN
                    )
                """)
                fragment_parameters: tuple[object, ...] = (
                    market_id,
                    _utc(from_time),
                    _utc(to_time),
                    _utc(to_time),
                    _utc(to_time),
                )
                for paths in groups:
                    columns = _parquet_columns(paths)
                    db.execute(
                        "INSERT INTO bar_fragments "
                        + _bar_fragment_query(
                            paths, interval_sql,
                            str(inputs.market.get("venue_id") or ""), columns,
                            control_contracts,
                        ),
                        fragment_parameters,
                    )
                copy = (
                    "COPY (" + query + ") TO '"
                    + _quote(str(temporary.resolve()))
                    + "' (FORMAT PARQUET,COMPRESSION ZSTD,"
                    "ROW_GROUP_SIZE 122880)"
                )
                db.execute(copy, parameters)
            finally:
                db.close()
    except BaseException:
        if temporary.exists():
            temporary.unlink()
        raise
    digest = sha256_file(temporary)
    destination = output_directory / f"research-panel-sha256-{digest}.parquet"
    if destination.exists():
        if sha256_file(destination) != digest:
            raise ValueError(f"既有研究面板散列冲突: {destination}")
        temporary.unlink()
    else:
        os.replace(temporary, destination)
    return destination, digest


def load_panel_bars(path: Path) -> tuple[ResearchBar, ...]:
    """读取紧凑面板进入数值研究域。"""
    db: Any = duckdb.connect()
    try:
        db.execute("SET TimeZone='UTC'")
        rows = db.execute(
            "SELECT open_time,decision_time,latest_available_time,"
            "CAST(open AS DOUBLE),CAST(high AS DOUBLE),CAST(low AS DOUBLE),"
            "CAST(close AS DOUBLE),CAST(base_volume AS DOUBLE),"
            "CAST(quote_volume AS DOUBLE),CAST(signed_base_volume AS DOUBLE),"
            "trade_count,source_trade_count,unqualified_trade_count,"
            "volume_qualified FROM read_parquet(?) ORDER BY open_time",
            (str(path.resolve()),),
        ).fetchall()
    finally:
        db.close()
    bars = tuple(ResearchBar(
        open_time=_utc(row[0]),
        decision_time=_utc(row[1]),
        latest_available_time=_utc(row[2]),
        open=float(row[3]),
        high=float(row[4]),
        low=float(row[5]),
        close=float(row[6]),
        base_volume=float(row[7]),
        quote_volume=float(row[8]),
        signed_base_volume=float(row[9]),
        trade_count=int(row[10]),
        source_trade_count=int(row[11]),
        unqualified_trade_count=int(row[12]),
        volume_qualified=bool(row[13]),
    ) for row in rows)
    if not bars:
        raise ValueError("紧凑研究面板为空")
    return bars


def build_panel_snapshot(
    inputs: FrozenPanelInputs,
    output_directory: Path,
    interval: str,
    from_time: datetime,
    to_time: datetime,
    notional_scale: int,
) -> PanelSnapshot:
    """构建面板并返回冻结血缘。"""
    path, digest = compact_trade_panel(
        inputs,
        output_directory,
        interval,
        from_time,
        to_time,
        notional_scale,
    )
    bars = load_panel_bars(path)
    return PanelSnapshot(
        market=inputs.market,
        bars=bars,
        head_generation=inputs.head_generation,
        attempt_ids=inputs.attempt_ids,
        artifact_ids=inputs.artifact_ids,
        normalization_versions=inputs.normalization_versions,
        panel_path=path,
        panel_sha256=digest,
        decision_time=bars[-1].decision_time,
        latest_available_time=bars[-1].latest_available_time,
    )


def parse_time(value: object, name: str) -> datetime:
    """解析配置中的 UTC 时间。"""
    if not isinstance(value, str):
        raise ValueError(f"{name} 必须为时间文本")
    parsed = datetime.fromisoformat(value.replace("Z", "+00:00"))
    return _utc(parsed)


def panel_inputs_payload(inputs: FrozenPanelInputs) -> Mapping[str, object]:
    """生成不暴露本机绝对路径的输入摘要。"""
    return {
        "market": dict(inputs.market),
        "head_generation": inputs.head_generation,
        "attempt_ids": list(inputs.attempt_ids),
        "artifact_ids": list(inputs.artifact_ids),
        "normalization_versions": list(inputs.normalization_versions),
        "input_file_count": len(inputs.paths),
        "maximum_event_time": inputs.maximum_event_time.isoformat(),
        "receipt_sha256": inputs.receipt_sha256,
        "trade_flow_input_method_version": (
            inputs.trade_flow_input_method_version
        ),
        "source_trade_rows": inputs.source_trade_rows,
        "economic_trade_rows": inputs.economic_trade_rows,
        "unqualified_trade_rows": inputs.unqualified_trade_rows,
        "volume_qualified": inputs.volume_qualified,
    }

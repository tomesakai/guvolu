"""从活动成交事实构造紧凑 PIT 面板。"""
from __future__ import annotations

import os
from datetime import UTC, datetime
from decimal import Decimal
from pathlib import Path
from typing import Any, Mapping

import duckdb

from guvolu.research.contracts import FrozenPanelInputs, PanelSnapshot
from guvolu.research.provenance import sha256_file
from guvolu.strategy.contracts import ResearchBar
from guvolu.ui.query_catalog import QueryCatalog

PANEL_SCHEMA_VERSION = 1
PANEL_METHOD_VERSION = "trade-bars-pit-v1"
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


def freeze_trade_inputs(data_root: Path, market_id: str) -> FrozenPanelInputs:
    """在只读事务中冻结成交活动 head。"""
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
    return FrozenPanelInputs(
        market=snapshot.market,
        paths=tuple(row.path for row in outputs),
        head_generation=snapshot.head_generation,
        attempt_ids=tuple(sorted({row.attempt_id for row in outputs})),
        artifact_ids=tuple(sorted({row.artifact_id for row in outputs})),
        normalization_versions=tuple(sorted({
            row.normalization_version for row in outputs
        })),
        maximum_event_time=max(maximum_event_times),
    )


def _panel_query(
    inputs: FrozenPanelInputs,
    interval: str,
    from_time: datetime,
    to_time: datetime,
    notional_scale: int,
) -> tuple[str, tuple[object, ...]]:
    """构造紧凑面板查询及参数。"""
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
    files = _path_list(inputs.paths)
    query = f"""
        WITH source AS (
          SELECT observation_id,event_time,available_time,side,
                 source_side_basis,price,size,
                 ROW_NUMBER() OVER (
                   PARTITION BY observation_id
                   ORDER BY ingest_time,source_artifact_id,source_row_index
                 ) AS duplicate_ordinal
          FROM read_parquet({files}, union_by_name=true)
          WHERE market_id=? AND event_time>=? AND event_time<?
            AND available_time<=?
        ), typed AS (
          SELECT observation_id,event_time,available_time,side,
                 source_side_basis,
                 TRY_CAST(price AS DECIMAL(38,12)) AS price_decimal,
                 TRY_CAST(size AS DECIMAL(38,12)) AS size_decimal,
                 time_bucket(INTERVAL '{interval_sql}',event_time) AS bucket_start
          FROM source WHERE duplicate_ordinal=1
        ), eligible AS (
          SELECT * FROM typed
          WHERE price_decimal>0 AND size_decimal>0
            AND available_time<=bucket_start+INTERVAL '{interval_sql}'
            AND bucket_start+INTERVAL '{interval_sql}'<=?
        ), bars AS (
          SELECT bucket_start,
                 bucket_start+INTERVAL '{interval_sql}' AS decision_time,
                 MAX(available_time) AS latest_available_time,
                 FIRST(price_decimal ORDER BY event_time,observation_id) AS open_price,
                 MAX(price_decimal) AS high_price,
                 MIN(price_decimal) AS low_price,
                 LAST(price_decimal ORDER BY event_time,observation_id) AS close_price,
                 SUM(size_decimal) AS base_volume,
                 SUM(price_decimal*size_decimal) AS quote_volume,
                 SUM(CASE
                       WHEN source_side_basis LIKE 'taker%' AND side='buy'
                         THEN size_decimal
                       WHEN source_side_basis LIKE 'taker%' AND side='sell'
                         THEN -size_decimal
                       ELSE 0
                     END)
                   AS signed_base_volume,
                 COUNT(*) AS trade_count
          FROM eligible GROUP BY bucket_start
        )
        SELECT ? AS market_id,? AS bar_interval,bucket_start AS open_time,
               decision_time,latest_available_time,
               open_price AS open,high_price AS high,low_price AS low,
               close_price AS close,base_volume,quote_volume,signed_base_volume,
               trade_count,
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
        _utc(from_time),
        _utc(to_time),
        _utc(to_time),
        _utc(to_time),
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
    query, parameters = _panel_query(
        inputs,
        interval,
        from_time,
        to_time,
        notional_scale,
    )
    try:
        db: Any = duckdb.connect()
        try:
            db.execute("SET TimeZone='UTC'")
            copy = (
                "COPY (" + query + ") TO '" + _quote(str(temporary.resolve()))
                + "' (FORMAT PARQUET,COMPRESSION ZSTD,ROW_GROUP_SIZE 122880)"
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
            "trade_count FROM read_parquet(?) ORDER BY open_time",
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
    }

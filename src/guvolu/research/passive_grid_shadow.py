"""以 L2/逐笔事实约束的被动网格成交上下界 shadow。"""
from __future__ import annotations

import hashlib
import json
import math
from collections.abc import Mapping, Sequence
from dataclasses import asdict, dataclass
from datetime import UTC, datetime
from decimal import Decimal
from pathlib import Path
from typing import Any

import duckdb

from guvolu.data.durable_io import atomic_write_text
from guvolu.data.store import connect_readonly
from guvolu.research.provenance import (
    artifact_record,
    canonical_json,
    code_identity,
    sha256_file,
    stable_identifier,
)
from guvolu.ui.query_catalog import ActiveOutput, ActiveOutputSnapshot, QueryCatalog

METHOD_VERSION = "passive-grid-snapshot-bounds-v3"
ORDERFLOW_TILE_METHOD_VERSION = "orderflow-tile-sparse-v8"
SCHEMA_VERSION = 1


@dataclass(frozen=True, slots=True)
class TradeAtPrice:
    """一个桶内按方向和价格聚合的主动成交。"""

    price: Decimal
    size: Decimal


@dataclass(frozen=True, slots=True)
class PassiveBucket:
    """可供被动成交回放的一个 PIT 桶。"""

    bucket_epoch: int
    bucket_start: datetime
    bucket_end: datetime
    clean: bool
    best_bid: Decimal | None
    best_ask: Decimal | None
    taker_buys: tuple[TradeAtPrice, ...]
    taker_sells: tuple[TradeAtPrice, ...]

    @property
    def mid(self) -> Decimal | None:
        """返回可信 BBO 中点。"""
        if self.best_bid is None or self.best_ask is None:
            return None
        return (self.best_bid + self.best_ask) / Decimal(2)


@dataclass(frozen=True, slots=True)
class PassiveCandidate:
    """一个可解释的库存约束报价候选。"""

    candidate_id: str
    quote_offset_rows: int
    maximum_inventory_steps: int


@dataclass(frozen=True, slots=True)
class PassiveFill:
    """一次模拟 maker 成交。"""

    bucket_epoch: int
    side: str
    price: Decimal
    size: Decimal
    rule: str
    decision_mid: Decimal


@dataclass(frozen=True, slots=True)
class SimulationMetrics:
    """一个成交规则下的库存和损益证据。"""

    rule: str
    segments: int
    quote_sides: int
    fill_events: int
    filled_base: Decimal
    ending_inventory: Decimal
    minimum_inventory: Decimal
    maximum_inventory: Decimal
    gross_pnl_quote: Decimal
    benchmark_pnl_quote: Decimal
    terminal_rebalance_cost_quote: Decimal
    terminal_pnl_quote: Decimal
    terminal_excess_pnl_quote: Decimal
    terminal_return_bps: float
    terminal_excess_return_bps: float
    maximum_drawdown_bps: float
    fill_rate: float
    markout_bps: Mapping[str, float | None]
    adverse_move_bps: Mapping[str, float | None]


@dataclass(frozen=True, slots=True)
class _Quote:
    bid: Decimal | None
    ask: Decimal | None
    decision_mid: Decimal


def _mapping(value: object, name: str) -> Mapping[str, object]:
    if not isinstance(value, Mapping):
        raise ValueError(f"{name} 必须为对象")
    return {str(key): item for key, item in value.items()}


def _number(value: object, name: str) -> float:
    if not isinstance(value, (int, float)) or isinstance(value, bool):
        raise ValueError(f"{name} 必须为数值")
    return float(value)


def _integer(value: object, name: str, *, minimum: int = 0) -> int:
    if not isinstance(value, int) or isinstance(value, bool) or value < minimum:
        raise ValueError(f"{name} 必须为不小于 {minimum} 的整数")
    return value


def _integer_list(value: object, name: str, *, minimum: int) -> tuple[int, ...]:
    if not isinstance(value, list) or not value:
        raise ValueError(f"{name} 必须为非空数组")
    result = tuple(_integer(item, name, minimum=minimum) for item in value)
    if len(set(result)) != len(result):
        raise ValueError(f"{name} 不得重复")
    return result


def _relative(repository: Path, path: Path) -> str:
    return path.resolve().relative_to(repository.resolve()).as_posix()


def _artifact(
    repository: Path, path: Path, kind: str,
) -> Mapping[str, object]:
    """生成可迁移的项目相对制品记录。"""
    record = dict(artifact_record(path, kind))
    record["path"] = _relative(repository, path)
    return record


def _verified_input_files(
    data_root: Path,
    snapshot: ActiveOutputSnapshot,
) -> tuple[Mapping[str, object], ...]:
    """把登记 artifact 身份闭合到实际文件字节。"""
    root = data_root.resolve()
    records: dict[tuple[str, str, str], Mapping[str, object]] = {}
    for output in snapshot.outputs:
        path = output.path.resolve()
        try:
            relative = path.relative_to(root).as_posix()
        except ValueError as error:
            raise ValueError(f"冻结输入路径越出数据根目录: {path}") from error
        if not path.is_file():
            raise FileNotFoundError(path)
        if not output.artifact_id.startswith("sha256-"):
            raise ValueError(f"冻结输入不是规范 SHA-256 artifact: {output.artifact_id}")
        expected = output.artifact_id.removeprefix("sha256-")
        actual = sha256_file(path)
        if actual != expected:
            raise ValueError(f"冻结输入文件散列不匹配: {relative}")
        records[(output.attempt_id, output.artifact_id, relative)] = {
            "attempt_id": output.attempt_id,
            "artifact_id": output.artifact_id,
            "dataset": output.dataset,
            "path": relative,
            "sha256": actual,
            "bytes": path.stat().st_size,
        }
    return tuple(records[key] for key in sorted(records))


def _verify_recorded_input_files(
    data_root: Path,
    raw_records: object,
) -> tuple[Mapping[str, object], ...]:
    """复核 manifest 冻结的输入路径、字节数和散列。"""
    if not isinstance(raw_records, list) or not raw_records:
        raise ValueError("被动网格 manifest 缺少冻结输入文件")
    root = data_root.resolve()
    verified: list[Mapping[str, object]] = []
    for index, raw in enumerate(raw_records):
        record = _mapping(raw, f"input_files.{index}")
        relative = str(record.get("path"))
        path = (root / relative).resolve()
        try:
            path.relative_to(root)
        except ValueError as error:
            raise ValueError(f"冻结输入路径越出数据根目录: {relative}") from error
        if not path.is_file():
            raise FileNotFoundError(path)
        expected_hash = str(record.get("sha256"))
        artifact_id = str(record.get("artifact_id"))
        if artifact_id != f"sha256-{expected_hash}":
            raise ValueError(f"冻结输入 artifact 与散列不一致: {relative}")
        expected_bytes = _integer(record.get("bytes"), f"input_files.{index}.bytes")
        if path.stat().st_size != expected_bytes:
            raise ValueError(f"冻结输入文件字节数不匹配: {relative}")
        if sha256_file(path) != expected_hash:
            raise ValueError(f"冻结输入文件散列不匹配: {relative}")
        verified.append(record)
    return tuple(verified)


def _paths(snapshot: ActiveOutputSnapshot, dataset: str) -> list[str]:
    return [str(row.path) for row in snapshot.outputs if row.dataset == dataset]


def _freeze_inputs(data_root: Path, market_id: str) -> tuple[
    ActiveOutputSnapshot, ActiveOutputSnapshot,
]:
    catalog = QueryCatalog(data_root)
    tiles = catalog.active_outputs(
        market_id,
        domains=("orderflow_tile",),
        datasets=("orderflow_tile_column", "orderflow_tile_cell"),
    )
    if {row.dataset for row in tiles.outputs} != {
        "orderflow_tile_column", "orderflow_tile_cell",
    }:
        raise LookupError(f"市场缺少完整订单流 tile: {market_id}")
    trades = _tile_trade_inputs(data_root, catalog, tiles)
    return tiles, trades


def _tile_trade_inputs(
    data_root: Path,
    catalog: QueryCatalog,
    tiles: ActiveOutputSnapshot,
) -> ActiveOutputSnapshot:
    """解析每个活动 tile 实际绑定的逐笔输出，不借用当前活动头。"""
    tile_attempts = sorted({row.attempt_id for row in tiles.outputs})
    if not tile_attempts:
        raise LookupError("订单流 tile 没有活动 attempt")
    conn = connect_readonly(data_root)
    if conn is None:
        raise LookupError("数据控制面不可读")
    placeholders = ",".join("?" for _ in tile_attempts)
    try:
        rows = conn.execute(
            f"""
            SELECT DISTINCT md.attempt_id,up.attempt_id,up.domain,
                   up.partition_key,up.normalization_version,
                   output.dataset,output.artifact_id,binding.storage_path,
                   output.row_count,output.min_event_time,output.max_event_time
            FROM materialization_dependency md
            JOIN partition_attempt up ON up.attempt_id=md.upstream_attempt_id
            JOIN materialization_output output ON output.attempt_id=up.attempt_id
            JOIN partition_input_binding binding
              ON binding.attempt_id=md.attempt_id
             AND binding.artifact_id=output.artifact_id
            WHERE md.attempt_id IN ({placeholders})
              AND up.domain IN ('trade','trade_realtime')
              AND output.dataset='trade_observation'
              AND up.status IN ('complete','complete_with_rejections')
            ORDER BY md.attempt_id,up.attempt_id,output.artifact_id,
                     binding.storage_path
            """,
            tile_attempts,
        ).fetchall()
    finally:
        conn.close()
    covered = {str(row[0]) for row in rows}
    missing = sorted(set(tile_attempts) - covered)
    if missing:
        raise ValueError(f"tile 缺少实际逐笔依赖: {','.join(missing)}")
    outputs: dict[tuple[str, str, str], ActiveOutput] = {}
    for row in rows:
        tile_attempt = str(row[0])
        lineage = catalog.attempt_lineage(tile_attempt)
        artifact = str(row[6])
        if lineage is None or artifact not in lineage.input_artifact_ids:
            raise ValueError(f"tile 逐笔制品不在输入血缘: {tile_attempt}/{artifact}")
        path = (data_root / str(row[7])).resolve()
        try:
            path.relative_to(data_root.resolve())
        except ValueError as exc:
            raise ValueError(f"tile 逐笔制品路径越界: {path}") from exc
        if not path.is_file():
            raise FileNotFoundError(path)
        key = (str(row[1]), artifact, path.as_posix())
        outputs[key] = ActiveOutput(
            domain=str(row[2]),
            partition_key=str(row[3]),
            normalization_version=str(row[4]),
            attempt_id=str(row[1]),
            dataset=str(row[5]),
            artifact_id=artifact,
            path=path,
            row_count=int(row[8]),
            min_event_time=(
                None if row[9] is None else datetime.fromisoformat(str(row[9]))
            ),
            max_event_time=(
                None if row[10] is None else datetime.fromisoformat(str(row[10]))
            ),
        )
    frozen = tuple(outputs[key] for key in sorted(outputs))
    generation = stable_identifier("sha256", [
        (row.attempt_id, row.artifact_id, row.normalization_version)
        for row in frozen
    ])
    return ActiveOutputSnapshot(tiles.market, frozen, generation)


def _trade_quality(
    snapshot: ActiveOutputSnapshot,
) -> dict[str, object]:
    """检查会把 maker/taker 双方误作两笔主动成交的镜像模式。"""
    files = _paths(snapshot, "trade_observation")
    db: Any = duckdb.connect(":memory:")
    try:
        row = db.execute(
            """
            WITH deduplicated AS (
              SELECT *,row_number() OVER (
                PARTITION BY observation_id
                ORDER BY available_time,ingest_time,source_artifact_id
              ) AS selected
              FROM read_parquet(?,union_by_name=true)
              WHERE market_id=?
            ), grouped AS (
              SELECT event_time,price,size,count(*) AS rows,
                     count(DISTINCT side) AS sides
              FROM deduplicated WHERE selected=1
              GROUP BY event_time,price,size
            )
            SELECT coalesce(sum(rows),0),
                   coalesce(sum(CASE WHEN sides>1 THEN rows ELSE 0 END),0)
            FROM grouped
            """,
            [files, snapshot.market["market_id"]],
        ).fetchone()
        if row is None:
            raise ValueError("实时逐笔质量查询没有返回")
        rows = int(row[0])
        mirrored = int(row[1])
        basis_rows = db.execute(
            "SELECT source_side_basis,count(*) FROM "
            "read_parquet(?,union_by_name=true) GROUP BY 1 ORDER BY 1",
            [files],
        ).fetchall()
    finally:
        db.close()
    return {
        "rows": rows,
        "mirrored_rows": mirrored,
        "mirrored_trade_ratio": 0.0 if rows == 0 else mirrored / rows,
        "source_side_basis": {
            str(basis): int(count) for basis, count in basis_rows
        },
    }


def _load_buckets(
    snapshot: ActiveOutputSnapshot,
    bucket: str,
) -> tuple[tuple[PassiveBucket, ...], Decimal]:
    """流式重建稀疏 tile 的桶末 BBO，并保留独立逐笔量。"""
    columns = _paths(snapshot, "orderflow_tile_column")
    cells = _paths(snapshot, "orderflow_tile_cell")
    db: Any = duckdb.connect(":memory:")
    db.execute("SET TimeZone='UTC'")
    try:
        cursor = db.execute(
            """
            SELECT c.bucket_epoch,c.bucket_start,c.bucket_end,c.is_anchor,c.is_gap,
                   c.coverage_state,c.method_version,x.book_side,x.price_key,
                   x.book_end_size,x.taker_buy_size,x.taker_sell_size,x.state_role,
                   c.row_size
            FROM read_parquet(?) c
            LEFT JOIN read_parquet(?) x USING(column_id)
            WHERE c.market_id=? AND c.bucket=?
            ORDER BY c.bucket_epoch,x.book_side,x.price_key
            """,
            [columns, cells, snapshot.market["market_id"], bucket],
        )
        books: dict[str, dict[int, Decimal]] = {"bid": {}, "ask": {}}
        result: list[PassiveBucket] = []
        observed_row_size: Decimal | None = None
        group: list[tuple[object, ...]] = []
        current_epoch: int | None = None

        def append_group(rows: Sequence[tuple[object, ...]]) -> None:
            nonlocal observed_row_size
            if not rows:
                return
            first = rows[0]
            if bool(first[3]):
                books["bid"].clear()
                books["ask"].clear()
            buys: list[TradeAtPrice] = []
            sells: list[TradeAtPrice] = []
            row_size = Decimal(str(first[13]))
            if observed_row_size is None:
                observed_row_size = row_size
            elif observed_row_size != row_size:
                raise ValueError("订单流 tile 的价格格不一致")
            for row in rows:
                if row[7] is None:
                    continue
                side = str(row[7])
                key = int(str(row[8]))
                size = None if row[9] is None else Decimal(str(row[9]))
                role = str(row[12])
                if role != "trade":
                    if size is None or size <= 0:
                        books[side].pop(key, None)
                    else:
                        books[side][key] = size
                price = Decimal(key) * row_size
                buy_size = Decimal(str(row[10]))
                sell_size = Decimal(str(row[11]))
                if buy_size > 0:
                    buys.append(TradeAtPrice(price, buy_size))
                if sell_size > 0:
                    sells.append(TradeAtPrice(price, sell_size))
            best_bid = (
                Decimal(max(books["bid"])) * row_size if books["bid"] else None
            )
            best_ask = (
                Decimal(min(books["ask"])) * row_size if books["ask"] else None
            )
            clean = (
                not bool(first[4])
                and str(first[5]) != "gap"
                and str(first[6]) == ORDERFLOW_TILE_METHOD_VERSION
                and best_bid is not None
                and best_ask is not None
                and best_bid < best_ask
            )
            bucket_start = first[1]
            bucket_end = first[2]
            if not isinstance(bucket_start, datetime) or not isinstance(
                bucket_end, datetime
            ):
                raise ValueError("订单流 tile 桶时刻类型非法")
            result.append(PassiveBucket(
                bucket_epoch=int(str(first[0])),
                bucket_start=bucket_start,
                bucket_end=bucket_end,
                clean=clean,
                best_bid=best_bid,
                best_ask=best_ask,
                taker_buys=tuple(buys),
                taker_sells=tuple(sells),
            ))

        while batch := cursor.fetchmany(50_000):
            for row in batch:
                epoch = int(str(row[0]))
                if current_epoch is not None and epoch != current_epoch:
                    append_group(group)
                    group = []
                group.append(row)
                current_epoch = epoch
        append_group(group)
    finally:
        db.close()
    if observed_row_size is None or observed_row_size <= 0:
        raise ValueError("订单流 tile 缺少正价格格")
    return tuple(result), observed_row_size


def _fill_quantity(
    trades: Sequence[TradeAtPrice],
    quote: Decimal,
    side: str,
    order_size: Decimal,
    rule: str,
) -> Decimal:
    """按严格穿价下界或忽略队列的触价上界计算成交量。"""
    if side == "buy":
        through = any(item.price < quote for item in trades)
        touched = sum(
            (item.size for item in trades if item.price <= quote), Decimal(0)
        )
    else:
        through = any(item.price > quote for item in trades)
        touched = sum(
            (item.size for item in trades if item.price >= quote), Decimal(0)
        )
    if through:
        return order_size
    if rule == "touch_queue_optimistic" and touched > 0:
        return min(order_size, touched)
    return Decimal(0)


def _mean(values: Sequence[float]) -> float | None:
    return None if not values else sum(values) / len(values)


def simulate_candidate(
    buckets: Sequence[PassiveBucket],
    candidate: PassiveCandidate,
    *,
    price_row_size: Decimal,
    order_size: Decimal,
    latency_buckets: int,
    maker_fee_bps: Decimal,
    terminal_rebalance_bps: Decimal,
    markout_horizons_seconds: Sequence[int],
    rule: str,
) -> tuple[SimulationMetrics, tuple[PassiveFill, ...]]:
    """回放一个候选；任何缺口都会取消待生效报价。"""
    if rule not in {"trade_through_pessimistic", "touch_queue_optimistic"}:
        raise ValueError(f"未知成交规则: {rule}")
    if not buckets:
        raise ValueError("没有被动成交桶")
    bucket_seconds = int(
        (buckets[0].bucket_end - buckets[0].bucket_start).total_seconds()
    )
    if any(horizon % bucket_seconds for horizon in markout_horizons_seconds):
        raise ValueError("markout horizon 必须是 tile 桶秒数的整数倍")
    segments: list[list[PassiveBucket]] = []
    current: list[PassiveBucket] = []
    for item in buckets:
        if not item.clean or item.mid is None:
            if current:
                segments.append(current)
                current = []
            continue
        if current and item.bucket_epoch != current[-1].bucket_epoch + bucket_seconds:
            segments.append(current)
            current = []
        current.append(item)
    if current:
        segments.append(current)
    segments = [
        item for item in segments if len(item) >= latency_buckets + 2
    ]
    if not segments:
        raise ValueError("没有可信 BBO 桶")
    maximum_inventory = order_size * candidate.maximum_inventory_steps
    target_inventory = maximum_inventory / Decimal(2)
    capital_mid = segments[0][0].mid
    assert capital_mid is not None
    capital = maximum_inventory * capital_mid
    minimum_inventory = target_inventory
    maximum_seen = target_inventory
    fills: list[PassiveFill] = []
    quote_sides = 0
    activation_delay = (latency_buckets + 1) * bucket_seconds
    fee_rate = maker_fee_bps / Decimal(10_000)
    gross_pnl = Decimal(0)
    benchmark_pnl = Decimal(0)
    rebalance_cost = Decimal(0)
    maximum_drawdown = Decimal(0)
    ending_inventory = target_inventory
    markouts: dict[str, list[float]] = {
        str(horizon): [] for horizon in markout_horizons_seconds
    }
    adverse: dict[str, list[float]] = {
        str(horizon): [] for horizon in markout_horizons_seconds
    }
    for segment in segments:
        initial_mid = segment[0].mid
        final_mid = segment[-1].mid
        assert initial_mid is not None
        assert final_mid is not None
        inventory = target_inventory
        cash = -inventory * initial_mid
        pending: dict[int, _Quote] = {}
        wealth_path: list[Decimal] = [Decimal(0)]
        segment_fills: list[PassiveFill] = []
        mids = {item.bucket_epoch: item.mid for item in segment}
        for item in segment:
            assert item.mid is not None
            quote = pending.pop(item.bucket_epoch, None)
            if quote is not None:
                buy_size = Decimal(0)
                sell_size = Decimal(0)
                if quote.bid is not None:
                    buy_size = min(
                        _fill_quantity(
                            item.taker_sells, quote.bid, "buy", order_size, rule,
                        ),
                        maximum_inventory - inventory,
                    )
                    quote_sides += 1
                if quote.ask is not None:
                    sell_size = min(
                        _fill_quantity(
                            item.taker_buys, quote.ask, "sell", order_size, rule,
                        ),
                        inventory,
                    )
                    quote_sides += 1
                if buy_size > 0 and quote.bid is not None:
                    cash -= buy_size * quote.bid * (Decimal(1) + fee_rate)
                    inventory += buy_size
                    segment_fills.append(PassiveFill(
                        item.bucket_epoch, "buy", quote.bid, buy_size, rule,
                        quote.decision_mid,
                    ))
                if sell_size > 0 and quote.ask is not None:
                    cash += sell_size * quote.ask * (Decimal(1) - fee_rate)
                    inventory -= sell_size
                    segment_fills.append(PassiveFill(
                        item.bucket_epoch, "sell", quote.ask, sell_size, rule,
                        quote.decision_mid,
                    ))
            minimum_inventory = min(minimum_inventory, inventory)
            maximum_seen = max(maximum_seen, inventory)
            wealth_path.append(cash + inventory * item.mid)
            offset = price_row_size * candidate.quote_offset_rows
            assert item.best_bid is not None
            assert item.best_ask is not None
            pending[item.bucket_epoch + activation_delay] = _Quote(
                bid=(
                    item.best_bid - offset
                    if inventory < maximum_inventory else None
                ),
                ask=(item.best_ask + offset if inventory > 0 else None),
                decision_mid=item.mid,
            )
        segment_gross = cash + inventory * final_mid
        segment_rebalance = (
            abs(inventory - target_inventory)
            * final_mid
            * terminal_rebalance_bps
            / Decimal(10_000)
        )
        gross_pnl += segment_gross
        rebalance_cost += segment_rebalance
        benchmark_pnl += target_inventory * (final_mid - initial_mid)
        peak = wealth_path[0]
        for wealth in wealth_path:
            peak = max(peak, wealth)
            maximum_drawdown = max(maximum_drawdown, peak - wealth)
        for fill in segment_fills:
            sign = Decimal(1) if fill.side == "buy" else Decimal(-1)
            for horizon in markout_horizons_seconds:
                future = mids.get(fill.bucket_epoch + horizon)
                if future is None:
                    continue
                markouts[str(horizon)].append(float(
                    sign * (future - fill.price)
                    / fill.price * Decimal(10_000)
                ))
                adverse[str(horizon)].append(float(
                    sign * (future - fill.decision_mid)
                    / fill.decision_mid * Decimal(10_000)
                ))
        fills.extend(segment_fills)
        ending_inventory = inventory
    terminal_pnl = gross_pnl - rebalance_cost
    terminal_excess = terminal_pnl - benchmark_pnl
    metrics = SimulationMetrics(
        rule=rule,
        segments=len(segments),
        quote_sides=quote_sides,
        fill_events=len(fills),
        filled_base=sum((item.size for item in fills), Decimal(0)),
        ending_inventory=ending_inventory,
        minimum_inventory=minimum_inventory,
        maximum_inventory=maximum_seen,
        gross_pnl_quote=gross_pnl,
        benchmark_pnl_quote=benchmark_pnl,
        terminal_rebalance_cost_quote=rebalance_cost,
        terminal_pnl_quote=terminal_pnl,
        terminal_excess_pnl_quote=terminal_excess,
        terminal_return_bps=float(terminal_pnl / capital * Decimal(10_000)),
        terminal_excess_return_bps=float(
            terminal_excess / capital * Decimal(10_000)
        ),
        maximum_drawdown_bps=float(maximum_drawdown / capital * Decimal(10_000)),
        fill_rate=0.0 if quote_sides == 0 else len(fills) / quote_sides,
        markout_bps={key: _mean(values) for key, values in markouts.items()},
        adverse_move_bps={key: _mean(values) for key, values in adverse.items()},
    )
    return metrics, tuple(fills)


def _metric_payload(value: SimulationMetrics) -> dict[str, object]:
    body = asdict(value)
    for key in (
        "filled_base", "ending_inventory", "minimum_inventory",
        "maximum_inventory", "gross_pnl_quote",
        "benchmark_pnl_quote", "terminal_rebalance_cost_quote",
        "terminal_pnl_quote", "terminal_excess_pnl_quote",
    ):
        body[key] = format(body[key], "f")
    return body


def _rank(values: Sequence[float]) -> list[float]:
    """返回带平均并列秩的秩向量。"""
    ordered = sorted(range(len(values)), key=lambda index: values[index])
    ranks = [0.0] * len(values)
    start = 0
    while start < len(ordered):
        end = start + 1
        while end < len(ordered) and values[ordered[end]] == values[ordered[start]]:
            end += 1
        rank = (start + end - 1) / 2.0
        for index in ordered[start:end]:
            ranks[index] = rank
        start = end
    return ranks


def _correlation(left: Sequence[float], right: Sequence[float]) -> float:
    if len(left) != len(right) or len(left) < 2:
        return 0.0
    x = _rank(left)
    y = _rank(right)
    x_mean = sum(x) / len(x)
    y_mean = sum(y) / len(y)
    numerator = sum((a - x_mean) * (b - y_mean) for a, b in zip(x, y))
    x_var = sum((a - x_mean) ** 2 for a in x)
    y_var = sum((b - y_mean) ** 2 for b in y)
    return 0.0 if x_var == 0 or y_var == 0 else numerator / math.sqrt(x_var * y_var)


def _write_content(directory: Path, stem: str, suffix: str, text: str) -> Path:
    digest = hashlib.sha256(text.encode("utf-8")).hexdigest()
    path = directory / f"{stem}-sha256-{digest}{suffix}"
    if path.exists():
        if sha256_file(path) != digest:
            raise ValueError(f"制品散列命名冲突: {path}")
        return path
    atomic_write_text(path, text)
    return path


def run_passive_grid_shadow(
    repository: Path,
    *,
    data_root: Path | None = None,
    config_path: Path | None = None,
) -> Mapping[str, object]:
    """运行独立被动网格 shadow 并发布可验证制品。"""
    repository = repository.resolve()
    selected_data = (data_root or repository / "data").resolve()
    selected_config = (config_path or repository / "config/passive_grid_shadow.json").resolve()
    config = json.loads(selected_config.read_text(encoding="utf-8"))
    if not isinstance(config, Mapping):
        raise ValueError("被动网格配置必须为对象")
    market_id = str(config.get("market_id"))
    bucket = str(config.get("bucket"))
    order_size = Decimal(str(_number(config.get("order_size_base"), "order_size_base")))
    offsets = _integer_list(
        config.get("quote_offset_rows"), "quote_offset_rows", minimum=0,
    )
    inventory_steps = _integer_list(
        config.get("maximum_inventory_steps"),
        "maximum_inventory_steps",
        minimum=2,
    )
    if any(value % 2 for value in inventory_steps):
        raise ValueError("maximum_inventory_steps 必须为偶数以固定中性初始库存")
    latency = _integer(config.get("latency_buckets"), "latency_buckets")
    if _integer(config.get("quote_lifetime_buckets"), "quote_lifetime_buckets", minimum=1) != 1:
        raise ValueError("当前 snapshot bound 只支持一桶报价寿命")
    maker_fee = Decimal(str(_number(config.get("maker_fee_bps"), "maker_fee_bps")))
    terminal_cost = Decimal(str(_number(
        config.get("terminal_rebalance_bps_assumption"),
        "terminal_rebalance_bps_assumption",
    )))
    horizons = _integer_list(
        config.get("markout_horizons_seconds"),
        "markout_horizons_seconds",
        minimum=1,
    )
    quality_config = _mapping(config.get("quality"), "quality")
    tile_snapshot, trade_snapshot = _freeze_inputs(selected_data, market_id)
    buckets, price_row_size = _load_buckets(tile_snapshot, bucket)
    if not buckets:
        raise ValueError("订单流 tile 没有目标桶")
    tick_size = Decimal(str(tile_snapshot.market.get("tick_size")))
    if tick_size <= 0:
        raise ValueError("市场 tick_size 非正")
    price_row_ticks = price_row_size / tick_size
    if price_row_ticks != price_row_ticks.to_integral_value():
        raise ValueError("tile 价格格不是市场 tick 的整数倍")
    trade_quality = _trade_quality(trade_snapshot)
    mirrored_limit = _number(
        quality_config.get("maximum_mirrored_trade_ratio"),
        "maximum_mirrored_trade_ratio",
    )
    mirrored_ok = float(str(trade_quality["mirrored_trade_ratio"])) <= mirrored_limit
    side_basis = _mapping(
        trade_quality.get("source_side_basis"), "source_side_basis",
    )
    side_basis_ok = bool(side_basis) and all(
        str(basis).startswith("taker") for basis in side_basis
    )
    clean_buckets = sum(item.clean for item in buckets)
    bucket_seconds = int((buckets[0].bucket_end - buckets[0].bucket_start).total_seconds())
    clean_hours = clean_buckets * bucket_seconds / 3600.0
    coverage_ok = (
        clean_buckets >= _integer(
            quality_config.get("minimum_clean_buckets"),
            "minimum_clean_buckets",
            minimum=1,
        )
        and clean_hours >= _number(
            quality_config.get("minimum_clean_hours"),
            "minimum_clean_hours",
        )
    )
    identity = code_identity(repository, (selected_config,))
    input_files = {
        "tiles": list(_verified_input_files(selected_data, tile_snapshot)),
        "trades": list(_verified_input_files(selected_data, trade_snapshot)),
    }
    input_body = {
        "tile_head_generation": tile_snapshot.head_generation,
        "tile_artifacts": sorted(row.artifact_id for row in tile_snapshot.outputs),
        "trade_head_generation": trade_snapshot.head_generation,
        "trade_artifacts": sorted(row.artifact_id for row in trade_snapshot.outputs),
        "config_sha256": sha256_file(selected_config),
        "method_version": METHOD_VERSION,
        "code_tree_digest": identity.tree_digest,
        "input_file_set_id": stable_identifier("sha256", input_files),
    }
    run_id = stable_identifier("passive-grid-shadow", input_body)
    output = repository / "reports/passive-grid-shadow" / run_id
    output.mkdir(parents=True, exist_ok=True)
    candidates = tuple(
        PassiveCandidate(
            stable_identifier("passive-grid-candidate", {
                "method_version": METHOD_VERSION,
                "quote_offset_rows": offset,
                "maximum_inventory_steps": steps,
                "order_size_base": format(order_size, "f"),
                "latency_buckets": latency,
            }),
            offset,
            steps,
        )
        for offset in offsets for steps in inventory_steps
    )
    results: list[dict[str, object]] = []
    lower_returns: list[float] = []
    fill_rows: list[str] = []
    minimum_lower_fills = _integer(
        quality_config.get("minimum_lower_fill_events"),
        "minimum_lower_fill_events",
        minimum=1,
    )
    for candidate in candidates:
        simulations: dict[str, object] = {}
        for rule in ("trade_through_pessimistic", "touch_queue_optimistic"):
            metrics, fills = simulate_candidate(
                buckets,
                candidate,
                price_row_size=price_row_size,
                order_size=order_size,
                latency_buckets=latency,
                maker_fee_bps=maker_fee,
                terminal_rebalance_bps=terminal_cost,
                markout_horizons_seconds=horizons,
                rule=rule,
            )
            simulations[rule] = _metric_payload(metrics)
            for fill in fills:
                fill_rows.append(canonical_json({
                    "candidate_id": candidate.candidate_id,
                    "rule": fill.rule,
                    "bucket_epoch": fill.bucket_epoch,
                    "side": fill.side,
                    "price_row_lower": format(fill.price, "f"),
                    "price_row_upper_exclusive": format(
                        fill.price + price_row_size, "f",
                    ),
                    "size": format(fill.size, "f"),
                    "decision_mid": format(fill.decision_mid, "f"),
                }))
        lower = simulations["trade_through_pessimistic"]
        assert isinstance(lower, Mapping)
        lower_returns.append(float(str(lower["terminal_excess_return_bps"])))
        fill_sample_ready = (
            mirrored_ok and side_basis_ok and coverage_ok
            and int(str(lower["fill_events"])) >= minimum_lower_fills
        )
        rejection_reasons = [
            "shadow_only",
            "private_order_fill_lifecycle_unavailable",
            "queue_position_unobservable",
        ]
        if float(str(lower["terminal_excess_return_bps"])) <= 0:
            rejection_reasons.append("non_positive_terminal_excess_return")
        if not fill_sample_ready:
            rejection_reasons.append("insufficient_lower_fill_sample")
        results.append({
            "candidate_id": candidate.candidate_id,
            "parameters": {
                "quote_offset_rows": candidate.quote_offset_rows,
                "quote_offset_ticks": int(
                    candidate.quote_offset_rows * price_row_ticks
                ),
                "maximum_inventory_steps": candidate.maximum_inventory_steps,
            },
            "simulations": simulations,
            "fill_sample_ready": fill_sample_ready,
            "calibration_ready": False,
            "rejection_reasons": rejection_reasons,
            "capital_weight": 0,
        })
    best_lower = max(lower_returns)
    monitor = {
        "method_version": "passive-grid-direction-monitor-v2",
        "status": (
            "rejected_negative_excess"
            if best_lower <= 0
            else "diagnostic_only_unvalidated_fills"
        ),
        "parameter_association": {
            "quote_offset_rows": _correlation(
                [float(item.quote_offset_rows) for item in candidates], lower_returns,
            ),
            "maximum_inventory_steps": _correlation(
                [float(item.maximum_inventory_steps) for item in candidates], lower_returns,
            ),
        },
        "automatic_expansion_allowed": False,
        "best_pessimistic_excess_return_bps": best_lower,
        "association_is_promotion_evidence": False,
        "reason": (
            "snapshot bounds expose direction only; private fills are required "
            "to calibrate queue priority and segment reset costs"
        ),
    }
    quality = {
        "mirrored_trade_gate": mirrored_ok,
        "taker_side_basis_gate": side_basis_ok,
        "coverage_gate": coverage_ok,
        "clean_buckets": clean_buckets,
        "total_buckets": len(buckets),
        "clean_hours": clean_hours,
        "trade_quality": trade_quality,
        "reasons": [
            *([] if mirrored_ok else ["mirrored_trade_ratio_exceeded"]),
            *([] if side_basis_ok else ["taker_side_basis_unproven"]),
            *([] if coverage_ok else ["insufficient_clean_l2_coverage"]),
        ],
    }
    summary = {
        "schema_version": SCHEMA_VERSION,
        "run_id": run_id,
        "method_version": METHOD_VERSION,
        "market": tile_snapshot.market,
        "bucket": bucket,
        "coverage": {
            "from": buckets[0].bucket_start.astimezone(UTC).isoformat(),
            "to": buckets[-1].bucket_end.astimezone(UTC).isoformat(),
        },
        "input_identity": input_body,
        "code_identity": asdict(identity),
        "quality": quality,
        "assumptions": {
            "maker_fee_bps": float(maker_fee),
            "terminal_rebalance_bps": float(terminal_cost),
            "order_size_base": format(order_size, "f"),
            "price_row_size_quote": format(price_row_size, "f"),
            "price_row_ticks": int(price_row_ticks),
            "price_point_interpretation": "floor of tile price row",
            "fill_price_interval": "[price_row_lower,price_row_upper_exclusive)",
            "latency_buckets": latency,
            "queue_position": "unobservable",
            "pessimistic_fill": "strict taker trade-through",
            "optimistic_fill": "touch with queue priority ignored",
            "touch_is_not_observed_fill": True,
        },
        "candidates": results,
        "evolution_monitor": monitor,
        "capital_weight": 0,
        "promotion_eligible": False,
        "promotion_blockers": [
            "shadow_only",
            "private_order_fill_lifecycle_unavailable",
            "queue_position_unobservable",
        ],
    }
    fill_text = "\n".join(fill_rows) + ("\n" if fill_rows else "")
    fills_path = _write_content(output, "fills", ".jsonl", fill_text)
    summary["fills_artifact"] = _artifact(
        repository, fills_path, "passive_grid_fills",
    )
    summary_path = _write_content(
        output, "summary", ".json", canonical_json(summary) + "\n",
    )
    manifest = {
        "schema_version": SCHEMA_VERSION,
        "run_id": run_id,
        "status": "complete",
        "summary": _artifact(repository, summary_path, "passive_grid_summary"),
        "fills": _artifact(repository, fills_path, "passive_grid_fills"),
        "input_identity": input_body,
        "input_files": input_files,
    }
    manifest_path = output / "manifest.json"
    atomic_write_text(manifest_path, canonical_json(manifest) + "\n")
    manifest_sha256 = sha256_file(manifest_path)
    atomic_write_text(
        repository / "reports/passive-grid-shadow/latest.json",
        canonical_json({
            "run_id": run_id,
            "manifest": _relative(repository, manifest_path),
            "manifest_sha256": manifest_sha256,
        }) + "\n",
    )
    return summary


def verify_passive_grid_shadow(
    repository: Path,
    run_id: str,
    data_root: Path | None = None,
) -> Mapping[str, object]:
    """复核独立 shadow manifest 和内容寻址制品。"""
    root = repository.resolve()
    manifest_path = root / "reports/passive-grid-shadow" / run_id / "manifest.json"
    manifest = json.loads(manifest_path.read_text(encoding="utf-8"))
    if manifest.get("run_id") != run_id or manifest.get("status") != "complete":
        raise ValueError("被动网格 manifest 身份或状态非法")
    input_identity = _mapping(manifest.get("input_identity"), "input_identity")
    if stable_identifier("passive-grid-shadow", input_identity) != run_id:
        raise ValueError("被动网格运行身份不能由输入身份复算")
    raw_input_files = _mapping(manifest.get("input_files"), "input_files")
    input_files = {
        "tiles": list(_verify_recorded_input_files(
            data_root or root / "data", raw_input_files.get("tiles"),
        )),
        "trades": list(_verify_recorded_input_files(
            data_root or root / "data", raw_input_files.get("trades"),
        )),
    }
    if input_identity.get("input_file_set_id") != stable_identifier(
        "sha256", input_files,
    ):
        raise ValueError("被动网格冻结输入文件集合身份不一致")

    def verified_path(record: Mapping[str, object], name: str) -> Path:
        path = (root / str(record["path"])).resolve()
        try:
            path.relative_to(root)
        except ValueError as exc:
            raise ValueError(f"被动网格制品路径越界: {name}") from exc
        if not path.is_file():
            raise FileNotFoundError(path)
        if sha256_file(path) != record.get("sha256"):
            raise ValueError(f"被动网格制品散列不符: {name}")
        if path.stat().st_size != int(str(record.get("bytes"))):
            raise ValueError(f"被动网格制品字节数不符: {name}")
        return path

    for name in ("summary", "fills"):
        record = _mapping(manifest.get(name), name)
        verified_path(record, name)
    summary_record = _mapping(manifest.get("summary"), "summary")
    summary_path = verified_path(summary_record, "summary")
    summary = json.loads(summary_path.read_text(encoding="utf-8"))
    if summary.get("run_id") != run_id:
        raise ValueError("被动网格摘要与 manifest 的运行身份不一致")
    if summary.get("input_identity") != manifest.get("input_identity"):
        raise ValueError("被动网格摘要与 manifest 的输入身份不一致")
    summary_fills = _mapping(summary.get("fills_artifact"), "fills_artifact")
    manifest_fills = _mapping(manifest.get("fills"), "fills")
    if summary_fills != manifest_fills:
        raise ValueError("被动网格摘要与 manifest 的成交制品不一致")
    latest_path = root / "reports/passive-grid-shadow/latest.json"
    if latest_path.is_file():
        latest = json.loads(latest_path.read_text(encoding="utf-8"))
        if latest.get("run_id") == run_id:
            expected_manifest = _relative(root, manifest_path)
            if latest.get("manifest") != expected_manifest:
                raise ValueError("被动网格活动指针路径不一致")
            if latest.get("manifest_sha256") != sha256_file(manifest_path):
                raise ValueError("被动网格活动指针 manifest 散列不一致")
    return {
        "run_id": run_id,
        "verified": True,
        "manifest_sha256": sha256_file(manifest_path),
    }

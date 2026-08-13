"""L2 overlay 与跨所可执行错位 shadow。"""
from __future__ import annotations

from collections.abc import Mapping, Sequence
from datetime import datetime
from decimal import Decimal
from pathlib import Path
from typing import Any

from guvolu.ui.cross_venue_query import CrossVenueQuery
from guvolu.ui.materialized_query import MaterializedQuery
from guvolu.ui.query_catalog import QueryCatalog


def _mapping(value: object, name: str) -> Mapping[str, object]:
    """验证配置对象。"""
    if not isinstance(value, Mapping):
        raise ValueError(f"{name} 必须为对象")
    return {str(key): item for key, item in value.items()}


def _integer(value: object, name: str) -> int:
    """验证正整数配置。"""
    if not isinstance(value, int) or isinstance(value, bool) or value <= 0:
        raise ValueError(f"{name} 必须为正整数")
    return value


def _number(value: object, name: str) -> float:
    """验证数值配置。"""
    if not isinstance(value, (int, float)) or isinstance(value, bool):
        raise ValueError(f"{name} 必须为数值")
    return float(value)


def _market_ids(value: object) -> tuple[str, ...]:
    """验证跨所市场集合。"""
    if not isinstance(value, list) or len(value) < 2:
        raise ValueError("跨所市场集合至少包含两个市场")
    result = tuple(str(item) for item in value)
    if len(set(result)) != len(result):
        raise ValueError("跨所市场集合不得重复")
    return result


def cross_venue_shadow(
    data_root: Path,
    decision_time: datetime,
    config: Mapping[str, object],
) -> Mapping[str, object]:
    """生成费用后双腿错位事实但不分配本金。"""
    market_ids = _market_ids(config.get("market_ids"))
    minimum_quorum = _integer(config.get("minimum_quorum"), "minimum_quorum")
    maximum_age = _integer(config.get("maximum_age_seconds"), "maximum_age_seconds")
    result: dict[str, Any] = CrossVenueQuery(
        MaterializedQuery(data_root),
    ).latest_top(
        market_ids,
        decision_time=decision_time,
        min_quorum=minimum_quorum,
        max_age_seconds=maximum_age,
    )
    fee_config = _mapping(
        config.get("taker_fee_bps_assumptions"),
        "taker_fee_bps_assumptions",
    )
    contributors = result.get("contributors")
    pairs: list[dict[str, object]] = []
    if isinstance(contributors, list):
        for buy in contributors:
            if not isinstance(buy, Mapping):
                continue
            for sell in contributors:
                if not isinstance(sell, Mapping):
                    continue
                if buy.get("venue_id") == sell.get("venue_id"):
                    continue
                buy_venue = str(buy.get("venue_id"))
                sell_venue = str(sell.get("venue_id"))
                buy_fee = Decimal(str(_number(fee_config.get(buy_venue), buy_venue)))
                sell_fee = Decimal(str(_number(fee_config.get(sell_venue), sell_venue)))
                ask = Decimal(str(buy["ask"]))
                bid = Decimal(str(sell["bid"]))
                buy_cost = ask * (Decimal(1) + buy_fee / Decimal(10_000))
                sell_value = bid * (Decimal(1) - sell_fee / Decimal(10_000))
                edge_bp = (sell_value / buy_cost - Decimal(1)) * Decimal(10_000)
                pairs.append({
                    "buy_market_id": str(buy["market_id"]),
                    "sell_market_id": str(sell["market_id"]),
                    "buy_cost": format(buy_cost, "f"),
                    "sell_value": format(sell_value, "f"),
                    "net_edge_bp": format(edge_bp, "f"),
                    "shadow_positive": edge_bp > 0,
                })
    pairs.sort(key=lambda item: Decimal(str(item["net_edge_bp"])), reverse=True)
    result["executable_dislocation_shadow"] = {
        "fee_basis": "configured_taker_fee_assumptions",
        "capital_weight": 0,
        "pairs": pairs,
    }
    return result


def latest_common_l2_decision(
    data_root: Path,
    market_ids: Sequence[str],
) -> datetime:
    """选择所有市场均有事实的最新共同决策时点。"""
    catalog = QueryCatalog(data_root)
    latest: list[datetime] = []
    for market_id in market_ids:
        snapshot = catalog.active_outputs(
            market_id,
            domains=("book_l2",),
            datasets=("book_l2_frame",),
        )
        values = [
            row.max_event_time for row in snapshot.outputs
            if row.max_event_time is not None
        ]
        if not values:
            raise LookupError(f"市场没有 L2 覆盖: {market_id}")
        latest.append(max(values))
    return min(latest)


def l2_overlay_from_shadow(
    shadow: Mapping[str, object],
    subject_market_id: str,
) -> tuple[float, Mapping[str, object]]:
    """从合格顶档生成有界流动性方向。"""
    contributors = shadow.get("contributors")
    if not isinstance(contributors, Sequence):
        return 0.0, {"eligible": False, "reason": "contributors_missing"}
    for item in contributors:
        if not isinstance(item, Mapping) or item.get("market_id") != subject_market_id:
            continue
        bid_size = Decimal(str(item["bid_size"]))
        ask_size = Decimal(str(item["ask_size"]))
        total = bid_size + ask_size
        if total <= 0:
            return 0.0, {"eligible": False, "reason": "top_size_invalid"}
        imbalance = (bid_size - ask_size) / total
        return float(imbalance), {
            "eligible": True,
            "basis": "best_level_size_imbalance",
            "value": format(imbalance, "f"),
            "capital_weight": 0,
        }
    return 0.0, {"eligible": False, "reason": "subject_market_excluded"}

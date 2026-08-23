"""真实研究面板接入：冻结活动 head、显式 to_time 上限与共享特征导出。

只读取活动成交 head 并写紧凑面板到搜索输出目录，不写 SQLite 与生产数据目录；
晚于 `panel_to_time` 的柱一律拒绝，且面板区间不得与未消费封存段重叠（G-08）。
"""
from __future__ import annotations

import sqlite3
from collections.abc import Callable, Mapping, Sequence
from dataclasses import dataclass
from datetime import UTC, datetime
from pathlib import Path

from guvolu.research.contracts import FrozenPanelInputs, PanelSnapshot
from guvolu.research.features import FEATURE_METHOD_VERSION, compute_features
from guvolu.research.panel import (
    build_panel_snapshot,
    freeze_trade_inputs,
    panel_inputs_payload,
    parse_time,
)
from guvolu.strategy.contracts import FeatureRow, ResearchBar

PANEL_SOURCE_METHOD_VERSION = "search-loop-panel-source-v1"
GOVERNANCE_REGISTRY_RELATIVE = Path("research") / "governance.sqlite3"
FreezeFunction = Callable[[Path, str], FrozenPanelInputs]
BuildFunction = Callable[
    [FrozenPanelInputs, Path, str, datetime, datetime, int], PanelSnapshot,
]


@dataclass(frozen=True)
class ResearchPanel:
    """循环使用的研究柱、特征与面板身份。"""

    market_id: str
    bars: tuple[ResearchBar, ...]
    features: tuple[FeatureRow, ...]
    lookbacks: tuple[int, ...]
    feature_method_version: str
    panel_sha256: str
    panel_path: Path | None
    from_time: datetime
    to_time: datetime
    inputs: Mapping[str, object]

    def payload(self) -> Mapping[str, object]:
        """导出不含本机绝对路径的面板身份摘要。"""
        return {
            "panel_source_method_version": PANEL_SOURCE_METHOD_VERSION,
            "market_id": self.market_id,
            "bar_count": len(self.bars),
            "lookbacks": list(self.lookbacks),
            "feature_method_version": self.feature_method_version,
            "panel_sha256": self.panel_sha256,
            "from_time": self.from_time.isoformat(),
            "to_time": self.to_time.isoformat(),
            "first_decision_time": (
                self.bars[0].decision_time.isoformat() if self.bars else None
            ),
            "last_decision_time": (
                self.bars[-1].decision_time.isoformat() if self.bars else None
            ),
            "inputs": dict(self.inputs),
        }


def _utc(value: datetime) -> datetime:
    """统一为 UTC。"""
    if value.tzinfo is None:
        return value.replace(tzinfo=UTC)
    return value.astimezone(UTC)


def enforce_to_time(
    bars: Sequence[ResearchBar],
    to_time: datetime,
) -> tuple[ResearchBar, ...]:
    """拒绝任何决策时间或可得时间晚于上限的柱。"""
    limit = _utc(to_time)
    for bar in bars:
        if _utc(bar.decision_time) > limit:
            raise ValueError(
                "面板含晚于 panel_to_time 的柱: "
                + bar.decision_time.isoformat()
            )
        if _utc(bar.latest_available_time) > limit:
            raise ValueError(
                "面板含晚于 panel_to_time 的可得时间: "
                + bar.latest_available_time.isoformat()
            )
    return tuple(bars)


def sealed_vintages_overlapping(
    registry_path: Path,
    market_id: str,
    start_time: datetime,
    end_time: datetime,
) -> tuple[str, ...]:
    """以只读连接列出与区间重叠的未消费封存段。"""
    if not registry_path.exists():
        return ()
    uri = f"file:{registry_path.resolve().as_posix()}?mode=ro"
    connection = sqlite3.connect(uri, uri=True)
    try:
        present = connection.execute(
            "SELECT name FROM sqlite_master WHERE type='table' "
            "AND name='holdout_vintage'"
        ).fetchone()
        if present is None:
            return ()
        rows = connection.execute(
            "SELECT vintage_id,start_time,end_time FROM holdout_vintage "
            "WHERE market_id=? AND status='sealed' ORDER BY start_time,vintage_id",
            (market_id,),
        ).fetchall()
    finally:
        connection.close()
    start = _utc(start_time)
    end = _utc(end_time)
    overlapping: list[str] = []
    for vintage_id, raw_start, raw_end in rows:
        vintage_start = _utc(datetime.fromisoformat(str(raw_start)))
        vintage_end = _utc(datetime.fromisoformat(str(raw_end)))
        if vintage_start < end and start < vintage_end:
            overlapping.append(str(vintage_id))
    return tuple(overlapping)


def resolve_panel_to_time(
    loop_config: Mapping[str, object],
    sealed_boundary: datetime | None = None,
) -> datetime:
    """读取显式面板上限；若登记了封存起点则必须早于它。"""
    to_time = parse_time(loop_config.get("panel_to_time"), "search_loop.panel_to_time")
    if sealed_boundary is not None and to_time >= _utc(sealed_boundary):
        raise ValueError("panel_to_time 不得晚于或等于封存段起点")
    return to_time


def _lookbacks(values: Sequence[int]) -> tuple[int, ...]:
    """规范化回看窗集合。"""
    windows = tuple(sorted({int(value) for value in values}))
    if not windows or windows[0] <= 1:
        raise ValueError("回看窗必须大于一且非空")
    return windows


def panel_from_bars(
    market_id: str,
    bars: Sequence[ResearchBar],
    lookbacks: Sequence[int],
    volume_lookback: int,
    maximum_structural_gap_bars: int,
    from_time: datetime,
    to_time: datetime,
    panel_sha256: str,
    panel_path: Path | None,
    inputs: Mapping[str, object],
) -> ResearchPanel:
    """由已冻结的研究柱计算共享特征并构造面板。"""
    windows = _lookbacks(lookbacks)
    accepted = enforce_to_time(bars, to_time)
    if not accepted:
        raise ValueError("面板为空")
    features = compute_features(
        accepted, windows, volume_lookback, maximum_structural_gap_bars,
    )
    return ResearchPanel(
        market_id=market_id,
        bars=accepted,
        features=tuple(features),
        lookbacks=windows,
        feature_method_version=FEATURE_METHOD_VERSION,
        panel_sha256=panel_sha256,
        panel_path=panel_path,
        from_time=_utc(from_time),
        to_time=_utc(to_time),
        inputs=dict(inputs),
    )


def load_research_panel(
    data_root: Path,
    research_config: Mapping[str, object],
    loop_config: Mapping[str, object],
    lookbacks: Sequence[int],
    output_directory: Path,
    *,
    freeze: FreezeFunction = freeze_trade_inputs,
    build: BuildFunction = build_panel_snapshot,
) -> ResearchPanel:
    """冻结活动 head，按显式 to_time 构建面板并计算特征。

    `freeze` 与 `build` 可注入，供测试替换真实数据读取。
    """
    market_id = research_config.get("market_id")
    if not isinstance(market_id, str) or not market_id:
        raise ValueError("研究配置缺少 market_id")
    interval = research_config.get("bar_interval")
    if not isinstance(interval, str) or not interval:
        raise ValueError("研究配置缺少 bar_interval")
    from_time = parse_time(research_config.get("from_time"), "from_time")
    to_time = resolve_panel_to_time(loop_config)
    if from_time >= to_time:
        raise ValueError("from_time 必须早于 panel_to_time")
    registry = data_root / GOVERNANCE_REGISTRY_RELATIVE
    overlapping = sealed_vintages_overlapping(registry, market_id, from_time, to_time)
    if overlapping:
        raise ValueError("面板区间与未消费封存段重叠: " + ",".join(overlapping))
    raw_scale = research_config.get("notional_scale")
    if not isinstance(raw_scale, int) or isinstance(raw_scale, bool) or raw_scale <= 0:
        raise ValueError("notional_scale 必须为正整数")
    feature_config = research_config.get("features")
    if not isinstance(feature_config, Mapping):
        raise ValueError("研究配置缺少 features")
    volume_lookback = feature_config.get("volume_lookback")
    gap_bars = feature_config.get("maximum_structural_gap_bars_assumption")
    if not isinstance(volume_lookback, int) or not isinstance(gap_bars, int):
        raise ValueError("features 缺少 volume_lookback 或结构空窗上限")
    inputs = freeze(data_root, market_id)
    if _utc(inputs.maximum_event_time) < to_time:
        raise ValueError(
            "活动 head 事件覆盖早于 panel_to_time: "
            + inputs.maximum_event_time.isoformat()
        )
    snapshot = build(
        inputs, output_directory, interval, from_time, to_time, raw_scale,
    )
    return panel_from_bars(
        market_id,
        snapshot.bars,
        lookbacks,
        volume_lookback,
        gap_bars,
        from_time,
        to_time,
        snapshot.panel_sha256,
        snapshot.panel_path,
        panel_inputs_payload(inputs),
    )

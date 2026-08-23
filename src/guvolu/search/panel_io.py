"""参考面板 JSON：CPU 侧 f64 行情柱与特征的序列化，供精确复算使用。"""
from __future__ import annotations

import json
from collections.abc import Mapping, Sequence
from datetime import datetime
from pathlib import Path

from guvolu.data.durable_io import atomic_write_text
from guvolu.search.identity import canonical_json
from guvolu.strategy.contracts import FeatureRow, ResearchBar

PANEL_PAYLOAD_SCHEMA_VERSION = 1


def _window_payload(values: Mapping[int, float | None]) -> Mapping[str, float | None]:
    """回看窗字典转为字符串键。"""
    return {str(key): value for key, value in sorted(values.items())}


def _window_from_payload(value: object, name: str) -> dict[int, float | None]:
    """由字符串键还原回看窗字典。"""
    if not isinstance(value, Mapping):
        raise ValueError(f"{name} 必须为对象")
    result: dict[int, float | None] = {}
    for key, item in value.items():
        if item is not None and (
            not isinstance(item, (int, float)) or isinstance(item, bool)
        ):
            raise ValueError(f"{name} 数值非法")
        result[int(key)] = None if item is None else float(item)
    return result


def _optional_number(value: object, name: str) -> float | None:
    """读取可空数值。"""
    if value is None:
        return None
    if not isinstance(value, (int, float)) or isinstance(value, bool):
        raise ValueError(f"{name} 必须为数值或空")
    return float(value)


def _number(value: object, name: str) -> float:
    """读取数值。"""
    if not isinstance(value, (int, float)) or isinstance(value, bool):
        raise ValueError(f"{name} 必须为数值")
    return float(value)


def _time(value: object, name: str) -> datetime:
    """读取 ISO 时间。"""
    if not isinstance(value, str):
        raise ValueError(f"{name} 必须为 ISO 时间文本")
    return datetime.fromisoformat(value)


def panel_payload(
    bars: Sequence[ResearchBar],
    features: Sequence[FeatureRow],
    feature_method_version: str,
) -> Mapping[str, object]:
    """把行情柱与特征导出为 JSON 载荷。"""
    if len(bars) != len(features):
        raise ValueError("行情柱与特征数量不一致")
    return {
        "schema_version": PANEL_PAYLOAD_SCHEMA_VERSION,
        "feature_method_version": feature_method_version,
        "bars": [
            {
                "open_time": bar.open_time.isoformat(),
                "decision_time": bar.decision_time.isoformat(),
                "latest_available_time": bar.latest_available_time.isoformat(),
                "open": bar.open,
                "high": bar.high,
                "low": bar.low,
                "close": bar.close,
                "base_volume": bar.base_volume,
                "quote_volume": bar.quote_volume,
                "signed_base_volume": bar.signed_base_volume,
                "trade_count": bar.trade_count,
                "source_trade_count": bar.source_trade_count,
                "unqualified_trade_count": bar.unqualified_trade_count,
                "volume_qualified": bar.volume_qualified,
            }
            for bar in bars
        ],
        "features": [
            {
                "decision_time": feature.decision_time.isoformat(),
                "as_of": feature.as_of.isoformat(),
                "return_one": feature.return_one,
                "trend_scores": _window_payload(feature.trend_scores),
                "volatility": _window_payload(feature.volatility),
                "price_scores": _window_payload(feature.price_scores),
                "prior_highs": _window_payload(feature.prior_highs),
                "prior_lows": _window_payload(feature.prior_lows),
                "flow_imbalance": feature.flow_imbalance,
                "volume_score": feature.volume_score,
                "jump_score": feature.jump_score,
                "contiguous": feature.contiguous,
                "volume_qualified": feature.volume_qualified,
            }
            for feature in features
        ],
    }


def write_panel_payload(path: Path, payload: Mapping[str, object]) -> None:
    """原子写入参考面板 JSON。"""
    atomic_write_text(path, canonical_json(payload) + "\n")


def load_panel_payload(
    path: Path,
) -> tuple[tuple[ResearchBar, ...], tuple[FeatureRow, ...], str]:
    """读取参考面板 JSON 为行情柱、特征与特征方法版本。"""
    raw = json.loads(path.read_text(encoding="utf-8"))
    if not isinstance(raw, Mapping):
        raise ValueError("面板载荷必须为对象")
    if raw.get("schema_version") != PANEL_PAYLOAD_SCHEMA_VERSION:
        raise ValueError("面板载荷版本不受支持")
    method = raw.get("feature_method_version")
    if not isinstance(method, str) or not method:
        raise ValueError("面板载荷缺少 feature_method_version")
    raw_bars = raw.get("bars")
    raw_features = raw.get("features")
    if not isinstance(raw_bars, list) or not isinstance(raw_features, list):
        raise ValueError("面板载荷 bars 与 features 必须为数组")
    bars: list[ResearchBar] = []
    for item in raw_bars:
        if not isinstance(item, Mapping):
            raise ValueError("bar 必须为对象")
        bars.append(ResearchBar(
            open_time=_time(item.get("open_time"), "open_time"),
            decision_time=_time(item.get("decision_time"), "decision_time"),
            latest_available_time=_time(
                item.get("latest_available_time"), "latest_available_time",
            ),
            open=_number(item.get("open"), "open"),
            high=_number(item.get("high"), "high"),
            low=_number(item.get("low"), "low"),
            close=_number(item.get("close"), "close"),
            base_volume=_number(item.get("base_volume"), "base_volume"),
            quote_volume=_number(item.get("quote_volume"), "quote_volume"),
            signed_base_volume=_number(
                item.get("signed_base_volume"), "signed_base_volume",
            ),
            trade_count=int(_number(item.get("trade_count"), "trade_count")),
            source_trade_count=int(_number(
                item.get("source_trade_count", 0), "source_trade_count",
            )),
            unqualified_trade_count=int(_number(
                item.get("unqualified_trade_count", 0), "unqualified_trade_count",
            )),
            volume_qualified=bool(item.get("volume_qualified", True)),
        ))
    features: list[FeatureRow] = []
    for item in raw_features:
        if not isinstance(item, Mapping):
            raise ValueError("feature 必须为对象")
        features.append(FeatureRow(
            decision_time=_time(item.get("decision_time"), "decision_time"),
            as_of=_time(item.get("as_of"), "as_of"),
            return_one=_optional_number(item.get("return_one"), "return_one"),
            trend_scores=_window_from_payload(item.get("trend_scores"), "trend_scores"),
            volatility=_window_from_payload(item.get("volatility"), "volatility"),
            price_scores=_window_from_payload(item.get("price_scores"), "price_scores"),
            prior_highs=_window_from_payload(item.get("prior_highs"), "prior_highs"),
            prior_lows=_window_from_payload(item.get("prior_lows"), "prior_lows"),
            flow_imbalance=_optional_number(item.get("flow_imbalance"), "flow_imbalance"),
            volume_score=_optional_number(item.get("volume_score"), "volume_score"),
            jump_score=_optional_number(item.get("jump_score"), "jump_score"),
            contiguous=bool(item.get("contiguous")),
            volume_qualified=bool(item.get("volume_qualified", True)),
        ))
    if len(bars) != len(features):
        raise ValueError("行情柱与特征数量不一致")
    return tuple(bars), tuple(features), method

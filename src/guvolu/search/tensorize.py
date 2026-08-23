"""PIT 面板到 f32 列矩阵与有效性掩码的导出。

`None` 只转为 NaN 并在掩码置零，不插值、不前向填充（G-02、禁区第 2 条）。
"""
from __future__ import annotations

import hashlib
import math
import struct
import sys
from array import array
from collections.abc import Mapping, Sequence
from dataclasses import dataclass

from guvolu.strategy.contracts import FeatureRow, ResearchBar

TENSORIZE_METHOD_VERSION = "searchfast-panel-tensor-v1"
PANEL_DTYPE = "f32"
MASK_SEMANTICS = (
    "float32-nan-with-int8-validity-mask;"
    "boolean-int8-true1-false0-unknown-1;v1"
)
BOOLEAN_TRUE = 1
BOOLEAN_FALSE = 0
BOOLEAN_UNKNOWN = -1
WINDOW_FIELDS = ("trend_score", "price_score", "prior_high", "volatility")
SCALAR_COLUMNS = (
    "close",
    "log_return",
    "gap_seconds",
    "gate_open",
    "flow_imbalance",
    "volume_score",
    "jump_score",
)


def window_column(field: str, lookback: int) -> str:
    """生成回看窗字段的列名。"""
    return f"{field}@{lookback}"


def panel_columns(lookbacks: Sequence[int]) -> tuple[str, ...]:
    """返回固定顺序的导出列名。"""
    windows = tuple(sorted(set(int(value) for value in lookbacks)))
    return SCALAR_COLUMNS + tuple(
        window_column(field, lookback)
        for lookback in windows
        for field in WINDOW_FIELDS
    )


def round_to_f32(value: float) -> float:
    """把 f64 数值按 IEEE binary32 取整。"""
    return float(struct.unpack("<f", struct.pack("<f", value))[0])


@dataclass(frozen=True)
class PanelTensor:
    """固定列名的 f32 面板矩阵与掩码。"""

    columns: tuple[str, ...]
    bar_count: int
    lookbacks: tuple[int, ...]
    values: Mapping[str, array[float]]
    masks: Mapping[str, array[int]]
    decision_times: tuple[str, ...]

    def column(self, name: str) -> tuple[array[float], array[int]]:
        """返回一列的数值与掩码。"""
        if name not in self.values:
            raise KeyError(f"面板缺少列: {name}")
        return self.values[name], self.masks[name]


def _new_values(count: int) -> array[float]:
    """构造全 NaN 的 f32 列。"""
    return array("f", [math.nan] * count)


def _new_mask(count: int) -> array[int]:
    """构造全零掩码列。"""
    return array("b", [0] * count)


def _assign(
    values: array[float],
    masks: array[int],
    index: int,
    value: float | None,
) -> None:
    """写入一格，缺失保持 NaN 与零掩码。"""
    if value is None:
        return
    numeric = float(value)
    if not math.isfinite(numeric):
        return
    values[index] = numeric
    masks[index] = 1


def tensorize_panel(
    bars: Sequence[ResearchBar],
    features: Sequence[FeatureRow],
    lookbacks: Sequence[int],
) -> PanelTensor:
    """把 ResearchBar 与 FeatureRow 序列导出为列矩阵。"""
    if len(bars) != len(features):
        raise ValueError("行情柱与特征数量不一致")
    if not bars:
        raise ValueError("面板不得为空")
    windows = tuple(sorted(set(int(value) for value in lookbacks)))
    if not windows:
        raise ValueError("回看窗不得为空")
    columns = panel_columns(windows)
    count = len(bars)
    values = {name: _new_values(count) for name in columns}
    masks = {name: _new_mask(count) for name in columns}
    decision_times: list[str] = []
    for index, (bar, feature) in enumerate(zip(bars, features, strict=True)):
        if bar.decision_time != feature.decision_time:
            raise ValueError("行情柱与特征决策时间不一致")
        decision_times.append(feature.decision_time.isoformat())
        _assign(values["close"], masks["close"], index, bar.close)
        if index == 0:
            log_return = 0.0
            gap_seconds = 0.0
        else:
            previous = bars[index - 1]
            log_return = math.log(bar.close / previous.close)
            gap_seconds = (bar.open_time - previous.open_time).total_seconds()
        _assign(values["log_return"], masks["log_return"], index, log_return)
        _assign(values["gap_seconds"], masks["gap_seconds"], index, gap_seconds)
        gate_open = (
            feature.as_of <= feature.decision_time and feature.contiguous
        )
        _assign(
            values["gate_open"],
            masks["gate_open"],
            index,
            1.0 if gate_open else 0.0,
        )
        _assign(
            values["flow_imbalance"],
            masks["flow_imbalance"],
            index,
            feature.flow_imbalance,
        )
        _assign(
            values["volume_score"],
            masks["volume_score"],
            index,
            feature.volume_score,
        )
        _assign(values["jump_score"], masks["jump_score"], index, feature.jump_score)
        sources: Mapping[str, Mapping[int, float | None]] = {
            "trend_score": feature.trend_scores,
            "price_score": feature.price_scores,
            "prior_high": feature.prior_highs,
            "volatility": feature.volatility,
        }
        for lookback in windows:
            for field in WINDOW_FIELDS:
                name = window_column(field, lookback)
                _assign(
                    values[name],
                    masks[name],
                    index,
                    sources[field].get(lookback),
                )
    return PanelTensor(
        columns=columns,
        bar_count=count,
        lookbacks=windows,
        values=values,
        masks=masks,
        decision_times=tuple(decision_times),
    )


def array_bytes(values: array[float] | array[int]) -> bytes:
    """以小端字节序导出数组。"""
    if sys.byteorder == "little":
        return values.tobytes()
    swapped = array(values.typecode, values)
    swapped.byteswap()
    return swapped.tobytes()


def array_from_bytes(typecode: str, body: bytes) -> array[float] | array[int]:
    """由小端字节重建数组。"""
    if typecode == "f":
        result_float: array[float] = array("f")
        result_float.frombytes(body)
        if sys.byteorder != "little":
            result_float.byteswap()
        return result_float
    if typecode == "b":
        result_int: array[int] = array("b")
        result_int.frombytes(body)
        return result_int
    raise ValueError(f"不支持的数组类型码: {typecode}")


def panel_sha256(panel: PanelTensor) -> str:
    """按列顺序散列数值、掩码与决策时间。"""
    digest = hashlib.sha256()
    digest.update(TENSORIZE_METHOD_VERSION.encode("utf-8"))
    digest.update(b"\0")
    for name in panel.columns:
        values, masks = panel.column(name)
        digest.update(name.encode("utf-8"))
        digest.update(b"\0")
        digest.update(array_bytes(values))
        digest.update(array_bytes(masks))
    digest.update("\n".join(panel.decision_times).encode("utf-8"))
    return digest.hexdigest()

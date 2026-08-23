"""状态机 sizing 扫描：候选维度并行，与 `baselines.generate_targets` 逐柱等价。

提供逐柱顺序实现与对数步结合律前缀扫描两种方法，结果必须逐格相同。
"""
from __future__ import annotations

import math
from collections.abc import Mapping

from guvolu.search.kernels import ChunkSignals, KernelSession
from guvolu.search.tensorize import BOOLEAN_FALSE, BOOLEAN_TRUE
from guvolu.search.torch_runtime import Tensor

SCAN_METHOD_VERSION = "searchfast-state-machine-scan-v1"
SCAN_METHODS = ("parallel", "sequential")
SIZING_VOLATILITY = "volatility_target"
SIZING_EXPRESSION = "expression_target"


def _parameter_column(signals: ChunkSignals, name: str) -> Tensor:
    """取候选参数列为 [C×1]。"""
    names = signals.family.parameter_names
    if name not in names:
        raise ValueError(f"流派缺少参数: {name}")
    column = names.index(name)
    return signals.parameters[:, column:column + 1]


def scaled_targets(
    session: KernelSession,
    signals: ChunkSignals,
    periods_per_year: float,
) -> Tensor:
    """按实现波动率缩放目标，缺失或非正波动率为零。"""
    if periods_per_year <= 0:
        raise ValueError("年化周期必须为正")
    torch = session.torch
    lookback = _parameter_column(signals, "lookback").reshape(-1)
    annual_target = _parameter_column(signals, "annual_volatility_target")
    maximum = _parameter_column(signals, "maximum_target")
    stack_values, stack_valid = session.panel.window_stack("volatility")
    index = session.panel.lookback_index(lookback)
    volatility = stack_values[index]
    valid = stack_valid[index] & (volatility > 0.0)
    annualized = volatility * math.sqrt(periods_per_year)
    scaled = torch.minimum(maximum, annual_target / annualized)
    zero = torch.zeros((), dtype=torch.float32, device=session.device)
    return torch.where(valid, scaled, zero)


def _expand(torch: object, tensor: Tensor, rows: int, columns: int) -> Tensor:
    """把可广播张量展开为连续的 [C×B]。"""
    del torch
    return tensor.expand(rows, columns).contiguous()


def _scan_sequential(
    session: KernelSession,
    open_mask: Tensor,
    entry: Tensor,
    exit_signal: Tensor,
    scaled: Tensor,
) -> Tensor:
    """逐柱顺序推进状态机，候选维度向量化。"""
    torch = session.torch
    rows, columns = open_mask.shape
    zero = torch.zeros((), dtype=torch.float32, device=session.device)
    position = torch.zeros(rows, dtype=torch.float32, device=session.device)
    targets = torch.empty(
        (rows, columns), dtype=torch.float32, device=session.device,
    )
    for index in range(columns):
        open_now = open_mask[:, index]
        enter = open_now & (position <= 0.0) & (entry[:, index] == BOOLEAN_TRUE)
        leave = open_now & (position > 0.0) & (exit_signal[:, index] != BOOLEAN_FALSE)
        position = torch.where(
            open_now,
            torch.where(enter, scaled[:, index], torch.where(leave, zero, position)),
            zero,
        )
        targets[:, index] = position
    return targets


def _scan_parallel(
    session: KernelSession,
    open_mask: Tensor,
    entry: Tensor,
    exit_signal: Tensor,
    scaled: Tensor,
) -> Tensor:
    """以结合律前缀扫描在对数步内完成状态机推进。

    每柱转移函数表示为三元组（空仓结果 Z、持仓是否保持 keep、
    持仓被平后的结果 R），复合满足结合律。
    """
    torch = session.torch
    _rows, columns = open_mask.shape
    zero = torch.zeros((), dtype=torch.float32, device=session.device)
    flat_result = torch.where(
        open_mask & (entry == BOOLEAN_TRUE), scaled, zero,
    )
    keep = open_mask & (exit_signal == BOOLEAN_FALSE)
    after_exit = torch.zeros_like(flat_result)
    step = 1
    while step < columns:
        earlier_flat = flat_result[:, :-step]
        earlier_keep = keep[:, :-step]
        earlier_exit = after_exit[:, :-step]
        later_flat = flat_result[:, step:]
        later_keep = keep[:, step:]
        later_exit = after_exit[:, step:]
        combined_flat = torch.where(
            earlier_flat > 0.0,
            torch.where(later_keep, earlier_flat, later_exit),
            later_flat,
        )
        combined_keep = earlier_keep & later_keep
        combined_exit = torch.where(
            earlier_keep,
            later_exit,
            torch.where(
                earlier_exit > 0.0,
                torch.where(later_keep, earlier_exit, later_exit),
                later_flat,
            ),
        )
        flat_result = torch.cat([flat_result[:, :step], combined_flat], dim=1)
        keep = torch.cat([keep[:, :step], combined_keep], dim=1)
        after_exit = torch.cat([after_exit[:, :step], combined_exit], dim=1)
        step *= 2
    return flat_result


def scan_targets(
    session: KernelSession,
    signals: ChunkSignals,
    periods_per_year: float,
    method: str = "parallel",
) -> Tensor:
    """生成一个候选分块的目标位置 [C×B] f32。"""
    if method not in SCAN_METHODS:
        raise ValueError(f"扫描方法不受支持: {method}")
    torch = session.torch
    rows = signals.candidate_count
    columns = session.panel.bar_count
    gate_values, _gate_valid = session.panel.column("gate_open")
    open_mask = _expand(
        torch, (gate_values > 0.5) & signals.required_valid, rows, columns,
    )
    zero = torch.zeros((), dtype=torch.float32, device=session.device)
    sizing = signals.family.sizing
    if sizing == SIZING_EXPRESSION:
        if signals.target is None:
            raise ValueError("表达式目标策略缺少 target 根")
        values = _expand(torch, signals.target.values, rows, columns)
        valid = _expand(torch, signals.target.valid, rows, columns)
        return torch.where(open_mask & valid, values, zero)
    if sizing != SIZING_VOLATILITY:
        raise ValueError(f"sizing 不受支持: {sizing}")
    if signals.entry is None or signals.exit is None:
        raise ValueError("状态策略缺少 entry 或 exit 根")
    entry = _expand(torch, signals.entry, rows, columns)
    exit_signal = _expand(torch, signals.exit, rows, columns)
    scaled = _expand(
        torch, scaled_targets(session, signals, periods_per_year), rows, columns,
    )
    if method == "sequential":
        return _scan_sequential(session, open_mask, entry, exit_signal, scaled)
    return _scan_parallel(session, open_mask, entry, exit_signal, scaled)


def scan_parameters_payload(method: str) -> Mapping[str, object]:
    """登记扫描方法与版本。"""
    if method not in SCAN_METHODS:
        raise ValueError(f"扫描方法不受支持: {method}")
    return {"scan_method_version": SCAN_METHOD_VERSION, "scan_method": method}

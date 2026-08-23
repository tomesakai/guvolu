"""粗筛指标：由目标序列与对数收益计算成本后收益、Sharpe、换手与回撤。

逐柱量在 f32 计算，归约在 f64 完成；口径与 `research/validation.py`
的 `strategy_returns` 与区段指标一致。
"""
from __future__ import annotations

import math
from collections.abc import Mapping
from dataclasses import dataclass

from guvolu.search.kernels import KernelSession
from guvolu.search.torch_runtime import Tensor

METRICS_METHOD_VERSION = "searchfast-coarse-metrics-v1"
METRIC_NAMES = (
    "bars",
    "net_return",
    "annual_return",
    "annual_volatility",
    "sharpe",
    "maximum_drawdown",
    "turnover",
    "annual_turnover",
    "hit_rate",
    "exposure",
    "cost",
)


@dataclass(frozen=True)
class ChunkMetrics:
    """一个候选分块的逐候选指标张量。"""

    bars: int
    net_return: Tensor
    annual_return: Tensor
    annual_volatility: Tensor
    sharpe: Tensor
    maximum_drawdown: Tensor
    turnover: Tensor
    annual_turnover: Tensor
    hit_rate: Tensor
    exposure: Tensor
    cost: Tensor

    def rows(self) -> tuple[Mapping[str, float | int], ...]:
        """导出为逐候选的 Python 数值行。"""
        columns = {
            name: getattr(self, name).cpu().tolist()
            for name in METRIC_NAMES
            if name != "bars"
        }
        count = len(columns["net_return"])
        return tuple(
            {
                "bars": self.bars,
                **{name: float(values[index]) for name, values in columns.items()},
            }
            for index in range(count)
        )


def _cost_inputs(
    cost_model: Mapping[str, object],
) -> tuple[float, float | None, float]:
    """读取成本率、最大空窗秒数与年化周期。"""
    rate = cost_model.get("one_way_cost_rate")
    if not isinstance(rate, (int, float)) or isinstance(rate, bool) or rate < 0:
        raise ValueError("one_way_cost_rate 必须为非负数值")
    gap = cost_model.get("maximum_gap_seconds")
    if gap is not None and (
        not isinstance(gap, (int, float)) or isinstance(gap, bool) or gap <= 0
    ):
        raise ValueError("maximum_gap_seconds 必须为正数值或空")
    periods = cost_model.get("periods_per_year")
    if (
        not isinstance(periods, (int, float))
        or isinstance(periods, bool)
        or periods <= 0
    ):
        raise ValueError("periods_per_year 必须为正数值")
    return float(rate), None if gap is None else float(gap), float(periods)


def strategy_returns_tensor(
    session: KernelSession,
    targets: Tensor,
    cost_model: Mapping[str, object],
) -> tuple[Tensor, Tensor, Tensor]:
    """以前一决策目标计算下一期成本后收益。

    返回收益 [C×B]、单边换手 [C×B-1] 与持仓 [C×B-1]，
    换手含数据断流时的平仓。
    """
    torch = session.torch
    rate, maximum_gap, _periods = _cost_inputs(cost_model)
    rows, columns = targets.shape
    if columns < 2:
        raise ValueError("柱数至少为二")
    zero = torch.zeros((), dtype=torch.float32, device=session.device)
    held = targets[:, :-1]
    previous = torch.cat(
        [torch.zeros((rows, 1), dtype=torch.float32, device=session.device),
         targets[:, :-2]],
        dim=1,
    )
    turnover = torch.abs(held - previous)
    log_returns, _log_valid = session.panel.column("log_return")
    gap_seconds, _gap_valid = session.panel.column("gap_seconds")
    market = log_returns[:, 1:]
    if maximum_gap is None:
        big_gap = torch.zeros_like(held, dtype=torch.bool)
    else:
        big_gap = (gap_seconds[:, 1:] > maximum_gap).expand(rows, columns - 1)
    normal = held * market - turnover * rate
    broken = -(turnover + torch.abs(held)) * rate
    later = torch.where(big_gap, broken, normal)
    returns = torch.cat(
        [torch.zeros((rows, 1), dtype=torch.float32, device=session.device), later],
        dim=1,
    )
    turnover_with_gap = turnover + torch.where(big_gap, torch.abs(held), zero)
    return returns, turnover_with_gap, held


def chunk_metrics(
    session: KernelSession,
    targets: Tensor,
    cost_model: Mapping[str, object],
) -> ChunkMetrics:
    """计算 [1, B) 区段的粗筛指标。"""
    torch = session.torch
    rate, _maximum_gap, periods = _cost_inputs(cost_model)
    returns, turnover, held = strategy_returns_tensor(session, targets, cost_model)
    segment = returns[:, 1:].to(torch.float64)
    count = segment.shape[1]
    mean = segment.mean(dim=1)
    variance = ((segment - mean.unsqueeze(1)) ** 2).mean(dim=1)
    standard = torch.sqrt(variance)
    scale = math.sqrt(periods)
    sharpe = torch.where(
        standard > 0.0,
        mean / torch.where(standard > 0.0, standard, torch.ones_like(standard)) * scale,
        torch.zeros_like(mean),
    )
    cumulative = torch.cumsum(segment, dim=1)
    peak = torch.clamp(torch.cummax(cumulative, dim=1).values, min=0.0)
    drawdown = (1.0 - torch.exp(cumulative - peak)).max(dim=1).values
    drawdown = torch.clamp(drawdown, min=0.0)
    total_turnover = turnover.to(torch.float64).sum(dim=1)
    return ChunkMetrics(
        bars=count,
        net_return=segment.sum(dim=1),
        annual_return=mean * periods,
        annual_volatility=standard * scale,
        sharpe=sharpe,
        maximum_drawdown=drawdown,
        turnover=total_turnover,
        annual_turnover=total_turnover / count * periods,
        hit_rate=(segment > 0.0).to(torch.float64).mean(dim=1),
        exposure=torch.abs(held).to(torch.float64).mean(dim=1),
        cost=total_turnover * rate,
    )

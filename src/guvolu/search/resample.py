"""P3-2 重采样粗筛：embargo walk-forward 折、循环块 bootstrap 与 CSCV。

折定义复用 `research.validation.make_folds`；子集与块起点由 CPU 同种子随机源
生成（G-03），GPU 只承担归约；每候选结果与 CPU `research.validation`
的对应函数在登记容差内对照。
"""
from __future__ import annotations

import hashlib
import itertools
import math
import random
from collections.abc import Mapping, Sequence
from dataclasses import dataclass

from guvolu.research.validation import (
    BLOCK_BOOTSTRAP_METHOD_VERSION,
    PBO_METHOD_VERSION,
    WalkForwardFold,
    make_folds,
)
from guvolu.search.kernels import KernelSession
from guvolu.search.torch_runtime import Tensor

RESAMPLE_METHOD_VERSION = "searchfast-resample-v1"
RESAMPLE_TOLERANCE_VERSION = "searchfast-resample-tolerance-v1"
_FOLD_FIELDS = ("minimum_train_bars", "test_bars", "step_bars", "embargo_bars")
_GATHER_ELEMENT_BUDGET = 16_000_000


@dataclass(frozen=True)
class ResampleSpec:
    """折、bootstrap 与 CSCV 的全部配置（G-06）。"""

    fold: Mapping[str, int]
    bootstrap_seed: int
    bootstrap_block: int
    bootstrap_paths: int
    bootstrap_one_sided_alpha: float
    cscv_split_budget: int
    cscv_seed: int

    def fold_spec_payload(self) -> Mapping[str, object]:
        """导出进入搜索束身份的折定义。"""
        return {
            "method": "embargo-walk-forward",
            **{name: int(self.fold[name]) for name in _FOLD_FIELDS},
        }

    def bootstrap_payload(self) -> Mapping[str, object]:
        """导出进入搜索束身份的 bootstrap 定义。"""
        return {
            "seed": self.bootstrap_seed,
            "block": self.bootstrap_block,
            "paths": self.bootstrap_paths,
            "one_sided_alpha": self.bootstrap_one_sided_alpha,
            "method_version": BLOCK_BOOTSTRAP_METHOD_VERSION,
        }

    def payload(self) -> Mapping[str, object]:
        """导出完整配置。"""
        return {
            "resample_method_version": RESAMPLE_METHOD_VERSION,
            "fold_spec": self.fold_spec_payload(),
            "bootstrap": self.bootstrap_payload(),
            "cscv": {
                "split_budget": self.cscv_split_budget,
                "seed": self.cscv_seed,
                "method_version": PBO_METHOD_VERSION,
            },
        }


def _integer(value: object, name: str) -> int:
    """验证正整数。"""
    if not isinstance(value, int) or isinstance(value, bool) or value <= 0:
        raise ValueError(f"{name} 必须为正整数")
    return value


def _number(value: object, name: str) -> float:
    """验证有限数值。"""
    if (
        not isinstance(value, (int, float))
        or isinstance(value, bool)
        or not math.isfinite(float(value))
    ):
        raise ValueError(f"{name} 必须为有限数值")
    return float(value)


def resample_spec_from_config(research_config: Mapping[str, object]) -> ResampleSpec:
    """由研究配置的 walk_forward 与 validation 段读取重采样参数。"""
    walk_forward = research_config.get("walk_forward")
    validation = research_config.get("validation")
    if not isinstance(walk_forward, Mapping) or not isinstance(validation, Mapping):
        raise ValueError("研究配置缺少 walk_forward 或 validation")
    fold = {name: _integer(walk_forward.get(name), name) for name in _FOLD_FIELDS}
    return ResampleSpec(
        fold=fold,
        bootstrap_seed=_integer(
            validation.get("block_bootstrap_random_seed"), "block_bootstrap_random_seed",
        ),
        bootstrap_block=_integer(
            validation.get("block_bootstrap_bars"), "block_bootstrap_bars",
        ),
        bootstrap_paths=_integer(
            validation.get("block_bootstrap_samples"), "block_bootstrap_samples",
        ),
        bootstrap_one_sided_alpha=_number(
            validation.get("block_bootstrap_one_sided_alpha"),
            "block_bootstrap_one_sided_alpha",
        ),
        cscv_split_budget=_integer(
            validation.get("pbo_split_budget"), "pbo_split_budget",
        ),
        cscv_seed=_integer(validation.get("pbo_random_seed"), "pbo_random_seed"),
    )


@dataclass(frozen=True)
class ResampleTolerance:
    """GPU 重采样与 CPU validation 的对照容差初值。"""

    fold_sharpe_abs: float = 1e-3
    oos_sharpe_abs: float = 1e-3
    bootstrap_lower_abs: float = 1e-3
    bootstrap_p_abs: float = 2e-3
    cscv_rank_abs: float = 1e-6
    pbo_abs: float = 1e-6

    def payload(self) -> Mapping[str, object]:
        """导出容差配置。"""
        return {
            "tolerance_version": RESAMPLE_TOLERANCE_VERSION,
            "fold_sharpe_abs": self.fold_sharpe_abs,
            "oos_sharpe_abs": self.oos_sharpe_abs,
            "bootstrap_lower_abs": self.bootstrap_lower_abs,
            "bootstrap_p_abs": self.bootstrap_p_abs,
            "cscv_rank_abs": self.cscv_rank_abs,
            "pbo_abs": self.pbo_abs,
        }


def resample_tolerance_from_config(
    config: Mapping[str, object] | None,
) -> ResampleTolerance:
    """由配置读取容差，缺省使用初值。"""
    if config is None:
        return ResampleTolerance()
    default = ResampleTolerance()
    values: dict[str, float] = {}
    for name in (
        "fold_sharpe_abs",
        "oos_sharpe_abs",
        "bootstrap_lower_abs",
        "bootstrap_p_abs",
        "cscv_rank_abs",
        "pbo_abs",
    ):
        raw = config.get(name, getattr(default, name))
        if (
            not isinstance(raw, (int, float))
            or isinstance(raw, bool)
            or not math.isfinite(float(raw))
            or float(raw) < 0
        ):
            raise ValueError(f"容差必须为非负有限数值: {name}")
        values[name] = float(raw)
    return ResampleTolerance(**values)


def family_bootstrap_seed(family: str, seed: int) -> int:
    """与 validation 同式派生流派 bootstrap 种子。"""
    return seed ^ int(
        hashlib.sha256(
            f"{family}:{BLOCK_BOOTSTRAP_METHOD_VERSION}".encode("utf-8")
        ).hexdigest()[:16],
        16,
    )


def family_cscv_seed(family: str, seed: int) -> int:
    """与 validation 同式派生流派 CSCV 种子。"""
    return seed ^ int(
        hashlib.sha256(family.encode("utf-8")).hexdigest()[:16], 16,
    )


def cscv_subsets(
    fold_count: int,
    split_budget: int,
    seed: int,
) -> tuple[tuple[int, ...], ...]:
    """按 validation 同序生成确定性 CSCV 半分割子集。"""
    usable = fold_count - fold_count % 2
    if usable < 4:
        return ()
    half = usable // 2
    total_unique = math.comb(usable, half) // 2
    target = min(split_budget, total_unique)
    subsets: set[tuple[int, ...]] = set()
    if total_unique <= split_budget:
        for raw_subset in itertools.combinations(range(usable), half):
            complement = tuple(
                index for index in range(usable) if index not in raw_subset
            )
            subsets.add(min(raw_subset, complement))
    else:
        generator = random.Random(seed)
        while len(subsets) < target:
            raw_subset = tuple(sorted(generator.sample(range(usable), half)))
            complement = tuple(
                index for index in range(usable) if index not in raw_subset
            )
            subsets.add(min(raw_subset, complement))
    return tuple(sorted(subsets))


def bootstrap_block_starts(
    count: int,
    block: int,
    paths: int,
    seed: int,
) -> tuple[tuple[tuple[int, ...], ...], tuple[int, ...] | None]:
    """按 validation 同序抽取每条路径的块起点与余块起点。"""
    if count < 2 or block <= 0 or block * 2 > count or paths <= 0:
        raise ValueError("bootstrap 参数与序列长度不匹配")
    full_blocks, remainder = divmod(count, block)
    generator = random.Random(seed)
    full: list[tuple[int, ...]] = []
    remainder_starts: list[int] = []
    for _path in range(paths):
        full.append(tuple(generator.randrange(count) for _block in range(full_blocks)))
        if remainder:
            remainder_starts.append(generator.randrange(count))
    return tuple(full), (tuple(remainder_starts) if remainder else None)


@dataclass(frozen=True)
class ResampleMetrics:
    """一个候选分块的重采样指标张量与流派级 CSCV 结论。"""

    folds: tuple[WalkForwardFold, ...]
    fold_train_sharpe: Tensor
    fold_test_sharpe: Tensor
    fold_test_net_return: Tensor
    oos_bars: int
    oos_sharpe: Tensor
    oos_net_return: Tensor
    bootstrap_lower: Tensor
    bootstrap_p: Tensor
    bootstrap_paths: int
    cscv_rank_median: Tensor
    pbo: float
    cscv_median_rank: float
    cscv_split_count: int

    def rows(self) -> tuple[Mapping[str, object], ...]:
        """导出逐候选的 JSON 可序列化行。"""
        train = self.fold_train_sharpe.cpu().tolist()
        test = self.fold_test_sharpe.cpu().tolist()
        net = self.fold_test_net_return.cpu().tolist()
        oos_sharpe = self.oos_sharpe.cpu().tolist()
        oos_net = self.oos_net_return.cpu().tolist()
        lower = self.bootstrap_lower.cpu().tolist()
        p_value = self.bootstrap_p.cpu().tolist()
        rank = self.cscv_rank_median.cpu().tolist()
        result: list[Mapping[str, object]] = []
        for index in range(len(oos_sharpe)):
            positive = sum(value > 0.0 for value in net[index])
            result.append({
                "fold_count": len(self.folds),
                "fold_train_sharpe": [float(value) for value in train[index]],
                "fold_test_sharpe": [float(value) for value in test[index]],
                "positive_fold_ratio": positive / len(self.folds),
                "oos_bars": self.oos_bars,
                "oos_sharpe": float(oos_sharpe[index]),
                "oos_net_return": float(oos_net[index]),
                "bootstrap_sharpe_lower_bound": float(lower[index]),
                "bootstrap_p_value": float(p_value[index]),
                "bootstrap_paths": self.bootstrap_paths,
                "cscv_median_oos_rank": float(rank[index]),
                "family_pbo": self.pbo,
                "family_cscv_median_rank": self.cscv_median_rank,
                "cscv_split_count": self.cscv_split_count,
            })
        return tuple(result)


def _segment_sharpe(
    torch: object,
    segment: Tensor,
    scale: float,
) -> tuple[Tensor, Tensor]:
    """计算 [C×N] 区段的年化 Sharpe 与净收益。"""
    del torch
    mean = segment.mean(dim=1)
    variance = ((segment - mean.unsqueeze(1)) ** 2).mean(dim=1)
    standard = variance.sqrt()
    safe = standard.where(standard > 0.0, standard.new_ones(()))
    sharpe = (mean / safe * scale).where(standard > 0.0, standard.new_zeros(()))
    return sharpe, segment.sum(dim=1)


def _sharpe_from_moments(
    mean: Tensor,
    second: Tensor,
    scale: float,
) -> Tensor:
    """由一阶与二阶矩计算年化 Sharpe，方差非正为零。"""
    variance = (second - mean * mean).clamp(min=0.0)
    safe = variance.where(variance > 0.0, variance.new_ones(()))
    return (mean / safe.sqrt() * scale).where(variance > 0.0, variance.new_zeros(()))


def _sharpe_standard_error(
    mean: Tensor,
    second: Tensor,
    covariance: tuple[Tensor, Tensor, Tensor],
    count: int,
    scale: float,
) -> Tensor:
    """以矩函数梯度与长程协方差计算 Sharpe 标准误。"""
    variance = second - mean * mean
    positive = variance > 0.0
    safe = variance.where(positive, variance.new_ones(()))
    power = safe ** 1.5
    gradient_mean = scale * second / power
    gradient_second = -scale * mean / (2.0 * power)
    covariance_mean, covariance_cross, covariance_second = covariance
    long_run = (
        gradient_mean * gradient_mean * covariance_mean
        + 2.0 * gradient_mean * gradient_second * covariance_cross
        + gradient_second * gradient_second * covariance_second
    )
    error = (long_run / count).clamp(min=0.0).sqrt()
    return error.where(positive, error.new_zeros(()))


def _bootstrap(
    session: KernelSession,
    values: Tensor,
    spec: ResampleSpec,
    seed: int,
    scale: float,
) -> tuple[Tensor, Tensor]:
    """studentized 循环块 bootstrap：下界与单侧 p 值，逐候选。"""
    torch = session.torch
    rows, count = values.shape
    block = spec.bootstrap_block
    paths = spec.bootstrap_paths
    alpha = spec.bootstrap_one_sided_alpha
    if alpha <= 0.0 or alpha >= 0.5:
        raise ValueError("bootstrap 单侧 alpha 必须位于 (0, 0.5)")
    full_starts, remainder_starts = bootstrap_block_starts(count, block, paths, seed)
    mean = values.mean(dim=1)
    second = (values * values).mean(dim=1)
    sharpe = _sharpe_from_moments(mean, second, scale)
    doubled = torch.cat([values, values], dim=1)
    zero_column = torch.zeros((rows, 1), dtype=values.dtype, device=values.device)
    prefix = torch.cat([zero_column, doubled.cumsum(dim=1)], dim=1)
    prefix_square = torch.cat([zero_column, (doubled * doubled).cumsum(dim=1)], dim=1)
    block_sum = prefix[:, block:block + count] - prefix[:, :count]
    block_square = prefix_square[:, block:block + count] - prefix_square[:, :count]
    normalization = math.sqrt(block)
    centered_mean = (block_sum - block * mean.unsqueeze(1)) / normalization
    centered_second = (block_square - block * second.unsqueeze(1)) / normalization
    covariance = (
        (centered_mean * centered_mean).mean(dim=1),
        (centered_mean * centered_second).mean(dim=1),
        (centered_second * centered_second).mean(dim=1),
    )
    standard_error = _sharpe_standard_error(mean, second, covariance, count, scale)
    starts = torch.tensor(
        [list(row) for row in full_starts], dtype=torch.int64, device=values.device,
    )
    block_count = starts.shape[1]
    remainder = count - block * block_count
    gathered_paths = max(1, _GATHER_ELEMENT_BUDGET // max(paths * max(block_count, 1), 1))
    lower_values: list[Tensor] = []
    p_values: list[Tensor] = []
    for start in range(0, rows, gathered_paths):
        stop = min(start + gathered_paths, rows)
        sums = block_sum[start:stop][:, starts]
        squares = block_square[start:stop][:, starts]
        total = sums.sum(dim=2)
        total_square = squares.sum(dim=2)
        sum_b = total.clone()
        sum_q = total_square.clone()
        sum_bb = (sums * sums).sum(dim=2)
        sum_qq = (squares * squares).sum(dim=2)
        sum_bq = (sums * squares).sum(dim=2)
        if remainder_starts is not None:
            remainder_index = torch.tensor(
                list(remainder_starts), dtype=torch.int64, device=values.device,
            )
            remainder_sum = (
                prefix[start:stop][:, remainder_index + remainder]
                - prefix[start:stop][:, remainder_index]
            )
            remainder_square = (
                prefix_square[start:stop][:, remainder_index + remainder]
                - prefix_square[start:stop][:, remainder_index]
            )
            total = total + remainder_sum
            total_square = total_square + remainder_square
        bootstrap_mean = total / count
        bootstrap_second = total_square / count
        bootstrap_sharpe = _sharpe_from_moments(bootstrap_mean, bootstrap_second, scale)
        k_l2 = block_count * block * block
        sum_centered_mean = (
            sum_bb - 2.0 * block * bootstrap_mean * sum_b
            + k_l2 * bootstrap_mean * bootstrap_mean
        ) / block
        sum_centered_cross = (
            sum_bq - block * bootstrap_second * sum_b
            - block * bootstrap_mean * sum_q
            + k_l2 * bootstrap_mean * bootstrap_second
        ) / block
        sum_centered_second = (
            sum_qq - 2.0 * block * bootstrap_second * sum_q
            + k_l2 * bootstrap_second * bootstrap_second
        ) / block
        divisor = max(block_count, 1)
        bootstrap_error = _sharpe_standard_error(
            bootstrap_mean,
            bootstrap_second,
            (
                sum_centered_mean / divisor,
                sum_centered_cross / divisor,
                sum_centered_second / divisor,
            ),
            count,
            scale,
        )
        safe_error = bootstrap_error.where(
            bootstrap_error > 0.0, bootstrap_error.new_ones(()),
        )
        statistic = (
            (bootstrap_sharpe - sharpe[start:stop].unsqueeze(1)) / safe_error
        ).where(bootstrap_error > 0.0, bootstrap_error.new_zeros(()))
        local_error = standard_error[start:stop]
        safe_local = local_error.where(local_error > 0.0, local_error.new_ones(()))
        observed = sharpe[start:stop] / safe_local
        upper = torch.quantile(statistic, 1.0 - alpha, dim=1, interpolation="linear")
        exceedances = (statistic >= observed.unsqueeze(1)).sum(dim=1).to(values.dtype)
        lower = sharpe[start:stop] - upper * local_error
        p_value = (exceedances + 1.0) / (paths + 1.0)
        has_error = local_error > 0.0
        lower_values.append(lower.where(has_error, lower.new_zeros(())))
        p_values.append(p_value.where(has_error, p_value.new_ones(())))
    return torch.cat(lower_values), torch.cat(p_values)


def _cscv(
    session: KernelSession,
    fold_test_sharpe: Tensor,
    spec: ResampleSpec,
    seed: int,
) -> tuple[Tensor, float, float, int]:
    """CSCV：逐候选样本外秩中位数与流派级 PBO。"""
    torch = session.torch
    rows, fold_count = fold_test_sharpe.shape
    subsets = cscv_subsets(fold_count, spec.cscv_split_budget, seed)
    if rows < 2 or not subsets:
        return (
            torch.full((rows,), 0.5, dtype=torch.float64, device=fold_test_sharpe.device),
            0.0,
            1.0,
            0,
        )
    usable = fold_count - fold_count % 2
    scores = fold_test_sharpe[:, fold_count % 2:]
    half = usable // 2
    mask = torch.zeros((len(subsets), usable), dtype=torch.float64, device=scores.device)
    for position, subset in enumerate(subsets):
        mask[position, list(subset)] = 1.0
    in_sample = scores @ mask.T / half
    out_sample = scores @ (1.0 - mask).T / half
    ordered = out_sample.T.contiguous().sort(dim=1).values
    queries = out_sample.T.contiguous()
    lower = torch.searchsorted(ordered, queries, right=False)
    upper = torch.searchsorted(ordered, queries, right=True)
    ranks = ((lower + (upper - lower) / 2.0) / rows).T
    winning = in_sample.max(dim=0).values
    winners = (in_sample == winning.unsqueeze(0)).to(torch.float64)
    relative = (ranks * winners).sum(dim=0) / winners.sum(dim=0)
    pbo = float((relative < 0.5).to(torch.float64).mean().item())
    median_rank = float(relative.median().item()) if len(subsets) % 2 else float(
        relative.sort().values[len(subsets) // 2 - 1:len(subsets) // 2 + 1].mean().item()
    )
    candidate_median = ranks.median(dim=1).values if len(subsets) % 2 else (
        ranks.sort(dim=1).values[:, len(subsets) // 2 - 1:len(subsets) // 2 + 1].mean(dim=1)
    )
    return candidate_median, pbo, median_rank, len(subsets)


def resample_chunk(
    session: KernelSession,
    family: str,
    returns: Tensor,
    spec: ResampleSpec,
    periods_per_year: float,
) -> ResampleMetrics:
    """对一个候选分块的成本后收益 [C×B] 计算全部重采样指标。"""
    torch = session.torch
    rows, bar_count = returns.shape
    folds = make_folds(bar_count, dict(spec.fold))
    scale = math.sqrt(periods_per_year)
    values = returns.to(torch.float64)
    train_sharpes: list[Tensor] = []
    test_sharpes: list[Tensor] = []
    test_nets: list[Tensor] = []
    oos_mask = torch.zeros(bar_count, dtype=torch.bool, device=values.device)
    for fold in folds:
        train_sharpe, _train_net = _segment_sharpe(
            torch, values[:, fold.train_start:fold.train_end], scale,
        )
        test_sharpe, test_net = _segment_sharpe(
            torch, values[:, fold.test_start:fold.test_end], scale,
        )
        train_sharpes.append(train_sharpe)
        test_sharpes.append(test_sharpe)
        test_nets.append(test_net)
        oos_mask[fold.test_start:fold.test_end] = True
    oos_mask[0] = False
    oos_values = values[:, oos_mask]
    oos_sharpe, oos_net = _segment_sharpe(torch, oos_values, scale)
    fold_test = torch.stack(test_sharpes, dim=1)
    lower, p_value = _bootstrap(
        session,
        oos_values,
        spec,
        family_bootstrap_seed(family, spec.bootstrap_seed),
        scale,
    )
    rank_median, pbo, median_rank, split_count = _cscv(
        session, fold_test, spec, family_cscv_seed(family, spec.cscv_seed),
    )
    return ResampleMetrics(
        folds=folds,
        fold_train_sharpe=torch.stack(train_sharpes, dim=1),
        fold_test_sharpe=fold_test,
        fold_test_net_return=torch.stack(test_nets, dim=1),
        oos_bars=int(oos_values.shape[1]),
        oos_sharpe=oos_sharpe,
        oos_net_return=oos_net,
        bootstrap_lower=lower,
        bootstrap_p=p_value,
        bootstrap_paths=spec.bootstrap_paths,
        cscv_rank_median=rank_median,
        pbo=pbo,
        cscv_median_rank=median_rank,
        cscv_split_count=split_count,
    )


@dataclass(frozen=True)
class ResampleScreen:
    """重采样粗筛阈值，全部来自配置（G-06）。"""

    minimum_oos_sharpe: float = 0.0
    minimum_positive_fold_ratio: float = 0.5
    maximum_bootstrap_p: float = 1.0
    maximum_pbo: float = 1.0

    def payload(self) -> Mapping[str, object]:
        """导出配置。"""
        return {
            "minimum_oos_sharpe": self.minimum_oos_sharpe,
            "minimum_positive_fold_ratio": self.minimum_positive_fold_ratio,
            "maximum_bootstrap_p": self.maximum_bootstrap_p,
            "maximum_pbo": self.maximum_pbo,
        }

    def passes(self, row: Mapping[str, object]) -> bool:
        """判断一行重采样指标是否通过粗筛。"""
        oos_sharpe = float(str(row["oos_sharpe"]))
        ratio = float(str(row["positive_fold_ratio"]))
        p_value = float(str(row["bootstrap_p_value"]))
        pbo = float(str(row["family_pbo"]))
        return (
            math.isfinite(oos_sharpe)
            and oos_sharpe >= self.minimum_oos_sharpe
            and ratio >= self.minimum_positive_fold_ratio
            and p_value <= self.maximum_bootstrap_p
            and pbo <= self.maximum_pbo
        )


def resample_screen_from_config(config: Mapping[str, object] | None) -> ResampleScreen:
    """由配置读取重采样粗筛阈值。"""
    if config is None:
        return ResampleScreen()
    default = ResampleScreen()
    values: dict[str, float] = {}
    for name in (
        "minimum_oos_sharpe",
        "minimum_positive_fold_ratio",
        "maximum_bootstrap_p",
        "maximum_pbo",
    ):
        values[name] = _number(config.get(name, getattr(default, name)), name)
    return ResampleScreen(**values)


def fold_payload(folds: Sequence[WalkForwardFold]) -> list[Mapping[str, object]]:
    """导出折边界。"""
    return [
        {
            "fold_id": fold.fold_id,
            "train_start": fold.train_start,
            "train_end": fold.train_end,
            "test_start": fold.test_start,
            "test_end": fold.test_end,
        }
        for fold in folds
    ]

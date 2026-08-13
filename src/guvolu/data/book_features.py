"""区域判读与报警规则（footprint-design 6.4、6.8 节，TBD-29 提案实施）。

四类判定（吸收、抽离、扫单、流动性真空）并列输出，不合成单一结论；
判读结果永远是带阈值的判定而非事实，响应字段以 met 与 confidence
表达判定成立与规则强度。阈值全部具名配置（G-06），配置散列随结果
落 book_feature 派生事件表保证可复现（D-09）。
报警是判读层之上的规则实例层：（种类, 品种, 阈值覆盖, 启停）四元组
加带几何维度（band_bp 标准带或显式价带，2026-08-10）入配置文件，
检测由 book_feature 事件流唯一承担，本模块只做流上匹配；
规则带几何决定其判定评估价带，缺省沿用请求带并记录来源。
数值一律 Decimal 字符串（T-08、D-07）。
"""
from __future__ import annotations

import json
from collections.abc import Mapping, Sequence
from dataclasses import dataclass
from decimal import Decimal
from pathlib import Path

from guvolu.domain.ids import sha256_hex

# 吸收：成交消耗分位阈（起步 P90）
ABSORPTION_EXECUTED_PERCENTILE = Decimal("0.90")
# 吸收：净增挂回补对消耗的阈比
ABSORPTION_REPLENISH_RATIO = Decimal("0.5")
# 吸收：价格位移上限（bp）
ABSORPTION_MAX_DRIFT_BP = Decimal("5")
# 抽离：净撤减分位阈（起步 P90）
PULL_CANCEL_PERCENTILE = Decimal("0.90")
# 抽离：成交消耗对净撤减的比上限
PULL_EXECUTED_RATIO = Decimal("0.25")
# 嫌疑强形态：单桶净撤减分位阈
SPOOFING_CANCEL_PERCENTILE = Decimal("0.95")
# 嫌疑强形态：存续桶数上限
SPOOFING_MAX_LIFETIME_BUCKETS = 60
# 扫单：连续档数下限 k
SWEEP_MIN_LEVELS = 3
# 扫单：桶跨度上限 m
SWEEP_MAX_BUCKETS = 3
# 真空：带内挂量低分位阈（起步 P10）
VACUUM_DEPTH_PERCENTILE = Decimal("0.10")
# 置信度保留位
CONFIDENCE_PLACES = Decimal("0.01")
# 分位基线最少有效列
BASELINE_MIN_SAMPLES = 30
# 强度公式版本
CONFIDENCE_VERSION = "rule-strength-min-v2"
# 判读算法身份
SIGNAL_CODE_VERSION = "region-analysis-v2"
# 报警规则文件名
ALERT_RULES_FILE_NAME = "alert_rules.json"
# 报警最低置信度缺省值
ALERT_MIN_CONFIDENCE = Decimal("0")
# 判定种类与术语表中文对照
JUDGMENT_LABELS: Mapping[str, str] = {
    "absorption": "吸收",
    "pull": "抽离",
    "sweep": "扫单",
    "liquidity_vacuum": "流动性真空",
}

# 指标键中文标签，界面展示用（X-07）
METRIC_LABELS: Mapping[str, str] = {
    "executed_total": "成交消耗合计",
    "executed_percentile": "消耗分位",
    "net_add_total": "净增挂合计",
    "replenish_need": "回补需要",
    "mid_drift_bp": "中价位移",
    "net_cancel_total": "净撤减合计",
    "cancel_percentile": "撤减分位",
    "executed_cap": "消耗上限",
    "approaching": "价格逼近",
    "spoofing_suspicion": "挂单欺诈嫌疑",
    "levels_broken": "击穿档数",
    "bucket_span": "桶跨度",
    "side": "方向",
    "min_levels": "档数下限",
    "max_buckets": "桶数上限",
    "min_depth": "最低挂量",
    "depth_percentile": "挂量分位",
    "threshold_percentile": "分位阈",
    "confidence": "规则强度",
    "baseline_samples": "基线样本数",
    "band_source": "带几何来源",
    "band_bp": "规则带宽",
    "rule_id": "规则标识",
}

# 带几何来源三态
BAND_SOURCE_REQUEST = "request"
BAND_SOURCE_RULE_BP = "band_bp"
BAND_SOURCE_EXPLICIT = "explicit"

_ZERO = Decimal("0")
_ONE = Decimal("1")


def region_config() -> dict[str, str]:
    """判定阈值全集，供散列与追溯。"""
    return {
        "absorption_executed_percentile": str(ABSORPTION_EXECUTED_PERCENTILE),
        "absorption_replenish_ratio": str(ABSORPTION_REPLENISH_RATIO),
        "absorption_max_drift_bp": str(ABSORPTION_MAX_DRIFT_BP),
        "pull_cancel_percentile": str(PULL_CANCEL_PERCENTILE),
        "pull_executed_ratio": str(PULL_EXECUTED_RATIO),
        "spoofing_cancel_percentile": str(SPOOFING_CANCEL_PERCENTILE),
        "spoofing_max_lifetime_buckets": str(SPOOFING_MAX_LIFETIME_BUCKETS),
        "sweep_min_levels": str(SWEEP_MIN_LEVELS),
        "sweep_max_buckets": str(SWEEP_MAX_BUCKETS),
        "vacuum_depth_percentile": str(VACUUM_DEPTH_PERCENTILE),
    }


def region_config_hash() -> str:
    """判定配置散列（D-09）。"""
    text = json.dumps(region_config(), sort_keys=True, ensure_ascii=False)
    return sha256_hex(text.encode("utf-8"))


def _text(value: Decimal) -> str:
    return format(value, "f")


@dataclass(frozen=True, slots=True)
class BandSample:
    """单列带内合计：判定分位基线的样本单元。"""

    executed: Decimal
    net_cancel: Decimal
    depth: Decimal


@dataclass(frozen=True, slots=True)
class RegionCell:
    """区域内单格提取值。"""

    price_bin: Decimal
    side: str
    last_qty: Decimal
    net_add: Decimal
    net_cancel: Decimal
    executed: Decimal


def _in_band(price_text: object, low: Decimal, high: Decimal) -> bool:
    if not isinstance(price_text, str):
        return False
    price = Decimal(price_text)
    return low <= price <= high


def _column_cells(
    column: Mapping[str, object], low: Decimal, high: Decimal
) -> list[RegionCell]:
    cells = column.get("cells")
    out: list[RegionCell] = []
    if not isinstance(cells, list):
        return out
    for cell in cells:
        if not (isinstance(cell, list) and len(cell) >= 6):
            continue
        if not _in_band(cell[0], low, high):
            continue
        out.append(
            RegionCell(
                price_bin=Decimal(str(cell[0])),
                side=str(cell[1]),
                last_qty=Decimal(str(cell[2])),
                net_add=Decimal(str(cell[3])),
                net_cancel=Decimal(str(cell[4])),
                executed=Decimal(str(cell[5])),
            )
        )
    return out


def band_sample(
    column: Mapping[str, object], low: Decimal, high: Decimal
) -> BandSample | None:
    """列的带内样本，空档列返回空。"""
    if column.get("gap") or column.get("carried"):
        return None
    executed = net_cancel = depth = _ZERO
    for cell in _column_cells(column, low, high):
        executed += cell.executed
        net_cancel += cell.net_cancel
        depth += cell.last_qty
    return BandSample(executed=executed, net_cancel=net_cancel, depth=depth)


def percentile_rank(population: Sequence[Decimal], value: Decimal) -> Decimal:
    """样本中不大于该值的占比，空样本返回零。"""
    if not population:
        return _ZERO
    below = sum(1 for item in population if item <= value)
    return (Decimal(below) / Decimal(len(population))).quantize(
        CONFIDENCE_PLACES
    )


def _score_at_least(value: Decimal, threshold: Decimal) -> Decimal:
    """下限条件强度，阈值处为二分之一。"""
    if value < threshold:
        if threshold <= _ZERO:
            return _ZERO
        return max(_ZERO, _ONE / 2 * value / threshold)
    if threshold >= _ZERO and value <= _ONE and threshold < _ONE:
        span = _ONE - threshold
    else:
        span = max(abs(threshold), _ONE)
    return min(_ONE, _ONE / 2 + (_ONE / 2) * (value - threshold) / span)


def _score_at_most(value: Decimal, threshold: Decimal) -> Decimal:
    """上限条件强度，阈值处为二分之一。"""
    if value <= threshold:
        if threshold <= _ZERO:
            return _ONE / 2
        return min(
            _ONE,
            _ONE / 2 + (_ONE / 2) * (threshold - value) / threshold,
        )
    if threshold <= _ZERO or value <= _ZERO:
        return _ZERO
    return min(_ONE / 2, (_ONE / 2) * threshold / value)


def _confidence(scores: Sequence[Decimal]) -> str:
    """规则强度：最弱条件分，量化两位。"""
    if not scores:
        return "0"
    return _text(min(scores).quantize(CONFIDENCE_PLACES))


def _criterion(
    name: str,
    observed: Decimal | bool,
    comparator: str,
    threshold: Decimal | bool,
) -> dict[str, object]:
    """构造可复读的单条件结果。"""
    if comparator == ">=":
        assert isinstance(observed, Decimal)
        assert isinstance(threshold, Decimal)
        met = observed >= threshold
        score = _score_at_least(observed, threshold)
    elif comparator == "<=":
        assert isinstance(observed, Decimal)
        assert isinstance(threshold, Decimal)
        met = observed <= threshold
        score = _score_at_most(observed, threshold)
    elif comparator == "is":
        met = observed is threshold
        score = _ONE if met else _ZERO
    else:
        raise ValueError("未知条件比较符")
    return {
        "name": name,
        "observed": observed if isinstance(observed, bool) else _text(observed),
        "comparator": comparator,
        "threshold": threshold if isinstance(threshold, bool) else _text(threshold),
        "met": met,
        "score": _text(score.quantize(CONFIDENCE_PLACES)),
    }


def _criteria_result(
    criteria: Sequence[dict[str, object]], active: bool
) -> tuple[bool, str]:
    """以全条件合取生成判定与强度。"""
    met = active and all(row.get("met") is True for row in criteria)
    if not active:
        return False, "0"
    scores = [Decimal(str(row["score"])) for row in criteria]
    return met, _confidence(scores)


def _mid_of(column: Mapping[str, object]) -> Decimal | None:
    mid = column.get("mid")
    return Decimal(mid) if isinstance(mid, str) else None


def analyze_region(
    region_columns: Sequence[Mapping[str, object]],
    context_samples: Sequence[BandSample],
    price_low: Decimal,
    price_high: Decimal,
    row_bin: Decimal,
) -> list[dict[str, object]]:
    """四类判定并列输出（6.4 节），各带指标值与置信度。

    region_columns 为窗口内列，context_samples 为同带全日
    逐列样本（分位基线）。返回判定清单，met 表示判定成立。
    """
    usable_columns = [
        column
        for column in region_columns
        if not column.get("gap") and not column.get("carried")
    ]
    samples = [
        sample
        for sample in (
            band_sample(column, price_low, price_high)
            for column in usable_columns
        )
        if sample is not None
    ]
    executed_sum = sum((s.executed for s in samples), _ZERO)
    cancel_sum = sum((s.net_cancel for s in samples), _ZERO)
    add_sum = _ZERO
    for column in usable_columns:
        for cell in _column_cells(column, price_low, price_high):
            add_sum += cell.net_add
    mids = [
        mid for mid in (_mid_of(column) for column in usable_columns)
        if mid is not None
    ]
    exec_population = [s.executed for s in context_samples]
    cancel_population = [s.net_cancel for s in context_samples]
    depth_population = [s.depth for s in context_samples]
    baseline_samples = len(context_samples)
    mean_executed = (
        executed_sum / Decimal(len(samples)) if samples else _ZERO
    )
    mean_cancel = cancel_sum / Decimal(len(samples)) if samples else _ZERO
    out: list[dict[str, object]] = []
    out.append(
        _judge_absorption(
            samples, mids, mean_executed, exec_population,
            executed_sum, add_sum, baseline_samples,
        )
    )
    out.append(
        _judge_pull(
            usable_columns, samples, mids, mean_cancel, cancel_population,
            executed_sum, cancel_sum, price_low, price_high,
            baseline_samples,
        )
    )
    out.append(
        _judge_sweep(
            usable_columns, price_low, price_high, row_bin, baseline_samples
        )
    )
    out.append(_judge_vacuum(samples, depth_population, baseline_samples))
    return out


def _judge_absorption(
    samples: Sequence[BandSample],
    mids: Sequence[Decimal],
    mean_executed: Decimal,
    population: Sequence[Decimal],
    executed_sum: Decimal,
    add_sum: Decimal,
    baseline_samples: int,
) -> dict[str, object]:
    """吸收：消耗高分位且回补达阈比且价格位移低于阈。"""
    rank = percentile_rank(population, mean_executed)
    replenish_need = executed_sum * ABSORPTION_REPLENISH_RATIO
    drift_bp = _ZERO
    if len(mids) >= 2 and mids[0] > _ZERO:
        drift_bp = abs(
            (mids[-1] - mids[0]) / mids[0] * Decimal("10000")
        ).quantize(Decimal("0.01"))
    active = bool(samples) and executed_sum > _ZERO
    criteria = [
        _criterion(
            "baseline_samples",
            Decimal(baseline_samples),
            ">=",
            Decimal(BASELINE_MIN_SAMPLES),
        ),
        _criterion(
            "executed_percentile",
            rank,
            ">=",
            ABSORPTION_EXECUTED_PERCENTILE,
        ),
        _criterion("net_add_total", add_sum, ">=", replenish_need),
        _criterion(
            "mid_drift_bp", drift_bp, "<=", ABSORPTION_MAX_DRIFT_BP
        ),
    ]
    met, confidence = _criteria_result(criteria, active)
    return {
        "kind": "absorption",
        "label": JUDGMENT_LABELS["absorption"],
        "met": met,
        "confidence": confidence,
        "confidence_version": CONFIDENCE_VERSION,
        "criteria": criteria,
        "metrics": {
            "executed_total": _text(executed_sum),
            "executed_percentile": _text(rank),
            "net_add_total": _text(add_sum),
            "replenish_need": _text(replenish_need),
            "mid_drift_bp": _text(drift_bp),
            "baseline_samples": str(baseline_samples),
        },
    }


def _judge_pull(
    region_columns: Sequence[Mapping[str, object]],
    samples: Sequence[BandSample],
    mids: Sequence[Decimal],
    mean_cancel: Decimal,
    population: Sequence[Decimal],
    executed_sum: Decimal,
    cancel_sum: Decimal,
    price_low: Decimal,
    price_high: Decimal,
    baseline_samples: int,
) -> dict[str, object]:
    """抽离：价格逼近中净撤减高分位且消耗低于阈比。"""
    rank = percentile_rank(population, mean_cancel)
    center = (price_low + price_high) / Decimal("2")
    approaching = False
    if len(mids) >= 2:
        approaching = abs(mids[-1] - center) < abs(mids[0] - center)
    executed_cap = cancel_sum * PULL_EXECUTED_RATIO
    active = bool(samples) and cancel_sum > _ZERO
    criteria = [
        _criterion(
            "baseline_samples",
            Decimal(baseline_samples),
            ">=",
            Decimal(BASELINE_MIN_SAMPLES),
        ),
        _criterion("approaching", approaching, "is", True),
        _criterion("cancel_percentile", rank, ">=", PULL_CANCEL_PERCENTILE),
        _criterion("executed_total", executed_sum, "<=", executed_cap),
    ]
    met, confidence = _criteria_result(criteria, active)
    spoofing = _spoofing_suspicion(
        region_columns, population, price_low, price_high
    )
    return {
        "kind": "pull",
        "label": JUDGMENT_LABELS["pull"],
        "met": met,
        "confidence": confidence,
        "confidence_version": CONFIDENCE_VERSION,
        "criteria": criteria,
        "metrics": {
            "net_cancel_total": _text(cancel_sum),
            "cancel_percentile": _text(rank),
            "executed_total": _text(executed_sum),
            "executed_cap": _text(executed_cap),
            "approaching": approaching,
            "spoofing_suspicion": spoofing,
            "baseline_samples": str(baseline_samples),
        },
    }


def _spoofing_suspicion(
    region_columns: Sequence[Mapping[str, object]],
    cancel_population: Sequence[Decimal],
    price_low: Decimal,
    price_high: Decimal,
) -> bool:
    """嫌疑强形态：大额短存续撤减，仅为嫌疑不断言。"""
    best_at = -1
    best_bin: Decimal | None = None
    best_cancel = _ZERO
    for at, column in enumerate(region_columns):
        for cell in _column_cells(column, price_low, price_high):
            if cell.net_cancel > best_cancel:
                best_cancel = cell.net_cancel
                best_bin = cell.price_bin
                best_at = at
    if best_bin is None or best_cancel <= _ZERO:
        return False
    rank = percentile_rank(cancel_population, best_cancel)
    if rank < SPOOFING_CANCEL_PERCENTILE:
        return False
    # 存续只数撤减桶之前的在档桶
    lifetime = 0
    for column in region_columns[:best_at]:
        present = False
        for cell in _column_cells(column, price_low, price_high):
            if cell.price_bin == best_bin and cell.last_qty > _ZERO:
                present = True
                break
        lifetime = lifetime + 1 if present else 0
    return 0 < lifetime <= SPOOFING_MAX_LIFETIME_BUCKETS


def _judge_sweep(
    region_columns: Sequence[Mapping[str, object]],
    price_low: Decimal,
    price_high: Decimal,
    row_bin: Decimal,
    baseline_samples: int,
) -> dict[str, object]:
    """扫单：单侧连续档在桶跨度内被成交击穿。"""
    swept: dict[str, dict[Decimal, int]] = {"ask": {}, "bid": {}}
    for at, column in enumerate(region_columns):
        for cell in _column_cells(column, price_low, price_high):
            if (
                cell.executed > _ZERO
                and cell.last_qty == _ZERO
                and cell.side in swept
            ):
                swept[cell.side].setdefault(cell.price_bin, at)
    best_run = 0
    best_span = 0
    best_side = ""
    for side, hits in swept.items():
        bins = sorted(hits)
        run_start = 0
        for at in range(len(bins)):
            if at > 0 and bins[at] - bins[at - 1] != row_bin:
                run_start = at
            length = at - run_start + 1
            if length >= 2:
                span = (
                    max(hits[b] for b in bins[run_start: at + 1])
                    - min(hits[b] for b in bins[run_start: at + 1])
                    + 1
                )
            else:
                span = 1
            if length > best_run and span <= SWEEP_MAX_BUCKETS:
                best_run = length
                best_span = span
                best_side = side
    criteria = [
        _criterion(
            "baseline_samples",
            Decimal(baseline_samples),
            ">=",
            Decimal(BASELINE_MIN_SAMPLES),
        ),
        _criterion(
            "levels_broken",
            Decimal(best_run),
            ">=",
            Decimal(SWEEP_MIN_LEVELS),
        ),
        _criterion(
            "bucket_span",
            Decimal(best_span),
            "<=",
            Decimal(SWEEP_MAX_BUCKETS),
        ),
    ]
    met, confidence = _criteria_result(criteria, best_run > 0)
    return {
        "kind": "sweep",
        "label": JUDGMENT_LABELS["sweep"],
        "met": met,
        "confidence": confidence,
        "confidence_version": CONFIDENCE_VERSION,
        "criteria": criteria,
        "metrics": {
            "levels_broken": str(best_run),
            "bucket_span": str(best_span),
            "side": best_side,
            "min_levels": str(SWEEP_MIN_LEVELS),
            "max_buckets": str(SWEEP_MAX_BUCKETS),
            "baseline_samples": str(baseline_samples),
        },
    }


def _judge_vacuum(
    samples: Sequence[BandSample],
    population: Sequence[Decimal],
    baseline_samples: int,
) -> dict[str, object]:
    """流动性真空：带内挂量分位跌破阈值。"""
    if samples:
        low_depth = min(s.depth for s in samples)
        rank = percentile_rank(population, low_depth)
        criteria = [
            _criterion(
                "baseline_samples",
                Decimal(baseline_samples),
                ">=",
                Decimal(BASELINE_MIN_SAMPLES),
            ),
            _criterion(
                "depth_percentile", rank, "<=", VACUUM_DEPTH_PERCENTILE
            ),
        ]
        met, confidence = _criteria_result(criteria, True)
        depth_text = _text(low_depth)
        rank_text = _text(rank)
    else:
        met = False
        confidence = "0"
        depth_text = None
        rank_text = None
        criteria = []
    return {
        "kind": "liquidity_vacuum",
        "label": JUDGMENT_LABELS["liquidity_vacuum"],
        "met": met,
        "confidence": confidence,
        "confidence_version": CONFIDENCE_VERSION,
        "criteria": criteria,
        "metrics": {
            "min_depth": depth_text,
            "depth_percentile": rank_text,
            "threshold_percentile": _text(VACUUM_DEPTH_PERCENTILE),
            "baseline_samples": str(baseline_samples),
        },
    }


@dataclass(frozen=True, slots=True)
class AlertRule:
    """报警规则实例：四元组加带几何维度（6.8 节）。"""

    rule_id: str
    kind: str
    symbol: str
    overrides: Mapping[str, str]
    enabled: bool
    band_bp: str | None = None
    band_low: str | None = None
    band_high: str | None = None


def _rule_band_fields(
    row: Mapping[str, object]
) -> tuple[str | None, str | None, str | None]:
    """校验并提取规则带几何三字段。"""
    band_bp = row.get("band_bp")
    band_low = row.get("band_low")
    band_high = row.get("band_high")
    if band_bp is not None:
        if band_low is not None or band_high is not None:
            raise ValueError("规则带几何两模式互斥")
        if Decimal(str(band_bp)) <= _ZERO:
            raise ValueError("规则带宽须为正")
        return str(band_bp), None, None
    if (band_low is None) != (band_high is None):
        raise ValueError("显式价带须成对给出")
    if band_low is not None and band_high is not None:
        if Decimal(str(band_low)) > Decimal(str(band_high)):
            raise ValueError("显式价带倒置")
        return None, str(band_low), str(band_high)
    return None, None, None


def rule_band(
    rule: AlertRule,
    request_low: Decimal,
    request_high: Decimal,
    window_mid: Decimal | None,
) -> tuple[Decimal, Decimal, str] | None:
    """规则评估价带与来源；标准带无中价时不可评估。"""
    if rule.band_bp is not None:
        if window_mid is None or window_mid <= _ZERO:
            return None
        width = window_mid * Decimal(rule.band_bp) / Decimal("10000")
        return window_mid - width, window_mid + width, BAND_SOURCE_RULE_BP
    if rule.band_low is not None and rule.band_high is not None:
        return (
            Decimal(rule.band_low),
            Decimal(rule.band_high),
            BAND_SOURCE_EXPLICIT,
        )
    return request_low, request_high, BAND_SOURCE_REQUEST


def load_alert_rules(config_dir: Path) -> tuple[AlertRule, ...]:
    """读规则实例配置，文件缺失即空集。"""
    path = config_dir / ALERT_RULES_FILE_NAME
    if not path.exists():
        return ()
    loaded = json.loads(path.read_text(encoding="utf-8"))
    if not isinstance(loaded, dict):
        return ()
    rows = loaded.get("rules")
    if not isinstance(rows, list):
        return ()
    out: list[AlertRule] = []
    seen: set[str] = set()
    for row in rows:
        if not isinstance(row, Mapping):
            continue
        overrides_raw = row.get("overrides")
        overrides = (
            {str(k): str(v) for k, v in overrides_raw.items()}
            if isinstance(overrides_raw, Mapping)
            else {}
        )
        rule_id = str(row.get("rule_id", ""))
        kind = str(row.get("kind", ""))
        unknown = set(overrides) - {"min_confidence"}
        if not rule_id or rule_id in seen:
            raise ValueError("报警规则标识缺失或重复")
        if kind not in JUDGMENT_LABELS:
            raise ValueError("报警规则种类非法")
        if unknown:
            raise ValueError("报警规则含未实现覆盖项")
        floor = Decimal(overrides.get("min_confidence", "0"))
        if floor < _ZERO or floor > _ONE:
            raise ValueError("报警最低强度越界")
        band_bp, band_low, band_high = _rule_band_fields(row)
        seen.add(rule_id)
        out.append(
            AlertRule(
                rule_id=rule_id,
                kind=kind,
                symbol=str(row.get("symbol", "")),
                overrides=overrides,
                enabled=bool(row.get("enabled", False)),
                band_bp=band_bp,
                band_low=band_low,
                band_high=band_high,
            )
        )
    return tuple(out)


def match_rules(
    rules: Sequence[AlertRule],
    kind: str,
    symbol: str,
    confidence: Decimal,
) -> list[AlertRule]:
    """流上匹配：种类与品种一致且启用且过最低置信度。"""
    out: list[AlertRule] = []
    for rule in rules:
        if not rule.enabled or rule.kind != kind or rule.symbol != symbol:
            continue
        floor_text = rule.overrides.get(
            "min_confidence", str(ALERT_MIN_CONFIDENCE)
        )
        if confidence >= Decimal(floor_text):
            out.append(rule)
    return out

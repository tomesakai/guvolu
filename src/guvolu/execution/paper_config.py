"""paper 执行器与执行目标适配器共用的版本化配置（G-06）。

名义预算 risk_budget_jpy 来自本配置文件而非命令行缺省，装载时
校验不超过 T-11 绝对硬顶。金额一律 Decimal，配置内以字符串
承载（T-08、D-07）。
"""
from __future__ import annotations

import json
from collections.abc import Mapping
from dataclasses import dataclass
from datetime import timedelta
from decimal import Decimal, InvalidOperation
from pathlib import Path

from guvolu.domain.config import MAX_ORDER_JPY_CEILING
from guvolu.domain.errors import ConfigError
from guvolu.domain.symbols import SpotSymbol

PAPER_CONFIG_SCHEMA_VERSION = 1
# 仓库内缺省配置路径
DEFAULT_PAPER_CONFIG_PATH = Path("config") / "paper_executor.json"
# 执行目标 mode 字段的合法取值
TARGET_MODES = frozenset({"dry-run", "paper", "live"})
# 决策柱间隔的合法单位与秒数
_INTERVAL_UNITS: Mapping[str, int] = {
    "min": 60,
    "hour": 3600,
    "day": 86400,
    "week": 7 * 86400,
}


def bar_interval_duration(text: str) -> timedelta:
    """把 1hour、4hour、15min 一类的间隔文本解析为时长。"""
    for unit, seconds in _INTERVAL_UNITS.items():
        if text.endswith(unit):
            digits = text[: -len(unit)]
            if digits.isdigit() and int(digits) > 0:
                return timedelta(seconds=int(digits) * seconds)
    raise ConfigError(f"决策柱间隔不受支持: {text!r}")


@dataclass(frozen=True, slots=True)
class OverlayThresholds:
    """L2 覆盖层门控阈值，阶段一只记录不生效。"""

    limit: Decimal
    maximum_spread_bps: Decimal
    minimum_top5_depth_base: Decimal
    maximum_anchor_age_seconds: int


@dataclass(frozen=True, slots=True)
class PaperExecutorConfig:
    """paper 执行器配置。"""

    path: Path
    market_id: str
    symbol: SpotSymbol
    bar_interval: str
    risk_budget_jpy: Decimal
    no_trade_band: Decimal
    taker_fee_fallback_bps: Decimal
    taker_fee_cache_seconds: int
    overlay: OverlayThresholds
    ledger_directory: Path


def _text(payload: Mapping[str, object], key: str) -> str:
    value = payload.get(key)
    if not isinstance(value, str) or not value:
        raise ConfigError(f"paper 配置字段 {key} 缺失或非文本")
    return value


def _decimal(payload: Mapping[str, object], key: str) -> Decimal:
    """配置金额必须以字符串承载，直接进 Decimal（T-08）。"""
    raw = payload.get(key)
    if not isinstance(raw, str):
        raise ConfigError(f"paper 配置字段 {key} 必须为字符串数值")
    try:
        return Decimal(raw)
    except InvalidOperation as exc:
        raise ConfigError(f"paper 配置字段 {key} 不是合法数值") from exc


def _integer(payload: Mapping[str, object], key: str) -> int:
    raw = payload.get(key)
    if isinstance(raw, bool) or not isinstance(raw, int) or raw < 0:
        raise ConfigError(f"paper 配置字段 {key} 必须为非负整数")
    return raw


def ensure_budget_within_ceiling(budget: Decimal) -> Decimal:
    """预算必须为正且不超过 T-11 单笔硬顶。"""
    if budget <= 0:
        raise ConfigError("risk_budget_jpy 必须为正")
    if budget > MAX_ORDER_JPY_CEILING:
        raise ConfigError(
            f"risk_budget_jpy {budget} 超过硬顶 {MAX_ORDER_JPY_CEILING}"
        )
    return budget


def load_paper_config(path: Path) -> PaperExecutorConfig:
    """装载并校验 paper 配置。"""
    try:
        raw: object = json.loads(path.read_text(encoding="utf-8"))
    except FileNotFoundError as exc:
        raise ConfigError(f"paper 配置不存在: {path}") from exc
    except json.JSONDecodeError as exc:
        raise ConfigError(f"paper 配置不是合法 JSON: {path}") from exc
    if not isinstance(raw, dict):
        raise ConfigError("paper 配置根必须为对象")
    if raw.get("schema_version") != PAPER_CONFIG_SCHEMA_VERSION:
        raise ConfigError("paper 配置 schema_version 不受支持")
    overlay_raw = raw.get("overlay")
    if not isinstance(overlay_raw, dict):
        raise ConfigError("paper 配置缺少 overlay 阈值")
    bar_interval = _text(raw, "bar_interval")
    bar_interval_duration(bar_interval)
    no_trade_band = _decimal(raw, "no_trade_band")
    if no_trade_band < 0 or no_trade_band >= 1:
        raise ConfigError("no_trade_band 必须在 [0, 1) 内")
    limit = _decimal(overlay_raw, "limit")
    if limit < 0 or limit > 1:
        raise ConfigError("overlay.limit 必须在 [0, 1] 内")
    return PaperExecutorConfig(
        path=path,
        market_id=_text(raw, "market_id"),
        symbol=SpotSymbol(_text(raw, "symbol")),
        bar_interval=bar_interval,
        risk_budget_jpy=ensure_budget_within_ceiling(
            _decimal(raw, "risk_budget_jpy")
        ),
        no_trade_band=no_trade_band,
        taker_fee_fallback_bps=_decimal(raw, "taker_fee_fallback_bps"),
        taker_fee_cache_seconds=_integer(raw, "taker_fee_cache_seconds"),
        overlay=OverlayThresholds(
            limit=limit,
            maximum_spread_bps=_decimal(overlay_raw, "maximum_spread_bps"),
            minimum_top5_depth_base=_decimal(
                overlay_raw, "minimum_top5_depth_base"
            ),
            maximum_anchor_age_seconds=_integer(
                overlay_raw, "maximum_anchor_age_seconds"
            ),
        ),
        ledger_directory=Path(_text(raw, "ledger_directory")),
    )

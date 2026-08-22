"""paper 成交模型 B2：按盘口深度逐档估算 taker 成交与成本分解。

模型输入为发送时刻的盘口快照（顶档与深度）、决策参考价与 taker
费率；输出模型成交价、成交数量与成本分解（费率、半价差、冲击、
相对参考价的滑点），金额一律 Decimal（T-08）。本模块不做任何
网络调用；盘口来源与费率来源由调用方注入并在产物中标注。
"""
from __future__ import annotations

import json
import time
from collections.abc import Callable, Sequence
from dataclasses import dataclass
from datetime import UTC, datetime
from decimal import Decimal
from pathlib import Path
from typing import Protocol

from guvolu.data.durable_io import atomic_write_text
from guvolu.domain.enums import Side
from guvolu.domain.errors import GuvoluError
from guvolu.domain.models import Orderbook, SymbolRule
from guvolu.domain.symbols import SpotSymbol

# 一个基点对应的比例
BPS = Decimal("10000")
# 公开盘口端点的成交依据标注
PUBLIC_ORDERBOOK_BASIS = "public_orderbook_snapshot"
# 费率来源标注
FEE_SOURCE_SYMBOLS = "public_symbols_taker_fee"
FEE_SOURCE_CACHE = "public_symbols_taker_fee_cached"
FEE_SOURCE_FALLBACK = "config_fallback"
FEE_CACHE_SCHEMA_VERSION = 1


class FillModelError(GuvoluError):
    """成交模型输入非法。"""


@dataclass(frozen=True, slots=True)
class BookLevel:
    """盘口单档，价格与数量均为 Decimal。"""

    price: Decimal
    size: Decimal


@dataclass(frozen=True, slots=True)
class BookSnapshot:
    """发送时刻的盘口快照。bids 价格降序，asks 价格升序。"""

    symbol: SpotSymbol
    bids: tuple[BookLevel, ...]
    asks: tuple[BookLevel, ...]
    observed_at: datetime
    basis: str

    def __post_init__(self) -> None:
        if not self.bids or not self.asks:
            raise FillModelError("盘口快照至少须有一档买卖")
        if any(level.price <= 0 or level.size <= 0
               for level in (*self.bids, *self.asks)):
            raise FillModelError("盘口档位价格与数量必须为正")
        if any(self.bids[i].price <= self.bids[i + 1].price
               for i in range(len(self.bids) - 1)):
            raise FillModelError("买盘必须价格严格降序")
        if any(self.asks[i].price >= self.asks[i + 1].price
               for i in range(len(self.asks) - 1)):
            raise FillModelError("卖盘必须价格严格升序")
        if self.best_bid >= self.best_ask:
            raise FillModelError("盘口交叉，拒绝用于成交模型")
        if self.observed_at.tzinfo is None:
            raise FillModelError("盘口观测时刻必须带时区")

    @property
    def best_bid(self) -> Decimal:
        return self.bids[0].price

    @property
    def best_ask(self) -> Decimal:
        return self.asks[0].price

    @property
    def mid(self) -> Decimal:
        return (self.best_bid + self.best_ask) / 2

    def spread_bps(self) -> Decimal:
        """最优买卖价差，以中间价为基准的基点。"""
        return (self.best_ask - self.best_bid) / self.mid * BPS

    def depth_base(self, side: Side, levels: int) -> Decimal:
        """前 N 档挂量合计（基础货币）。"""
        book = self.bids if side is Side.BUY else self.asks
        return sum((level.size for level in book[:levels]), Decimal("0"))

    def top_imbalance(self) -> Decimal:
        """顶档不平衡 (bid - ask) / (bid + ask)，取值 [-1, 1]。"""
        bid = self.bids[0].size
        ask = self.asks[0].size
        return (bid - ask) / (bid + ask)

    @classmethod
    def from_orderbook(
        cls, book: Orderbook, *, observed_at: datetime, basis: str
    ) -> "BookSnapshot":
        """自公开端点板情報模型构造快照。"""
        return cls(
            symbol=SpotSymbol(book.symbol),
            bids=tuple(BookLevel(level.price, level.size) for level in book.bids),
            asks=tuple(BookLevel(level.price, level.size) for level in book.asks),
            observed_at=observed_at,
            basis=basis,
        )


def load_book_snapshot_file(path: Path, *, basis: str) -> BookSnapshot:
    """从公开端点响应格式的 JSON 文件装载盘口快照（测试夹具）。"""
    try:
        payload: object = json.loads(path.read_text(encoding="utf-8"))
    except FileNotFoundError as exc:
        raise FillModelError(f"盘口快照不存在: {path}") from exc
    except json.JSONDecodeError as exc:
        raise FillModelError(f"盘口快照不是合法 JSON: {path}") from exc
    if isinstance(payload, dict) and "data" in payload:
        payload = payload["data"]
    if not isinstance(payload, dict):
        raise FillModelError("盘口快照载荷必须为对象")
    observed_raw = payload.get("observed_at")
    observed_at = (
        datetime.fromisoformat(str(observed_raw))
        if isinstance(observed_raw, str)
        else datetime.fromtimestamp(path.stat().st_mtime, UTC)
    )
    return BookSnapshot.from_orderbook(
        Orderbook.from_api(payload), observed_at=observed_at, basis=basis,
    )


@dataclass(frozen=True, slots=True)
class FeeQuote:
    """taker 费率与其来源。"""

    bps: Decimal
    source: str
    fetched_at: datetime | None
    detail: str | None = None


class TakerFeeResolver:
    """运行时读取 GET /v1/symbols 的 takerFee，带缓存与降级。

    缓存以 JSON 文件保存，超过时效即重新拉取；拉取失败时退回
    配置费率并标注来源为 config_fallback，绝不静默伪造。
    """

    def __init__(
        self,
        cache_path: Path,
        *,
        fallback_bps: Decimal,
        cache_seconds: int,
    ) -> None:
        self._cache_path = cache_path
        self._fallback_bps = fallback_bps
        self._cache_seconds = cache_seconds

    def resolve(
        self,
        symbol: SpotSymbol,
        fetch: Callable[[], Sequence[SymbolRule]],
    ) -> FeeQuote:
        """取费率：缓存命中优先，其次拉取，最后降级。"""
        cached = self._read_cache(symbol)
        if cached is not None:
            return cached
        try:
            rules = fetch()
        except Exception as exc:  # noqa: BLE001
            return self._fallback(f"拉取失败: {exc}")
        for rule in rules:
            if rule.symbol == str(symbol):
                quote = FeeQuote(
                    bps=rule.taker_fee * BPS,
                    source=FEE_SOURCE_SYMBOLS,
                    fetched_at=datetime.now(UTC),
                )
                self._write_cache(symbol, quote)
                return quote
        return self._fallback("品种缺失")

    def _fallback(self, detail: str) -> FeeQuote:
        """退回配置费率并保留降级原因，不静默伪造。"""
        return FeeQuote(
            bps=self._fallback_bps,
            source=FEE_SOURCE_FALLBACK,
            fetched_at=None,
            detail=detail,
        )

    def _read_cache(self, symbol: SpotSymbol) -> FeeQuote | None:
        if not self._cache_path.is_file():
            return None
        try:
            payload: object = json.loads(
                self._cache_path.read_text(encoding="utf-8")
            )
        except (OSError, json.JSONDecodeError):
            return None
        if not isinstance(payload, dict):
            return None
        entry = payload.get(str(symbol))
        if not isinstance(entry, dict):
            return None
        fetched_raw = entry.get("fetched_at")
        bps_raw = entry.get("taker_fee_bps")
        if not isinstance(fetched_raw, str) or not isinstance(bps_raw, str):
            return None
        fetched_at = datetime.fromisoformat(fetched_raw)
        age = (datetime.now(UTC) - fetched_at).total_seconds()
        if age < 0 or age > self._cache_seconds:
            return None
        return FeeQuote(
            bps=Decimal(bps_raw), source=FEE_SOURCE_CACHE, fetched_at=fetched_at,
        )

    def _write_cache(self, symbol: SpotSymbol, quote: FeeQuote) -> None:
        payload: dict[str, object] = {}
        if self._cache_path.is_file():
            try:
                existing: object = json.loads(
                    self._cache_path.read_text(encoding="utf-8")
                )
            except (OSError, json.JSONDecodeError):
                existing = None
            if isinstance(existing, dict):
                payload = dict(existing)
        payload["schema_version"] = FEE_CACHE_SCHEMA_VERSION
        payload[str(symbol)] = {
            "taker_fee_bps": format(quote.bps, "f"),
            "fetched_at": (
                quote.fetched_at.isoformat()
                if quote.fetched_at is not None
                else datetime.now(UTC).isoformat()
            ),
            "written_ns": time.time_ns(),
        }
        atomic_write_text(
            self._cache_path,
            json.dumps(payload, ensure_ascii=False, sort_keys=True) + "\n",
        )


@dataclass(frozen=True, slots=True)
class FillEstimate:
    """模型成交与成本分解，金额 Decimal（T-08）。"""

    side: Side
    fill_size: Decimal
    expected_price: Decimal
    model_fill_price: Decimal
    notional_jpy: Decimal
    fee_jpy: Decimal
    levels_consumed: int
    fee_bps: Decimal
    half_spread_bps: Decimal
    impact_bps: Decimal
    slippage_vs_reference_bps: Decimal
    total_cost_bps: Decimal
    fill_basis: str
    fee_source: str
    book_observed_at: datetime

    def as_evidence(self) -> dict[str, str]:
        """转为账本迁移证据键值表（D-07）。"""
        return {
            "fill_basis": self.fill_basis,
            "fee_source": self.fee_source,
            "fill_size": format(self.fill_size, "f"),
            "expected_price": format(self.expected_price, "f"),
            "model_fill_price": format(self.model_fill_price, "f"),
            "notional_jpy": format(self.notional_jpy, "f"),
            "fee_jpy": format(self.fee_jpy, "f"),
            "fee_bps": format(self.fee_bps, "f"),
            "half_spread_bps": format(self.half_spread_bps, "f"),
            "impact_bps": format(self.impact_bps, "f"),
            "slippage_vs_reference_bps": format(
                self.slippage_vs_reference_bps, "f"
            ),
            "total_cost_bps": format(self.total_cost_bps, "f"),
            "levels_consumed": str(self.levels_consumed),
            "book_observed_at": self.book_observed_at.isoformat(),
        }

    def fill_record(self) -> dict[str, object]:
        """转为差异账中的模型成交记录。"""
        return {
            "side": self.side.value,
            "fill_size": format(self.fill_size, "f"),
            "expected_price": format(self.expected_price, "f"),
            "model_fill_price": format(self.model_fill_price, "f"),
            "notional_jpy": format(self.notional_jpy, "f"),
            "fee_jpy": format(self.fee_jpy, "f"),
            "levels_consumed": self.levels_consumed,
            "fill_basis": self.fill_basis,
            "fee_source": self.fee_source,
            "book_observed_at": self.book_observed_at.isoformat(),
        }

    def cost_record(self) -> dict[str, object]:
        """转为差异账中的成本分解记录，单位基点。"""
        return {
            "fee_bps": format(self.fee_bps, "f"),
            "half_spread_bps": format(self.half_spread_bps, "f"),
            "impact_bps": format(self.impact_bps, "f"),
            "slippage_vs_reference_bps": format(
                self.slippage_vs_reference_bps, "f"
            ),
            "total_cost_bps": format(self.total_cost_bps, "f"),
        }


class InsufficientDepth(GuvoluError):
    """盘口深度不足以完全成交。"""


def estimate_taker_fill(
    *,
    side: Side,
    size: Decimal,
    book: BookSnapshot,
    expected_price: Decimal,
    fee: FeeQuote,
) -> FillEstimate:
    """按深度逐档吃单，估算成交均价、冲击与成本分解。

    买入吃卖盘自最优卖价向上，卖出吃买盘自最优买价向下；深度不足
    时拒绝而非部分成交。半价差以中间价为基准；冲击为成交均价相对
    触价的不利偏移；相对参考价滑点为成交均价相对 expected_price 的
    不利偏移，三者均为基点且不利方向为正。
    """
    if size <= 0:
        raise FillModelError("成交数量必须为正")
    if expected_price <= 0:
        raise FillModelError("参考价必须为正")
    levels = book.asks if side is Side.BUY else book.bids
    remaining = size
    notional = Decimal("0")
    consumed = 0
    for level in levels:
        take = min(level.size, remaining)
        notional += take * level.price
        remaining -= take
        consumed += 1
        if remaining == 0:
            break
    if remaining > 0:
        raise InsufficientDepth(
            f"盘口 {len(levels)} 档深度不足，剩余 {remaining} 未成交"
        )
    fill_price = notional / size
    mid = book.mid
    touch = book.best_ask if side is Side.BUY else book.best_bid
    sign = Decimal("1") if side is Side.BUY else Decimal("-1")
    half_spread_bps = (book.best_ask - book.best_bid) / 2 / mid * BPS
    impact_bps = sign * (fill_price - touch) / mid * BPS
    slippage_bps = sign * (fill_price - expected_price) / expected_price * BPS
    fee_jpy = notional * fee.bps / BPS
    return FillEstimate(
        side=side,
        fill_size=size,
        expected_price=expected_price,
        model_fill_price=fill_price,
        notional_jpy=notional,
        fee_jpy=fee_jpy,
        levels_consumed=consumed,
        fee_bps=fee.bps,
        half_spread_bps=half_spread_bps,
        impact_bps=impact_bps,
        slippage_vs_reference_bps=slippage_bps,
        total_cost_bps=fee.bps + slippage_bps,
        fill_basis=book.basis,
        fee_source=fee.source,
        book_observed_at=book.observed_at,
    )


class BookSource(Protocol):
    """盘口来源抽象，返回发送时刻的快照。"""

    def snapshot(self, symbol: SpotSymbol) -> BookSnapshot: ...

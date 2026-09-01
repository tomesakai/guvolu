"""授权信封：live 自动交易的边界合同（执行链设计第 14 节）。

信封是维护者填值签发的内容寻址 JSON 文件，身份为文件字节的
SHA-256。本模块负责装载校验、用量与状态的持久追踪（重启不
重置）、以及各门禁的纯函数判定（C-02）。金额一律以字符串解析
进 Decimal（T-08、D-07）；单笔与当日上限不得超过 T-11 硬顶；
触界处置语义见设计文档第 14 节表格。本模块不触任何网络端点，
市场输入由调用方注入（C-13）。
"""
from __future__ import annotations

import hashlib
import json
from collections.abc import Mapping
from dataclasses import dataclass, replace
from datetime import UTC, date, datetime, timedelta
from decimal import Decimal, InvalidOperation
from enum import StrEnum
from pathlib import Path

from guvolu.data.durable_io import atomic_write_bytes, durable_append_bytes
from guvolu.data.paths import data_root
from guvolu.domain.config import (
    MAX_DAY_COUNT_CEILING,
    MAX_DAY_JPY_CEILING,
    MAX_ORDER_JPY_CEILING,
)
from guvolu.domain.enums import Side
from guvolu.domain.errors import ConfigError, SymbolError
from guvolu.domain.symbols import SpotSymbol
from guvolu.risk.circuit_breaker import BreakerThresholds
from guvolu.risk.limits import trading_day

# 信封缺省路径（C-04）
DEFAULT_ENVELOPE_PATH = Path("config") / "authorization_envelope.json"
ENVELOPE_SCHEMA_VERSION = 1
# 状态与用量在数据根下的目录
ENVELOPE_STATE_RELATIVE_DIR = Path("execution") / "envelope"
USAGE_SCHEMA_VERSION = 1
STATE_SCHEMA_VERSION = 1
# 参考价历史保留窗口秒数
PRICE_HISTORY_WINDOW_SECONDS = 600
# 基点换算分母
_BP = Decimal("10000")

# 门禁判定的裁决语义
VERDICT_ALLOW = "allow"
VERDICT_SKIP = "skip"
VERDICT_PAUSE = "pause"
VERDICT_HALT = "halt"
VERDICT_TRIP = "trip"
VERDICT_REJECT = "reject"


class EnvelopeError(ConfigError):
    """信封装载、校验或状态文件非法。"""


class OnTrip(StrEnum):
    """熔断动作登记（第 14 节 on_trip）。"""

    CANCEL_ONLY = "cancel_only"
    CANCEL_AND_FLATTEN = "cancel_and_flatten"


@dataclass(frozen=True, slots=True)
class PriceMovePause:
    """参考价急变暂停参数。"""

    window_seconds: int
    threshold_bp: Decimal
    pause_seconds: int


@dataclass(frozen=True, slots=True)
class MarketRisk:
    """市场风险门参数。"""

    price_move_pause: PriceMovePause
    spread_skip_bp: Decimal
    min_book_depth_ratio: Decimal
    stream_gap_seconds: int


@dataclass(frozen=True, slots=True)
class OpsBreaker:
    """运行熔断参数（R-02）。"""

    consecutive_failure_limit: int
    asset_deviation_ratio: Decimal
    asset_deviation_floor_jpy: Decimal


@dataclass(frozen=True, slots=True)
class AuthorizationEnvelope:
    """已签发信封的全字段视图，身份为文件字节 SHA-256。"""

    path: Path
    sha256: str
    schema_version: int
    issued_at: datetime
    valid_from: datetime
    valid_until: datetime
    symbols: frozenset[SpotSymbol]
    order_jpy_max: Decimal
    day_jpy_max: Decimal
    day_count_max: int
    envelope_jpy_total: Decimal
    max_position_jpy: Decimal
    max_cumulative_loss_jpy: Decimal
    day_loss_jpy_max: Decimal
    canary_first_order_jpy_max: Decimal
    max_prediction_age_minutes: int
    market_risk: MarketRisk
    ops_breaker: OpsBreaker
    on_trip: OnTrip

    @property
    def sha12(self) -> str:
        """状态与用量文件名使用的短身份。"""
        return self.sha256[:12]

    def breaker_thresholds(self) -> BreakerThresholds:
        """信封字段折算的熔断阈值（R-02、G-06）。"""
        return BreakerThresholds(
            schema_version=self.schema_version,
            consecutive_failure_limit=(
                self.ops_breaker.consecutive_failure_limit
            ),
            stream_gap_seconds=self.market_risk.stream_gap_seconds,
            asset_deviation_ratio=self.ops_breaker.asset_deviation_ratio,
            asset_deviation_floor_jpy=(
                self.ops_breaker.asset_deviation_floor_jpy
            ),
        )


_TOP_FIELDS = frozenset({
    "schema_version", "issued_at", "valid_from", "valid_until", "symbols",
    "order_jpy_max", "day_jpy_max", "day_count_max", "envelope_jpy_total",
    "max_position_jpy", "max_cumulative_loss_jpy", "day_loss_jpy_max",
    "canary_first_order_jpy_max", "max_prediction_age_minutes",
    "market_risk", "ops_breaker", "on_trip",
})
_MARKET_RISK_FIELDS = frozenset({
    "price_move_pause", "spread_skip_bp", "min_book_depth_ratio",
    "stream_gap_seconds",
})
_PAUSE_FIELDS = frozenset({"window_seconds", "threshold_bp", "pause_seconds"})
_OPS_FIELDS = frozenset({
    "consecutive_failure_limit", "asset_deviation_ratio",
    "asset_deviation_floor_jpy",
})


def _mapping(payload: Mapping[str, object], key: str) -> Mapping[str, object]:
    value = payload.get(key)
    if not isinstance(value, Mapping):
        raise EnvelopeError(f"信封字段 {key} 缺失或非对象")
    return {str(name): item for name, item in value.items()}


def _positive_int(payload: Mapping[str, object], key: str) -> int:
    value = payload.get(key)
    if isinstance(value, bool) or not isinstance(value, int) or value <= 0:
        raise EnvelopeError(f"信封字段 {key} 必须为正整数")
    return value


def _positive_decimal(payload: Mapping[str, object], key: str) -> Decimal:
    """金额与比例只接受字符串承载（T-08、D-07）。"""
    raw = payload.get(key)
    if not isinstance(raw, str):
        raise EnvelopeError(f"信封字段 {key} 必须为字符串数值")
    try:
        value = Decimal(raw)
    except InvalidOperation as exc:
        raise EnvelopeError(f"信封字段 {key} 不是合法数值") from exc
    if not value.is_finite() or value <= 0:
        raise EnvelopeError(f"信封字段 {key} 必须为正")
    return value


def _aware_utc(payload: Mapping[str, object], key: str) -> datetime:
    raw = payload.get(key)
    if not isinstance(raw, str) or not raw:
        raise EnvelopeError(f"信封字段 {key} 缺失或非文本")
    try:
        parsed = datetime.fromisoformat(raw.replace("Z", "+00:00"))
    except ValueError as exc:
        raise EnvelopeError(f"信封字段 {key} 不是合法时刻") from exc
    if parsed.tzinfo is None or parsed.utcoffset() is None:
        raise EnvelopeError(f"信封字段 {key} 缺少时区")
    return parsed.astimezone(UTC)


def load_envelope(
    path: Path, *, whitelist: frozenset[SpotSymbol]
) -> AuthorizationEnvelope:
    """装载并全字段校验已签发信封，任一失败抛配置错误。

    校验项：字段集合恰为合同全集；schema_version 受支持；有效期
    为 UTC 且起点早于终点；symbols 非空且为 T-09 现物白名单子集；
    单笔与当日上限不超过 T-11 硬顶常量；各金额与阈值为正；
    on_trip 在登记集合内。身份为文件字节 SHA-256。
    """
    try:
        raw_bytes = path.read_bytes()
    except FileNotFoundError as exc:
        raise EnvelopeError(f"信封不存在: {path}") from exc
    except OSError as exc:
        raise EnvelopeError(f"信封不可读取: {path}") from exc
    sha256 = hashlib.sha256(raw_bytes).hexdigest()
    try:
        payload: object = json.loads(raw_bytes.decode("utf-8"))
    except (UnicodeDecodeError, json.JSONDecodeError) as exc:
        raise EnvelopeError(f"信封不是合法 JSON: {path}") from exc
    if not isinstance(payload, dict):
        raise EnvelopeError("信封根必须是对象")
    body: Mapping[str, object] = {
        str(key): value for key, value in payload.items()
    }
    if set(body) != _TOP_FIELDS:
        raise EnvelopeError(
            f"信封字段集合不符: 多 {sorted(set(body) - _TOP_FIELDS)}"
            f" 缺 {sorted(_TOP_FIELDS - set(body))}"
        )
    if body.get("schema_version") != ENVELOPE_SCHEMA_VERSION:
        raise EnvelopeError("信封 schema_version 不受支持")
    issued_at = _aware_utc(body, "issued_at")
    valid_from = _aware_utc(body, "valid_from")
    valid_until = _aware_utc(body, "valid_until")
    if valid_from >= valid_until:
        raise EnvelopeError("信封有效期起点必须早于终点")
    raw_symbols = body.get("symbols")
    if not isinstance(raw_symbols, list) or not raw_symbols:
        raise EnvelopeError("信封 symbols 必须为非空列表")
    symbols: set[SpotSymbol] = set()
    for item in raw_symbols:
        if not isinstance(item, str):
            raise EnvelopeError("信封 symbols 含非文本项")
        try:
            symbol = SpotSymbol(item)
        except SymbolError as exc:
            raise EnvelopeError(f"信封品种非现物形态: {item!r}") from exc
        if symbol not in whitelist:
            raise EnvelopeError(f"信封品种 {symbol} 不在现物白名单")
        if symbol in symbols:
            raise EnvelopeError(f"信封品种 {symbol} 重复")
        symbols.add(symbol)
    order_jpy_max = _positive_decimal(body, "order_jpy_max")
    day_jpy_max = _positive_decimal(body, "day_jpy_max")
    day_count_max = _positive_int(body, "day_count_max")
    if order_jpy_max > MAX_ORDER_JPY_CEILING:
        raise EnvelopeError(
            f"order_jpy_max 超过 T-11 硬顶 {MAX_ORDER_JPY_CEILING}"
        )
    if day_jpy_max > MAX_DAY_JPY_CEILING:
        raise EnvelopeError(
            f"day_jpy_max 超过 T-11 硬顶 {MAX_DAY_JPY_CEILING}"
        )
    if day_count_max > MAX_DAY_COUNT_CEILING:
        raise EnvelopeError(
            f"day_count_max 超过 T-11 硬顶 {MAX_DAY_COUNT_CEILING}"
        )
    market_body = _mapping(body, "market_risk")
    if set(market_body) != _MARKET_RISK_FIELDS:
        raise EnvelopeError("market_risk 字段集合不符")
    pause_body = _mapping(market_body, "price_move_pause")
    if set(pause_body) != _PAUSE_FIELDS:
        raise EnvelopeError("price_move_pause 字段集合不符")
    ops_body = _mapping(body, "ops_breaker")
    if set(ops_body) != _OPS_FIELDS:
        raise EnvelopeError("ops_breaker 字段集合不符")
    on_trip_raw = body.get("on_trip")
    if not isinstance(on_trip_raw, str):
        raise EnvelopeError("信封 on_trip 缺失或非文本")
    try:
        on_trip = OnTrip(on_trip_raw)
    except ValueError as exc:
        raise EnvelopeError(f"信封 on_trip 非法: {on_trip_raw!r}") from exc
    return AuthorizationEnvelope(
        path=path,
        sha256=sha256,
        schema_version=ENVELOPE_SCHEMA_VERSION,
        issued_at=issued_at,
        valid_from=valid_from,
        valid_until=valid_until,
        symbols=frozenset(symbols),
        order_jpy_max=order_jpy_max,
        day_jpy_max=day_jpy_max,
        day_count_max=day_count_max,
        envelope_jpy_total=_positive_decimal(body, "envelope_jpy_total"),
        max_position_jpy=_positive_decimal(body, "max_position_jpy"),
        max_cumulative_loss_jpy=_positive_decimal(
            body, "max_cumulative_loss_jpy"
        ),
        day_loss_jpy_max=_positive_decimal(body, "day_loss_jpy_max"),
        canary_first_order_jpy_max=_positive_decimal(
            body, "canary_first_order_jpy_max"
        ),
        max_prediction_age_minutes=_positive_int(
            body, "max_prediction_age_minutes"
        ),
        market_risk=MarketRisk(
            price_move_pause=PriceMovePause(
                window_seconds=_positive_int(pause_body, "window_seconds"),
                threshold_bp=_positive_decimal(pause_body, "threshold_bp"),
                pause_seconds=_positive_int(pause_body, "pause_seconds"),
            ),
            spread_skip_bp=_positive_decimal(market_body, "spread_skip_bp"),
            min_book_depth_ratio=_positive_decimal(
                market_body, "min_book_depth_ratio"
            ),
            stream_gap_seconds=_positive_int(
                market_body, "stream_gap_seconds"
            ),
        ),
        ops_breaker=OpsBreaker(
            consecutive_failure_limit=_positive_int(
                ops_body, "consecutive_failure_limit"
            ),
            asset_deviation_ratio=_positive_decimal(
                ops_body, "asset_deviation_ratio"
            ),
            asset_deviation_floor_jpy=_positive_decimal(
                ops_body, "asset_deviation_floor_jpy"
            ),
        ),
        on_trip=on_trip,
    )


def envelope_state_directory(directory: Path | None = None) -> Path:
    """信封状态与用量目录，缺省在数据根下解析（C-04）。"""
    return (
        directory
        if directory is not None
        else data_root() / ENVELOPE_STATE_RELATIVE_DIR
    )


@dataclass(frozen=True, slots=True)
class UsageRow:
    """一笔消耗写预算的委托用量行。"""

    intent_id: str
    notional_jpy: Decimal
    at: datetime


class EnvelopeUsage:
    """追加式用量账：重放求信封总用量与当日用量（重启不重置）。"""

    def __init__(self, path: Path) -> None:
        self._path = path
        self._rows: list[UsageRow] = []
        self._load()

    @classmethod
    def for_envelope(
        cls,
        envelope: AuthorizationEnvelope,
        *,
        directory: Path | None = None,
    ) -> "EnvelopeUsage":
        """按信封身份打开用量账，文件名含 SHA 前十二位。"""
        base = envelope_state_directory(directory)
        return cls(base / f"usage-{envelope.sha12}.jsonl")

    @property
    def path(self) -> Path:
        return self._path

    @property
    def rows(self) -> tuple[UsageRow, ...]:
        return tuple(self._rows)

    def append(
        self, *, intent_id: str, notional_jpy: Decimal, at: datetime
    ) -> None:
        """追加一笔消耗写预算的委托用量并 fsync（R-07）。"""
        if not intent_id:
            raise EnvelopeError("用量行缺少 intent_id")
        if notional_jpy <= 0:
            raise EnvelopeError("用量行名义金额必须为正")
        if at.tzinfo is None:
            raise EnvelopeError("用量行时刻必须带时区")
        record = {
            "schema_version": USAGE_SCHEMA_VERSION,
            "record": "usage",
            "intent_id": intent_id,
            "notional_jpy": format(notional_jpy, "f"),
            "at": at.isoformat(),
        }
        line = json.dumps(record, ensure_ascii=False, separators=(",", ":"))
        durable_append_bytes(self._path, (line + "\n").encode("utf-8"))
        self._rows.append(UsageRow(intent_id, notional_jpy, at))

    def total_jpy(self) -> Decimal:
        """信封生命周期累计下单总额。"""
        return sum((row.notional_jpy for row in self._rows), Decimal("0"))

    def day_jpy(self, day: date) -> Decimal:
        """指定交易日累计下单额（JST 06:00 边界）。"""
        return sum(
            (
                row.notional_jpy
                for row in self._rows
                if trading_day(row.at) == day
            ),
            Decimal("0"),
        )

    def day_count(self, day: date) -> int:
        """指定交易日累计下单笔数。"""
        return sum(1 for row in self._rows if trading_day(row.at) == day)

    def _load(self) -> None:
        if not self._path.exists():
            return
        for number, line in enumerate(
            self._path.read_text(encoding="utf-8").splitlines(), 1
        ):
            if not line.strip():
                continue
            try:
                parsed: object = json.loads(line)
            except json.JSONDecodeError as exc:
                raise EnvelopeError(
                    f"用量账第 {number} 行不是合法 JSON"
                ) from exc
            if not isinstance(parsed, dict) or parsed.get("record") != "usage":
                raise EnvelopeError(f"用量账第 {number} 行不是用量行")
            intent_id = parsed.get("intent_id")
            notional_raw = parsed.get("notional_jpy")
            at_raw = parsed.get("at")
            if (
                not isinstance(intent_id, str)
                or not intent_id
                or not isinstance(notional_raw, str)
                or not isinstance(at_raw, str)
            ):
                raise EnvelopeError(f"用量账第 {number} 行字段非法")
            try:
                notional = Decimal(notional_raw)
                at = datetime.fromisoformat(at_raw)
            except (InvalidOperation, ValueError) as exc:
                raise EnvelopeError(
                    f"用量账第 {number} 行数值或时刻非法"
                ) from exc
            if notional <= 0 or at.tzinfo is None:
                raise EnvelopeError(f"用量账第 {number} 行取值非法")
            self._rows.append(UsageRow(intent_id, notional, at))


@dataclass(frozen=True, slots=True)
class ValuationBaseline:
    """资产估值快照：JPY 与 BTC 数量及当时参考价。"""

    at: datetime
    jpy_amount: Decimal
    btc_amount: Decimal
    reference_price: Decimal

    def value_jpy(self, reference_price: Decimal | None = None) -> Decimal:
        """按参考价折算估值，缺省用快照当时参考价。"""
        price = (
            reference_price
            if reference_price is not None
            else self.reference_price
        )
        return self.jpy_amount + self.btc_amount * price


@dataclass(frozen=True, slots=True)
class PriceObservation:
    """一次参考价观测。"""

    at: datetime
    price: Decimal


@dataclass(frozen=True, slots=True)
class EnvelopeState:
    """信封状态，原子覆写持久化（重启不重置）。"""

    first_order_cleared: bool = False
    loss_baseline: ValuationBaseline | None = None
    day_baseline: ValuationBaseline | None = None
    day_baseline_day: date | None = None
    price_history: tuple[PriceObservation, ...] = ()
    paused_until: datetime | None = None
    tripped_at: datetime | None = None
    trip_reason: str | None = None


def _baseline_payload(
    baseline: ValuationBaseline | None,
) -> dict[str, str] | None:
    if baseline is None:
        return None
    return {
        "at": baseline.at.isoformat(),
        "jpy_amount": format(baseline.jpy_amount, "f"),
        "btc_amount": format(baseline.btc_amount, "f"),
        "reference_price": format(baseline.reference_price, "f"),
    }


def _baseline_from(value: object, name: str) -> ValuationBaseline | None:
    if value is None:
        return None
    if not isinstance(value, Mapping):
        raise EnvelopeError(f"状态字段 {name} 非对象")
    try:
        return ValuationBaseline(
            at=datetime.fromisoformat(str(value["at"])),
            jpy_amount=Decimal(str(value["jpy_amount"])),
            btc_amount=Decimal(str(value["btc_amount"])),
            reference_price=Decimal(str(value["reference_price"])),
        )
    except (KeyError, ValueError, InvalidOperation) as exc:
        raise EnvelopeError(f"状态字段 {name} 取值非法") from exc


class EnvelopeStateStore:
    """信封状态文件的装载与原子覆写。"""

    def __init__(self, path: Path) -> None:
        self._path = path

    @classmethod
    def for_envelope(
        cls,
        envelope: AuthorizationEnvelope,
        *,
        directory: Path | None = None,
    ) -> "EnvelopeStateStore":
        """按信封身份打开状态文件，文件名含 SHA 前十二位。"""
        base = envelope_state_directory(directory)
        return cls(base / f"state-{envelope.sha12}.json")

    @property
    def path(self) -> Path:
        return self._path

    def load(self) -> EnvelopeState:
        """装载状态；文件缺失返回初始状态。"""
        if not self._path.exists():
            return EnvelopeState()
        try:
            parsed: object = json.loads(
                self._path.read_text(encoding="utf-8")
            )
        except (OSError, json.JSONDecodeError) as exc:
            raise EnvelopeError(f"状态文件非法: {self._path}") from exc
        if not isinstance(parsed, Mapping):
            raise EnvelopeError("状态文件根必须是对象")
        history_raw = parsed.get("price_history")
        history: list[PriceObservation] = []
        if history_raw is not None:
            if not isinstance(history_raw, list):
                raise EnvelopeError("状态字段 price_history 非列表")
            for item in history_raw:
                if not isinstance(item, Mapping):
                    raise EnvelopeError("price_history 项非对象")
                try:
                    history.append(PriceObservation(
                        at=datetime.fromisoformat(str(item["at"])),
                        price=Decimal(str(item["price"])),
                    ))
                except (KeyError, ValueError, InvalidOperation) as exc:
                    raise EnvelopeError("price_history 项非法") from exc
        paused_raw = parsed.get("paused_until")
        tripped_raw = parsed.get("tripped_at")
        day_raw = parsed.get("day_baseline_day")
        trip_reason = parsed.get("trip_reason")
        try:
            paused_until = (
                None
                if paused_raw is None
                else datetime.fromisoformat(str(paused_raw))
            )
            tripped_at = (
                None
                if tripped_raw is None
                else datetime.fromisoformat(str(tripped_raw))
            )
            day_baseline_day = (
                None if day_raw is None else date.fromisoformat(str(day_raw))
            )
        except ValueError as exc:
            raise EnvelopeError("状态时刻字段非法") from exc
        return EnvelopeState(
            first_order_cleared=bool(parsed.get("first_order_cleared", False)),
            loss_baseline=_baseline_from(
                parsed.get("loss_baseline"), "loss_baseline"
            ),
            day_baseline=_baseline_from(
                parsed.get("day_baseline"), "day_baseline"
            ),
            day_baseline_day=day_baseline_day,
            price_history=tuple(history),
            paused_until=paused_until,
            tripped_at=tripped_at,
            trip_reason=None if trip_reason is None else str(trip_reason),
        )

    def save(self, state: EnvelopeState) -> None:
        """原子覆写状态文件。"""
        payload: dict[str, object] = {
            "schema_version": STATE_SCHEMA_VERSION,
            "first_order_cleared": state.first_order_cleared,
            "loss_baseline": _baseline_payload(state.loss_baseline),
            "day_baseline": _baseline_payload(state.day_baseline),
            "day_baseline_day": (
                None
                if state.day_baseline_day is None
                else state.day_baseline_day.isoformat()
            ),
            "price_history": [
                {"at": row.at.isoformat(), "price": format(row.price, "f")}
                for row in state.price_history
            ],
            "paused_until": (
                None
                if state.paused_until is None
                else state.paused_until.isoformat()
            ),
            "tripped_at": (
                None
                if state.tripped_at is None
                else state.tripped_at.isoformat()
            ),
            "trip_reason": state.trip_reason,
        }
        body = json.dumps(
            payload, ensure_ascii=False, sort_keys=True, indent=2
        ) + "\n"
        atomic_write_bytes(self._path, body.encode("utf-8"))


def observe_price(
    state: EnvelopeState,
    *,
    price: Decimal,
    at: datetime,
    window_seconds: int = PRICE_HISTORY_WINDOW_SECONDS,
) -> EnvelopeState:
    """追加一次参考价观测并裁剪保留窗口（纯函数，C-02）。"""
    if price <= 0:
        raise EnvelopeError("参考价必须为正")
    horizon = at - timedelta(seconds=window_seconds)
    kept = tuple(
        row for row in state.price_history if row.at >= horizon
    ) + (PriceObservation(at=at, price=price),)
    return replace(state, price_history=kept)


def price_move_bp(
    history: tuple[PriceObservation, ...],
    *,
    now: datetime,
    window_seconds: int,
) -> Decimal | None:
    """窗口内参考价最大涨跌幅（基点）；观测不足返回空。"""
    horizon = now - timedelta(seconds=window_seconds)
    window = [row.price for row in history if row.at >= horizon]
    if len(window) < 2:
        return None
    low = min(window)
    high = max(window)
    if low <= 0:
        raise EnvelopeError("参考价历史含非正价格")
    return (high - low) / low * _BP


def apply_price_move_gate(
    envelope: AuthorizationEnvelope,
    state: EnvelopeState,
    *,
    now: datetime,
) -> tuple[EnvelopeState, Decimal | None]:
    """急变门：涨跌幅超阈即设定暂停截止（纯函数，C-02）。

    暂停期内拒发新单、允许撤单；届满自动解除，不需人工复位。
    返回新状态与本次测得的涨跌幅。
    """
    pause = envelope.market_risk.price_move_pause
    move = price_move_bp(
        state.price_history, now=now, window_seconds=pause.window_seconds
    )
    if move is not None and move > pause.threshold_bp:
        return (
            replace(
                state,
                paused_until=now + timedelta(seconds=pause.pause_seconds),
            ),
            move,
        )
    return state, move


@dataclass(frozen=True, slots=True)
class GateRecord:
    """单条门禁判定记录，报告义务的数据载体（A-03）。"""

    name: str
    passed: bool
    verdict: str
    detail: str


@dataclass(frozen=True, slots=True)
class EnvelopeDecision:
    """一轮门禁判定的裁决：首个未通过门决定语义。"""

    verdict: str
    gates: tuple[GateRecord, ...]
    reason: str | None


@dataclass(frozen=True, slots=True)
class GateInputs:
    """门禁判定输入。市场与委托输入由调用方注入（C-13）。

    order_notional_jpy 为空表示本轮无新委托，仅评估安全门。
    current_value_jpy 为当前估值（JPY amount 加 BTC 折算）；
    position_notional_jpy 为 BTC 持仓名义；opposite_depth_jpy
    为对手侧一档累计深度折 JPY；spread_bp 为盘口价差基点。
    """

    now: datetime
    price_observed_at: datetime
    used_total_jpy: Decimal
    day_used_jpy: Decimal
    day_order_count: int
    current_value_jpy: Decimal | None = None
    position_notional_jpy: Decimal | None = None
    order_side: Side | None = None
    order_notional_jpy: Decimal | None = None
    spread_bp: Decimal | None = None
    opposite_depth_jpy: Decimal | None = None
    decision_time: datetime | None = None


def _gate(
    name: str, passed: bool, verdict: str, detail: str
) -> GateRecord:
    return GateRecord(
        name=name,
        passed=passed,
        verdict=VERDICT_ALLOW if passed else verdict,
        detail=detail,
    )


def check_validity(
    envelope: AuthorizationEnvelope, now: datetime
) -> GateRecord:
    """有效期门：期外拒绝进入 live。"""
    inside = envelope.valid_from <= now < envelope.valid_until
    return _gate(
        "validity",
        inside,
        VERDICT_HALT,
        f"{envelope.valid_from.isoformat()} <= {now.isoformat()}"
        f" < {envelope.valid_until.isoformat()}",
    )


def check_trip_lock(state: EnvelopeState) -> GateRecord:
    """熔断锁定门：已触发的信封停机待人工复核。"""
    return _gate(
        "trip_lock",
        state.tripped_at is None,
        VERDICT_HALT,
        "未锁定" if state.tripped_at is None else (
            f"已于 {state.tripped_at.isoformat()} 熔断:"
            f" {state.trip_reason}"
        ),
    )


def check_stream_freshness(
    envelope: AuthorizationEnvelope,
    *,
    price_observed_at: datetime,
    now: datetime,
) -> GateRecord:
    """行情陈旧门：参考价观测超时距即熔断（R-02）。"""
    age = (now - price_observed_at).total_seconds()
    limit = envelope.market_risk.stream_gap_seconds
    return _gate(
        "stream_freshness",
        0 <= age <= limit,
        VERDICT_TRIP,
        f"参考价观测距今 {age:.1f} 秒，阈值 {limit} 秒",
    )


def check_pause(state: EnvelopeState, now: datetime) -> GateRecord:
    """急变暂停门：暂停期内拒发新单，允许撤单。"""
    active = state.paused_until is not None and now < state.paused_until
    return _gate(
        "price_move_pause",
        not active,
        VERDICT_PAUSE,
        "无暂停" if state.paused_until is None else (
            f"暂停截止 {state.paused_until.isoformat()}"
        ),
    )


def check_cumulative_loss(
    envelope: AuthorizationEnvelope,
    *,
    baseline: ValuationBaseline | None,
    current_value_jpy: Decimal | None,
) -> GateRecord:
    """累计亏损门：估值较基线回撤达阈值即熔断。"""
    if baseline is None or current_value_jpy is None:
        return _gate("cumulative_loss", True, VERDICT_TRIP, "基线未建立")
    loss = baseline.value_jpy() - current_value_jpy
    return _gate(
        "cumulative_loss",
        loss < envelope.max_cumulative_loss_jpy,
        VERDICT_TRIP,
        f"累计亏损 {loss} JPY，阈值 {envelope.max_cumulative_loss_jpy} JPY",
    )


def check_day_loss(
    envelope: AuthorizationEnvelope,
    *,
    baseline: ValuationBaseline | None,
    current_value_jpy: Decimal | None,
) -> GateRecord:
    """当日亏损门：达阈值当日停机。"""
    if baseline is None or current_value_jpy is None:
        return _gate("day_loss", True, VERDICT_HALT, "当日基线未建立")
    loss = baseline.value_jpy() - current_value_jpy
    return _gate(
        "day_loss",
        loss < envelope.day_loss_jpy_max,
        VERDICT_HALT,
        f"当日亏损 {loss} JPY，阈值 {envelope.day_loss_jpy_max} JPY",
    )


def check_envelope_total(
    envelope: AuthorizationEnvelope,
    *,
    used_total_jpy: Decimal,
    order_notional_jpy: Decimal | None,
) -> GateRecord:
    """信封总额门：耗尽或本单越额即停机复核。"""
    projected = used_total_jpy + (
        order_notional_jpy if order_notional_jpy is not None else Decimal("0")
    )
    exhausted = used_total_jpy >= envelope.envelope_jpy_total
    passed = not exhausted and projected <= envelope.envelope_jpy_total
    return _gate(
        "envelope_total",
        passed,
        VERDICT_HALT,
        f"已用 {used_total_jpy} JPY，本单后 {projected} JPY，"
        f"总额 {envelope.envelope_jpy_total} JPY",
    )


def check_day_budget(
    envelope: AuthorizationEnvelope,
    *,
    day_used_jpy: Decimal,
    day_order_count: int,
    order_notional_jpy: Decimal,
) -> GateRecord:
    """当日额与笔数门，与 LimitGate 并联不替代（T-11）。"""
    amount_ok = day_used_jpy + order_notional_jpy <= envelope.day_jpy_max
    count_ok = day_order_count + 1 <= envelope.day_count_max
    return _gate(
        "day_budget",
        amount_ok and count_ok,
        VERDICT_TRIP,
        f"当日已用 {day_used_jpy} JPY / {day_order_count} 笔，"
        f"本单 {order_notional_jpy} JPY，上限 {envelope.day_jpy_max}"
        f" JPY / {envelope.day_count_max} 笔",
    )


def check_order_max(
    envelope: AuthorizationEnvelope, *, order_notional_jpy: Decimal
) -> GateRecord:
    """单笔名义上限门。"""
    return _gate(
        "order_max",
        order_notional_jpy <= envelope.order_jpy_max,
        VERDICT_REJECT,
        f"本单 {order_notional_jpy} JPY，上限 {envelope.order_jpy_max} JPY",
    )


def check_first_order_canary(
    envelope: AuthorizationEnvelope,
    state: EnvelopeState,
    *,
    order_notional_jpy: Decimal,
) -> GateRecord:
    """首单 canary 门：首单终态对账通过前额外压额（T-12）。"""
    if state.first_order_cleared:
        return _gate("first_order_canary", True, VERDICT_REJECT, "首单已解除")
    return _gate(
        "first_order_canary",
        order_notional_jpy <= envelope.canary_first_order_jpy_max,
        VERDICT_REJECT,
        f"首单 {order_notional_jpy} JPY，canary 上限"
        f" {envelope.canary_first_order_jpy_max} JPY",
    )


def check_position_cap(
    envelope: AuthorizationEnvelope,
    *,
    order_side: Side,
    position_notional_jpy: Decimal | None,
    order_notional_jpy: Decimal,
) -> GateRecord:
    """持仓名义上限门：买入越顶拒绝加仓，卖出放行。"""
    if order_side is not Side.BUY:
        return _gate("position_cap", True, VERDICT_REJECT, "卖出不受限")
    if position_notional_jpy is None:
        return _gate(
            "position_cap", False, VERDICT_REJECT, "持仓名义不可得，拒绝加仓"
        )
    projected = position_notional_jpy + order_notional_jpy
    return _gate(
        "position_cap",
        projected <= envelope.max_position_jpy,
        VERDICT_REJECT,
        f"持仓 {position_notional_jpy} JPY，本单后 {projected} JPY，"
        f"上限 {envelope.max_position_jpy} JPY",
    )


def check_spread(
    envelope: AuthorizationEnvelope, *, spread_bp: Decimal | None
) -> GateRecord:
    """点差门：盘口价差超阈跳过本轮。"""
    if spread_bp is None:
        return _gate("spread", False, VERDICT_SKIP, "盘口不可得，跳过")
    return _gate(
        "spread",
        spread_bp <= envelope.market_risk.spread_skip_bp,
        VERDICT_SKIP,
        f"价差 {spread_bp} bp，阈值"
        f" {envelope.market_risk.spread_skip_bp} bp",
    )


def check_depth(
    envelope: AuthorizationEnvelope,
    *,
    opposite_depth_jpy: Decimal | None,
    order_notional_jpy: Decimal,
) -> GateRecord:
    """深度门：对手侧一档深度不足名义倍数跳过本轮。"""
    required = order_notional_jpy * envelope.market_risk.min_book_depth_ratio
    if opposite_depth_jpy is None:
        return _gate("book_depth", False, VERDICT_SKIP, "盘口不可得，跳过")
    return _gate(
        "book_depth",
        opposite_depth_jpy >= required,
        VERDICT_SKIP,
        f"对手侧一档 {opposite_depth_jpy} JPY，需 {required} JPY",
    )


def check_prediction_age(
    envelope: AuthorizationEnvelope,
    *,
    decision_time: datetime | None,
    now: datetime,
) -> GateRecord:
    """目标陈旧门：预测超龄跳过本轮。"""
    if decision_time is None:
        return _gate("prediction_age", True, VERDICT_SKIP, "无目标血缘时刻")
    age = now - decision_time
    limit = timedelta(minutes=envelope.max_prediction_age_minutes)
    return _gate(
        "prediction_age",
        timedelta(0) <= age <= limit,
        VERDICT_SKIP,
        f"预测年龄 {age.total_seconds():.1f} 秒，"
        f"上限 {envelope.max_prediction_age_minutes} 分钟",
    )


def evaluate_envelope_gates(
    envelope: AuthorizationEnvelope,
    state: EnvelopeState,
    inputs: GateInputs,
) -> tuple[EnvelopeDecision, EnvelopeState]:
    """按固定次序评估全部门禁并返回裁决与新状态（C-02）。

    次序：有效期、熔断锁定、行情陈旧、价格急变（含设定暂停）、
    累计亏损、当日亏损、信封总额、目标陈旧、当日额与笔数、单笔
    上限、首单 canary、持仓上限、点差、深度。首个未通过门的
    裁决语义生效；新委托相关门仅在本轮有委托时评估。
    """
    new_state, move = apply_price_move_gate(
        envelope, state, now=inputs.now
    )
    records: list[GateRecord] = [
        check_validity(envelope, inputs.now),
        check_trip_lock(new_state),
        check_stream_freshness(
            envelope,
            price_observed_at=inputs.price_observed_at,
            now=inputs.now,
        ),
        _gate(
            "price_move",
            new_state.paused_until == state.paused_until,
            VERDICT_PAUSE,
            f"窗口涨跌幅 {move} bp，阈值"
            f" {envelope.market_risk.price_move_pause.threshold_bp} bp",
        ),
        check_pause(new_state, inputs.now),
        check_cumulative_loss(
            envelope,
            baseline=new_state.loss_baseline,
            current_value_jpy=inputs.current_value_jpy,
        ),
        check_day_loss(
            envelope,
            baseline=new_state.day_baseline,
            current_value_jpy=inputs.current_value_jpy,
        ),
        check_envelope_total(
            envelope,
            used_total_jpy=inputs.used_total_jpy,
            order_notional_jpy=inputs.order_notional_jpy,
        ),
        check_prediction_age(
            envelope, decision_time=inputs.decision_time, now=inputs.now
        ),
    ]
    if inputs.order_notional_jpy is not None:
        if inputs.order_side is None:
            raise EnvelopeError("委托门禁缺少方向输入")
        records.extend([
            check_day_budget(
                envelope,
                day_used_jpy=inputs.day_used_jpy,
                day_order_count=inputs.day_order_count,
                order_notional_jpy=inputs.order_notional_jpy,
            ),
            check_order_max(
                envelope, order_notional_jpy=inputs.order_notional_jpy
            ),
            check_first_order_canary(
                envelope,
                new_state,
                order_notional_jpy=inputs.order_notional_jpy,
            ),
            check_position_cap(
                envelope,
                order_side=inputs.order_side,
                position_notional_jpy=inputs.position_notional_jpy,
                order_notional_jpy=inputs.order_notional_jpy,
            ),
            check_spread(envelope, spread_bp=inputs.spread_bp),
            check_depth(
                envelope,
                opposite_depth_jpy=inputs.opposite_depth_jpy,
                order_notional_jpy=inputs.order_notional_jpy,
            ),
        ])
    failed = next((row for row in records if not row.passed), None)
    if failed is None:
        decision = EnvelopeDecision(
            verdict=VERDICT_ALLOW, gates=tuple(records), reason=None
        )
    else:
        decision = EnvelopeDecision(
            verdict=failed.verdict,
            gates=tuple(records),
            reason=f"{failed.name}: {failed.detail}",
        )
    return decision, new_state


def gate_records_payload(
    decision: EnvelopeDecision,
) -> list[dict[str, object]]:
    """门禁判定的报告载荷（A-03）。"""
    return [
        {
            "name": row.name,
            "passed": row.passed,
            "verdict": row.verdict,
            "detail": row.detail,
        }
        for row in decision.gates
    ]

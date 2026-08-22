"""paper 执行器：执行目标到 paper 成交模型的零资金闭环（T-04、T-13）。

消费适配器产出的第 2 版执行目标快照，校验有效期、市场与品种、
目标域后，以本地 paper 持仓账为库存做差分转换（G-05），经全量
风控闸门链进入发送边界；发送边界由 paper 成交模型结算或拒绝，
绝不构造任何私有客户端，零写请求。每次决策在差异账追加一行，
含目标、差分意图、模型成交、成本分解与 L2 覆盖层门控记录。

命令行入口见 scripts/run_paper_executor.py。取引ルール、盘口快照
与服务状态可由文件参数离线给定；缺省经公开只读端点拉取。
"""
from __future__ import annotations

import argparse
import hashlib
import json
from collections.abc import Callable, Mapping, Sequence
from dataclasses import dataclass, field
from datetime import UTC, datetime
from decimal import Decimal, InvalidOperation
from pathlib import Path

from guvolu.api.public_client import PublicClient
from guvolu.data.durable_io import durable_append_bytes
from guvolu.data.intent_ledger import IntentLedger
from guvolu.data.paths import data_root
from guvolu.domain.config import MAX_ORDER_JPY_CEILING, load_config
from guvolu.domain.enums import ExecutionType, ServiceStatus, Side
from guvolu.domain.errors import GuvoluError, PaperRejected, PaperSettled
from guvolu.domain.ids import new_intent_id
from guvolu.domain.intent import (
    LOCAL_TERMINAL_STATES,
    IntentState,
    OrderIntent,
)
from guvolu.domain.models import SymbolRule
from guvolu.domain.symbols import SpotSymbol
from guvolu.execution.conversion import (
    DeltaDecision,
    MarketRule,
    convert_target_to_delta_order,
)
from guvolu.execution.dispatch import DispatchResult, dispatch_order_intent
from guvolu.execution.dry_run_executor import ORDER_ENDPOINT, load_market_rule
from guvolu.execution.frozen_target_adapter import TARGET_SEMANTICS
from guvolu.execution.paper_config import (
    DEFAULT_PAPER_CONFIG_PATH,
    OverlayThresholds,
    PaperExecutorConfig,
    load_paper_config,
)
from guvolu.execution.paper_fill_model import (
    PUBLIC_ORDERBOOK_BASIS,
    BookSnapshot,
    BookSource,
    FeeQuote,
    FillEstimate,
    FillModelError,
    InsufficientDepth,
    TakerFeeResolver,
    estimate_taker_fill,
    load_book_snapshot_file,
)
from guvolu.risk.circuit_breaker import (
    DEFAULT_THRESHOLDS_PATH,
    CircuitBreaker,
    load_breaker_thresholds,
)
from guvolu.risk.limits import LimitGate, trading_day
from guvolu.risk.service_gate import allows_new_intent

# 执行目标快照的最低可消费版本
MIN_TARGET_SCHEMA_VERSION = 2
# 执行器只消费 paper 模式目标
PAPER_MODE = "paper"
# 差异账、持仓账与认领账的版本
DIFFERENCE_LEDGER_SCHEMA_VERSION = 1
POSITION_LEDGER_SCHEMA_VERSION = 1
CLAIM_LEDGER_SCHEMA_VERSION = 1
# 账目目录下的文件名
INTENT_LEDGER_NAME = "intent_ledger.jsonl"
POSITION_LEDGER_NAME = "positions.jsonl"
DIFFERENCE_LEDGER_NAME = "difference_ledger.jsonl"
CLAIM_LEDGER_NAME = "prediction_claims.jsonl"
FEE_CACHE_NAME = "taker_fee_cache.json"
# 不生成意图的决策状态
DUPLICATE_PREDICTION = "duplicate_prediction"
NEEDS_RECONCILIATION = "needs_reconciliation"
# 启动恢复结清遗留发送的理由
PAPER_RECOVERY_REASON = "进程恢复，paper 发送中断，未触达写端点"
# 覆盖层门控输入不可得时的标注
GATE_UNAVAILABLE = "unavailable"
GATE_EVALUATED = "evaluated"
# 覆盖层深度门控取前五档
OVERLAY_DEPTH_LEVELS = 5
# 预期终点状态，进程返回零
EXPECTED_END_STATES = frozenset(
    {IntentState.PAPER_FILLED, IntentState.PAPER_REJECTED}
)
# 公开端点名，报告义务（A-03）
SYMBOLS_ENDPOINT = "GET /v1/symbols"
ORDERBOOKS_ENDPOINT = "GET /v1/orderbooks"
STATUS_ENDPOINT = "GET /v1/status"

SymbolRulesFetch = Callable[[], Sequence[SymbolRule]]


class PaperExecutorError(GuvoluError):
    """执行目标非法、与配置不符或账目损坏。"""


@dataclass(frozen=True, slots=True)
class ExecutionTarget:
    """第 2 版执行目标快照中执行器消费的字段。"""

    path: Path
    sha256: str
    schema_version: int
    prediction_id: str
    correlation_id: str
    decision_time: datetime
    valid_from: datetime
    valid_until: datetime
    valid_until_source: str
    market_id: str
    symbol: SpotSymbol
    exposure_target: float
    risk_budget_jpy: Decimal
    mode: str
    quality_eligible: bool
    lineage: Mapping[str, object]


def _text(payload: Mapping[str, object], key: str) -> str:
    value = payload.get(key)
    if not isinstance(value, str) or not value:
        raise PaperExecutorError(f"执行目标字段 {key} 缺失或非文本")
    return value


def _aware(payload: Mapping[str, object], key: str) -> datetime:
    try:
        parsed = datetime.fromisoformat(_text(payload, key))
    except ValueError as exc:
        raise PaperExecutorError(f"执行目标字段 {key} 非法时刻") from exc
    if parsed.tzinfo is None or parsed.utcoffset() is None:
        raise PaperExecutorError(f"执行目标字段 {key} 缺少时区")
    return parsed


def load_execution_target(path: Path) -> ExecutionTarget:
    """装载第 2 版执行目标快照并校验结构与目标域。"""
    try:
        raw_bytes = path.read_bytes()
    except FileNotFoundError as exc:
        raise PaperExecutorError(f"执行目标不存在: {path}") from exc
    try:
        payload: object = json.loads(raw_bytes.decode("utf-8"))
    except (UnicodeDecodeError, json.JSONDecodeError) as exc:
        raise PaperExecutorError(f"执行目标不是合法 JSON: {path}") from exc
    if not isinstance(payload, dict):
        raise PaperExecutorError("执行目标根必须为对象")
    version = payload.get("schema_version")
    if (
        isinstance(version, bool)
        or not isinstance(version, int)
        or version < MIN_TARGET_SCHEMA_VERSION
    ):
        raise PaperExecutorError(
            f"执行目标 schema_version 须不低于 {MIN_TARGET_SCHEMA_VERSION}"
        )
    semantics = payload.get("target_semantics")
    if not isinstance(semantics, Mapping) or any(
        semantics.get(key) != value for key, value in TARGET_SEMANTICS.items()
    ):
        raise PaperExecutorError("执行目标 target_semantics 与目标域不一致")
    exposure = payload.get("exposure_target")
    if isinstance(exposure, bool) or not isinstance(exposure, (int, float)):
        raise PaperExecutorError("执行目标 exposure_target 非数值")
    exposure_value = float(exposure)
    if not (0.0 <= exposure_value <= 1.0):
        raise PaperExecutorError(
            f"执行目标 exposure_target {exposure_value!r} 不在 [0, 1]"
        )
    try:
        budget = Decimal(_text(payload, "risk_budget_jpy"))
    except InvalidOperation as exc:
        raise PaperExecutorError("执行目标 risk_budget_jpy 非法") from exc
    if budget <= 0 or budget > MAX_ORDER_JPY_CEILING:
        raise PaperExecutorError(
            f"执行目标 risk_budget_jpy 必须在 (0, {MAX_ORDER_JPY_CEILING}] 内"
        )
    quality = payload.get("quality")
    if not isinstance(quality, Mapping) or not isinstance(
        quality.get("eligible"), bool
    ):
        raise PaperExecutorError("执行目标缺少质量合同 eligible 位")
    lineage = payload.get("lineage")
    if not isinstance(lineage, Mapping):
        raise PaperExecutorError("执行目标缺少 lineage")
    decision_time = _aware(payload, "decision_time")
    valid_from = _aware(payload, "valid_from")
    valid_until = _aware(payload, "valid_until")
    if valid_until <= valid_from:
        raise PaperExecutorError("执行目标 valid_until 不晚于 valid_from")
    return ExecutionTarget(
        path=path,
        sha256=hashlib.sha256(raw_bytes).hexdigest(),
        schema_version=version,
        prediction_id=_text(payload, "run_id"),
        correlation_id=_text(payload, "correlation_id"),
        decision_time=decision_time,
        valid_from=valid_from,
        valid_until=valid_until,
        valid_until_source=_text(payload, "valid_until_source"),
        market_id=_text(payload, "market_id"),
        symbol=SpotSymbol(_text(payload, "symbol")),
        exposure_target=exposure_value,
        risk_budget_jpy=budget,
        mode=_text(payload, "mode"),
        quality_eligible=bool(quality["eligible"]),
        lineage={str(key): value for key, value in lineage.items()},
    )


def validate_execution_target(
    target: ExecutionTarget,
    config: PaperExecutorConfig,
    *,
    now: datetime,
) -> None:
    """消费前义务：有效期、市场与品种一致、模式与预算上限。"""
    if target.mode != PAPER_MODE:
        raise PaperExecutorError(
            f"执行目标 mode {target.mode!r} 不是 {PAPER_MODE!r}"
        )
    if target.market_id != config.market_id:
        raise PaperExecutorError(
            f"执行目标 market_id {target.market_id!r} 与配置 "
            f"{config.market_id!r} 不一致"
        )
    if target.symbol != config.symbol:
        raise PaperExecutorError(
            f"执行目标 symbol {target.symbol} 与配置 {config.symbol} 不一致"
        )
    if now >= target.valid_until:
        raise PaperExecutorError(
            f"执行目标已越期: valid_until {target.valid_until.isoformat()}"
            f" 不晚于当前 {now.isoformat()}"
        )
    if now < target.valid_from:
        raise PaperExecutorError("执行目标尚未生效")
    if target.risk_budget_jpy > config.risk_budget_jpy:
        raise PaperExecutorError(
            f"执行目标 risk_budget_jpy {target.risk_budget_jpy} 超过配置 "
            f"{config.risk_budget_jpy}"
        )


class PaperPositionLedger:
    """paper 持仓账：追加式成交行，重放求库存。

    库存只来自本账中的模型成交，不来自任何交易所事实；与
    READ_ONLY 持仓无关，二者不得混用（T-03）。
    """

    def __init__(self, path: Path) -> None:
        self._path = path
        self._positions: dict[str, Decimal] = {}
        self._rows = 0
        self._load()

    @property
    def path(self) -> Path:
        return self._path

    @property
    def rows(self) -> int:
        """已记录的成交行数。"""
        return self._rows

    def position_size(self, symbol: SpotSymbol) -> Decimal:
        """取品种当前 paper 持仓数量。"""
        return self._positions.get(str(symbol), Decimal("0"))

    def record_fill(
        self,
        *,
        intent: OrderIntent,
        estimate: FillEstimate,
        at: datetime,
    ) -> Decimal:
        """追加一笔模型成交并返回成交后持仓。"""
        before = self.position_size(intent.symbol)
        signed = (
            estimate.fill_size
            if estimate.side is Side.BUY
            else -estimate.fill_size
        )
        after = before + signed
        if after < 0:
            raise PaperExecutorError(
                f"paper 持仓不得为负: {before} 卖出 {estimate.fill_size}"
            )
        record: dict[str, object] = {
            "schema_version": POSITION_LEDGER_SCHEMA_VERSION,
            "record": "paper_fill",
            "at": at.isoformat(),
            "intent_id": intent.intent_id,
            "correlation_id": intent.correlation_id,
            "prediction_id": intent.prediction_id,
            "symbol": str(intent.symbol),
            "side": estimate.side.value,
            "fill_size": format(estimate.fill_size, "f"),
            "model_fill_price": format(estimate.model_fill_price, "f"),
            "fee_jpy": format(estimate.fee_jpy, "f"),
            "position_before": format(before, "f"),
            "position_after": format(after, "f"),
        }
        _append_json_line(self._path, record)
        self._positions[str(intent.symbol)] = after
        self._rows += 1
        return after

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
                raise PaperExecutorError(
                    f"paper 持仓账第 {number} 行不是合法 JSON"
                ) from exc
            if not isinstance(parsed, dict):
                raise PaperExecutorError(f"paper 持仓账第 {number} 行不是对象")
            try:
                symbol = str(parsed["symbol"])
                side = Side(str(parsed["side"]))
                size = Decimal(str(parsed["fill_size"]))
                after = Decimal(str(parsed["position_after"]))
            except (KeyError, ValueError, InvalidOperation) as exc:
                raise PaperExecutorError(
                    f"paper 持仓账第 {number} 行字段非法"
                ) from exc
            before = self._positions.get(symbol, Decimal("0"))
            expected = before + (size if side is Side.BUY else -size)
            if expected != after or after < 0:
                raise PaperExecutorError(
                    f"paper 持仓账第 {number} 行持仓不连续"
                )
            self._positions[symbol] = after
            self._rows += 1


class DifferenceLedger:
    """差异账：每决策一行的追加式 JSONL。"""

    def __init__(self, path: Path) -> None:
        self._path = path
        self._prediction_ids: set[str] = set()
        self._load()

    @property
    def path(self) -> Path:
        return self._path

    def has_prediction(self, prediction_id: str) -> bool:
        """同一预测是否已有决策行，重跑不得重复追加意图。"""
        return prediction_id in self._prediction_ids

    def append(self, row: Mapping[str, object]) -> None:
        """追加一行决策记录。"""
        prediction_id = row.get("prediction_id")
        if not isinstance(prediction_id, str) or not prediction_id:
            raise PaperExecutorError("差异账行缺少 prediction_id")
        _append_json_line(self._path, dict(row))
        self._prediction_ids.add(prediction_id)

    def _load(self) -> None:
        for row in read_difference_rows(self._path):
            prediction_id = row.get("prediction_id")
            if isinstance(prediction_id, str):
                self._prediction_ids.add(prediction_id)


class PredictionClaims:
    """预测认领账：发送前先认领，重跑据此拒绝重复意图。

    追加式 JSONL，认领行先于意图账本与持仓账落盘。进程在认领
    与差异行之间中断时，重跑发现认领无差异行即报告待对账，
    不自动新增意图；认领行只增不改，同一预测至多一行。
    """

    def __init__(self, path: Path) -> None:
        self._path = path
        self._claims: dict[str, dict[str, object]] = {}
        self._load()

    @property
    def path(self) -> Path:
        return self._path

    def claim_of(self, prediction_id: str) -> Mapping[str, object] | None:
        """取预测的认领行；未认领返回空。"""
        return self._claims.get(prediction_id)

    def has_claim(self, prediction_id: str) -> bool:
        """预测是否已认领。"""
        return prediction_id in self._claims

    def claim(
        self, *, prediction_id: str, correlation_id: str, at: datetime
    ) -> Mapping[str, object]:
        """为预测追加一行认领并 fsync；已认领即拒绝。"""
        if not prediction_id:
            raise PaperExecutorError("认领缺少 prediction_id")
        if prediction_id in self._claims:
            raise PaperExecutorError(f"预测 {prediction_id} 已认领")
        record: dict[str, object] = {
            "schema_version": CLAIM_LEDGER_SCHEMA_VERSION,
            "record": "claim",
            "prediction_id": prediction_id,
            "correlation_id": correlation_id,
            "claimed_at": at.isoformat(),
        }
        _append_json_line(self._path, record)
        self._claims[prediction_id] = record
        return record

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
                raise PaperExecutorError(
                    f"认领账第 {number} 行不是合法 JSON"
                ) from exc
            if not isinstance(parsed, dict) or parsed.get("record") != "claim":
                raise PaperExecutorError(f"认领账第 {number} 行不是认领行")
            prediction_id = parsed.get("prediction_id")
            if not isinstance(prediction_id, str) or not prediction_id:
                raise PaperExecutorError(
                    f"认领账第 {number} 行缺少 prediction_id"
                )
            if prediction_id in self._claims:
                raise PaperExecutorError(
                    f"认领账第 {number} 行重复认领 {prediction_id}"
                )
            self._claims[prediction_id] = {
                str(key): value for key, value in parsed.items()
            }


def read_difference_rows(path: Path) -> list[dict[str, object]]:
    """读取差异账全部行；文件缺失视为空账。"""
    if not path.exists():
        return []
    rows: list[dict[str, object]] = []
    for number, line in enumerate(
        path.read_text(encoding="utf-8").splitlines(), 1
    ):
        if not line.strip():
            continue
        try:
            parsed: object = json.loads(line)
        except json.JSONDecodeError as exc:
            raise PaperExecutorError(
                f"差异账第 {number} 行不是合法 JSON"
            ) from exc
        if not isinstance(parsed, dict):
            raise PaperExecutorError(f"差异账第 {number} 行不是对象")
        rows.append({str(key): value for key, value in parsed.items()})
    return rows


def _append_json_line(path: Path, record: Mapping[str, object]) -> None:
    """追加一行并 fsync（R-07）。"""
    line = json.dumps(record, ensure_ascii=False, separators=(",", ":"))
    durable_append_bytes(path, (line + "\n").encode("utf-8"))


class StaticBookSource:
    """固定盘口快照来源，供测试夹具与离线回放。"""

    def __init__(self, snapshot: BookSnapshot) -> None:
        self._snapshot = snapshot

    def snapshot(self, symbol: SpotSymbol) -> BookSnapshot:
        if self._snapshot.symbol != symbol:
            raise FillModelError(
                f"盘口快照品种 {self._snapshot.symbol} 与 {symbol} 不符"
            )
        return self._snapshot


class PublicBookSource:
    """经公开端点 GET /v1/orderbooks 取发送时刻盘口（只读）。"""

    def __init__(
        self, public: PublicClient, read_touched: list[str]
    ) -> None:
        self._public = public
        self._read_touched = read_touched
        self._cached: BookSnapshot | None = None

    def snapshot(self, symbol: SpotSymbol) -> BookSnapshot:
        """同一进程内只拉取一次，决策参考与发送共用。"""
        if self._cached is not None and self._cached.symbol == symbol:
            return self._cached
        book = self._public.orderbooks(str(symbol))
        self._read_touched.append(ORDERBOOKS_ENDPOINT)
        self._cached = BookSnapshot.from_orderbook(
            book,
            observed_at=datetime.now(UTC),
            basis=PUBLIC_ORDERBOOK_BASIS,
        )
        return self._cached


class PaperFillSender:
    """paper 发送边界：以成交模型结算，绝不触达写端点（T-04）。

    send 只会抛出 PaperSettled 或 PaperRejected，永不返回委托号；
    本类不持有任何私有客户端或密钥（T-02、T-13）。
    """

    def __init__(
        self,
        *,
        book_source: BookSource,
        fee_resolver: TakerFeeResolver,
        fee_fetch: SymbolRulesFetch,
    ) -> None:
        self._book_source = book_source
        self._fee_resolver = fee_resolver
        self._fee_fetch = fee_fetch
        self.last_estimate: FillEstimate | None = None
        self.last_fee: FeeQuote | None = None
        self.last_book: BookSnapshot | None = None

    def send(self, intent: OrderIntent) -> int:
        if intent.price is None:
            raise PaperRejected("paper 成交模型只接受限价意图作参考价")
        try:
            book = self._book_source.snapshot(intent.symbol)
        except (FillModelError, GuvoluError) as exc:
            raise PaperRejected(f"盘口不可用: {exc}") from exc
        self.last_book = book
        fee = self._fee_resolver.resolve(intent.symbol, self._fee_fetch)
        self.last_fee = fee
        try:
            estimate = estimate_taker_fill(
                side=intent.side,
                size=intent.size,
                book=book,
                expected_price=intent.price,
                fee=fee,
            )
        except InsufficientDepth as exc:
            raise PaperRejected(str(exc)) from exc
        except FillModelError as exc:
            raise PaperRejected(f"成交模型输入非法: {exc}") from exc
        self.last_estimate = estimate
        raise PaperSettled(
            f"paper 成交模型结算 {estimate.side.value} {estimate.fill_size}",
            estimate.as_evidence(),
        )


def _gate(
    name: str,
    *,
    passed: bool | None,
    value: object,
    threshold: object,
) -> dict[str, object]:
    """单条门控记录；passed 为空表示输入不可得。"""
    return {
        "name": name,
        "status": GATE_EVALUATED if passed is not None else GATE_UNAVAILABLE,
        "passed": passed,
        "value": value,
        "threshold": threshold,
    }


def evaluate_overlay(
    book: BookSnapshot | None,
    *,
    quality_eligible: bool,
    service_status: ServiceStatus,
    thresholds: OverlayThresholds,
    anchor_age_seconds: int | None,
) -> dict[str, object]:
    """L2 覆盖层门控：阶段一只记录判定，不改目标。

    乘子候选以顶档不平衡为基础，幅度夹在 ±limit；任一门控输入
    不可得即标注 unavailable，would_apply 不成立，不臆造。
    """
    gates: list[dict[str, object]] = [
        _gate(
            "quality_eligible",
            passed=quality_eligible,
            value=quality_eligible,
            threshold=True,
        ),
        _gate(
            "service_status",
            passed=allows_new_intent(service_status),
            value=service_status.value,
            threshold=ServiceStatus.OPEN.value,
        ),
        _gate(
            "rest_anchor_age_seconds",
            passed=(
                None
                if anchor_age_seconds is None
                else anchor_age_seconds <= thresholds.maximum_anchor_age_seconds
            ),
            value=anchor_age_seconds,
            threshold=thresholds.maximum_anchor_age_seconds,
        ),
    ]
    multiplier: Decimal | None = None
    if book is None:
        gates.append(_gate(
            "best_spread_bps", passed=None, value=None,
            threshold=format(thresholds.maximum_spread_bps, "f"),
        ))
        gates.append(_gate(
            "top5_depth_base", passed=None, value=None,
            threshold=format(thresholds.minimum_top5_depth_base, "f"),
        ))
        imbalance_text: str | None = None
    else:
        spread = book.spread_bps()
        bid_depth = book.depth_base(Side.BUY, OVERLAY_DEPTH_LEVELS)
        ask_depth = book.depth_base(Side.SELL, OVERLAY_DEPTH_LEVELS)
        depth = min(bid_depth, ask_depth)
        imbalance = book.top_imbalance()
        limit = thresholds.limit
        multiplier = max(-limit, min(limit, imbalance * limit))
        imbalance_text = format(imbalance, "f")
        gates.append(_gate(
            "best_spread_bps",
            passed=spread <= thresholds.maximum_spread_bps,
            value=format(spread, "f"),
            threshold=format(thresholds.maximum_spread_bps, "f"),
        ))
        gates.append(_gate(
            "top5_depth_base",
            passed=depth >= thresholds.minimum_top5_depth_base,
            value={
                "bid": format(bid_depth, "f"),
                "ask": format(ask_depth, "f"),
            },
            threshold=format(thresholds.minimum_top5_depth_base, "f"),
        ))
    complete = all(gate["passed"] is not None for gate in gates)
    would_apply = complete and all(gate["passed"] is True for gate in gates)
    return {
        "applied": False,
        "would_apply": would_apply,
        "complete": complete,
        "value": None if multiplier is None else format(multiplier, "f"),
        "multiplier": None if multiplier is None else format(multiplier, "f"),
        "top_imbalance": imbalance_text,
        "limit": format(thresholds.limit, "f"),
        "gates": gates,
    }


@dataclass(frozen=True, slots=True)
class PaperDecisionOutcome:
    """一次 paper 决策的全部结果，报告义务的数据载体（A-03）。"""

    target: ExecutionTarget
    status: str
    reference_price: Decimal | None
    position_before: Decimal
    position_after: Decimal
    delta: DeltaDecision | None
    intent: OrderIntent | None
    dispatch: DispatchResult | None
    estimate: FillEstimate | None
    fee: FeeQuote | None
    overlay: Mapping[str, object]
    read_touched: tuple[str, ...]
    write_touched: tuple[str, ...]
    difference_row: Mapping[str, object] = field(default_factory=dict)
    reconciliation: Mapping[str, object] | None = None


@dataclass(frozen=True, slots=True)
class PaperRuntime:
    """一次运行的可注入依赖集合。"""

    config: PaperExecutorConfig
    rule: MarketRule
    book_source: BookSource
    fee_resolver: TakerFeeResolver
    fee_fetch: SymbolRulesFetch
    service_status: ServiceStatus
    ledger_directory: Path
    limit_gate: LimitGate
    breaker: CircuitBreaker
    whitelist: frozenset[SpotSymbol]
    anchor_age_seconds: int | None = None
    read_touched: list[str] = field(default_factory=list)

    @property
    def intent_ledger_path(self) -> Path:
        return self.ledger_directory / INTENT_LEDGER_NAME

    @property
    def position_ledger_path(self) -> Path:
        return self.ledger_directory / POSITION_LEDGER_NAME

    @property
    def difference_ledger_path(self) -> Path:
        return self.ledger_directory / DIFFERENCE_LEDGER_NAME

    @property
    def claim_ledger_path(self) -> Path:
        return self.ledger_directory / CLAIM_LEDGER_NAME


def _delta_record(delta: DeltaDecision | None) -> dict[str, object] | None:
    if delta is None:
        return None
    proposal: dict[str, str] | None = None
    if delta.proposal is not None:
        proposal = {
            "symbol": str(delta.proposal.symbol),
            "side": delta.proposal.side.value,
            "size": format(delta.proposal.size, "f"),
            "price": format(delta.proposal.price, "f"),
            "notional_jpy": format(delta.proposal.notional_jpy, "f"),
        }
    return {
        "desired_size": format(delta.desired_size, "f"),
        "position_size": format(delta.position_size, "f"),
        "delta_size": format(delta.delta_size, "f"),
        "proposal": proposal,
        "skip_reason": delta.skip_reason,
    }


def run_paper_decision(
    target: ExecutionTarget,
    runtime: PaperRuntime,
    *,
    moment: datetime | None = None,
) -> PaperDecisionOutcome:
    """执行一次 paper 决策并在差异账追加一行。

    流程：校验目标、同预测去重、取 paper 库存、差分转换、
    发送前认领预测、意图携带血缘经闸门链到 paper 发送边界、
    结算后更新持仓账、记录覆盖层门控与差异行。

    去重判定分两层：差异账已有该预测的决策行即为重复；差异账
    无行但认领账或意图账本已有该预测，说明上次运行在认领与
    差异行之间中断，报告待对账并拒绝自动重跑，不新增意图。
    """
    now = moment if moment is not None else datetime.now(UTC)
    config = runtime.config
    validate_execution_target(target, config, now=now)
    runtime.ledger_directory.mkdir(parents=True, exist_ok=True)
    difference = DifferenceLedger(runtime.difference_ledger_path)
    claims = PredictionClaims(runtime.claim_ledger_path)
    ledger = IntentLedger(runtime.intent_ledger_path)
    positions = PaperPositionLedger(runtime.position_ledger_path)
    position_before = positions.position_size(target.symbol)
    read_before = len(runtime.read_touched)

    def without_intent(
        status: str, reconciliation: Mapping[str, object] | None = None
    ) -> PaperDecisionOutcome:
        return PaperDecisionOutcome(
            target=target,
            status=status,
            reference_price=None,
            position_before=position_before,
            position_after=position_before,
            delta=None,
            intent=None,
            dispatch=None,
            estimate=None,
            fee=None,
            overlay={},
            read_touched=tuple(runtime.read_touched[read_before:]),
            write_touched=(),
            reconciliation=reconciliation,
        )

    if difference.has_prediction(target.prediction_id):
        return without_intent(DUPLICATE_PREDICTION)
    prior_claim = claims.claim_of(target.prediction_id)
    prior_intents = ledger.intent_ids_for_prediction(target.prediction_id)
    if prior_claim is not None or prior_intents:
        return without_intent(NEEDS_RECONCILIATION, {
            "reason": "预测已认领或已有意图，但差异账无决策行",
            "claim": None if prior_claim is None else dict(prior_claim),
            "intents": [
                {
                    "intent_id": intent_id,
                    "state": ledger.state(intent_id).value,
                }
                for intent_id in prior_intents
            ],
            "claim_ledger": str(runtime.claim_ledger_path),
        })
    book: BookSnapshot | None
    book_error: str | None = None
    try:
        book = runtime.book_source.snapshot(target.symbol)
    except (FillModelError, GuvoluError) as exc:
        book = None
        book_error = str(exc)
    overlay = evaluate_overlay(
        book,
        quality_eligible=target.quality_eligible,
        service_status=runtime.service_status,
        thresholds=config.overlay,
        anchor_age_seconds=runtime.anchor_age_seconds,
    )
    reference_price = None if book is None else book.mid
    delta: DeltaDecision | None = None
    intent: OrderIntent | None = None
    dispatch: DispatchResult | None = None
    estimate: FillEstimate | None = None
    fee: FeeQuote | None = None
    position_after = position_before
    status: str
    if book is None:
        status = "book_unavailable"
    else:
        delta = convert_target_to_delta_order(
            target.exposure_target,
            position_size=position_before,
            budget_jpy=target.risk_budget_jpy,
            reference_price=book.mid,
            rule=runtime.rule,
            no_trade_band=config.no_trade_band,
        )
        if delta.proposal is None:
            status = "skipped"
        elif (
            delta.proposal.side is Side.SELL
            and delta.proposal.size > position_before
        ):
            # 纯多头：卖出不得超过 paper 持仓
            # 差分转换已保证不可达，纵深防御
            status = "sell_exceeds_position"
        else:
            sender = PaperFillSender(
                book_source=runtime.book_source,
                fee_resolver=runtime.fee_resolver,
                fee_fetch=runtime.fee_fetch,
            )
            intent = OrderIntent(
                intent_id=new_intent_id(),
                correlation_id=target.correlation_id,
                symbol=delta.proposal.symbol,
                side=delta.proposal.side,
                execution_type=ExecutionType.LIMIT,
                size=delta.proposal.size,
                price=delta.proposal.price,
                time_in_force=None,
                created_at=now,
                prediction_id=target.prediction_id,
                decision_time=target.decision_time,
            )
            # 认领先于意图落盘，中断后重跑不重复生成
            claims.claim(
                prediction_id=target.prediction_id,
                correlation_id=target.correlation_id,
                at=now,
            )
            dispatch = dispatch_order_intent(
                intent,
                ledger=ledger,
                limit_gate=runtime.limit_gate,
                breaker=runtime.breaker,
                service_status=runtime.service_status,
                whitelist=runtime.whitelist,
                sender=sender,
                moment=now,
            )
            estimate = sender.last_estimate
            fee = sender.last_fee
            status = dispatch.state.value
            if dispatch.state is IntentState.PAPER_FILLED and estimate is not None:
                position_after = positions.record_fill(
                    intent=intent, estimate=estimate, at=now
                )
    write_touched: tuple[str, ...] = ()
    if dispatch is not None and dispatch.state not in LOCAL_TERMINAL_STATES:
        write_touched = (ORDER_ENDPOINT,)
    read_touched = tuple(runtime.read_touched[read_before:])
    row: dict[str, object] = {
        "schema_version": DIFFERENCE_LEDGER_SCHEMA_VERSION,
        "record": "paper_decision",
        "at": now.isoformat(),
        "prediction_id": target.prediction_id,
        "decision_time": target.decision_time.isoformat(),
        "valid_until": target.valid_until.isoformat(),
        "correlation_id": target.correlation_id,
        "market_id": target.market_id,
        "symbol": str(target.symbol),
        "mode": target.mode,
        "target_path": str(target.path),
        "target_sha256": target.sha256,
        "exposure_target": target.exposure_target,
        "risk_budget_jpy": format(target.risk_budget_jpy, "f"),
        "target_notional_jpy": (
            None
            if delta is None or reference_price is None
            else format(delta.desired_size * reference_price, "f")
        ),
        "reference_price": (
            None if reference_price is None else format(reference_price, "f")
        ),
        "position_before": format(position_before, "f"),
        "position_after": format(position_after, "f"),
        "status": status,
        "book_error": book_error,
        "delta": _delta_record(delta),
        "intent": (
            None
            if intent is None or dispatch is None
            else {
                "intent_id": intent.intent_id,
                "side": intent.side.value,
                "size": format(intent.size, "f"),
                "price": (
                    None if intent.price is None else format(intent.price, "f")
                ),
                "state": dispatch.state.value,
                "reason": dispatch.reason,
            }
        ),
        "fill": None if estimate is None else estimate.fill_record(),
        "cost": None if estimate is None else estimate.cost_record(),
        "fee": (
            None
            if fee is None
            else {
                "bps": format(fee.bps, "f"),
                "source": fee.source,
                "detail": fee.detail,
            }
        ),
        "overlay": overlay,
        "service_status": runtime.service_status.value,
        "endpoints": {
            "read_touched": list(read_touched),
            "write_planned": [],
            "write_touched": list(write_touched),
        },
    }
    difference.append(row)
    return PaperDecisionOutcome(
        target=target,
        status=status,
        reference_price=reference_price,
        position_before=position_before,
        position_after=position_after,
        delta=delta,
        intent=intent,
        dispatch=dispatch,
        estimate=estimate,
        fee=fee,
        overlay=overlay,
        read_touched=read_touched,
        write_touched=write_touched,
        difference_row=row,
    )


def recover_interrupted_paper_sends(
    ledger: IntentLedger, *, at: datetime
) -> tuple[str, ...]:
    """paper 恢复：遗留 SENDING 结清为 PAPER_REJECTED。

    paper 发送边界不触达任何写端点，SENDING 中断不存在未知的
    交易所结果，故不转超时态等待查询，而直接以拒绝结清，品种
    不再被在途占用。只可用于 paper 专用账本（T-04、T-06）。
    """
    marked = ledger.interrupted_sends()
    for intent_id in marked:
        ledger.paper_reject(intent_id, reason=PAPER_RECOVERY_REASON, at=at)
    return marked


def replay_limit_usage(
    limit_gate: LimitGate, ledger: IntentLedger, *, moment: datetime
) -> dict[str, object]:
    """自意图账本重放当日已过限额闸门的用量（T-11）。

    逐小时单发命令行的闸门只在内存，须按交易日重建累计。
    口径与内存闸门一致：进入 SENDING 及其后状态的意图在过闸
    时已计入，paper 拒绝亦不回退，保守计数。
    """
    day = trading_day(moment)
    total_jpy = Decimal("0")
    order_count = 0
    replayed: list[str] = []
    for intent_id in ledger.intent_ids():
        state = ledger.state(intent_id)
        if state in {IntentState.RECORDED, IntentState.GATE_REJECTED}:
            continue
        intent = ledger.intent(intent_id)
        if trading_day(intent.created_at) != day:
            continue
        if intent.price is None:
            raise PaperExecutorError(
                f"paper 意图账本含非限价意图 {intent_id}"
            )
        total_jpy += intent.notional_jpy()
        order_count += 1
        replayed.append(intent_id)
    limit_gate.seed_usage(day, total_jpy, order_count)
    usage = limit_gate.usage()
    return {
        "trading_day": day.isoformat(),
        "total_jpy": format(usage.total_jpy, "f"),
        "order_count": usage.order_count,
        "replayed_intents": replayed,
    }


def render_report(
    outcome: PaperDecisionOutcome,
    runtime: PaperRuntime,
    *,
    startup: Mapping[str, object] | None = None,
) -> dict[str, object]:
    """生成运行报告：目标、差分、成交、成本与触碰端点（A-03）。"""
    report: dict[str, object] = dict(outcome.difference_row)
    report["generated_at"] = datetime.now(UTC).isoformat()
    report["ledger_paths"] = {
        "intent_ledger": str(runtime.intent_ledger_path),
        "position_ledger": str(runtime.position_ledger_path),
        "difference_ledger": str(runtime.difference_ledger_path),
        "claim_ledger": str(runtime.claim_ledger_path),
    }
    if not outcome.difference_row:
        report.update({
            "prediction_id": outcome.target.prediction_id,
            "status": outcome.status,
            "endpoints": {
                "read_touched": list(outcome.read_touched),
                "write_planned": [],
                "write_touched": list(outcome.write_touched),
            },
        })
    if outcome.reconciliation is not None:
        report["reconciliation"] = dict(outcome.reconciliation)
    if startup is not None:
        report["startup"] = dict(startup)
    return report


def load_symbol_rules(path: Path) -> tuple[SymbolRule, ...]:
    """从取引ルール快照文件装载全部品种规则（费率来源夹具）。"""
    try:
        payload: object = json.loads(path.read_text(encoding="utf-8"))
    except FileNotFoundError as exc:
        raise PaperExecutorError(f"取引ルール快照不存在: {path}") from exc
    except json.JSONDecodeError as exc:
        raise PaperExecutorError(f"取引ルール快照不是合法 JSON: {path}") from exc
    if isinstance(payload, dict):
        payload = payload.get("data")
    if not isinstance(payload, list):
        raise PaperExecutorError("取引ルール快照必须是列表")
    return tuple(
        SymbolRule.from_api(row) for row in payload if isinstance(row, dict)
    )


def _file_rules_fetch(path: Path) -> SymbolRulesFetch:
    """费率来源为取引ルール快照文件，离线。"""

    def fetch() -> Sequence[SymbolRule]:
        return load_symbol_rules(path)

    return fetch


def _public_rules_fetch(
    get_public: Callable[[], PublicClient], read_touched: list[str]
) -> SymbolRulesFetch:
    """费率来源为公开端点 GET /v1/symbols，只读。"""

    def fetch() -> Sequence[SymbolRule]:
        read_touched.append(SYMBOLS_ENDPOINT)
        return get_public().symbols()

    return fetch


def _emit(report: Mapping[str, object], destination: str) -> None:
    text = json.dumps(report, ensure_ascii=False, indent=2, sort_keys=True)
    if destination == "-":
        print(text)
    else:
        path = Path(destination)
        path.parent.mkdir(parents=True, exist_ok=True)
        path.write_text(text + "\n", encoding="utf-8")


def build_parser() -> argparse.ArgumentParser:
    """构造命令行参数定义。"""
    parser = argparse.ArgumentParser(
        description="paper 执行器：执行目标到 paper 成交模型的零资金闭环"
    )
    parser.add_argument("--target", type=Path, required=True)
    parser.add_argument(
        "--config", type=Path, default=DEFAULT_PAPER_CONFIG_PATH,
        help="paper 执行器配置（G-06）",
    )
    parser.add_argument(
        "--rules", type=Path, default=None,
        help="取引ルール快照 JSON；缺省经公开端点拉取",
    )
    parser.add_argument(
        "--book", type=Path, default=None,
        help="盘口快照 JSON；缺省经公开端点拉取",
    )
    parser.add_argument(
        "--service-status", default=None,
        choices=[status.value for status in ServiceStatus],
        help="服务状态；缺省经公开端点拉取",
    )
    parser.add_argument(
        "--anchor-age-seconds", type=int, default=None,
        help="REST 锚点年龄秒；缺省标注不可得",
    )
    parser.add_argument(
        "--ledger-root", type=Path, default=None,
        help="账目根目录；缺省数据根",
    )
    parser.add_argument(
        "--breaker-config", type=Path, default=DEFAULT_THRESHOLDS_PATH,
    )
    parser.add_argument("--env-file", type=Path, default=None)
    parser.add_argument(
        "--now", default=None, help="决策时刻 ISO 文本，缺省当前时刻",
    )
    parser.add_argument(
        "--report", default="-", help="报告输出路径，- 表示标准输出",
    )
    return parser


def main(argv: Sequence[str] | None = None) -> int:
    """命令行入口。paper 模式零资金、零写请求（T-04、T-13）。"""
    args = build_parser().parse_args(argv)
    env_file: Path | None = args.env_file
    process_config = load_config(env_file)
    config_path: Path = args.config
    config = load_paper_config(config_path)
    target_path: Path = args.target
    target = load_execution_target(target_path)
    now_text: str | None = args.now
    moment = (
        datetime.now(UTC) if now_text is None
        else datetime.fromisoformat(now_text)
    )
    if moment.tzinfo is None:
        raise PaperExecutorError("--now 必须带时区")
    root_arg: Path | None = args.ledger_root
    root = root_arg if root_arg is not None else data_root()
    ledger_directory = root / config.ledger_directory
    read_touched: list[str] = []
    public: PublicClient | None = None

    def get_public() -> PublicClient:
        nonlocal public
        if public is None:
            public = PublicClient.from_config(process_config)
        return public

    rules_arg: Path | None = args.rules
    fee_fetch: SymbolRulesFetch
    if rules_arg is not None:
        rule = load_market_rule(rules_arg, config.symbol)
        fee_fetch = _file_rules_fetch(rules_arg)
    else:
        fetched = get_public().symbols()
        read_touched.append(SYMBOLS_ENDPOINT)
        matched = [row for row in fetched if row.symbol == str(config.symbol)]
        if not matched:
            raise PaperExecutorError(f"公开端点无品种 {config.symbol}")
        rule = MarketRule.from_symbol_rule(matched[0])
        fee_fetch = _public_rules_fetch(get_public, read_touched)
    book_arg: Path | None = args.book
    book_source: BookSource
    if book_arg is not None:
        book_source = StaticBookSource(
            load_book_snapshot_file(book_arg, basis=PUBLIC_ORDERBOOK_BASIS)
        )
    else:
        book_source = PublicBookSource(get_public(), read_touched)
    status_arg: str | None = args.service_status
    if status_arg is not None:
        service_status = ServiceStatus(status_arg)
    else:
        service_status = get_public().status()
        read_touched.append(STATUS_ENDPOINT)
    breaker_config: Path = args.breaker_config
    anchor_age: int | None = args.anchor_age_seconds
    # 启动恢复：结清遗留发送，重建当日用量
    ledger_directory.mkdir(parents=True, exist_ok=True)
    startup_ledger = IntentLedger(ledger_directory / INTENT_LEDGER_NAME)
    recovered = recover_interrupted_paper_sends(startup_ledger, at=moment)
    limit_gate = LimitGate(process_config.limits)
    usage = replay_limit_usage(limit_gate, startup_ledger, moment=moment)
    startup: dict[str, object] = {
        "recovered_sends": {
            "intent_ids": list(recovered),
            "state": IntentState.PAPER_REJECTED.value,
            "reason": PAPER_RECOVERY_REASON,
        },
        "limit_usage": usage,
    }
    runtime = PaperRuntime(
        config=config,
        rule=rule,
        book_source=book_source,
        fee_resolver=TakerFeeResolver(
            ledger_directory / FEE_CACHE_NAME,
            fallback_bps=config.taker_fee_fallback_bps,
            cache_seconds=config.taker_fee_cache_seconds,
        ),
        fee_fetch=fee_fetch,
        service_status=service_status,
        ledger_directory=ledger_directory,
        limit_gate=limit_gate,
        breaker=CircuitBreaker(load_breaker_thresholds(breaker_config)),
        whitelist=process_config.spot_whitelist,
        anchor_age_seconds=anchor_age,
        read_touched=read_touched,
    )
    outcome = run_paper_decision(target, runtime, moment=moment)
    _emit(
        render_report(outcome, runtime, startup=startup), str(args.report)
    )
    if outcome.write_touched:
        return 2
    if outcome.status == NEEDS_RECONCILIATION:
        return 1
    if outcome.dispatch is None:
        return 0
    return 0 if outcome.dispatch.state in EXPECTED_END_STATES else 1

"""熔断器（R-02）。阈值从版本化配置读取（G-06）。

计数与触发状态机之外，可登记触发动作：执行侧把
ops.kill_switch 的全量撤单接为动作（T-07），使触发从拒绝新
意图升级为拒绝新意图加全撤。本模块只持回调，不依赖运维模块。
阈值数值为 TBD-10 提案，待人工确认。
"""
from __future__ import annotations

import json
from collections.abc import Callable
from dataclasses import dataclass
from decimal import Decimal, InvalidOperation
from enum import StrEnum
from pathlib import Path

from guvolu.domain.errors import ConfigError
from guvolu.risk.errors import CircuitTripped

# 缺省配置相对路径，部署时相对进程工作目录
DEFAULT_THRESHOLDS_PATH = Path("config") / "circuit_breaker.json"


class BreakerState(StrEnum):
    """熔断状态机的两态。"""

    NORMAL = "NORMAL"
    TRIPPED = "TRIPPED"


@dataclass(frozen=True, slots=True)
class BreakerThresholds:
    """熔断阈值。数值提案见执行链设计第 6 节（TBD-10）。"""

    schema_version: int
    consecutive_failure_limit: int
    stream_gap_seconds: int
    asset_deviation_ratio: Decimal
    asset_deviation_floor_jpy: Decimal


def load_breaker_thresholds(path: Path) -> BreakerThresholds:
    """从版本化配置装载阈值（G-06），字段缺失或非法即配置错误。"""
    try:
        payload = json.loads(path.read_text(encoding="utf-8"))
    except FileNotFoundError as exc:
        raise ConfigError(f"熔断配置不存在: {path}") from exc
    except json.JSONDecodeError as exc:
        raise ConfigError(f"熔断配置不是合法 JSON: {path}") from exc
    if not isinstance(payload, dict):
        raise ConfigError("熔断配置根必须是对象")

    def positive_int(key: str) -> int:
        value = payload.get(key)
        if not isinstance(value, int) or isinstance(value, bool) or value <= 0:
            raise ConfigError(f"熔断配置 {key} 必须为正整数")
        return value

    def positive_decimal(key: str) -> Decimal:
        value = payload.get(key)
        # 金额与比例以字符串承载（D-07）
        if not isinstance(value, str):
            raise ConfigError(f"熔断配置 {key} 必须为字符串数值")
        try:
            number = Decimal(value)
        except InvalidOperation as exc:
            raise ConfigError(f"熔断配置 {key} 不是合法数值") from exc
        if number <= 0:
            raise ConfigError(f"熔断配置 {key} 必须为正")
        return number

    return BreakerThresholds(
        schema_version=positive_int("schema_version"),
        consecutive_failure_limit=positive_int("consecutive_failure_limit"),
        stream_gap_seconds=positive_int("stream_gap_seconds"),
        asset_deviation_ratio=positive_decimal("asset_deviation_ratio"),
        asset_deviation_floor_jpy=positive_decimal("asset_deviation_floor_jpy"),
    )


class CircuitBreaker:
    """计数与触发状态机（R-02）。复位仅经显式运维调用。"""

    def __init__(self, thresholds: BreakerThresholds) -> None:
        self._thresholds = thresholds
        self._state = BreakerState.NORMAL
        self._consecutive_failures = 0
        self._trip_reason: str | None = None
        self._trip_action: Callable[[str], None] | None = None

    @property
    def state(self) -> BreakerState:
        """当前熔断状态。"""
        return self._state

    @property
    def trip_reason(self) -> str | None:
        """首个触发原因，未触发为 None。"""
        return self._trip_reason

    @property
    def consecutive_failures(self) -> int:
        """当前连续异常计数。"""
        return self._consecutive_failures

    def ensure_can_send(self) -> None:
        """已触发即拒绝新意图（R-02）。"""
        if self._state is BreakerState.TRIPPED:
            raise CircuitTripped(f"熔断已触发: {self._trip_reason}")

    def set_trip_action(
        self, action: Callable[[str], None] | None
    ) -> None:
        """登记触发动作，仅首次触发时调用一次（T-07 接线）。

        动作在状态转入 TRIPPED 之后执行，动作异常不回滚状态；
        调用方须自行收敛异常，见 execution.emergency_stop。
        """
        self._trip_action = action

    def trip(self, reason: str) -> None:
        """触发熔断并保留首个触发原因；重复触发不重复动作。"""
        if self._state is BreakerState.TRIPPED:
            return
        self._trip_reason = reason
        self._state = BreakerState.TRIPPED
        if self._trip_action is not None:
            self._trip_action(reason)

    def reset(self) -> None:
        """人工复位并清零计数，不自动恢复。"""
        self._state = BreakerState.NORMAL
        self._consecutive_failures = 0
        self._trip_reason = None

    def record_write_success(self) -> None:
        """写路径成功，连续异常清零。"""
        self._consecutive_failures = 0

    def record_write_failure(self) -> None:
        """写路径异常计一次，含超时与网络错（T-06 事后另行查询）。"""
        self._consecutive_failures += 1
        if self._consecutive_failures >= self._thresholds.consecutive_failure_limit:
            self.trip(f"连续写路径异常 {self._consecutive_failures} 次")

    def record_reconciliation_mismatch(self) -> None:
        """双通道对账不一致计入同一异常计数（R-08）。"""
        self.record_write_failure()

    def record_stream_gap(self, gap_seconds: float) -> None:
        """行情断流达到阈值秒数即触发（R-02）。"""
        if gap_seconds < 0:
            raise ValueError("断流秒数不得为负")
        if gap_seconds >= self._thresholds.stream_gap_seconds:
            self.trip(f"行情断流 {gap_seconds} 秒")

    def record_asset_deviation(
        self, unexplained_jpy: Decimal, total_amount_jpy: Decimal
    ) -> None:
        """资产异动达到阈值即触发（R-02）。

        口径为对账时点无法由意图账本解释的資産残高 amount 合计
        差额（U-03 语义限定）。阈值取比例与绝对下限的较大者。
        """
        if total_amount_jpy < 0:
            raise ValueError("资产总额不得为负")
        threshold = self._thresholds.asset_deviation_ratio * total_amount_jpy
        floor = self._thresholds.asset_deviation_floor_jpy
        if threshold < floor:
            threshold = floor
        if abs(unexplained_jpy) >= threshold:
            self.trip(
                f"资产异动 {unexplained_jpy} JPY 达到阈值 {threshold} JPY"
            )

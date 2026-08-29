"""熔断骨架单测（R-02、C-15）。全部离线（C-13）。"""
from __future__ import annotations

import json
from decimal import Decimal
from pathlib import Path

import pytest

from guvolu.domain.errors import ConfigError
from guvolu.risk.circuit_breaker import (
    BreakerState,
    BreakerThresholds,
    CircuitBreaker,
    load_breaker_thresholds,
)
from guvolu.risk.errors import CircuitTripped

REPO_CONFIG = Path(__file__).resolve().parents[1] / "config" / "circuit_breaker.json"

THRESHOLDS = BreakerThresholds(
    schema_version=1,
    consecutive_failure_limit=3,
    stream_gap_seconds=90,
    asset_deviation_ratio=Decimal("0.01"),
    asset_deviation_floor_jpy=Decimal("30"),
)


def breaker() -> CircuitBreaker:
    """构造标准阈值的熔断器。"""
    return CircuitBreaker(THRESHOLDS)


def test_consecutive_failures_trip_at_limit() -> None:
    """连续异常达阈值即触发（R-02）。"""
    unit = breaker()
    unit.record_write_failure()
    unit.record_write_failure()
    assert unit.state is BreakerState.NORMAL
    unit.record_write_failure()
    assert unit.state is BreakerState.TRIPPED
    with pytest.raises(CircuitTripped, match="连续写路径异常"):
        unit.ensure_can_send()


def test_success_resets_consecutive_counter() -> None:
    """写路径成功清零连续计数。"""
    unit = breaker()
    unit.record_write_failure()
    unit.record_write_failure()
    unit.record_write_success()
    assert unit.consecutive_failures == 0
    unit.record_write_failure()
    unit.record_write_failure()
    assert unit.state is BreakerState.NORMAL


def test_reconciliation_mismatch_counts_as_failure() -> None:
    """对账不一致计入同一计数（R-08）。"""
    unit = breaker()
    unit.record_write_failure()
    unit.record_write_failure()
    unit.record_reconciliation_mismatch()
    assert unit.state is BreakerState.TRIPPED


def test_stream_gap_boundary() -> None:
    """断流达阈值秒数即触发，未达不触发。"""
    unit = breaker()
    unit.record_stream_gap(89.9)
    assert unit.state is BreakerState.NORMAL
    unit.record_stream_gap(90)
    assert unit.state is BreakerState.TRIPPED
    assert unit.trip_reason is not None
    assert "断流" in unit.trip_reason


def test_negative_stream_gap_rejected() -> None:
    """负断流秒数为调用缺陷。"""
    unit = breaker()
    with pytest.raises(ValueError, match="不得为负"):
        unit.record_stream_gap(-1)


def test_asset_deviation_ratio_path() -> None:
    """资产规模较大时按比例阈值判定。"""
    unit = breaker()
    unit.record_asset_deviation(Decimal("99"), Decimal("10000"))
    assert unit.state is BreakerState.NORMAL
    unit.record_asset_deviation(Decimal("100"), Decimal("10000"))
    assert unit.state is BreakerState.TRIPPED


def test_asset_deviation_floor_path() -> None:
    """资产规模较小时按绝对下限判定。"""
    unit = breaker()
    unit.record_asset_deviation(Decimal("29"), Decimal("1000"))
    assert unit.state is BreakerState.NORMAL
    unit.record_asset_deviation(Decimal("-30"), Decimal("1000"))
    assert unit.state is BreakerState.TRIPPED


def test_trip_keeps_first_reason() -> None:
    """重复触发保留首个原因。"""
    unit = breaker()
    unit.trip("第一原因")
    unit.trip("第二原因")
    assert unit.trip_reason == "第一原因"


def test_reset_restores_normal() -> None:
    """人工复位后恢复放行。"""
    unit = breaker()
    unit.trip("触发一次")
    unit.reset()
    assert unit.state is BreakerState.NORMAL
    assert unit.trip_reason is None
    unit.ensure_can_send()


def test_load_thresholds_from_file(tmp_path: Path) -> None:
    """阈值从版本化配置装载（G-06）。"""
    path = tmp_path / "breaker.json"
    path.write_text(
        json.dumps(
            {
                "schema_version": 2,
                "consecutive_failure_limit": 5,
                "stream_gap_seconds": 120,
                "asset_deviation_ratio": "0.02",
                "asset_deviation_floor_jpy": "50",
            }
        ),
        encoding="utf-8",
    )
    loaded = load_breaker_thresholds(path)
    assert loaded.schema_version == 2
    assert loaded.consecutive_failure_limit == 5
    assert loaded.stream_gap_seconds == 120
    assert loaded.asset_deviation_ratio == Decimal("0.02")
    assert loaded.asset_deviation_floor_jpy == Decimal("50")


def test_load_missing_field_rejected(tmp_path: Path) -> None:
    """字段缺失即配置错误。"""
    path = tmp_path / "breaker.json"
    path.write_text(json.dumps({"schema_version": 1}), encoding="utf-8")
    with pytest.raises(ConfigError):
        load_breaker_thresholds(path)


def test_load_float_ratio_rejected(tmp_path: Path) -> None:
    """比例以浮点承载即配置错误（D-07）。"""
    path = tmp_path / "breaker.json"
    path.write_text(
        json.dumps(
            {
                "schema_version": 1,
                "consecutive_failure_limit": 3,
                "stream_gap_seconds": 90,
                "asset_deviation_ratio": 0.01,
                "asset_deviation_floor_jpy": "30",
            }
        ),
        encoding="utf-8",
    )
    with pytest.raises(ConfigError, match="字符串"):
        load_breaker_thresholds(path)


def test_repo_config_matches_proposal() -> None:
    """仓库配置与 TBD-10 提案值一致。"""
    loaded = load_breaker_thresholds(REPO_CONFIG)
    assert loaded == THRESHOLDS

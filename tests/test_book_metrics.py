"""当前盘口快照派生指标的离线测试。"""
from decimal import Decimal

from guvolu.domain.models import OrderbookLevel
from guvolu.ui.book_metrics import snapshot_metrics


def _level(price: str, size: str) -> OrderbookLevel:
    return OrderbookLevel(Decimal(price), Decimal(size))


def test_snapshot_metrics_uses_best_quotes_and_common_bands() -> None:
    """最优报价、微价格与带内盘口不平衡可复算。"""
    metrics = snapshot_metrics(
        [_level("100.10", "3"), _level("100.02", "1"), _level("100.40", "1")],
        [_level("99.98", "2"), _level("100.00", "4"), _level("99.70", "1")],
    )
    assert metrics.best_ask == Decimal("100.02")
    assert metrics.best_bid == Decimal("100")
    assert metrics.spread == Decimal("0.02")
    assert metrics.mid == Decimal("100.01")
    assert metrics.microprice == Decimal("100.016")
    assert metrics.ask_coverage_bp > Decimal("0")
    assert metrics.bid_coverage_bp > Decimal("0")
    narrow = metrics.bands[0]
    assert narrow.band_bp == Decimal("5")
    assert narrow.ask_size == Decimal("1")
    assert narrow.bid_size == Decimal("6")
    assert narrow.imbalance_size == Decimal("0.7142857142857142857142857143")


def test_snapshot_metrics_rejects_missing_side() -> None:
    """双侧不全时禁止构造伪指标。"""
    try:
        snapshot_metrics([], [_level("100", "1")])
    except ValueError as exc:
        assert str(exc) == "盘口双侧不能为空"
    else:
        raise AssertionError("缺侧盘口必须失败")

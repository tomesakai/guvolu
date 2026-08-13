"""领域层单测：标识、配置、品种、模型解析。"""
from datetime import UTC
from decimal import Decimal
from pathlib import Path

import pytest

from guvolu.domain.config import (
    MAX_DAY_JPY_CEILING,
    MAX_ORDER_JPY_CEILING,
    load_config,
)
from guvolu.domain.enums import (
    ExecutionType,
    OrderStatus,
    RunMode,
    ServiceStatus,
    SettleType,
    Side,
)
from guvolu.domain.errors import ConfigError, SymbolError
from guvolu.domain.ids import new_correlation_id, new_intent_id, sha256_hex
from guvolu.domain.models import (
    Asset,
    Execution,
    Order,
    Orderbook,
    parse_service_status,
)
from guvolu.domain.symbols import LeverageSymbol, SpotSymbol, parse_symbol


def test_intent_id_unique_and_prefixed() -> None:
    """意图标识唯一且带前缀。"""
    ids = {new_intent_id() for _ in range(200)}
    assert len(ids) == 200
    assert all(item.startswith("it") for item in ids)


def test_correlation_id_format() -> None:
    """因果链标识格式。"""
    value = new_correlation_id()
    assert value.startswith("co") and len(value) == 18


def test_sha256_hex() -> None:
    """内容散列确定性。"""
    assert sha256_hex(b"") == (
        "e3b0c44298fc1c149afbf4c8996fb92427ae41e4649b934ca495991b7852b855"
    )


def test_spot_symbol_rejects_leverage_form() -> None:
    """现物类型拒绝杠杆形态（U-02）。"""
    with pytest.raises(SymbolError):
        SpotSymbol("BTC_JPY")


def test_parse_symbol_dispatch() -> None:
    """形态分派解析。"""
    assert isinstance(parse_symbol("BTC"), SpotSymbol)
    assert isinstance(parse_symbol("BTC_JPY"), LeverageSymbol)


def test_config_defaults(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    """缺省模拟运行（T-04）与缺省白名单。"""
    for name in ("GUVOLU_MODE", "GUVOLU_SPOT_WHITELIST"):
        monkeypatch.delenv(name, raising=False)
    config = load_config(env_file=tmp_path / "absent.env")
    assert config.mode is RunMode.DRY_RUN
    assert config.spot_whitelist == frozenset({SpotSymbol("BTC")})


def test_config_limits_clamped_to_ceiling(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    """限额越顶时被压回硬顶（T-11）。"""
    monkeypatch.setenv("GUVOLU_ORDER_JPY_MAX", "99999")
    monkeypatch.setenv("GUVOLU_DAY_JPY_MAX", "99999")
    config = load_config(env_file=tmp_path / "absent.env")
    assert config.limits.order_jpy_max == MAX_ORDER_JPY_CEILING
    assert config.limits.day_jpy_max == MAX_DAY_JPY_CEILING


def test_config_invalid_mode_raises(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    """非法模式立即报错。"""
    monkeypatch.setenv("GUVOLU_MODE", "prod")
    with pytest.raises(ConfigError):
        load_config(env_file=tmp_path / "absent.env")


def test_config_missing_credentials_raise(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    """密钥缺失在索取时报错。"""
    for name in (
        "GMO_COIN_READ_ONLY_API_KEY",
        "GMO_COIN_READ_ONLY_API_SECRET",
        "GMO_COIN_TRADE_API_KEY",
        "GMO_COIN_TRADE_API_SECRET",
    ):
        monkeypatch.delenv(name, raising=False)
    config = load_config(env_file=tmp_path / "absent.env")
    with pytest.raises(ConfigError):
        config.require_read_credentials()
    with pytest.raises(ConfigError):
        config.require_trade_credentials()


def test_asset_parse_decimal() -> None:
    """金额解析为 Decimal（T-08）。"""
    asset = Asset.from_api(
        {"amount": "3009", "available": "3009", "conversionRate": "1", "symbol": "JPY"}
    )
    assert asset.amount == Decimal("3009")
    assert isinstance(asset.available, Decimal)


def test_order_parse_market_price_absent() -> None:
    """MARKET 委托无价格字段。"""
    order = Order.from_api(
        {
            "rootOrderId": 123456789,
            "orderId": 123456789,
            "symbol": "BTC",
            "side": "BUY",
            "orderType": "NORMAL",
            "executionType": "MARKET",
            "settleType": "OPEN",
            "size": "0.02",
            "executedSize": "0",
            "losscutPrice": "0",
            "status": "ORDERED",
            "timeInForce": "FAK",
            "timestamp": "2019-03-19T01:07:24.217Z",
        }
    )
    assert order.price is None
    assert order.status is OrderStatus.ORDERED
    assert order.execution_type is ExecutionType.MARKET
    assert order.timestamp.tzinfo is not None


def test_execution_parse() -> None:
    """成交解析，字段语义区分（U-01）。"""
    execution = Execution.from_api(
        {
            "executionId": 72123911,
            "orderId": 123456789,
            "positionId": 1234567,
            "symbol": "BTC",
            "side": "BUY",
            "settleType": "OPEN",
            "size": "0.7361",
            "price": "877404",
            "lossGain": "0",
            "fee": "323",
            "timestamp": "2019-03-19T02:15:06.081Z",
        }
    )
    assert execution.side is Side.BUY
    assert execution.settle_type is SettleType.OPEN
    assert execution.fee == Decimal("323")
    assert execution.timestamp.astimezone(UTC).year == 2019


def test_orderbook_parse() -> None:
    """盘口解析。"""
    book = Orderbook.from_api(
        {
            "symbol": "BTC",
            "asks": [{"price": "455659", "size": "0.1"}],
            "bids": [{"price": "455659", "size": "0.3"}],
        }
    )
    assert book.asks[0].price == Decimal("455659")
    assert len(book.bids) == 1


def test_service_status_parse() -> None:
    """服务状态解析。"""
    assert parse_service_status({"status": "OPEN"}) is ServiceStatus.OPEN

"""传输层单测：留痕脱敏与业务校验。"""
from decimal import Decimal

import pytest

from guvolu.api.transport import (
    PublicTransport,
    RateLimiter,
    _extract_data,
    redact_body,
    redact_payload,
)
from guvolu.domain.errors import GmoApiError
from guvolu.domain.models import PublicTrade


class _FakeResponse:
    """按预置载荷返回的响应替身。"""

    def __init__(self, payload: dict[str, object], status: int = 200) -> None:
        self._payload = payload
        self.status_code = status

    def json(self) -> dict[str, object]:
        return self._payload


class _FakeSession:
    """按调用次序回放响应的会话替身。"""

    def __init__(self, responses: list[_FakeResponse]) -> None:
        self._responses = responses
        self.calls: list[str] = []

    def get(self, url: str, params: object = None, timeout: float = 0.0) -> _FakeResponse:
        self.calls.append(url)
        at = min(len(self.calls), len(self._responses)) - 1
        return self._responses[max(0, at)]


def test_redact_body_masks_token() -> None:
    """请求体中的令牌脱敏（T-01）。"""
    cleaned = redact_body({"token": "secret-value", "other": 1})
    assert cleaned is not None
    assert cleaned["token"] == "***"
    assert cleaned["other"] == 1


def test_redact_body_none_passthrough() -> None:
    """空体照常返回。"""
    assert redact_body(None) is None


def test_redact_payload_masks_ws_auth_data() -> None:
    """ws-auth 签发响应的令牌脱敏（T-01）。"""
    payload = {"status": 0, "data": "issued-token"}
    cleaned = redact_payload("/v1/ws-auth", payload)
    assert cleaned is not None
    assert cleaned["data"] == "***"


def test_redact_payload_other_paths_untouched() -> None:
    """其他端点载荷不改动。"""
    payload = {"status": 0, "data": "123456"}
    cleaned = redact_payload("/v1/order", payload)
    assert cleaned is not None
    assert cleaned["data"] == "123456"


def test_extract_data_raises_with_codes() -> None:
    """status 非 0 抛业务错误并带错误码（T-10）。"""
    payload = {
        "status": 1,
        "messages": [
            {"message_code": "ERR-5012", "message_string": "无权限"}
        ],
    }
    with pytest.raises(GmoApiError) as info:
        _extract_data(payload, "/v1/order", 200)
    assert info.value.codes == ("ERR-5012",)


def test_public_get_retries_rate_limit(monkeypatch) -> None:
    """公开 GET 遇 ERR-5003 按错误处置册退避重试。"""
    monkeypatch.setattr("guvolu.api.transport._sleep_backoff", lambda _: None)
    rate_limit = {
        "status": 4,
        "messages": [{"message_code": "ERR-5003", "message_string": "限速"}],
    }
    ok = {"status": 0, "data": {"symbol": "BTC"}}
    session = _FakeSession(
        [_FakeResponse(rate_limit), _FakeResponse(rate_limit), _FakeResponse(ok)]
    )
    transport = PublicTransport(RateLimiter(1000.0))
    transport._session = session  # type: ignore[attr-defined]
    assert transport.get("/v1/symbols") == {"symbol": "BTC"}
    assert len(session.calls) == 3


def test_public_get_no_retry_other_business_error(monkeypatch) -> None:
    """非限速业务错误不重试，立即抛出（C-08 范围）。"""
    monkeypatch.setattr("guvolu.api.transport._sleep_backoff", lambda _: None)
    no_data = {
        "status": 4,
        "messages": [{"message_code": "ERR-5207", "message_string": "无数据"}],
    }
    session = _FakeSession([_FakeResponse(no_data)])
    transport = PublicTransport(RateLimiter(1000.0))
    transport._session = session  # type: ignore[attr-defined]
    with pytest.raises(GmoApiError) as info:
        transport.get("/v1/klines")
    assert info.value.codes == ("ERR-5207",)
    assert len(session.calls) == 1


def test_public_trade_ws_symbol() -> None:
    """WS 逐笔帧的品种字段保留。"""
    trade = PublicTrade.from_api(
        {
            "channel": "trades",
            "price": "750760",
            "side": "BUY",
            "size": "0.1",
            "timestamp": "2018-03-30T12:34:56.789Z",
            "symbol": "BTC",
        }
    )
    assert trade.symbol == "BTC"
    assert trade.price == Decimal("750760")

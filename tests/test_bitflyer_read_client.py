"""bitFlyer 读客户端单测：429 退避重试与 4xx 立即失败。

全部离线，绝不打真实端点（C-13、C-14）。
"""
from __future__ import annotations

from decimal import Decimal

import pytest

from guvolu.api.bitflyer_read_client import (
    GET_RETRY_MAX,
    BitflyerReadClient,
    BitflyerAsset,
)
from guvolu.api.transport import RateLimiter
from guvolu.domain.errors import ApiHttpError

_BALANCE_ROW = {"currency_code": "JPY", "amount": "100", "available": "90"}


class _FakeResponse:
    def __init__(self, status_code: int, payload: object = None) -> None:
        self.status_code = status_code
        self._payload = payload

    def json(self) -> object:
        return self._payload


class _FakeSession:
    """离线会话替身，按脚本应答（C-13）。"""

    def __init__(self, responses: list[_FakeResponse]) -> None:
        self._responses = iter(responses)
        self.calls = 0

    def get(self, url: str, headers: dict[str, str], timeout: float) -> _FakeResponse:
        del url, headers, timeout
        self.calls += 1
        return next(self._responses)


class _NoWaitLimiter(RateLimiter):
    """测试限速替身：不等待，避免污染退避计数。"""

    def __init__(self) -> None:
        super().__init__(1000000.0)

    def acquire(self) -> None:
        return None


def _client(session: _FakeSession) -> BitflyerReadClient:
    return BitflyerReadClient(
        "key", "secret",
        limiter=_NoWaitLimiter(),
        session=session,  # type: ignore[arg-type]
    )


def _record_sleeps(monkeypatch: pytest.MonkeyPatch) -> list[float]:
    sleeps: list[float] = []
    monkeypatch.setattr(
        "guvolu.api.bitflyer_read_client.time.sleep",
        lambda delay: sleeps.append(delay),
    )
    return sleeps


def test_429_is_retried_with_backoff_until_success(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """429 属可恢复限速，退避后重试而非立即抛错（C-08）。"""
    sleeps = _record_sleeps(monkeypatch)
    session = _FakeSession([
        _FakeResponse(429),
        _FakeResponse(429),
        _FakeResponse(200, [_BALANCE_ROW]),
    ])

    assets = _client(session).assets()

    assert session.calls == 3
    assert len(sleeps) == 2
    assert assets == (BitflyerAsset(
        symbol="JPY", amount=Decimal("100"), available=Decimal("90"),
    ),)


def test_429_exhaustion_raises_http_error(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """429 连续超过重试上限时抛出最后的 HTTP 错误。"""
    _record_sleeps(monkeypatch)
    session = _FakeSession(
        [_FakeResponse(429) for _ in range(GET_RETRY_MAX)]
    )

    with pytest.raises(ApiHttpError) as caught:
        _client(session).assets()

    assert session.calls == GET_RETRY_MAX
    assert caught.value.http_status == 429


def test_other_4xx_still_raises_immediately(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """凭据类 4xx 重试无用，保持立即抛错。"""
    sleeps = _record_sleeps(monkeypatch)
    session = _FakeSession([_FakeResponse(401)])

    with pytest.raises(ApiHttpError) as caught:
        _client(session).assets()

    assert session.calls == 1
    assert sleeps == []
    assert caught.value.http_status == 401

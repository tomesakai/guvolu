"""紧急停止开关单测。只用替身，绝不触发真实端点（C-13、C-14）。"""
from __future__ import annotations

from collections.abc import Mapping, Sequence
from dataclasses import dataclass, field
from pathlib import Path

import pytest

from guvolu.api.public_client import PublicClient
from guvolu.api.trade_client import TradeClient
from guvolu.api.transport import (
    HttpMethod,
    Params,
    PrivateTransport,
    PublicTransport,
    RateLimiter,
)
from guvolu.domain.enums import RunMode
from guvolu.domain.errors import GmoApiError
from guvolu.domain.symbols import LeverageSymbol, SpotSymbol
from guvolu.ops import kill_switch

SYMBOL_ROWS: list[object] = [
    {
        "symbol": "BTC",
        "minOrderSize": "0.00001",
        "maxOrderSize": "5",
        "sizeStep": "0.00001",
        "tickSize": "1",
        "takerFee": "0.0005",
        "makerFee": "-0.0001",
    },
    {
        "symbol": "ETH",
        "minOrderSize": "0.01",
        "maxOrderSize": "80",
        "sizeStep": "0.01",
        "tickSize": "1",
        "takerFee": "0.0005",
        "makerFee": "-0.0001",
    },
    {
        "symbol": "BTC_JPY",
        "minOrderSize": "0.01",
        "maxOrderSize": "5",
        "sizeStep": "0.01",
        "tickSize": "1",
        "takerFee": "0",
        "makerFee": "0",
    },
]
# 禁止出现在本模块的依赖（T-07）
FORBIDDEN_MODULES = (
    "guvolu.strategy",
    "guvolu.risk",
    "guvolu.ui",
    "guvolu.research",
)


@dataclass(frozen=True, slots=True)
class Call:
    """一次请求的记录。"""

    method: str
    path: str
    body: dict[str, object] = field(default_factory=dict)


class FakePublicTransport(PublicTransport):
    """公开传输替身，只返回品种一览。"""

    def __init__(self, data: object) -> None:
        super().__init__(RateLimiter(1000.0))
        self._data = data
        self.paths: list[str] = []

    def get_payload(
        self, path: str, params: Params | None = None
    ) -> Mapping[str, object]:
        self.paths.append(path)
        return {"status": 0, "data": self._data}

    def get(self, path: str, params: Params | None = None) -> object:
        return self.get_payload(path, params).get("data")


class FakePrivateTransport(PrivateTransport):
    """私有传输替身，可返回预置 data 或抛出业务错误。"""

    def __init__(
        self,
        tmp_path: Path,
        responses: Sequence[object] = (),
        error: Exception | None = None,
    ) -> None:
        super().__init__("k", "s", RateLimiter(1000.0), tmp_path)
        self.calls: list[Call] = []
        self._responses: list[object] = list(responses)
        self._error = error

    def request(
        self,
        method: HttpMethod,
        path: str,
        *,
        params: Params | None = None,
        body: Mapping[str, object] | None = None,
    ) -> object:
        self.calls.append(
            Call(
                method=method,
                path=path,
                body=dict(body) if body is not None else {},
            )
        )
        if self._error is not None:
            raise self._error
        return self._responses.pop(0) if self._responses else None


def build(
    tmp_path: Path,
    responses: Sequence[object] = (),
    error: Exception | None = None,
) -> tuple[PublicClient, TradeClient, FakePrivateTransport]:
    """构造两个客户端与写路径的传输替身。"""
    public = PublicClient(FakePublicTransport(SYMBOL_ROWS))
    private = FakePrivateTransport(tmp_path, responses, error)
    trade = TradeClient(private, RunMode.DRY_RUN, frozenset({SpotSymbol("BTC")}))
    return public, trade, private


def test_collect_symbols_covers_spot_and_leverage() -> None:
    """现物与杠杆品种都要撤单，按形态分类型（U-02）。"""
    public = PublicClient(FakePublicTransport(SYMBOL_ROWS))
    symbols = kill_switch.collect_symbols(public)
    assert symbols == ("BTC", "ETH", "BTC_JPY")
    assert isinstance(symbols[0], SpotSymbol)
    assert isinstance(symbols[2], LeverageSymbol)


def test_cancel_all_sends_every_symbol(
    tmp_path: Path, capsys: pytest.CaptureFixture[str]
) -> None:
    """全撤请求体覆盖全部品种（T-07）。"""
    public, trade, private = build(tmp_path, [[637000, 637001]])
    assert kill_switch.cancel_all(public, trade) == 0
    call = private.calls[0]
    assert (call.method, call.path) == ("POST", "/v1/cancelBulkOrder")
    assert call.body == {"symbols": ["BTC", "ETH", "BTC_JPY"]}
    output = capsys.readouterr().out
    assert "3" in output
    assert "637000,637001" in output


def test_cancel_all_works_in_dry_run(tmp_path: Path) -> None:
    """撤单不受模拟运行守卫限制，紧急路径必须可达（T-07）。"""
    public, trade, private = build(tmp_path, [])
    assert kill_switch.cancel_all(public, trade) == 0
    assert len(private.calls) == 1


def test_cancel_all_reports_api_error(
    tmp_path: Path, capsys: pytest.CaptureFixture[str]
) -> None:
    """业务错误时打印错误码并返回非零。"""
    error = GmoApiError(
        codes=("ERR-5106",),
        messages=("Invalid request parameter.",),
        path="/v1/cancelBulkOrder",
        http_status=200,
    )
    public, trade, _ = build(tmp_path, [], error)
    assert kill_switch.cancel_all(public, trade) == 1
    assert "ERR-5106" in capsys.readouterr().out


def test_module_has_no_strategy_dependency() -> None:
    """独立入口不得依赖策略、风控与控制面（T-07）。"""
    source = Path(str(kill_switch.__file__)).read_text(encoding="utf-8")
    for name in FORBIDDEN_MODULES:
        assert name not in source

"""venues 适配器单测：限速、退避、分页、原文切分。全程离线（C-13、C-14）。"""
import json
from dataclasses import dataclass

from guvolu.venues.archive import split_json_array_items
from guvolu.venues.base import VenueRequestError, Window, window_days
from guvolu.venues.bitbank import BitbankPublicSource
from guvolu.venues.bitflyer import BitflyerPublicSource
from guvolu.venues.ratelimit import FixedRateLimiter

import pytest


@dataclass
class FakeResponse:
    status_code: int
    content: bytes


class FakeSession:
    """按 URL 出队响应的仿真会话。"""

    def __init__(self, script: dict[str, list[FakeResponse]]) -> None:
        self.script = script
        self.calls: list[str] = []

    def get(self, url: str, *, timeout: float) -> FakeResponse:
        self.calls.append(url)
        queue = self.script[url]
        return queue.pop(0) if len(queue) > 1 else queue[0]


def fast_limiter() -> FixedRateLimiter:
    return FixedRateLimiter(1000.0, sleeper=lambda _: None)


def test_rate_limiter_spaces_requests() -> None:
    """第二次获取应产生等待。"""
    waits: list[float] = []
    limiter = FixedRateLimiter(10.0, sleeper=waits.append)
    limiter.acquire()
    limiter.acquire()
    assert len(waits) == 1
    assert 0.0 < waits[0] <= 0.11


def test_window_days_inclusive() -> None:
    """日窗闭区间展开。"""
    days = window_days(Window("20260130", "20260202"))
    assert days == ["20260130", "20260131", "20260201", "20260202"]


def test_bitbank_fetch_day_url_and_body() -> None:
    """单日地址形态与原文字节保真。"""
    body = b'{"success":1,"data":{"transactions":[]}}'
    url = "https://public.bitbank.cc/btc_jpy/transactions/20260807"
    session = FakeSession({url: [FakeResponse(200, body)]})
    source = BitbankPublicSource(fast_limiter(), session, sleeper=lambda _: None)
    result = source.fetch_day("btc_jpy", "20260807")
    assert result.http_status == 200
    assert result.body == body
    assert session.calls == [url]


def test_bitbank_backoff_then_success() -> None:
    """429 与 5xx 按表退避后成功。"""
    url = "https://public.bitbank.cc/btc_jpy/transactions/20260807"
    session = FakeSession(
        {url: [FakeResponse(429, b""), FakeResponse(500, b""), FakeResponse(200, b"ok")]}
    )
    sleeps: list[float] = []
    source = BitbankPublicSource(fast_limiter(), session, sleeper=sleeps.append)
    result = source.fetch_day("btc_jpy", "20260807")
    assert result.body == b"ok"
    assert sleeps == [2.0, 4.0]


def test_bitbank_backoff_exhausted_raises() -> None:
    """退避用尽后抛来源请求错误。"""
    url = "https://public.bitbank.cc/btc_jpy/transactions/20260807"
    session = FakeSession({url: [FakeResponse(503, b"")]})
    sleeps: list[float] = []
    source = BitbankPublicSource(fast_limiter(), session, sleeper=sleeps.append)
    with pytest.raises(VenueRequestError):
        source.fetch_day("btc_jpy", "20260807")
    assert sleeps == [2.0, 4.0, 8.0, 16.0]


def test_bitbank_404_not_retried() -> None:
    """404 为缺日语义，不退避直接返回。"""
    url = "https://public.bitbank.cc/btc_jpy/transactions/20170101"
    session = FakeSession(
        {url: [FakeResponse(404, b'{"success":0,"data":{"code":10000}}')]}
    )
    sleeps: list[float] = []
    source = BitbankPublicSource(fast_limiter(), session, sleeper=sleeps.append)
    result = source.fetch_day("btc_jpy", "20170101")
    assert result.http_status == 404
    assert sleeps == []


def test_bitbank_trades_iterates_days() -> None:
    """窗内逐日产出包络并解析载荷。"""
    base = "https://public.bitbank.cc/btc_jpy/transactions/"
    script = {
        base + "20260806": [FakeResponse(200, b'{"success":1,"data":{"transactions":[{"transaction_id":1}]}}')],
        base + "20260807": [FakeResponse(404, b'{"success":0,"data":{"code":10000}}')],
    }
    session = FakeSession(script)
    source = BitbankPublicSource(fast_limiter(), session, sleeper=lambda _: None)
    envelopes = list(source.trades("btc_jpy", Window("20260806", "20260807")))
    assert len(envelopes) == 2
    assert envelopes[0]["http_status"] == 200
    assert envelopes[0]["params"] == {"pair": "btc_jpy", "day": "20260806"}
    payload = envelopes[0]["payload"]
    assert isinstance(payload, dict) and payload["success"] == 1
    assert envelopes[1]["http_status"] == 404


def test_bitflyer_page_url_with_cursor() -> None:
    """游标分页参数形态。"""
    first = (
        "https://api.bitflyer.com/v1/executions"
        "?product_code=BTC_JPY&count=500"
    )
    second = first + "&before=98"
    session = FakeSession(
        {
            first: [FakeResponse(200, b"[]")],
            second: [FakeResponse(200, b"[]")],
        }
    )
    source = BitflyerPublicSource(fast_limiter(), session, sleeper=lambda _: None)
    source.fetch_executions_page("BTC_JPY", None)
    source.fetch_executions_page("BTC_JPY", 98)
    assert session.calls == [first, second]


def test_bitflyer_boundary_detection() -> None:
    """400 且 status 为 -156 判定边界。"""
    boundary_body = (
        b'{"status":-156,"error_message":'
        b'"Execution history is limited to the most recent 31 days.",'
        b'"data":null}'
    )
    url = (
        "https://api.bitflyer.com/v1/executions"
        "?product_code=BTC_JPY&count=500&before=1000"
    )
    session = FakeSession({url: [FakeResponse(400, boundary_body)]})
    source = BitflyerPublicSource(fast_limiter(), session, sleeper=lambda _: None)
    page = source.fetch_executions_page("BTC_JPY", 1000)
    assert page.is_boundary()


def test_bitflyer_trades_pages_until_boundary() -> None:
    """回扫按最旧 id 推进游标至边界。"""
    rows = [
        {"id": 100, "side": "BUY", "price": 1.0, "size": 1.0,
         "exec_date": "2026-08-08T01:00:00.1"},
        {"id": 99, "side": "SELL", "price": 1.0, "size": 1.0,
         "exec_date": "2026-08-08T00:59:00.1"},
    ]
    first = (
        "https://api.bitflyer.com/v1/executions"
        "?product_code=BTC_JPY&count=500"
    )
    second = first + "&before=99"
    boundary_body = b'{"status":-156,"error_message":"x","data":null}'
    session = FakeSession(
        {
            first: [FakeResponse(200, json.dumps(rows).encode())],
            second: [FakeResponse(400, boundary_body)],
        }
    )
    source = BitflyerPublicSource(fast_limiter(), session, sleeper=lambda _: None)
    envelopes = list(source.trades("BTC_JPY", Window("20260701", "20260808")))
    assert len(envelopes) == 1
    assert session.calls == [first, second]


def test_split_json_array_items_preserves_number_text() -> None:
    """原文切分保留数字字面形态（T-08）。"""
    text = (
        '[{"id":1,"price":10259554.0,"size":1e-05},\n'
        ' {"id":2,"price":10259554.5,"size":0.010}]'
    )
    items = split_json_array_items(text)
    assert items == [
        '{"id":1,"price":10259554.0,"size":1e-05}',
        '{"id":2,"price":10259554.5,"size":0.010}',
    ]


def test_split_json_array_items_rejects_unclosed() -> None:
    """未闭合数组应报错。"""
    with pytest.raises(ValueError):
        split_json_array_items('[{"id":1}')

"""响应契约违例的解析行为。"""
import pytest

from guvolu.domain.errors import ApiSchemaError
from guvolu.domain.models import Ticker


def _ticker_row(**overrides: object) -> dict[str, object]:
    row: dict[str, object] = {
        "ask": "10247001",
        "bid": "10247000",
        "high": "10333000",
        "last": "10247001",
        "low": "10235120",
        "symbol": "BTC",
        "timestamp": "2026-08-19T05:49:56.220Z",
        "volume": "87.79929",
    }
    row.update(overrides)
    return row


def test_ticker_parses_normal_row() -> None:
    ticker = Ticker.from_api(_ticker_row())
    assert str(ticker.high) == "10333000"


@pytest.mark.parametrize("bad", ["", " ", "-", "N/A"])
def test_empty_decimal_field_raises_schema_error(bad: str) -> None:
    with pytest.raises(ApiSchemaError) as info:
        Ticker.from_api(_ticker_row(high=bad))
    assert "high" in str(info.value)


def test_bad_timestamp_raises_schema_error() -> None:
    with pytest.raises(ApiSchemaError):
        Ticker.from_api(_ticker_row(timestamp="not-a-time"))

"""fx_rate 采集单测：录制回放夹具、分段封口与数据静默看门狗（C-13）。"""
from __future__ import annotations

import asyncio
import json
from collections.abc import Callable, Sequence
from datetime import UTC, datetime
from decimal import Decimal
from pathlib import Path

import pytest

from guvolu.data import fx_capture
from guvolu.data.fx_capture import (
    FetchResult,
    FxFetchError,
    fx_channel_id,
    parse_ticker_frame,
    record_fx_rates,
)

FIXTURE = Path(__file__).parent / "fixtures" / "gmo_fx_ticker_2026-09-04.json"
SAMPLE = FIXTURE.read_text(encoding="utf-8").strip()
CONTROL = json.dumps({
    "status": 5,
    "messages": [{"message_code": "ERR-5201", "message_string": "maintenance"}],
})
FAST_INTERVAL = 0.01
FAST_SILENCE = 0.05
SHORT_RUN_MINUTES = 0.5 / 60
SILENCE_RUN_MINUTES = 0.3 / 60


def _fetch_result(text: str, http_status: int = 200) -> FetchResult:
    now = datetime.now(UTC).isoformat()
    return FetchResult(text, http_status, now, now, 1, 0.001)


class _ScriptedSession:
    """按脚本出队；末项重复；异常项抛出。"""

    def __init__(
        self, script: Sequence[FetchResult | FxFetchError],
        log: list[str],
    ) -> None:
        self._script = list(script)
        self._log = log
        log.append("open")

    def fetch(self) -> FetchResult:
        item = self._script.pop(0) if len(self._script) > 1 else self._script[0]
        if isinstance(item, FxFetchError):
            raise item
        return item

    def close(self) -> None:
        self._log.append("close")


def _factory(
    script: Sequence[FetchResult | FxFetchError], log: list[str],
) -> Callable[[], _ScriptedSession]:
    return lambda: _ScriptedSession(script, log)


def test_sample_response_parses_into_decimal_quotes() -> None:
    """2026-09-04 实测响应解析为 Decimal 报价与带时区事件时刻。"""
    assert fx_channel_id(SAMPLE) == "ticker"
    frame = parse_ticker_frame(SAMPLE)
    assert frame is not None
    assert len(frame.quotes) == 21
    quote = frame.quotes["USD_JPY"]
    assert (quote.bid, quote.ask) == (Decimal("155.943"), Decimal("155.948"))
    assert quote.status == "OPEN"
    assert quote.event_time == datetime(
        2026, 9, 4, 2, 19, 26, 944780, tzinfo=UTC,
    )
    assert frame.response_time == datetime(
        2026, 9, 4, 2, 19, 27, 9000, tzinfo=UTC,
    )


def test_non_zero_status_and_bad_quotes_are_not_data() -> None:
    """status 非整数 0 是控制帧；坏项使整帧失效（T-10）。"""
    assert fx_channel_id(CONTROL) == "protocol_control"
    assert parse_ticker_frame(CONTROL) is None
    assert fx_channel_id('{"status":false,"data":[]}') == "protocol_control"
    assert fx_channel_id("not json") == "protocol_control"
    body = json.loads(SAMPLE)
    for field, value in (("bid", "abc"), ("bid", "-1"), ("status", "HALT"),
                         ("timestamp", "2026-09-04T02:19:26")):
        broken = json.loads(SAMPLE)
        broken["data"][0][field] = value
        with pytest.raises(ValueError):
            parse_ticker_frame(json.dumps(broken))
    body["data"].append(dict(body["data"][0]))
    with pytest.raises(ValueError, match="重复"):
        parse_ticker_frame(json.dumps(body))


def test_record_writes_sealed_segments_per_symbol(tmp_path: Path) -> None:
    """同一响应写入各 symbol 的 run，逐帧封口并绑定 EP-0076 r0。"""
    log: list[str] = []
    stats, manifests = asyncio.run(record_fx_rates(
        tmp_path, ["USD_JPY", "EUR_JPY", "USD_JPY"], SHORT_RUN_MINUTES,
        3600.0, 1,
        interval_seconds=FAST_INTERVAL,
        data_silence_seconds=FAST_SILENCE,
        session_factory=_factory([_fetch_result(SAMPLE)], log),
    ))
    assert stats.venue_symbols == ("USD_JPY", "EUR_JPY")
    assert stats.wire_frames >= 2
    assert stats.data_frames == stats.wire_frames
    assert (stats.sessions, stats.data_silence_resets, stats.control_frames) == (
        1, 0, 0,
    )
    assert stats.last_http_status == 200
    assert stats.last_fetch_seconds == 0.001
    assert log == ["open", "close"]
    assert len(manifests) == 2
    for manifest_path, symbol in zip(manifests, ("USD_JPY", "EUR_JPY"), strict=True):
        run = json.loads(manifest_path.read_text(encoding="utf-8"))
        assert run["status"] == "complete"
        assert (run["venue_id"], run["venue_symbol"], run["domain"]) == (
            "gmo_fx", symbol, "fx_rate",
        )
        assert (run["endpoint_id"], run["endpoint_revision"]) == ("EP-0076", 0)
        assert run["record_count"] == stats.wire_frames
        assert run["segment_count"] == stats.wire_frames
        segments = sorted(manifest_path.parent.glob("segment-*.manifest.json"))
        assert len(segments) == run["segment_count"]
        first = json.loads(segments[0].read_text(encoding="utf-8"))
        assert (first["status"], first["completion_claim"]) == ("sealed", True)
        assert first["record_count"] == 1
        row = json.loads(
            (tmp_path / first["storage_path"]).read_text(encoding="utf-8")
        )
        assert row["schema_version"] == 3
        assert row["payload_raw"] == SAMPLE
        assert (row["channel_id"], row["source_endpoint"], row["source"]) == (
            "ticker", "public/v1/ticker", "https",
        )
        assert row["connection_id"] == f"{run['run_id']}-c000001"


def test_control_frame_is_persisted_but_counts_as_failure(
    tmp_path: Path,
) -> None:
    """HTTP 200 而 status 非 0 的帧原样落盘，不刷新数据健康。"""
    log: list[str] = []
    stats, manifests = asyncio.run(record_fx_rates(
        tmp_path, ["USD_JPY"], SHORT_RUN_MINUTES, 3600.0, 1,
        interval_seconds=FAST_INTERVAL,
        data_silence_seconds=FAST_SILENCE,
        session_factory=_factory(
            [_fetch_result(CONTROL), _fetch_result(SAMPLE)], log,
        ),
    ))
    assert stats.control_frames == 1
    assert stats.data_frames >= 1
    assert stats.wire_frames == stats.control_frames + stats.data_frames
    segments = sorted(manifests[0].parent.glob("segment-*.manifest.json"))
    first = json.loads(segments[0].read_text(encoding="utf-8"))
    row = json.loads(
        (tmp_path / first["storage_path"]).read_text(encoding="utf-8")
    )
    assert row["channel_id"] == "protocol_control"
    assert row["payload_raw"] == CONTROL


def test_data_silence_watchdog_recreates_session(tmp_path: Path) -> None:
    """持续 HTTP 失败超过静默预算时重建会话，run 仍正常封口。"""
    log: list[str] = []
    stats, manifests = asyncio.run(record_fx_rates(
        tmp_path, ["USD_JPY"], SILENCE_RUN_MINUTES, 3600.0, 1024,
        interval_seconds=FAST_INTERVAL,
        data_silence_seconds=FAST_SILENCE,
        session_factory=_factory([FxFetchError("boom")], log),
    ))
    assert stats.data_silence_resets >= 1
    assert stats.sessions == stats.data_silence_resets + 1
    assert stats.reconnects == stats.data_silence_resets
    assert (stats.wire_frames, stats.data_frames) == (0, 0)
    assert stats.fetch_attempts > stats.sessions
    assert stats.consecutive_failures > 0
    assert stats.last_failure == "boom"
    assert log.count("open") == stats.sessions
    assert log.count("close") == stats.sessions
    run = json.loads(manifests[0].read_text(encoding="utf-8"))
    assert run["status"] == "complete"
    assert (run["record_count"], run["segment_count"]) == (0, 0)
    assert list(manifests[0].parent.glob("segment-*")) == []


def test_record_validates_symbols_and_silence_budget(tmp_path: Path) -> None:
    with pytest.raises(ValueError, match="形态非法"):
        asyncio.run(record_fx_rates(tmp_path, ["usd_jpy"], 0.001, 300.0, 1024))
    with pytest.raises(ValueError, match="须大于轮询间隔"):
        asyncio.run(record_fx_rates(
            tmp_path, ["USD_JPY"], 0.001, 300.0, 1024,
            interval_seconds=60.0, data_silence_seconds=60.0,
        ))
    assert list(tmp_path.iterdir()) == []


def test_failure_delay_is_bounded_by_interval() -> None:
    assert fx_capture._failure_delay(0, 60.0) <= 60.0
    assert fx_capture._failure_delay(100, 60.0) == 60.0
    assert fx_capture._failure_delay(5, FAST_INTERVAL) == FAST_INTERVAL


def test_cli_rejects_too_frequent_polling(tmp_path: Path) -> None:
    """公共端点：命令行不允许低于下限的轮询间隔。"""
    with pytest.raises(ValueError, match="interval-seconds"):
        fx_capture.main([
            "--data-root", str(tmp_path), "record", "--interval-seconds", "1",
        ])

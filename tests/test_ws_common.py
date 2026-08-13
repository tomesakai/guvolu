"""WS 共通构件单测：错误码提取与命令节奏器。"""
import asyncio

import pytest

from guvolu.api.ws_common import CommandPacer, decode_frame
from guvolu.domain.errors import WsError, extract_error_code


def test_decode_frame_attaches_error_code() -> None:
    """错误帧抛出并带结构化错误码（C-09）。"""
    with pytest.raises(WsError) as info:
        decode_frame('{"error": "ERR-5012 Invalid permissions for action"}')
    assert info.value.code == "ERR-5012"


def test_decode_frame_error_without_code() -> None:
    """无错误码的错误帧 code 为空。"""
    with pytest.raises(WsError) as info:
        decode_frame('{"error": "unknown failure"}')
    assert info.value.code is None


def test_extract_error_code() -> None:
    """错误码提取辅助。"""
    assert extract_error_code("ERR-151 detail") == "ERR-151"
    assert extract_error_code("no code here") is None


def test_pacer_first_call_no_wait() -> None:
    """首条命令不等待。"""
    waits: list[float] = []

    async def scenario() -> None:
        clock_values = iter([100.0, 100.0])
        pacer = CommandPacer(clock=lambda: next(clock_values))
        await pacer.wait_turn()

    asyncio.run(scenario())
    assert waits == []


def test_pacer_second_call_waits_interval(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """次条命令等待完整间隔，确定性时钟。"""
    waits: list[float] = []

    async def fake_sleep(delay: float) -> None:
        waits.append(delay)

    monkeypatch.setattr(asyncio, "sleep", fake_sleep)

    async def scenario() -> None:
        clock_values = iter([100.0, 100.0, 100.0, 101.1])
        pacer = CommandPacer(
            interval_seconds=1.1, clock=lambda: next(clock_values)
        )
        await pacer.wait_turn()
        await pacer.wait_turn()

    asyncio.run(scenario())
    assert waits == [pytest.approx(1.1)]


def test_pacer_rejects_non_positive_interval() -> None:
    """非正间隔立即拒绝。"""
    with pytest.raises(ValueError):
        CommandPacer(interval_seconds=0.0)

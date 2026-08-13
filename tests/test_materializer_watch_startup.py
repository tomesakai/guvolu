from __future__ import annotations

import importlib
from pathlib import Path
from types import ModuleType
from typing import Any

import pytest

from guvolu.data import watch_connection


class FakeConnection:
    """记录常驻物化器是否释放连接。"""

    def __init__(self) -> None:
        self.closed = False

    def close(self) -> None:
        self.closed = True


WATCH_CASES = (
    (
        "guvolu.data.orderflow_tile_materialize",
        "direct",
        ("watch", "--poll-seconds", "30"),
        "orderflow_tile_startup_error",
        "OFL tile watcher 已停止",
        30.0,
    ),
    (
        "guvolu.data.trade_realtime_materialize",
        "store",
        ("watch", "--interval-seconds", "10"),
        "trade_realtime_materialization_startup_error",
        "实时逐笔物化已停止",
        10.0,
    ),
    (
        "guvolu.data.l2_materialize",
        "store",
        ("watch", "--interval-seconds", "10"),
        "l2_materialization_startup_error",
        "L2 增量物化已停止",
        10.0,
    ),
    (
        "guvolu.data.book_state_materialize",
        "direct",
        ("watch", "--poll-seconds", "5"),
        "book_state_materialization_startup_error",
        "盘口末态物化已停止",
        5.0,
    ),
)


def _patch_connector(
    monkeypatch: pytest.MonkeyPatch,
    module: ModuleType,
    target: str,
    connector: Any,
) -> None:
    if target == "store":
        monkeypatch.setattr(module.store, "connect", connector)
    else:
        monkeypatch.setattr(module, "connect", connector)


@pytest.mark.parametrize(
    (
        "module_name",
        "connect_target",
        "command",
        "startup_event",
        "stop_message",
        "retry_seconds",
    ),
    WATCH_CASES,
)
def test_watch_startup_retries_transient_connect_failure_and_stops_cleanly(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
    capsys: pytest.CaptureFixture[str],
    module_name: str,
    connect_target: str,
    command: tuple[str, ...],
    startup_event: str,
    stop_message: str,
    retry_seconds: float,
) -> None:
    module = importlib.import_module(module_name)
    connection = FakeConnection()
    connect_calls = 0

    def connect_after_timeout(_root: Path) -> FakeConnection:
        nonlocal connect_calls
        connect_calls += 1
        if connect_calls == 1:
            raise TimeoutError("writer busy")
        return connection

    def stop_first_cycle(_root: Path) -> object:
        raise KeyboardInterrupt

    _patch_connector(
        monkeypatch,
        module,
        connect_target,
        connect_after_timeout,
    )
    monkeypatch.setattr(module, "sqlite_writer_lock", stop_first_cycle)
    sleeps: list[float] = []
    monkeypatch.setattr(watch_connection, "sleep", sleeps.append)

    result = module.main(("--data-root", str(tmp_path), *command))

    output = capsys.readouterr().out
    assert result == 0
    assert connect_calls == 2
    assert sleeps == [retry_seconds]
    assert f'"event": "{startup_event}"' in output
    assert '"error": "TimeoutError: writer busy"' in output
    assert f'"retry_seconds": {retry_seconds:g}' in output
    assert stop_message in output
    assert connection.closed is True


@pytest.mark.parametrize(
    ("module_name", "connect_target"),
    tuple((case[0], case[1]) for case in WATCH_CASES),
)
def test_one_shot_commands_keep_connect_fail_fast(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
    module_name: str,
    connect_target: str,
) -> None:
    module = importlib.import_module(module_name)
    connect_calls = 0

    def fail_connect(_root: Path) -> FakeConnection:
        nonlocal connect_calls
        connect_calls += 1
        raise TimeoutError("writer busy")

    _patch_connector(monkeypatch, module, connect_target, fail_connect)

    with pytest.raises(TimeoutError, match="writer busy"):
        module.main(("--data-root", str(tmp_path), "audit"))

    assert connect_calls == 1

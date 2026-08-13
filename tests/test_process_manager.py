"""进程管理器单测：状态机、判活双通道、事件留痕。

假可执行以本解释器短命令模拟，时钟可注入，全程离线（C-13）。
"""
from __future__ import annotations

import json
import os
import sys
import time
from collections.abc import Callable
from datetime import UTC, datetime
from pathlib import Path

import pytest

from guvolu.ops.process_manager import (
    COLLECT_MODULE,
    DEFAULT_REGISTRY,
    L2_COLLECT_MODULE,
    TRADE_COLLECT_MODULE,
    ProcessManager,
    ProcessSpec,
    collect_spec,
    l2_collect_spec,
    realtime_trade_collect_spec,
)

# 假可执行：长驻与立即退出
_SLEEP_ARGV = (sys.executable, "-c", "import time; time.sleep(60)")
_FAIL_ARGV = (sys.executable, "-c", "raise SystemExit(7)")


def _flag_argv(flag: Path) -> tuple[str, ...]:
    """有旗标文件则长驻，否则退出码 5。"""
    code = (
        "import pathlib, sys, time\n"
        f"flag = pathlib.Path({str(flag)!r})\n"
        "time.sleep(60) if flag.exists() else sys.exit(5)\n"
    )
    return (sys.executable, "-c", code)


class _Clock:
    """可推进的注入时钟。"""

    def __init__(self) -> None:
        self.value = time.time()

    def __call__(self) -> float:
        return self.value

    def advance(self, seconds: float) -> None:
        self.value += seconds


def _shutdown(manager: ProcessManager) -> None:
    """收尾停掉全部登记进程。"""
    for name in manager.names():
        manager.stop(name)


_Builder = Callable[..., ProcessManager]


@pytest.fixture()
def build(tmp_path: Path, request: pytest.FixtureRequest) -> _Builder:
    """管理器工厂，测试结束自动收尾。"""

    def _build(
        spec: ProcessSpec,
        clock: _Clock,
        external_scan: Callable[[tuple[str, ...]], list[int]] | None = None,
        external_terminate: Callable[[int], bool] | None = None,
    ) -> ProcessManager:
        manager = ProcessManager(
            (spec,),
            data_root=tmp_path / "data",
            log_dir=tmp_path / "logs",
            clock=clock,
            external_scan=(
                external_scan if external_scan is not None else lambda argv: []
            ),
            # 缺省终止器为替身，离线且不触真进程
            external_terminate=(
                external_terminate
                if external_terminate is not None
                else lambda pid: True
            ),
        )
        request.addfinalizer(lambda: _shutdown(manager))
        return manager

    return _build


def _spec(argv: tuple[str, ...], cwd: Path, **overrides: object) -> ProcessSpec:
    """构造测试登记项。"""
    merged: dict[str, object] = {
        "name": "record-fake",
        "argv": argv,
        "cwd": cwd,
        "auto_restart": True,
    }
    merged.update(overrides)
    return ProcessSpec(**merged)  # type: ignore[arg-type]


def _row(manager: ProcessManager, name: str) -> dict[str, object]:
    """取单进程快照行。"""
    for row in manager.snapshot():
        if row["name"] == name:
            return row
    raise AssertionError(f"未登记 {name}")


def _wait_state(
    manager: ProcessManager, name: str, target: str, seconds: float = 15.0
) -> dict[str, object]:
    """轮询等待进入目标状态，超时即失败。"""
    row: dict[str, object] = {}
    deadline = time.monotonic() + seconds
    while time.monotonic() < deadline:
        row = _row(manager, name)
        if row["status"] == target:
            return row
        time.sleep(0.05)
    raise AssertionError(f"状态未达 {target}: {row}")


def _events(log_dir: Path) -> list[dict[str, object]]:
    """读运维事件日志全量行。"""
    path = log_dir / "ops-events.jsonl"
    if not path.exists():
        return []
    return [
        json.loads(line)
        for line in path.read_text(encoding="utf-8").splitlines()
        if line
    ]


def _write_manifest(
    data_root: Path, clock: _Clock, symbol: str, mtime: float
) -> Path:
    """按当日目录写心跳清单并设定文件时戳。"""
    day = datetime.fromtimestamp(clock(), UTC).strftime("%Y-%m-%d")
    directory = data_root / "raw" / day
    directory.mkdir(parents=True, exist_ok=True)
    path = directory / f"manifest-{symbol.lower()}1.json"
    path.write_text(
        json.dumps({"record_symbol": symbol, "heartbeat": True}),
        encoding="utf-8",
    )
    os.utime(path, (mtime, mtime))
    return path


def _write_l2_checkpoint(
    data_root: Path, clock: _Clock, venue: str, symbol: str, mtime: float
) -> Path:
    """写 run-scoped L2 open checkpoint 并设定时戳。"""
    directory = (
        data_root / "raw" / "realtime" / "book_l2"
        / f"venue_id={venue}" / f"venue_symbol={symbol}" / "run_id=test"
    )
    directory.mkdir(parents=True, exist_ok=True)
    path = directory / "checkpoint.json"
    path.write_text(
        json.dumps({"status": "open", "venue_id": venue, "venue_symbol": symbol}),
        encoding="utf-8",
    )
    os.utime(path, (mtime, mtime))
    return path


def test_default_registry_collect_only() -> None:
    """登记表只含三所 L2 与逐笔公共采集，均有 run 心跳。"""
    assert [spec.name for spec in DEFAULT_REGISTRY] == [
        "l2-gmo-btc",
        "l2-bitbank-btc-jpy",
        "l2-bitflyer-btc-jpy",
        "trade-gmo-btc",
        "trade-bitbank-btc-jpy",
        "trade-bitflyer-btc-jpy",
    ]
    for spec in DEFAULT_REGISTRY[:3]:
        assert spec.argv[1:3] == ("-m", L2_COLLECT_MODULE)
    for spec in DEFAULT_REGISTRY[3:]:
        assert spec.argv[1:3] == ("-m", TRADE_COLLECT_MODULE)
    for spec in DEFAULT_REGISTRY:
        assert spec.argv[3] == "record"
        assert Path(spec.argv[0]).is_absolute()
        assert spec.auto_restart is True
        assert spec.heartbeat_glob is not None


def test_collect_spec_locks_module() -> None:
    """登记工厂只能生成采集模块命令行。"""
    made = collect_spec("record-x", ("record", "--symbol", "XRP"), "XRP")
    assert made.argv[1:3] == ("-m", COLLECT_MODULE)


def test_l2_collect_spec_locks_module_and_heartbeat() -> None:
    """L2 工厂锁定采集模块、长期模式、分段参数与 run 心跳。"""
    made = l2_collect_spec("l2-x", "bitbank", "xrp_jpy")
    assert made.argv[1:3] == ("-m", L2_COLLECT_MODULE)
    assert made.argv[3:] == (
        "record", "--venue", "bitbank", "--symbol", "xrp_jpy",
        "--minutes", "0", "--segment-seconds", "300",
        "--segment-max-mib", "128",
    )
    assert made.heartbeat_glob is not None
    assert "venue_symbol=xrp_jpy" in made.heartbeat_glob


def test_realtime_trade_spec_locks_module_and_heartbeat() -> None:
    """逐笔工厂锁定采集模块、分段参数与独立心跳域。"""
    made = realtime_trade_collect_spec("trade-x", "bitbank", "xrp_jpy")
    assert made.argv[1:3] == ("-m", TRADE_COLLECT_MODULE)
    assert made.argv[3:] == (
        "record", "--venue", "bitbank", "--symbol", "xrp_jpy",
        "--minutes", "0", "--segment-seconds", "300",
        "--segment-max-mib", "32",
    )
    assert made.heartbeat_glob is not None
    assert "trade_realtime" in made.heartbeat_glob


def test_spec_validation() -> None:
    """空命令行与非法参数拒绝构造。"""
    with pytest.raises(ValueError):
        ProcessSpec(name="a", argv=(), cwd=Path("."), auto_restart=True)
    with pytest.raises(ValueError):
        ProcessSpec(
            name="a",
            argv=("x",),
            cwd=Path("."),
            auto_restart=True,
            max_consecutive_failures=0,
        )


def test_start_idempotent(build: _Builder, tmp_path: Path) -> None:
    """拉起幂等：已运行再拉返回现状且不重启。"""
    clock = _Clock()
    manager = build(_spec(_SLEEP_ARGV, tmp_path), clock)
    first = manager.start("record-fake")
    assert first["status"] == "运行"
    pid = first["pid"]
    again = manager.start("record-fake")
    assert again["pid"] == pid
    starts = [e for e in _events(tmp_path / "logs") if e["event"] == "start"]
    assert len(starts) == 1
    assert starts[0]["run_id"] == manager.run_id


def test_stop_cancels_auto_restart(build: _Builder, tmp_path: Path) -> None:
    """人工停止转停止态，不再自动重启。"""
    clock = _Clock()
    manager = build(_spec(_SLEEP_ARGV, tmp_path), clock)
    manager.start("record-fake")
    stopped = manager.stop("record-fake")
    assert stopped["status"] == "停止"
    assert stopped["pid"] is None
    clock.advance(120.0)
    manager.poll()
    assert _row(manager, "record-fake")["status"] == "停止"
    assert manager.stop("record-fake")["status"] == "停止"
    assert any(e["event"] == "stop" for e in _events(tmp_path / "logs"))


def test_exit_backoff_then_manual_required(
    build: _Builder, tmp_path: Path
) -> None:
    """退出转退避，连续失败达上限转须人工。"""
    clock = _Clock()
    spec = _spec(_FAIL_ARGV, tmp_path, max_consecutive_failures=2)
    manager = build(spec, clock)
    manager.start("record-fake")
    backing = _wait_state(manager, "record-fake", "退避中")
    assert backing["last_exit_code"] == 7
    assert backing["consecutive_failures"] == 1
    clock.advance(2.0)
    manager.poll()
    manual = _wait_state(manager, "record-fake", "须人工")
    assert manual["restart_count"] == 1
    clock.advance(600.0)
    manager.poll()
    assert _row(manager, "record-fake")["status"] == "须人工"
    events = _events(tmp_path / "logs")
    assert any(e["event"] == "manual_required" for e in events)
    exits = [e for e in events if e["event"] == "exit"]
    assert exits and all(e["exit_code"] == 7 for e in exits)


def test_manual_start_resets_failures(build: _Builder, tmp_path: Path) -> None:
    """须人工后人工拉起可复位失败计数。"""
    clock = _Clock()
    spec = _spec(_FAIL_ARGV, tmp_path, max_consecutive_failures=1)
    manager = build(spec, clock)
    manager.start("record-fake")
    _wait_state(manager, "record-fake", "须人工")
    manager.start("record-fake")
    backing = _wait_state(manager, "record-fake", "须人工")
    events = _events(tmp_path / "logs")
    assert len([e for e in events if e["event"] == "start"]) == 2
    assert backing["consecutive_failures"] == 1


def test_backoff_sequence_capped(build: _Builder, tmp_path: Path) -> None:
    """退避序列 2/4/8/16 秒且封顶复用末值。"""
    clock = _Clock()
    spec = _spec(_FAIL_ARGV, tmp_path, max_consecutive_failures=6)
    manager = build(spec, clock)
    manager.start("record-fake")
    for expected in (2.0, 4.0, 8.0, 16.0, 16.0):
        _wait_state(manager, "record-fake", "退避中")
        exits = [
            e for e in _events(tmp_path / "logs") if e["event"] == "exit"
        ]
        assert exits[-1]["backoff_seconds"] == expected
        clock.advance(expected)
        manager.poll()
    manual = _wait_state(manager, "record-fake", "须人工")
    assert manual["restart_count"] == 5
    assert manual["consecutive_failures"] == 6


def test_zombie_detection_and_restart(build: _Builder, tmp_path: Path) -> None:
    """进程活而心跳超时判僵死，杀后按策略重启。"""
    clock = _Clock()
    spec = _spec(_SLEEP_ARGV, tmp_path, heartbeat_symbol="BTC")
    manager = build(spec, clock)
    manager.start("record-fake")
    data_root = tmp_path / "data"
    manifest = _write_manifest(data_root, clock, "BTC", clock())
    clock.advance(410.0)
    fresh = _row(manager, "record-fake")
    assert fresh["status"] == "运行"
    assert fresh["heartbeat_at"] is not None
    os.utime(manifest, (clock(), clock()))
    clock.advance(415.0)
    assert _row(manager, "record-fake")["status"] == "运行"
    clock.advance(10.0)
    backing = _row(manager, "record-fake")
    assert backing["status"] == "退避中"
    assert backing["pid"] is None
    assert any(
        e["event"] == "zombie" for e in _events(tmp_path / "logs")
    )
    clock.advance(2.0)
    restarted = _row(manager, "record-fake")
    assert restarted["status"] == "运行"
    assert restarted["restart_count"] == 1


def test_l2_checkpoint_keeps_process_healthy(
    build: _Builder, tmp_path: Path
) -> None:
    """run-scoped open checkpoint 是 L2 判活依据。"""
    clock = _Clock()
    spec = _spec(
        _SLEEP_ARGV,
        tmp_path,
        heartbeat_glob=(
            "raw/realtime/book_l2/venue_id=gmo/"
            "venue_symbol=BTC/run_id=*/checkpoint.json"
        ),
    )
    manager = build(spec, clock)
    manager.start("record-fake")
    checkpoint = _write_l2_checkpoint(
        tmp_path / "data", clock, "gmo", "BTC", clock()
    )
    clock.advance(410.0)
    assert _row(manager, "record-fake")["status"] == "运行"
    os.utime(checkpoint, (clock(), clock()))
    clock.advance(415.0)
    assert _row(manager, "record-fake")["status"] == "运行"
    clock.advance(10.0)
    assert _row(manager, "record-fake")["status"] == "退避中"


def test_zombie_ignores_other_symbol_heartbeat(
    build: _Builder, tmp_path: Path
) -> None:
    """他品种心跳清单不作本进程判活依据。"""
    clock = _Clock()
    spec = _spec(_SLEEP_ARGV, tmp_path, heartbeat_symbol="BTC")
    manager = build(spec, clock)
    manager.start("record-fake")
    data_root = tmp_path / "data"
    alien = _write_manifest(data_root, clock, "ETH", clock())
    clock.advance(430.0)
    os.utime(alien, (clock(), clock()))
    row = _row(manager, "record-fake")
    assert row["status"] == "退避中"
    assert row["heartbeat_at"] is None


def test_stable_run_resets_failures(build: _Builder, tmp_path: Path) -> None:
    """稳定运行达阈值即清零连续失败计数。"""
    clock = _Clock()
    flag = tmp_path / "flag.txt"
    spec = _spec(_flag_argv(flag), tmp_path)
    manager = build(spec, clock)
    manager.start("record-fake")
    backing = _wait_state(manager, "record-fake", "退避中")
    assert backing["consecutive_failures"] == 1
    flag.write_text("on", encoding="utf-8")
    clock.advance(2.0)
    running = _wait_state(manager, "record-fake", "运行")
    assert running["consecutive_failures"] == 1
    clock.advance(300.0)
    assert _row(manager, "record-fake")["consecutive_failures"] == 0


def test_spawn_error_escalates(build: _Builder, tmp_path: Path) -> None:
    """可执行缺失按失败退避，达上限转须人工。"""
    clock = _Clock()
    spec = _spec(
        (str(tmp_path / "absent.exe"),),
        tmp_path,
        max_consecutive_failures=3,
        backoff_seconds=(2.0, 4.0),
    )
    manager = build(spec, clock)
    first = manager.start("record-fake")
    assert first["status"] == "退避中"
    clock.advance(2.0)
    manager.poll()
    assert _row(manager, "record-fake")["status"] == "退避中"
    clock.advance(4.0)
    manager.poll()
    assert _row(manager, "record-fake")["status"] == "须人工"
    kinds = [e["event"] for e in _events(tmp_path / "logs")]
    assert kinds.count("start_error") == 3
    assert kinds[-1] == "manual_required"


def test_events_carry_time_and_run_id(build: _Builder, tmp_path: Path) -> None:
    """全部事件含时刻与 run 标识（D-09）。"""
    clock = _Clock()
    manager = build(_spec(_SLEEP_ARGV, tmp_path), clock)
    manager.start("record-fake")
    manager.stop("record-fake")
    events = _events(tmp_path / "logs")
    assert events
    for event in events:
        assert event["run_id"] == manager.run_id
        assert str(event["time"]).endswith("+00:00")
        assert event["process"] == "record-fake"


def test_external_instance_adopted_idempotent(
    build: _Builder, tmp_path: Path
) -> None:
    """外启同命令实例被收编，拉起幂等不重复拉起。"""
    clock = _Clock()
    manager = build(
        _spec(_SLEEP_ARGV, tmp_path), clock, lambda argv: [4242]
    )
    row = _row(manager, "record-fake")
    assert row["status"] == "运行"
    assert row["pid"] == 4242
    assert row["external"] is True
    started = manager.start("record-fake")
    assert started["status"] == "运行"
    assert started["pid"] == 4242
    events = [e["event"] for e in _events(tmp_path / "logs")]
    assert events == ["adopt"]


def test_adopted_external_exit_then_respawn(
    build: _Builder, tmp_path: Path
) -> None:
    """收编实例消失记外启退出，按重启策略拉起。"""
    clock = _Clock()
    live: list[list[int]] = [[4242]]
    manager = build(
        _spec(_SLEEP_ARGV, tmp_path), clock, lambda argv: list(live[0])
    )
    assert _row(manager, "record-fake")["status"] == "运行"
    live[0] = []
    # 越过扫描缓存期再判活
    clock.advance(11.0)
    manager.poll()
    backing = _row(manager, "record-fake")
    assert backing["status"] == "退避中"
    assert backing["external"] is False
    clock.advance(2.0)
    running = _wait_state(manager, "record-fake", "运行")
    assert running["external"] is False
    assert running["pid"] != 4242
    events = [e["event"] for e in _events(tmp_path / "logs")]
    assert "external_exit" in events
    assert events.index("adopt") < events.index("external_exit")


def test_stop_adopted_external_terminates(
    build: _Builder, tmp_path: Path
) -> None:
    """停止收编实例经终止器执行，且停后不再收编。"""
    clock = _Clock()
    live: list[list[int]] = [[4242]]
    killed: list[int] = []

    def terminate(pid: int) -> bool:
        killed.append(pid)
        live[0] = []
        return True

    manager = build(
        _spec(_SLEEP_ARGV, tmp_path), clock,
        lambda argv: list(live[0]), terminate,
    )
    assert _row(manager, "record-fake")["status"] == "运行"
    stopped = manager.stop("record-fake")
    assert stopped["status"] == "停止"
    assert stopped["pid"] is None
    assert killed == [4242]
    manager.poll()
    assert _row(manager, "record-fake")["status"] == "停止"


def test_adopted_zombie_by_heartbeat(
    build: _Builder, tmp_path: Path
) -> None:
    """收编实例心跳超时判僵死，终止后按策略处置。"""
    clock = _Clock()
    live: list[list[int]] = [[4242]]
    killed: list[int] = []

    def terminate(pid: int) -> bool:
        killed.append(pid)
        live[0] = []
        return True

    spec = _spec(_SLEEP_ARGV, tmp_path, heartbeat_symbol="BTC")
    manager = build(spec, clock, lambda argv: list(live[0]), terminate)
    assert _row(manager, "record-fake")["status"] == "运行"
    clock.advance(430.0)
    row = _row(manager, "record-fake")
    assert row["status"] == "退避中"
    assert killed == [4242]
    assert any(
        e["event"] == "zombie" for e in _events(tmp_path / "logs")
    )


def test_external_scan_error_tolerated(
    build: _Builder, tmp_path: Path
) -> None:
    """扫描失败按无冲突继续拉起。"""

    def broken(argv: tuple[str, ...]) -> list[int]:
        raise OSError("扫描器故障")

    clock = _Clock()
    manager = build(_spec(_SLEEP_ARGV, tmp_path), clock, broken)
    row = manager.start("record-fake")
    assert row["status"] == "运行"

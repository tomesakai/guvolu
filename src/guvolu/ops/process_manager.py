"""采集进程管理器：登记表驱动的保活与操作后端（TBD-31 提案实施）。

登记表为白名单常量，结构上只容采集类进程：登记工厂只能生成
调用采集模块的命令行。本管理器绝不管理任何持有 TRADE 密钥的
进程（T-13 的运维侧延伸），交易进程管理始终属人工确认（A-01）。
判活双通道为子进程存活状态加采集器自己的心跳时戳；旧式逐日采集
读取 ``data/raw/<日>`` 清单，新式 L2 与逐笔读取 run-scoped checkpoint。
进程活而心跳超时判僵死，
杀后按策略重启；连续失败达上限转「须人工」，不再自动重启。
外部已启动的同命令行实例按命令行匹配识别并收编为运行态：
拉起因此幂等不重复采集，判活、心跳与停止对收编实例同样生效；
扫描器与终止器可注入，单测以替身保持全程离线（C-13）。
全部事件追加 logs/ops-events.jsonl，含时刻与 run 标识（D-09）。
"""
from __future__ import annotations

import json
import os
import signal
import subprocess
import sys
import threading
import time
from collections.abc import Callable, Sequence
from dataclasses import dataclass
from datetime import UTC, datetime, timedelta
from enum import Enum
from pathlib import Path

from guvolu.domain.ids import new_run_id

# 采集模块限定，白名单边界
COLLECT_MODULE = "guvolu.data.collect"
L2_COLLECT_MODULE = "guvolu.data.l2_capture"
TRADE_COLLECT_MODULE = "guvolu.data.trade_capture"
# 仓库根目录，缺省工作目录
REPO_ROOT = Path(__file__).resolve().parents[3]
# 退避序列 2/4/8/16 秒封顶
BACKOFF_SEQUENCE_SECONDS: tuple[float, ...] = (2.0, 4.0, 8.0, 16.0)
# 连续失败上限，达则须人工
MAX_CONSECUTIVE_FAILURES = 5
# 心跳清单宽限秒（七分钟）
HEARTBEAT_GRACE_SECONDS = 420.0
# 稳定运行秒，达则清零失败
STABLE_RESET_SECONDS = 300.0
# 判活轮询间隔秒
POLL_INTERVAL_SECONDS = 2.0
# 终止等待秒，超时补杀
TERMINATE_WAIT_SECONDS = 5.0
# 运维事件日志文件名
EVENTS_FILE_NAME = "ops-events.jsonl"
# 外部实例扫描缓存秒
EXTERNAL_SCAN_TTL_SECONDS = 10.0
# 扫描子命令超时秒
EXTERNAL_SCAN_TIMEOUT_SECONDS = 15.0


def default_external_terminator(pid: int) -> bool:
    """终止外部实例：Windows 走 taskkill，含子进程树。"""
    if sys.platform == "win32":
        completed = subprocess.run(
            ["taskkill", "/PID", str(pid), "/T", "/F"],
            capture_output=True,
            text=True,
            timeout=EXTERNAL_SCAN_TIMEOUT_SECONDS,
            check=False,
        )
        return completed.returncode == 0
    try:
        os.kill(pid, signal.SIGTERM)
    except OSError:
        return False
    return True


class ProcessState(Enum):
    """进程状态，值为控制面文案（X-07）。"""

    RUNNING = "运行"
    STOPPED = "停止"
    BACKING_OFF = "退避中"
    MANUAL_REQUIRED = "须人工"


@dataclass(frozen=True, slots=True)
class ProcessSpec:
    """登记表条目：命令行、工作目录与重启策略。"""

    name: str
    argv: tuple[str, ...]
    cwd: Path
    auto_restart: bool
    backoff_seconds: tuple[float, ...] = BACKOFF_SEQUENCE_SECONDS
    max_consecutive_failures: int = MAX_CONSECUTIVE_FAILURES
    heartbeat_symbol: str | None = None
    heartbeat_glob: str | None = None
    heartbeat_grace_seconds: float = HEARTBEAT_GRACE_SECONDS
    stable_reset_seconds: float = STABLE_RESET_SECONDS

    def __post_init__(self) -> None:
        if not self.argv:
            raise ValueError("命令行不得为空")
        if not self.backoff_seconds:
            raise ValueError("退避序列不得为空")
        if self.max_consecutive_failures < 1:
            raise ValueError("最大连续失败数须为正")


def collect_spec(
    name: str, args: Sequence[str], heartbeat_symbol: str | None
) -> ProcessSpec:
    """构造采集进程登记项。

    命令行锁定为本解释器绝对路径加采集模块（PATH 漂移教训），
    结构上排除任何持有密钥的交易进程（T-13、A-01）。
    """
    return ProcessSpec(
        name=name,
        # 沿用本解释器绝对路径
        argv=(sys.executable, "-m", COLLECT_MODULE, *args),
        cwd=REPO_ROOT,
        auto_restart=True,
        heartbeat_symbol=heartbeat_symbol,
    )


def l2_collect_spec(name: str, venue: str, symbol: str) -> ProcessSpec:
    """构造 run-scoped 分段 L2 长驻采集登记项。"""
    return ProcessSpec(
        name=name,
        argv=(
            sys.executable,
            "-m",
            L2_COLLECT_MODULE,
            "record",
            "--venue",
            venue,
            "--symbol",
            symbol,
            "--minutes",
            "0",
            "--segment-seconds",
            "300",
            "--segment-max-mib",
            "128",
        ),
        cwd=REPO_ROOT,
        auto_restart=True,
        heartbeat_glob=(
            "raw/realtime/book_l2/"
            f"venue_id={venue}/venue_symbol={symbol}/run_id=*/checkpoint.json"
        ),
    )


def realtime_trade_collect_spec(
    name: str, venue: str, symbol: str
) -> ProcessSpec:
    """构造 run-scoped 实时逐笔采集登记项。"""
    return ProcessSpec(
        name=name,
        argv=(
            sys.executable,
            "-m",
            TRADE_COLLECT_MODULE,
            "record",
            "--venue",
            venue,
            "--symbol",
            symbol,
            "--minutes",
            "0",
            "--segment-seconds",
            "300",
            "--segment-max-mib",
            "32",
        ),
        cwd=REPO_ROOT,
        auto_restart=True,
        heartbeat_glob=(
            "raw/realtime/trade_realtime/"
            f"venue_id={venue}/venue_symbol={symbol}/run_id=*/checkpoint.json"
        ),
    )


# 登记表白名单常量，只容采集类
# 绝不登记持有 TRADE 密钥的进程
DEFAULT_REGISTRY: tuple[ProcessSpec, ...] = (
    l2_collect_spec("l2-gmo-btc", "gmo", "BTC"),
    l2_collect_spec("l2-bitbank-btc-jpy", "bitbank", "btc_jpy"),
    l2_collect_spec("l2-bitflyer-btc-jpy", "bitflyer", "BTC_JPY"),
    realtime_trade_collect_spec("trade-gmo-btc", "gmo", "BTC"),
    realtime_trade_collect_spec(
        "trade-bitbank-btc-jpy", "bitbank", "btc_jpy"
    ),
    realtime_trade_collect_spec(
        "trade-bitflyer-btc-jpy", "bitflyer", "BTC_JPY"
    ),
)


@dataclass(slots=True)
class _ProcessRuntime:
    """单进程运行时状态，仅管理器内部使用。"""

    spec: ProcessSpec
    state: ProcessState = ProcessState.STOPPED
    popen: subprocess.Popen[bytes] | None = None
    external_pid: int | None = None
    started_at: float | None = None
    resume_at: float | None = None
    restart_count: int = 0
    consecutive_failures: int = 0
    last_exit_code: int | None = None
    last_event_at: float | None = None


_SCAN_SCRIPT = (
    "Get-CimInstance Win32_Process -Filter \"Name='python.exe'\" | "
    "ForEach-Object { '{0}|{1}' -f $_.ProcessId, $_.CommandLine }"
)


def _scan_same_command(argv: tuple[str, ...]) -> list[int]:
    """扫描外启同命令进程，返回进程号清单。

    判据为模块与参数尾串包含匹配，
    规避解释器路径引号形态差异。
    非 Windows 平台返回空集。
    """
    if sys.platform != "win32":
        return []
    tail = " ".join(argv[1:])
    result = subprocess.run(
        ["powershell", "-NoProfile", "-NonInteractive", "-Command", _SCAN_SCRIPT],
        capture_output=True,
        text=True,
        timeout=EXTERNAL_SCAN_TIMEOUT_SECONDS,
        check=False,
    )
    found: list[int] = []
    for line in result.stdout.splitlines():
        pid_text, _, command = line.partition("|")
        if tail and tail in command:
            try:
                found.append(int(pid_text))
            except ValueError:
                continue
    return found


class ProcessManager:
    """登记表驱动的采集进程管理器。

    时钟可注入以便离线单测（C-13）；公开方法线程安全，
    每次访问先推进一轮判活，守护线程仅是兜底节拍。
    """

    def __init__(
        self,
        specs: Sequence[ProcessSpec] = DEFAULT_REGISTRY,
        *,
        data_root: Path = Path("data"),
        log_dir: Path = Path("logs"),
        clock: Callable[[], float] = time.time,
        run_id: str | None = None,
        external_scan: Callable[[tuple[str, ...]], list[int]] | None = None,
        external_terminate: Callable[[int], bool] | None = None,
    ) -> None:
        names = [spec.name for spec in specs]
        if len(set(names)) != len(names):
            raise ValueError("登记名不得重复")
        self._runtimes: dict[str, _ProcessRuntime] = {
            spec.name: _ProcessRuntime(spec=spec) for spec in specs
        }
        self._data_root = data_root
        self._log_dir = log_dir
        self._clock = clock
        self._external_scan = (
            external_scan if external_scan is not None else _scan_same_command
        )
        self._external_terminate = (
            external_terminate
            if external_terminate is not None
            else default_external_terminator
        )
        # 扫描结果按名缓存（时刻, 进程号集）
        self._scan_cache: dict[str, tuple[float, list[int]]] = {}
        self.run_id = run_id if run_id is not None else new_run_id()
        self._lock = threading.Lock()
        self._poll_thread: threading.Thread | None = None

    def has(self, name: str) -> bool:
        """名称是否在登记表内。"""
        return name in self._runtimes

    def names(self) -> tuple[str, ...]:
        """登记表全部名称。"""
        return tuple(self._runtimes)

    def snapshot(self) -> list[dict[str, object]]:
        """登记表全量状态，供操作端点。"""
        with self._lock:
            self._poll_locked()
            return [self._row(rt) for rt in self._runtimes.values()]

    def start(self, name: str) -> dict[str, object]:
        """拉起进程，已运行返回现状（幂等）。"""
        with self._lock:
            self._poll_locked()
            rt = self._runtimes[name]
            if rt.state is not ProcessState.RUNNING:
                # 人工拉起即清零失败计数
                rt.consecutive_failures = 0
                rt.resume_at = None
                self._spawn(rt, "start")
            return self._row(rt)

    def stop(self, name: str) -> dict[str, object]:
        """停止进程并取消自动重启（幂等），含收编实例。"""
        with self._lock:
            self._poll_locked()
            rt = self._runtimes[name]
            if rt.state is ProcessState.RUNNING and rt.popen is not None:
                pid = rt.popen.pid
                self._terminate(rt.popen)
                rt.popen = None
                rt.state = ProcessState.STOPPED
                self._event(rt, "stop", pid=pid)
            elif rt.state is ProcessState.RUNNING and rt.external_pid is not None:
                pid = rt.external_pid
                self._external_terminate(pid)
                rt.external_pid = None
                rt.state = ProcessState.STOPPED
                self._scan_cache.pop(rt.spec.name, None)
                self._event(rt, "stop", pid=pid, external=True)
            elif rt.state in (
                ProcessState.BACKING_OFF,
                ProcessState.MANUAL_REQUIRED,
            ):
                rt.resume_at = None
                rt.state = ProcessState.STOPPED
                self._event(rt, "stop", pid=None)
            return self._row(rt)

    def poll(self) -> None:
        """推进一轮判活与状态迁移。"""
        with self._lock:
            self._poll_locked()

    def start_poll_thread(self) -> None:
        """启动守护轮询线程（服务进程内使用）。"""
        if self._poll_thread is not None and self._poll_thread.is_alive():
            return
        thread = threading.Thread(
            target=self._poll_forever, name="ops-poll", daemon=True
        )
        self._poll_thread = thread
        thread.start()

    def _poll_forever(self) -> None:
        while True:
            time.sleep(POLL_INTERVAL_SECONDS)
            try:
                self.poll()
            except OSError:
                # 判活失败不终止守护线程
                continue

    def _poll_locked(self) -> None:
        for rt in self._runtimes.values():
            if rt.state is ProcessState.RUNNING:
                self._check_running(rt)
            else:
                self._try_adopt(rt)
                if rt.state is ProcessState.BACKING_OFF:
                    self._check_backing_off(rt)

    def _scan_pids(self, rt: _ProcessRuntime, *, force: bool = False) -> list[int]:
        """按登记名带缓存扫描外启同命令进程号。"""
        name = rt.spec.name
        now = self._clock()
        cached = self._scan_cache.get(name)
        if (
            not force
            and cached is not None
            and now - cached[0] <= EXTERNAL_SCAN_TTL_SECONDS
        ):
            return cached[1]
        try:
            pids = list(self._external_scan(rt.spec.argv))
        except (OSError, subprocess.SubprocessError):
            # 扫描失败按无外启处理
            pids = []
        own = rt.popen.pid if rt.popen is not None else None
        pids = [pid for pid in pids if pid != own and pid != os.getpid()]
        self._scan_cache[name] = (now, pids)
        return pids

    def _try_adopt(self, rt: _ProcessRuntime) -> None:
        """识别外启同命令实例并收编为运行态。"""
        if rt.popen is not None:
            return
        pids = self._scan_pids(rt)
        if not pids:
            return
        rt.external_pid = pids[0]
        rt.state = ProcessState.RUNNING
        rt.started_at = self._clock()
        rt.resume_at = None
        # 外启视同人工拉起，清零失败
        rt.consecutive_failures = 0
        self._event(rt, "adopt", pid=pids[0], pids=pids)

    def _check_running(self, rt: _ProcessRuntime) -> None:
        if rt.popen is None and rt.external_pid is None:
            rt.state = ProcessState.STOPPED
            return
        now = self._clock()
        if rt.popen is not None:
            exit_code = rt.popen.poll()
            if exit_code is not None:
                rt.popen = None
                rt.last_exit_code = exit_code
                self._fail(rt, "exit", exit_code=exit_code)
                return
        else:
            if rt.external_pid not in self._scan_pids(rt):
                # 收编实例消失按退出处理
                pid = rt.external_pid
                rt.external_pid = None
                self._fail(rt, "external_exit", pid=pid)
                return
        started = rt.started_at if rt.started_at is not None else now
        if (
            rt.consecutive_failures > 0
            and now - started >= rt.spec.stable_reset_seconds
        ):
            # 稳定运行即清零连续失败
            rt.consecutive_failures = 0
        if (
            rt.spec.heartbeat_symbol is None
            and rt.spec.heartbeat_glob is None
        ):
            return
        heartbeat = self._heartbeat_time(rt.spec)
        base = started if heartbeat is None else max(heartbeat, started)
        if now - base <= rt.spec.heartbeat_grace_seconds:
            return
        # 进程活而心跳超时判僵死
        if rt.popen is not None:
            pid = rt.popen.pid
            self._terminate(rt.popen)
            rt.popen = None
        else:
            pid = rt.external_pid
            if rt.external_pid is not None:
                self._external_terminate(rt.external_pid)
            rt.external_pid = None
            self._scan_cache.pop(rt.spec.name, None)
        self._fail(rt, "zombie", pid=pid, silent_seconds=round(now - base, 1))

    def _check_backing_off(self, rt: _ProcessRuntime) -> None:
        if rt.resume_at is None or self._clock() < rt.resume_at:
            return
        rt.resume_at = None
        rt.restart_count += 1
        self._spawn(rt, "restart")

    def _fail(self, rt: _ProcessRuntime, event: str, **details: object) -> None:
        """记失败事件并按重启策略安排后续。"""
        rt.consecutive_failures += 1
        spec = rt.spec
        if rt.consecutive_failures >= spec.max_consecutive_failures:
            rt.state = ProcessState.MANUAL_REQUIRED
            rt.resume_at = None
            self._event(rt, event, failures=rt.consecutive_failures, **details)
            # 连续失败达上限转须人工
            self._event(rt, "manual_required", failures=rt.consecutive_failures)
            return
        if not spec.auto_restart:
            rt.state = ProcessState.STOPPED
            self._event(rt, event, failures=rt.consecutive_failures, **details)
            return
        index = min(rt.consecutive_failures - 1, len(spec.backoff_seconds) - 1)
        delay = spec.backoff_seconds[index]
        rt.state = ProcessState.BACKING_OFF
        rt.resume_at = self._clock() + delay
        self._event(
            rt,
            event,
            failures=rt.consecutive_failures,
            backoff_seconds=delay,
            **details,
        )

    def _spawn(self, rt: _ProcessRuntime, event: str) -> None:
        spec = rt.spec
        now = self._clock()
        if rt.popen is None:
            # 外启同命令即收编，不重复拉起
            self._try_adopt(rt)
            if rt.state is ProcessState.RUNNING:
                return
        self._log_dir.mkdir(parents=True, exist_ok=True)
        output_path = self._log_dir / f"proc-{spec.name}.log"
        try:
            with output_path.open("ab") as output:
                popen = subprocess.Popen(
                    spec.argv,
                    cwd=spec.cwd,
                    stdin=subprocess.DEVNULL,
                    stdout=output,
                    stderr=subprocess.STDOUT,
                    creationflags=int(
                        getattr(subprocess, "CREATE_NO_WINDOW", 0)
                    ),
                )
        except OSError as error:
            rt.popen = None
            self._fail(rt, "start_error", error=str(error))
            return
        rt.popen = popen
        rt.state = ProcessState.RUNNING
        rt.started_at = now
        self._event(rt, event, pid=popen.pid)

    @staticmethod
    def _terminate(popen: subprocess.Popen[bytes]) -> None:
        """终止子进程，terminate 后 kill 兜底。"""
        popen.terminate()
        try:
            popen.wait(timeout=TERMINATE_WAIT_SECONDS)
        except subprocess.TimeoutExpired:
            popen.kill()
            try:
                popen.wait(timeout=TERMINATE_WAIT_SECONDS)
            except subprocess.TimeoutExpired:
                # 两度超时交由下轮判活
                pass

    def _heartbeat_time(self, spec: ProcessSpec) -> float | None:
        """读心跳清单最新时戳，无匹配返回空。"""
        if spec.heartbeat_glob is not None:
            newest: float | None = None
            for path in self._data_root.glob(spec.heartbeat_glob):
                try:
                    payload = json.loads(path.read_text(encoding="utf-8"))
                    stamp = path.stat().st_mtime
                except (OSError, json.JSONDecodeError):
                    continue
                if not isinstance(payload, dict) or payload.get("status") != "open":
                    continue
                if newest is None or stamp > newest:
                    newest = stamp
            return newest
        if spec.heartbeat_symbol is None:
            return None
        moment = datetime.fromtimestamp(self._clock(), UTC)
        legacy_newest: float | None = None
        for day in (moment, moment - timedelta(days=1)):
            directory = self._data_root / "raw" / day.strftime("%Y-%m-%d")
            if not directory.is_dir():
                continue
            for pattern in ("manifest-*.json", "checkpoint-*.json"):
                for path in directory.glob(pattern):
                    try:
                        payload = json.loads(path.read_text(encoding="utf-8"))
                        stamp = path.stat().st_mtime
                    except (OSError, json.JSONDecodeError):
                        continue
                    if not isinstance(payload, dict):
                        continue
                    if payload.get("record_symbol") != spec.heartbeat_symbol:
                        continue
                    if legacy_newest is None or stamp > legacy_newest:
                        legacy_newest = stamp
        return legacy_newest

    def _event(self, rt: _ProcessRuntime, event: str, **details: object) -> None:
        """事件追加运维日志，含时刻与 run 标识。"""
        now = self._clock()
        rt.last_event_at = now
        record: dict[str, object] = {
            "time": self._iso(now),
            "run_id": self.run_id,
            "process": rt.spec.name,
            "event": event,
            "state": rt.state.value,
        }
        record.update(details)
        self._log_dir.mkdir(parents=True, exist_ok=True)
        path = self._log_dir / EVENTS_FILE_NAME
        with path.open("a", encoding="utf-8") as fh:
            fh.write(json.dumps(record, ensure_ascii=False) + "\n")

    def _row(self, rt: _ProcessRuntime) -> dict[str, object]:
        popen = rt.popen
        heartbeat = self._heartbeat_time(rt.spec)
        pid = rt.external_pid if popen is None else popen.pid
        return {
            "name": rt.spec.name,
            "status": rt.state.value,
            "pid": pid,
            "external": rt.external_pid is not None and popen is None,
            "started_at": (
                None if rt.started_at is None else self._iso(rt.started_at)
            ),
            "heartbeat_at": (
                None if heartbeat is None else self._iso(heartbeat)
            ),
            "restart_count": rt.restart_count,
            "consecutive_failures": rt.consecutive_failures,
            "last_exit_code": rt.last_exit_code,
            "last_event_at": (
                None if rt.last_event_at is None else self._iso(rt.last_event_at)
            ),
            "resume_at": None if rt.resume_at is None else self._iso(rt.resume_at),
            "auto_restart": rt.spec.auto_restart,
        }

    @staticmethod
    def _iso(stamp: float) -> str:
        return datetime.fromtimestamp(stamp, UTC).isoformat()

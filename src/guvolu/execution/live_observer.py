"""live 伴随观察进程：上膛期间的零写实时监视（执行链设计第 14 节）。

live 执行器每小时一轮 REST 闭环，两轮之间没有进程监视在途
委托与持仓；本进程以 READ_ONLY 周期巡视补上这段盲区。全程
零写：不导入 TRADE 客户端，不写意图账本，不改任何执行状态。
意图账本以宽容方式只读扫描——只解析完整行、忽略尾部不完整
行、绝不隔离或截断，避免与执行器的单写边界冲突（T-05）。

每轮告警判定：超龄在途挂单、卡滞在途意图（SENDING 或
SEND_TIMEOUT）、持仓名义超信封上限、信封熔断或暂停状态。
观察逐轮追加 JSONL 并写心跳文件；发现告警只留痕与提示，
处置动作留给人工（kill-switch 见 scripts/run_kill_switch.ps1）。
命令行入口即本模块；--once 单轮运行，有告警退出码 1。
"""
from __future__ import annotations

import argparse
import json
import time
from collections.abc import Mapping, Sequence
from dataclasses import dataclass
from datetime import UTC, datetime
from decimal import Decimal
from pathlib import Path

from guvolu.api.public_client import PublicClient
from guvolu.api.read_client import ReadClient
from guvolu.data.durable_io import atomic_write_text, durable_append_bytes
from guvolu.data.paths import data_root
from guvolu.domain.config import load_config
from guvolu.domain.intent import IN_FLIGHT_STATES, IntentState
from guvolu.domain.models import Asset
from guvolu.execution.authorization_envelope import (
    DEFAULT_ENVELOPE_PATH,
    AuthorizationEnvelope,
    EnvelopeState,
    EnvelopeStateStore,
    load_envelope,
)
from guvolu.execution.live_executor import (
    LIVE_LEDGER_NAME,
    LIVE_RELATIVE_DIR,
)

# 观察落盘的 schema 版本
OBSERVER_SCHEMA_VERSION = 1
# 数据根下的缺省落盘位置
OBSERVATION_RELATIVE_PATH = LIVE_RELATIVE_DIR / "observer.jsonl"
HEARTBEAT_RELATIVE_PATH = LIVE_RELATIVE_DIR / "observer_heartbeat.json"
STOP_FILE_RELATIVE_PATH = LIVE_RELATIVE_DIR / "observer.stop"
# 执行器一轮的等待与撤单确认加余量
DEFAULT_STALE_AGE_SECONDS = 420.0
DEFAULT_INTERVAL_SECONDS = 60.0
# JPY 资产键名
_JPY = "JPY"


def _btc_amount(assets: Sequence[Asset], symbol: str) -> Decimal:
    for asset in assets:
        if asset.symbol == symbol:
            return asset.amount
    return Decimal("0")


def scan_ledger_pending(
    path: Path, *, now: datetime, stale_age_seconds: float
) -> tuple[list[dict[str, object]], int]:
    """宽容只读扫描账本，返回超龄在途意图与总行数。

    只解析完整行；尾部不完整行与非法行一律忽略，不隔离、
    不截断、不写任何字节（与 IntentLedger 装载器不同，规避
    与执行器并发时的破坏性恢复）。
    """
    if not path.exists():
        return [], 0
    raw = path.read_bytes()
    cut = raw.rfind(b"\n") + 1
    states: dict[str, tuple[str, str]] = {}
    lines = 0
    for blob in raw[:cut].split(b"\n")[:-1]:
        lines += 1
        try:
            parsed: object = json.loads(blob)
        except json.JSONDecodeError:
            continue
        if not isinstance(parsed, dict):
            continue
        kind = parsed.get("record")
        moment = str(parsed.get("at", ""))
        intent_id = str(parsed.get("intent_id", ""))
        if not intent_id:
            continue
        if kind == "intent":
            states[intent_id] = (IntentState.RECORDED.value, moment)
        elif kind == "transition":
            states[intent_id] = (str(parsed.get("target", "")), moment)
    pending: list[dict[str, object]] = []
    inflight = {state.value for state in IN_FLIGHT_STATES}
    for intent_id, (state, moment) in states.items():
        if state not in inflight:
            continue
        try:
            at = datetime.fromisoformat(moment)
        except ValueError:
            at = None
        age = None if at is None else (now - at).total_seconds()
        if age is None or age >= stale_age_seconds:
            pending.append({
                "intent_id": intent_id,
                "state": state,
                "at": moment,
                "age_seconds": age,
            })
    return pending, lines


@dataclass(frozen=True, slots=True)
class ObservationCycle:
    """一轮观察的结论与告警集合。"""

    record: dict[str, object]
    alerts: tuple[str, ...]


def observe_once(
    *,
    reader: ReadClient,
    public: PublicClient,
    envelope: AuthorizationEnvelope,
    state: EnvelopeState,
    ledger_path: Path,
    now: datetime,
    stale_age_seconds: float = DEFAULT_STALE_AGE_SECONDS,
) -> ObservationCycle:
    """执行一轮零写观察，产出记录与告警。"""
    alerts: list[str] = []
    symbols = sorted(str(symbol) for symbol in envelope.symbols)
    active_view: list[dict[str, object]] = []
    position_view: dict[str, str] = {}
    assets = reader.assets()
    for symbol in symbols:
        for order in reader.active_orders(symbol):
            age = (now - order.timestamp).total_seconds()
            active_view.append({
                "order_id": order.order_id,
                "symbol": order.symbol,
                "status": order.status.value,
                "size": format(order.size, "f"),
                "executed_size": format(order.executed_size, "f"),
                "timestamp": order.timestamp.isoformat(),
                "age_seconds": age,
            })
            if age >= stale_age_seconds:
                alerts.append(
                    f"挂单 {order.order_id} 已存续 {age:.0f} 秒，"
                    "超出执行器单轮闭环时限，须人工核查"
                )
        tickers = public.ticker(symbol)
        if tickers:
            price = tickers[0].last
            notional = _btc_amount(assets, symbol) * price
            position_view[symbol] = format(notional, "f")
            if notional > envelope.max_position_jpy:
                alerts.append(
                    f"品种 {symbol} 持仓名义 {notional} JPY 超信封"
                    f"上限 {envelope.max_position_jpy} JPY"
                )
        else:
            alerts.append(f"品种 {symbol} 无最新レート，无法估值")
    pending, ledger_lines = scan_ledger_pending(
        ledger_path, now=now, stale_age_seconds=stale_age_seconds
    )
    for item in pending:
        alerts.append(
            f"意图 {item['intent_id']} 停在 {item['state']}"
            "，超龄未收敛，须按 T-06 人工对账"
        )
    if state.tripped_at is not None:
        alerts.append(
            f"信封已于 {state.tripped_at.isoformat()} 熔断锁定:"
            f" {state.trip_reason}"
        )
    record: dict[str, object] = {
        "schema_version": OBSERVER_SCHEMA_VERSION,
        "record": "observation",
        "at": now.isoformat(),
        "envelope_sha256": envelope.sha256,
        "active_orders": active_view,
        "position_notional_jpy": position_view,
        "pending_intents": pending,
        "ledger_lines": ledger_lines,
        "envelope_tripped_at": (
            None if state.tripped_at is None
            else state.tripped_at.isoformat()
        ),
        "envelope_paused_until": (
            None if state.paused_until is None
            else state.paused_until.isoformat()
        ),
        "alerts": alerts,
        "status": "alert" if alerts else "ok",
    }
    return ObservationCycle(record=record, alerts=tuple(alerts))


def _append_jsonl(path: Path, record: Mapping[str, object]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    line = json.dumps(record, ensure_ascii=False, sort_keys=True)
    durable_append_bytes(path, (line + "\n").encode("utf-8"))


def _write_heartbeat(path: Path, *, now: datetime, status: str) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    atomic_write_text(path, json.dumps({
        "schema_version": OBSERVER_SCHEMA_VERSION,
        "at": now.isoformat(),
        "status": status,
    }, ensure_ascii=False) + "\n")


def build_parser() -> argparse.ArgumentParser:
    """构造命令行参数定义。"""
    parser = argparse.ArgumentParser(
        description="live 伴随观察：上膛期间的零写实时监视"
    )
    parser.add_argument(
        "--envelope", type=Path, default=DEFAULT_ENVELOPE_PATH,
        help="授权信封路径，用于持仓上限与状态观察",
    )
    parser.add_argument(
        "--ledger", type=Path, default=None,
        help="live 意图账本路径；缺省数据根 execution/live 下",
    )
    parser.add_argument(
        "--interval-seconds", type=float,
        default=DEFAULT_INTERVAL_SECONDS,
    )
    parser.add_argument(
        "--stale-age-seconds", type=float,
        default=DEFAULT_STALE_AGE_SECONDS,
        help="挂单或在途意图判为超龄的秒数",
    )
    parser.add_argument(
        "--once", action="store_true",
        help="只跑一轮；有告警退出码 1",
    )
    parser.add_argument("--env-file", type=Path, default=None)
    return parser


def main(argv: Sequence[str] | None = None) -> int:
    """命令行入口。READ_ONLY 巡视循环，停止走标记文件。"""
    args = build_parser().parse_args(argv)
    env_file: Path | None = args.env_file
    config = load_config(env_file)
    envelope = load_envelope(
        Path(args.envelope), whitelist=config.spot_whitelist
    )
    state_store = EnvelopeStateStore.for_envelope(envelope)
    reader = ReadClient.from_config(config)
    public = PublicClient.from_config(config)
    root = data_root()
    ledger_arg: Path | None = args.ledger
    ledger_path = (
        ledger_arg if ledger_arg is not None
        else root / LIVE_RELATIVE_DIR / LIVE_LEDGER_NAME
    )
    observation_path = root / OBSERVATION_RELATIVE_PATH
    heartbeat_path = root / HEARTBEAT_RELATIVE_PATH
    stop_path = root / STOP_FILE_RELATIVE_PATH
    while True:
        now = datetime.now(UTC)
        cycle = observe_once(
            reader=reader,
            public=public,
            envelope=envelope,
            state=state_store.load(),
            ledger_path=ledger_path,
            now=now,
            stale_age_seconds=float(args.stale_age_seconds),
        )
        _append_jsonl(observation_path, cycle.record)
        _write_heartbeat(
            heartbeat_path, now=now, status=str(cycle.record["status"])
        )
        for alert in cycle.alerts:
            print(f"告警: {alert}")
        if args.once:
            return 1 if cycle.alerts else 0
        if stop_path.exists():
            print("发现停止标记文件，观察循环退出")
            return 0
        time.sleep(float(args.interval_seconds))


if __name__ == "__main__":
    raise SystemExit(main())

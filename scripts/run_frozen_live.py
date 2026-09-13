"""冻结预测的 live 串联：dry-run 校验通过后进入信封约束执行。

复用 run_frozen_shadow 的刷新、预测、dry-run 与 paper 步骤
（导入函数，不复制）；随后以 live 模式目标调用执行仓的
run_live_executor.py（参数含来源预测血缘）。live 步骤失败不
影响预测登记，命令行返回非零码。链内 dry-run 与 paper 子进程
保持缺省模式；GUVOLU_MODE=live 仅注入 live 执行器这一个子
进程（T-04 缺省不实盘）。本脚本唯一入口是维护者按上膛协议
亲自注册的 -live 任务（A-01，执行链设计第 14 节）。
"""
from __future__ import annotations

import argparse
import json
import os
from datetime import UTC, datetime
from pathlib import Path
from typing import Sequence

from run_frozen_shadow import (
    DEFAULT_MAX_PREDICTION_AGE_MINUTES,
    DEFAULT_STALE_RETRY_COUNT,
    DEFAULT_STALE_RETRY_WAIT_SECONDS,
    PAPER_CONFIG,
    _adapt_target,
    _append_record,
    _object,
    _run,
    _text,
    resolve_target_config,
    run_shadow,
)

# 执行仓内 live 相对路径
LIVE_MODE = "live"
LIVE_REPORT_ROOT = "data/execution/live/reports"
LIVE_TASK_LOG = "data/execution/live/frozen-forward/task.jsonl"


def _validate_live_report(
    report: dict[str, object], prediction_id: str
) -> dict[str, object]:
    """校验 live 报告：live 模式与预测身份。"""
    if report.get("mode") != LIVE_MODE:
        raise ValueError("live report 不是 live 模式")
    artifact = _object(report.get("artifact"), "live report artifact")
    if artifact.get("run_id") != prediction_id:
        raise ValueError("live report 预测身份不符")
    return report


def run_live_step(
    execution: Path,
    exec_python: Path,
    prediction_path: Path,
    prediction_id: str,
    *,
    market_id: str,
    symbol: str,
    prediction_sha: str,
    target_config: str = PAPER_CONFIG,
) -> dict[str, object]:
    """适配 live 目标并调用执行仓 live 执行器。

    预算以执行仓版本化配置为准（G-06），不经命令行覆盖。同一
    预测的报告已存在时复用，不重复发送。执行器正当拒绝记为
    status=refused；任何失败只记为 status=failed，不抛出，
    不影响前序登记结果。
    """
    live: dict[str, object] = {"status": "failed"}
    try:
        target_path = _adapt_target(
            execution, exec_python, prediction_path,
            market_id=market_id, symbol=symbol, mode=LIVE_MODE,
            budget_jpy=None, target_config=target_config,
        )
        live["target_path"] = str(target_path)
        report_path = execution / LIVE_REPORT_ROOT / f"{prediction_id}.json"
        live["report_path"] = str(report_path)
        reused = report_path.is_file()
        returncode: int | None = None
        if not reused:
            report_path.parent.mkdir(parents=True, exist_ok=True)
            # live 模式仅注入本子进程（T-04）
            live_env = dict(os.environ)
            live_env["GUVOLU_MODE"] = LIVE_MODE
            result = _run(
                (
                    str(exec_python),
                    str(execution / "scripts/run_live_executor.py"),
                    "--target", str(target_path),
                    "--target-config",
                    str(resolve_target_config(execution, target_config)),
                    "--source-prediction", str(prediction_path),
                    "--source-prediction-sha256", prediction_sha,
                    "--report", str(report_path),
                ),
                cwd=execution,
                env=live_env,
            )
            returncode = result.returncode
            if not report_path.is_file():
                detail = (result.stderr or result.stdout).strip()
                raise RuntimeError(
                    f"live 执行失败({result.returncode}): {detail}"
                )
        parsed = _object(
            json.loads(report_path.read_text(encoding="utf-8")),
            "live report",
        )
        if parsed.get("kind") == "live_refusal_report":
            # 正当拒绝不算失败（第 14 节）
            live.update({
                "status": "refused",
                "returncode": returncode,
                "refusal_status": parsed.get("status"),
                "detail": parsed.get("detail"),
            })
            return live
        report = _validate_live_report(parsed, prediction_id)
        live.update({
            "status": "reused" if reused else "completed",
            "returncode": returncode,
            "gate_verdict": report.get("gate_verdict"),
            "resolution": report.get("resolution"),
            "final_order_status": report.get("final_order_status"),
            "write_touched": _object(
                report.get("endpoints"), "live report endpoints",
            ).get("write_touched"),
            "envelope_sha256": _object(
                report.get("envelope"), "live report envelope",
            ).get("sha256"),
        })
        if returncode not in (None, 0):
            live["status"] = "failed"
            live["error"] = f"live 执行器返回非零码: {returncode}"
    except Exception as exc:
        live["status"] = "failed"
        live["error"] = f"{type(exc).__name__}: {exc}"
    return live


def run_live(
    repository: Path,
    runtime_root: Path,
    execution_repository: Path,
    plan_id: str,
    market_id: str,
    *,
    symbol: str = "BTC",
    budget_jpy: str = "15000",
    max_prediction_age_minutes: int = DEFAULT_MAX_PREDICTION_AGE_MINUTES,
    paper_enabled: bool = True,
    target_config: str = PAPER_CONFIG,
    stale_retry_count: int = DEFAULT_STALE_RETRY_COUNT,
    stale_retry_wait_seconds: float = DEFAULT_STALE_RETRY_WAIT_SECONDS,
) -> dict[str, object]:
    """串联 shadow 全流程后执行信封约束下的 live 步骤。

    dry-run 报告校验失败会在 run_shadow 内抛出，live 步骤不会
    开始；live 步骤自身失败记入 live 字段，预测与 dry-run 登记
    不受影响。
    """
    execution = execution_repository.resolve()
    summary = run_shadow(
        repository, runtime_root, execution_repository, plan_id, market_id,
        symbol=symbol, budget_jpy=budget_jpy,
        max_prediction_age_minutes=max_prediction_age_minutes,
        paper_enabled=paper_enabled, target_config=target_config,
        stale_retry_count=stale_retry_count,
        stale_retry_wait_seconds=stale_retry_wait_seconds,
    )
    prediction_id = _text(summary.get("prediction_id"), "prediction_id")
    prediction_path = Path(_text(
        summary.get("prediction_path"), "prediction_path",
    ))
    prediction_sha = _text(
        summary.get("prediction_sha256"), "prediction_sha256",
    )
    exec_python = execution / ".venv/Scripts/python.exe"
    live = run_live_step(
        execution, exec_python, prediction_path, prediction_id,
        market_id=market_id, symbol=symbol, prediction_sha=prediction_sha,
        target_config=target_config,
    )
    summary["live"] = live
    _append_record(execution / LIVE_TASK_LOG, {
        "recorded_at": datetime.now(UTC).isoformat(),
        "plan_id": plan_id,
        "prediction_id": prediction_id,
        "live": live,
    })
    return summary


def main(argv: Sequence[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description="冻结目标每小时 live 串联")
    parser.add_argument("--repository", type=Path, required=True)
    parser.add_argument("--runtime-root", type=Path, required=True)
    parser.add_argument("--execution-repository", type=Path, required=True)
    parser.add_argument("--plan-id", required=True)
    parser.add_argument("--market-id", default="mkt__gmo__btc__r0")
    parser.add_argument("--symbol", default="BTC")
    parser.add_argument("--budget-jpy", default="15000")
    parser.add_argument(
        "--max-prediction-age-minutes", type=int,
        default=DEFAULT_MAX_PREDICTION_AGE_MINUTES,
        help="进入执行适配前允许的最大预测年龄；缺省 55 分钟",
    )
    parser.add_argument(
        "--no-paper", action="store_true", help="跳过 paper 执行步骤",
    )
    parser.add_argument(
        "--target-config", default=PAPER_CONFIG,
        help="执行仓内目标配置相对路径；缺省 config/paper_executor.json",
    )
    parser.add_argument(
        "--stale-retry-count", type=int, default=DEFAULT_STALE_RETRY_COUNT,
        help="预测过期时重刷重预测的次数；缺省 2",
    )
    parser.add_argument(
        "--stale-retry-wait-seconds", type=float,
        default=DEFAULT_STALE_RETRY_WAIT_SECONDS,
        help="每次重试前等待秒数；缺省 240",
    )
    args = parser.parse_args(argv)
    summary = run_live(
        args.repository, args.runtime_root, args.execution_repository,
        str(args.plan_id), str(args.market_id), symbol=str(args.symbol),
        budget_jpy=str(args.budget_jpy),
        max_prediction_age_minutes=int(args.max_prediction_age_minutes),
        paper_enabled=not bool(args.no_paper),
        target_config=str(args.target_config),
        stale_retry_count=int(args.stale_retry_count),
        stale_retry_wait_seconds=float(args.stale_retry_wait_seconds),
    )
    print(json.dumps(summary, ensure_ascii=False, sort_keys=True))
    exit_code = 0
    paper = summary.get("paper")
    if isinstance(paper, dict) and paper.get("status") == "failed":
        exit_code = 1
    live = summary.get("live")
    if isinstance(live, dict) and live.get("status") == "failed":
        exit_code = 1
    return exit_code


if __name__ == "__main__":
    raise SystemExit(main())

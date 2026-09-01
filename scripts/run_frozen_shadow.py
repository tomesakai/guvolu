"""刷新冻结运行根，生成预测并在独立执行仓完成 dry-run 与 paper。"""
from __future__ import annotations

import argparse
import hashlib
import json
import os
import subprocess
import sys
from datetime import UTC, datetime, timedelta
from pathlib import Path
from typing import Mapping, Sequence

from refresh_frozen_runtime import refresh_runtime

# 执行仓内 paper 相对路径
PAPER_CONFIG = "config/paper_executor.json"
PAPER_LEDGER_ROOT = "data"
PAPER_ROOT = "data/execution/paper"
TARGET_DIRECTORY = "data/execution/targets"
PAPER_MODE = "paper"
DRY_RUN_MODE = "dry-run"
DEFAULT_MAX_PREDICTION_AGE_MINUTES = 55
# 去重与待对账报告不含 mode
PAPER_STATUSES_WITHOUT_ROW = frozenset({
    "duplicate_prediction", "needs_reconciliation",
})
PAPER_FAILURE_OUTCOMES = frozenset({"needs_reconciliation"})


def _object(value: object, name: str) -> dict[str, object]:
    if not isinstance(value, dict):
        raise ValueError(f"{name} 不是 JSON 对象")
    return {str(key): item for key, item in value.items()}


def _text(value: object, name: str) -> str:
    if not isinstance(value, str) or not value:
        raise ValueError(f"{name} 缺失")
    return value


def _json_stdout(result: subprocess.CompletedProcess[str], name: str) -> dict[str, object]:
    if result.returncode != 0:
        detail = (result.stderr or result.stdout).strip()
        raise RuntimeError(f"{name} 失败({result.returncode}): {detail}")
    lines = [line for line in result.stdout.splitlines() if line.strip()]
    if not lines:
        raise RuntimeError(f"{name} 没有 JSON 输出")
    return _object(json.loads(lines[-1]), name)


def _run(
    command: Sequence[str], *, cwd: Path, env: Mapping[str, str] | None = None,
) -> subprocess.CompletedProcess[str]:
    child_env = dict(os.environ if env is None else env)
    child_env["PYTHONUTF8"] = "1"
    child_env["PYTHONIOENCODING"] = "utf-8"
    return subprocess.run(
        list(command), cwd=cwd, env=child_env,
        capture_output=True, text=True, encoding="utf-8", check=False,
    )


def _append_record(path: Path, record: Mapping[str, object]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    with path.open("a", encoding="utf-8", newline="\n") as stream:
        stream.write(json.dumps(
            dict(record), ensure_ascii=False, separators=(",", ":"), sort_keys=True,
        ) + "\n")
        stream.flush()
        os.fsync(stream.fileno())


def _validate_report(path: Path, prediction_id: str) -> dict[str, object]:
    report = _object(json.loads(path.read_text(encoding="utf-8")), "shadow report")
    artifact = _object(report.get("artifact"), "shadow report artifact")
    endpoints = _object(report.get("endpoints"), "shadow report endpoints")
    if report.get("mode") != DRY_RUN_MODE:
        raise ValueError("shadow report 不是 dry-run")
    if artifact.get("run_id") != prediction_id:
        raise ValueError("shadow report 预测身份不符")
    if endpoints.get("write_touched") != []:
        raise ValueError("shadow report 触及了写端点")
    return report


def _validate_paper_report(path: Path, prediction_id: str) -> dict[str, object]:
    """校验 paper 报告：paper 模式、预测身份、零写端点。"""
    report = _object(json.loads(path.read_text(encoding="utf-8")), "paper report")
    endpoints = _object(report.get("endpoints"), "paper report endpoints")
    mode = report.get("mode")
    # 无差异行的报告不带 mode
    if mode is None and report.get("status") in PAPER_STATUSES_WITHOUT_ROW:
        mode = PAPER_MODE
    if mode != PAPER_MODE:
        raise ValueError("paper report 不是 paper 模式")
    if report.get("prediction_id") != prediction_id:
        raise ValueError("paper report 预测身份不符")
    if endpoints.get("write_touched") != []:
        raise ValueError("paper report 触及了写端点")
    return report


def _adapt_target(
    execution: Path,
    exec_python: Path,
    prediction_path: Path,
    *,
    market_id: str,
    symbol: str,
    mode: str,
    budget_jpy: str | None,
) -> Path:
    """经执行仓适配器生成内容寻址目标快照。

    预算显式给出时覆盖 paper 配置；paper 模式不传预算，
    预算以配置为准（G-06）。
    """
    command = [
        str(exec_python), str(execution / "scripts/adapt_frozen_target.py"),
        "--prediction", str(prediction_path),
        "--output-directory", str(execution / TARGET_DIRECTORY),
        "--config", str(execution / PAPER_CONFIG),
        "--market-id", market_id, "--symbol", symbol, "--mode", mode,
    ]
    if budget_jpy is not None:
        command.extend(("--risk-budget-jpy", budget_jpy))
    adapted = _json_stdout(_run(command, cwd=execution), f"{mode} target adapter")
    expected = f"ready_for_{mode.replace('-', '_')}"
    if adapted.get("status") != expected:
        raise ValueError(f"{mode} 目标状态不符: {adapted.get('status')!r}")
    target_path = Path(_text(adapted.get("path"), "target path")).resolve()
    if not target_path.is_relative_to(execution) or not target_path.is_file():
        raise ValueError("执行目标路径越界")
    return target_path


def _paper_summary(
    report: Mapping[str, object],
    *,
    status: str,
    target_path: Path,
    report_path: Path,
    returncode: int | None,
) -> dict[str, object]:
    """提取 paper 报告的终态、模型成交与成本摘要。"""
    endpoints = _object(report.get("endpoints"), "paper report endpoints")
    intent = report.get("intent")
    return {
        "status": status,
        "target_path": str(target_path),
        "report_path": str(report_path),
        "returncode": returncode,
        "outcome": report.get("status"),
        "intent_state": (
            None if intent is None else _object(intent, "paper intent").get("state")
        ),
        "position_after": report.get("position_after"),
        "fill": report.get("fill"),
        "cost": report.get("cost"),
        "read_touched": endpoints.get("read_touched"),
        "write_touched": endpoints.get("write_touched"),
    }


def run_paper_step(
    execution: Path,
    exec_python: Path,
    prediction_path: Path,
    prediction_id: str,
    *,
    market_id: str,
    symbol: str,
    prediction_sha: str,
) -> dict[str, object]:
    """以独立 paper 目标运行 paper 执行器并校验零写报告。

    账目与报告固定在执行仓 data/execution/paper/，与 shadow 账本分离。
    同一预测的报告已存在时复用；执行器自身按认领账去重。
    任何失败只记为 status=failed，不抛出，不影响前序结果。
    """
    paper: dict[str, object] = {"status": "failed"}
    try:
        target_path = _adapt_target(
            execution, exec_python, prediction_path,
            market_id=market_id, symbol=symbol, mode=PAPER_MODE, budget_jpy=None,
        )
        paper["target_path"] = str(target_path)
        report_path = execution / PAPER_ROOT / "reports" / f"{prediction_id}.json"
        paper["report_path"] = str(report_path)
        reused = report_path.is_file()
        returncode: int | None = None
        if not reused:
            result = _run(
                (
                    str(exec_python), str(execution / "scripts/run_paper_executor.py"),
                    "--target", str(target_path),
                    "--config", str(execution / PAPER_CONFIG),
                    "--ledger-root", str(execution / PAPER_LEDGER_ROOT),
                    "--report", str(report_path),
                    "--source-prediction", str(prediction_path),
                    "--source-prediction-sha256", prediction_sha,
                ),
                cwd=execution,
            )
            returncode = result.returncode
            if not report_path.is_file():
                detail = (result.stderr or result.stdout).strip()
                raise RuntimeError(f"paper 执行失败({result.returncode}): {detail}")
        report = _validate_paper_report(report_path, prediction_id)
        paper.update(_paper_summary(
            report, status="reused" if reused else "completed",
            target_path=target_path, report_path=report_path, returncode=returncode,
        ))
        if returncode not in (None, 0):
            paper["status"] = "failed"
            paper["error"] = f"paper 执行器返回非零码: {returncode}"
        elif paper.get("outcome") in PAPER_FAILURE_OUTCOMES:
            paper["status"] = "failed"
            paper["error"] = f"paper 终态需要人工处置: {paper['outcome']}"
    except Exception as exc:
        paper["status"] = "failed"
        paper["error"] = f"{type(exc).__name__}: {exc}"
    return paper


def run_shadow(
    repository: Path,
    runtime_root: Path,
    execution_repository: Path,
    plan_id: str,
    market_id: str,
    *,
    symbol: str = "BTC",
    budget_jpy: str = "500",
    max_prediction_age_minutes: int = DEFAULT_MAX_PREDICTION_AGE_MINUTES,
    paper_enabled: bool = True,
) -> dict[str, object]:
    """串联快照、冻结预测、目标适配、零写彩排与 paper 执行。

    paper 步骤在 dry-run 报告校验通过后运行，结果记入 paper 字段；
    其失败不改变预测与 dry-run 的登记结果，但 CLI 必须返回非零码。
    """
    started = datetime.now(UTC)
    source_root = repository.resolve()
    runtime = runtime_root.resolve()
    execution = execution_repository.resolve()
    task_log = execution / "data/execution/shadow/frozen-forward/task.jsonl"
    try:
        refresh = refresh_runtime(source_root / "data", runtime, market_id)
        prediction_env = dict(os.environ)
        prediction_env["PYTHONPATH"] = str(runtime / "src")
        prediction = _json_stdout(_run(
            (
                sys.executable,
                str(runtime / "scripts/manage_frozen_forward.py"),
                "--root", str(runtime), "predict", plan_id,
                "--registry", str(runtime / "data/research/governance.sqlite3"),
            ),
            cwd=runtime,
            env=prediction_env,
        ), "frozen prediction")
        prediction_id = _text(prediction.get("prediction_id"), "prediction_id")
        prediction_path = Path(_text(
            prediction.get("prediction_path"), "prediction_path",
        )).resolve()
        if not prediction_path.is_relative_to(runtime) or not prediction_path.is_file():
            raise ValueError("预测路径越出冻结运行根")
        # 编排侧固定来源预测散列（v2 血缘）
        prediction_sha = hashlib.sha256(
            prediction_path.read_bytes()
        ).hexdigest()
        decision_time = datetime.fromisoformat(_text(
            prediction.get("decision_time"), "decision_time",
        )).astimezone(UTC)
        age = datetime.now(UTC) - decision_time
        if age < timedelta(0) or age > timedelta(minutes=max_prediction_age_minutes):
            raise ValueError(f"冻结预测过期: {age.total_seconds():.1f}s")

        exec_python = execution / ".venv/Scripts/python.exe"
        target_path = _adapt_target(
            execution, exec_python, prediction_path,
            market_id=market_id, symbol=symbol, mode=DRY_RUN_MODE,
            budget_jpy=budget_jpy,
        )
        shadow_root = execution / "data/execution/shadow/frozen-forward"
        report_path = shadow_root / "reports" / f"{prediction_id}.json"
        ledger_path = shadow_root / "intent_ledger.jsonl"
        reused = report_path.is_file()
        if reused:
            report = _validate_report(report_path, prediction_id)
        else:
            result = _run(
                (
                    str(exec_python), str(execution / "scripts/run_dry_run_executor.py"),
                    "--target", str(target_path), "--symbol", symbol,
                    "--budget-jpy", budget_jpy, "--ledger", str(ledger_path),
                    "--dry-run-report", str(report_path),
                    "--source-prediction", str(prediction_path),
                    "--source-prediction-sha256", prediction_sha,
                ),
                cwd=execution,
            )
            if result.returncode != 0:
                detail = (result.stderr or result.stdout).strip()
                raise RuntimeError(f"dry-run 失败({result.returncode}): {detail}")
            report = _validate_report(report_path, prediction_id)
        # paper 步骤失败不外抛
        paper: dict[str, object]
        if paper_enabled:
            paper = run_paper_step(
                execution, exec_python, prediction_path, prediction_id,
                market_id=market_id, symbol=symbol,
                prediction_sha=prediction_sha,
            )
        else:
            paper = {"status": "skipped", "reason": "--no-paper"}
        summary: dict[str, object] = {
            "status": "reused" if reused else "completed",
            "started_at": started.isoformat(),
            "completed_at": datetime.now(UTC).isoformat(),
            "plan_id": plan_id,
            "prediction_id": prediction_id,
            "prediction_path": str(prediction_path),
            "prediction_sha256": prediction_sha,
            "decision_time": decision_time.isoformat(),
            "prediction_age_seconds": round(age.total_seconds(), 3),
            "aggregate_target": prediction.get("aggregate_target"),
            "target_path": str(target_path),
            "report_path": str(report_path),
            "intent_state": (
                None if report.get("intent") is None
                else _object(report["intent"], "intent").get("state")
            ),
            "write_touched": _object(
                report["endpoints"], "endpoints",
            ).get("write_touched"),
            "paper": paper,
            "refresh": refresh,
        }
        _append_record(task_log, summary)
        return summary
    except BaseException as exc:
        _append_record(task_log, {
            "status": "failed", "started_at": started.isoformat(),
            "completed_at": datetime.now(UTC).isoformat(), "plan_id": plan_id,
            "error": f"{type(exc).__name__}: {exc}",
        })
        raise


def main(argv: Sequence[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description="冻结目标每小时 shadow 串联")
    parser.add_argument("--repository", type=Path, required=True)
    parser.add_argument("--runtime-root", type=Path, required=True)
    parser.add_argument("--execution-repository", type=Path, required=True)
    parser.add_argument("--plan-id", required=True)
    parser.add_argument("--market-id", default="mkt__gmo__btc__r0")
    parser.add_argument("--symbol", default="BTC")
    parser.add_argument("--budget-jpy", default="500")
    parser.add_argument(
        "--max-prediction-age-minutes", type=int,
        default=DEFAULT_MAX_PREDICTION_AGE_MINUTES,
        help="进入执行适配前允许的最大预测年龄；缺省 55 分钟，预留过期缓冲",
    )
    parser.add_argument(
        "--no-paper", action="store_true", help="跳过 paper 执行步骤",
    )
    args = parser.parse_args(argv)
    summary = run_shadow(
        args.repository, args.runtime_root, args.execution_repository,
        str(args.plan_id), str(args.market_id), symbol=str(args.symbol),
        budget_jpy=str(args.budget_jpy),
        max_prediction_age_minutes=int(args.max_prediction_age_minutes),
        paper_enabled=not bool(args.no_paper),
    )
    print(json.dumps(summary, ensure_ascii=False, sort_keys=True))
    paper = summary.get("paper")
    if isinstance(paper, dict) and paper.get("status") == "failed":
        return 1
    return 0


if __name__ == "__main__":
    raise SystemExit(main())

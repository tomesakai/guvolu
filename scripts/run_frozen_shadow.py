"""刷新冻结运行根，生成预测并在独立执行仓完成 dry-run。"""
from __future__ import annotations

import argparse
import json
import os
import subprocess
import sys
from datetime import UTC, datetime, timedelta
from pathlib import Path
from typing import Mapping, Sequence

from refresh_frozen_runtime import refresh_runtime  # type: ignore[import-not-found]


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
    if report.get("mode") != "dry-run":
        raise ValueError("shadow report 不是 dry-run")
    if artifact.get("run_id") != prediction_id:
        raise ValueError("shadow report 预测身份不符")
    if endpoints.get("write_touched") != []:
        raise ValueError("shadow report 触及了写端点")
    return report


def run_shadow(
    repository: Path,
    runtime_root: Path,
    execution_repository: Path,
    plan_id: str,
    market_id: str,
    *,
    symbol: str = "BTC",
    budget_jpy: str = "500",
    max_prediction_age_minutes: int = 90,
) -> dict[str, object]:
    """串联快照、冻结预测、目标适配和零写彩排。"""
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
        decision_time = datetime.fromisoformat(_text(
            prediction.get("decision_time"), "decision_time",
        )).astimezone(UTC)
        age = datetime.now(UTC) - decision_time
        if age < timedelta(0) or age > timedelta(minutes=max_prediction_age_minutes):
            raise ValueError(f"冻结预测过期: {age.total_seconds():.1f}s")

        exec_python = execution / ".venv/Scripts/python.exe"
        adapted = _json_stdout(_run(
            (
                str(exec_python), str(execution / "scripts/adapt_frozen_target.py"),
                "--prediction", str(prediction_path),
                "--output-directory", str(execution / "data/execution/targets"),
                "--market-id", market_id,
            ),
            cwd=execution,
        ), "target adapter")
        target_path = Path(_text(adapted.get("path"), "target path")).resolve()
        if not target_path.is_relative_to(execution) or not target_path.is_file():
            raise ValueError("执行目标路径越界")
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
                ),
                cwd=execution,
            )
            if result.returncode != 0:
                detail = (result.stderr or result.stdout).strip()
                raise RuntimeError(f"dry-run 失败({result.returncode}): {detail}")
            report = _validate_report(report_path, prediction_id)
        summary: dict[str, object] = {
            "status": "reused" if reused else "completed",
            "started_at": started.isoformat(),
            "completed_at": datetime.now(UTC).isoformat(),
            "plan_id": plan_id,
            "prediction_id": prediction_id,
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
    parser.add_argument("--max-prediction-age-minutes", type=int, default=90)
    args = parser.parse_args(argv)
    summary = run_shadow(
        args.repository, args.runtime_root, args.execution_repository,
        str(args.plan_id), str(args.market_id), symbol=str(args.symbol),
        budget_jpy=str(args.budget_jpy),
        max_prediction_age_minutes=int(args.max_prediction_age_minutes),
    )
    print(json.dumps(summary, ensure_ascii=False, sort_keys=True))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())

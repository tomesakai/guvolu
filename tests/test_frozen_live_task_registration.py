"""冻结 live 计划任务注册脚本与 live 串联的离线校验。

注册脚本本身由维护者按上膛协议执行（执行链设计第 14 节）；
本测试只用描述模式验证定义，不注册任何任务（C-14 口径）。
"""
from __future__ import annotations

import json
import shutil
import subprocess
import sys
from datetime import datetime, timedelta
from pathlib import Path

import pytest

REPO = Path(__file__).resolve().parents[1]
POWERSHELL = shutil.which("powershell.exe")
PLAN_ID = "frozen-forward-plan-" + "d" * 64


@pytest.mark.skipif(POWERSHELL is None, reason="需要 Windows PowerShell")
def test_live_registration_describes_direct_versioned_action(
    tmp_path: Path,
) -> None:
    """描述模式不得注册任务，任务名带 -live 后缀并指向 live 包装。"""
    repository = tmp_path / "repository with spaces"
    runtime = tmp_path / "runtime with spaces"
    execution = tmp_path / "execution with spaces"
    scripts = repository / "scripts"
    scripts.mkdir(parents=True)
    runtime.mkdir()
    execution.mkdir()
    (scripts / "run_frozen_live_task.ps1").write_text(
        "exit 0\n", encoding="utf-8",
    )
    register = REPO / "scripts" / "register_frozen_live_task.ps1"
    result = subprocess.run(
        [
            str(POWERSHELL), "-NoProfile", "-File", str(register),
            "-PlanId", PLAN_ID,
            "-StartUtc", "2026-09-03T00:00:00Z",
            "-EndUtc", "2026-12-02T00:00:00Z",
            "-RuntimeRoot", str(runtime),
            "-ExecutionRepository", str(execution),
            "-Repository", str(repository),
            "-NoPaper", "-DescribeOnly",
        ],
        check=False,
        capture_output=True,
        text=True,
        encoding="utf-8",
    )
    assert result.returncode == 0, result.stderr
    definition = json.loads(result.stdout)
    arguments = definition["arguments"]
    assert definition["task_name"] == "guvolu-frozen-forward-dddddddddddd-live"
    assert definition["execute"] == "powershell.exe"
    assert definition["working_directory"] == str(repository.resolve())
    assert str((scripts / "run_frozen_live_task.ps1").resolve()) in arguments
    assert f'-PlanId "{PLAN_ID}"' in arguments
    assert f'-RuntimeRoot "{runtime.resolve()}"' in arguments
    assert f'-ExecutionRepository "{execution.resolve()}"' in arguments
    assert arguments.endswith(" -NoPaper")
    assert definition["no_paper"] is True
    assert definition["minute_offset"] == 25
    first_run = datetime.fromisoformat(definition["first_run_local"])
    assert (first_run.hour, first_run.minute) == (9, 25)
    assert first_run.utcoffset() == timedelta(hours=9)
    assert definition["execution_time_limit_minutes"] == 45


@pytest.mark.skipif(POWERSHELL is None, reason="需要 Windows PowerShell")
def test_live_registration_rejects_malformed_plan(tmp_path: Path) -> None:
    """计划身份必须在任何路径解析或注册前通过白名单。"""
    register = REPO / "scripts" / "register_frozen_live_task.ps1"
    result = subprocess.run(
        [
            str(POWERSHELL), "-NoProfile", "-File", str(register),
            "-PlanId", PLAN_ID + " --help",
            "-StartUtc", "2026-09-03T00:00:00Z",
            "-EndUtc", "2026-12-02T00:00:00Z",
            "-RuntimeRoot", str(tmp_path / "missing-runtime"),
            "-ExecutionRepository", str(tmp_path / "missing-execution"),
            "-DescribeOnly",
        ],
        check=False,
        capture_output=True,
        text=True,
        encoding="utf-8",
        errors="replace",
    )
    assert result.returncode != 0
    assert "ParameterArgumentValidationError" in result.stderr


@pytest.mark.skipif(POWERSHELL is None, reason="需要 Windows PowerShell")
def test_live_registration_rejects_unaligned_start(tmp_path: Path) -> None:
    """小时偏移必须以整点边界为锚，不能静默漂移。"""
    repository = tmp_path / "repository"
    runtime = tmp_path / "runtime"
    execution = tmp_path / "execution"
    (repository / "scripts").mkdir(parents=True)
    runtime.mkdir()
    execution.mkdir()
    (repository / "scripts" / "run_frozen_live_task.ps1").write_text(
        "exit 0\n", encoding="utf-8",
    )
    register = REPO / "scripts" / "register_frozen_live_task.ps1"
    result = subprocess.run(
        [
            str(POWERSHELL), "-NoProfile", "-File", str(register),
            "-PlanId", PLAN_ID,
            "-StartUtc", "2026-09-03T00:30:00Z",
            "-EndUtc", "2026-12-02T00:00:00Z",
            "-RuntimeRoot", str(runtime),
            "-ExecutionRepository", str(execution),
            "-Repository", str(repository),
            "-DescribeOnly",
        ],
        check=False,
        capture_output=True,
        text=True,
        encoding="utf-8",
        errors="replace",
    )
    assert result.returncode != 0
    assert "exact UTC hour" in result.stderr


def test_run_frozen_live_reuses_existing_report(tmp_path: Path) -> None:
    """同一预测的 live 报告已存在时复用，不再调用执行器。"""
    sys.path.insert(0, str(REPO / "scripts"))
    try:
        from run_frozen_live import run_live_step
    finally:
        sys.path.pop(0)
    execution = tmp_path / "execution"
    prediction_id = "prediction-live-0001"
    report_dir = execution / "data/execution/live/reports"
    report_dir.mkdir(parents=True)
    report = {
        "mode": "live",
        "artifact": {"run_id": prediction_id},
        "gate_verdict": "skip",
        "resolution": None,
        "final_order_status": None,
        "endpoints": {"write_touched": []},
        "envelope": {"sha256": "e" * 64},
    }
    (report_dir / f"{prediction_id}.json").write_text(
        json.dumps(report, ensure_ascii=False), encoding="utf-8",
    )
    # 目标适配以替身给出
    prediction_path = tmp_path / "prediction.json"
    prediction_path.write_text("{}", encoding="utf-8")

    def fake_adapt(*args: object, **kwargs: object) -> Path:
        return report_dir / "target-known.json"

    import run_frozen_live as module

    original = module._adapt_target
    module._adapt_target = fake_adapt  # type: ignore[assignment]
    try:
        live = run_live_step(
            execution, execution / ".venv/Scripts/python.exe",
            prediction_path, prediction_id,
            market_id="mkt__gmo__btc__r0", symbol="BTC",
            prediction_sha="f" * 64,
        )
    finally:
        module._adapt_target = original
    assert live["status"] == "reused"
    assert live["gate_verdict"] == "skip"
    assert live["write_touched"] == []
    assert live["envelope_sha256"] == "e" * 64


def test_run_frozen_live_records_failure_without_raising(
    tmp_path: Path,
) -> None:
    """live 步骤失败只记 status=failed，不外抛（不影响预测登记）。"""
    sys.path.insert(0, str(REPO / "scripts"))
    try:
        from run_frozen_live import run_live_step
    finally:
        sys.path.pop(0)
    execution = tmp_path / "execution"
    execution.mkdir()
    live = run_live_step(
        execution, execution / ".venv/Scripts/python.exe",
        tmp_path / "prediction.json", "prediction-live-0002",
        market_id="mkt__gmo__btc__r0", symbol="BTC",
        prediction_sha="f" * 64,
    )
    assert live["status"] == "failed"
    assert "error" in live

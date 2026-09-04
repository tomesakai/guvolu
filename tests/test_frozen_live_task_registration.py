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
    assert definition["execution_time_limit_minutes"] == 55


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


def test_run_frozen_live_marks_refusal_as_refused(tmp_path: Path) -> None:
    """执行器正当拒绝判读为 refused，与崩溃 failed 区分。"""
    sys.path.insert(0, str(REPO / "scripts"))
    try:
        from run_frozen_live import run_live_step
    finally:
        sys.path.pop(0)
    execution = tmp_path / "execution"
    prediction_id = "prediction-live-0003"
    report_dir = execution / "data/execution/live/reports"
    report_dir.mkdir(parents=True)
    refusal = {
        "schema_version": 1,
        "kind": "live_refusal_report",
        "status": "envelope_expired",
        "detail": "信封不在有效期内，拒绝进入 live",
    }
    (report_dir / f"{prediction_id}.json").write_text(
        json.dumps(refusal, ensure_ascii=False), encoding="utf-8",
    )
    prediction_path = tmp_path / "prediction.json"
    prediction_path.write_text("{}", encoding="utf-8")

    import run_frozen_live as module

    original = module._adapt_target
    module._adapt_target = (  # type: ignore[assignment]
        lambda *args, **kwargs: report_dir / "target-known.json"
    )
    try:
        live = run_live_step(
            execution, execution / ".venv/Scripts/python.exe",
            prediction_path, prediction_id,
            market_id="mkt__gmo__btc__r0", symbol="BTC",
            prediction_sha="f" * 64,
        )
    finally:
        module._adapt_target = original
    assert live["status"] == "refused"
    assert live["refusal_status"] == "envelope_expired"
    assert "error" not in live


def test_live_mode_injected_only_into_live_subprocess(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch,
) -> None:
    """GUVOLU_MODE=live 仅进入 live 执行器子进程环境（T-04）。"""
    sys.path.insert(0, str(REPO / "scripts"))
    try:
        from run_frozen_live import run_live_step
    finally:
        sys.path.pop(0)
    monkeypatch.delenv("GUVOLU_MODE", raising=False)
    execution = tmp_path / "execution"
    prediction_id = "prediction-live-0004"
    report_dir = execution / "data/execution/live/reports"
    report_dir.mkdir(parents=True)
    prediction_path = tmp_path / "prediction.json"
    prediction_path.write_text("{}", encoding="utf-8")
    captured_envs: list[dict[str, str] | None] = []

    class _Result:
        returncode = 0
        stdout = ""
        stderr = ""

    def fake_run(
        command: object, *, cwd: object = None, env: object = None,
    ) -> object:
        assert env is None or isinstance(env, dict)
        captured_envs.append(env)
        (report_dir / f"{prediction_id}.json").write_text(
            json.dumps({
                "mode": "live",
                "artifact": {"run_id": prediction_id},
                "gate_verdict": "skip",
                "resolution": None,
                "final_order_status": None,
                "endpoints": {"write_touched": []},
                "envelope": {"sha256": "e" * 64},
            }, ensure_ascii=False),
            encoding="utf-8",
        )
        return _Result()

    import os

    import run_frozen_live as module

    original_adapt = module._adapt_target
    original_run = module._run
    module._adapt_target = (  # type: ignore[assignment]
        lambda *args, **kwargs: report_dir / "target-known.json"
    )
    module._run = fake_run  # type: ignore[assignment]
    try:
        live = run_live_step(
            execution, execution / ".venv/Scripts/python.exe",
            prediction_path, prediction_id,
            market_id="mkt__gmo__btc__r0", symbol="BTC",
            prediction_sha="f" * 64,
        )
    finally:
        module._adapt_target = original_adapt
        module._run = original_run
    assert live["status"] == "completed"
    assert len(captured_envs) == 1
    live_env = captured_envs[0]
    assert live_env is not None
    assert live_env["GUVOLU_MODE"] == "live"
    # 父进程环境不被污染
    assert "GUVOLU_MODE" not in os.environ


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


def test_live_registration_carries_second_market_arguments(tmp_path: Path) -> None:
    """第二市场的市场、品种与目标配置固化进任务参数。"""
    repository = tmp_path / "repository"
    runtime = tmp_path / "runtime"
    execution = tmp_path / "execution"
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
            "-StartUtc", "2026-09-05T00:00:00Z",
            "-EndUtc", "2026-12-31T00:00:00Z",
            "-RuntimeRoot", str(runtime),
            "-ExecutionRepository", str(execution),
            "-Repository", str(repository),
            "-MarketId", "mkt__gmo__eth__r0", "-Symbol", "ETH",
            "-TargetConfig", "config/paper_executor_eth.json",
            "-MinuteOffset", "30", "-DescribeOnly",
        ],
        check=False,
        capture_output=True,
        text=True,
        encoding="utf-8",
    )
    assert result.returncode == 0, result.stderr
    definition = json.loads(result.stdout)
    arguments = definition["arguments"]
    assert '-MarketId "mkt__gmo__eth__r0"' in arguments
    assert '-Symbol "ETH"' in arguments
    assert '-TargetConfig "config/paper_executor_eth.json"' in arguments
    assert definition["market_id"] == "mkt__gmo__eth__r0"
    assert definition["symbol"] == "ETH"
    assert definition["target_config"] == "config/paper_executor_eth.json"
    assert definition["minute_offset"] == 30


def test_run_frozen_live_passes_target_config_to_live_executor(
    tmp_path: Path,
) -> None:
    """live 执行器收到执行仓内解析后的目标配置路径。"""
    sys.path.insert(0, str(REPO / "scripts"))
    try:
        import run_frozen_live as module
        from run_frozen_live import run_live_step
    finally:
        sys.path.pop(0)
    execution = tmp_path / "execution"
    execution.mkdir()
    prediction_id = "prediction-live-0003"
    prediction_path = tmp_path / "prediction.json"
    prediction_path.write_text("{}", encoding="utf-8")
    captured: list[list[str]] = []

    def fake_run(
        command: object, *, cwd: object = None, env: object = None,
    ) -> subprocess.CompletedProcess[str]:
        parts = [str(part) for part in command]  # type: ignore[union-attr]
        captured.append(parts)
        report_path = Path(parts[parts.index("--report") + 1])
        report_path.parent.mkdir(parents=True, exist_ok=True)
        report_path.write_text(json.dumps({
            "mode": "live",
            "artifact": {"run_id": prediction_id},
            "gate_verdict": "skip",
            "resolution": None,
            "final_order_status": None,
            "endpoints": {"write_touched": []},
            "envelope": {"sha256": "e" * 64},
        }), encoding="utf-8")
        return subprocess.CompletedProcess(parts, 0, "", "")

    original_adapt = module._adapt_target
    original_run = module._run
    module._adapt_target = (  # type: ignore[assignment]
        lambda *args, **kwargs: tmp_path / "target.json"
    )
    module._run = fake_run  # type: ignore[assignment]
    try:
        live = run_live_step(
            execution, execution / "python.exe", prediction_path, prediction_id,
            market_id="mkt__gmo__eth__r0", symbol="ETH",
            prediction_sha="f" * 64,
            target_config="config/paper_executor_eth.json",
        )
    finally:
        module._adapt_target = original_adapt  # type: ignore[assignment]
        module._run = original_run  # type: ignore[assignment]
    assert live["status"] == "completed"
    command = captured[0]
    expected = str((execution / "config/paper_executor_eth.json").resolve())
    assert command[command.index("--target-config") + 1] == expected

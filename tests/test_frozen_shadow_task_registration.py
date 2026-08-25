"""冻结 shadow 计划任务必须直接绑定受版本控制的包装器。"""
from __future__ import annotations

import json
import shutil
import subprocess
from datetime import datetime, timedelta
from pathlib import Path

import pytest


POWERSHELL = shutil.which("powershell.exe")
PLAN_ID = "frozen-forward-plan-" + "c" * 64


@pytest.mark.skipif(POWERSHELL is None, reason="需要 Windows PowerShell")
def test_shadow_registration_describes_direct_versioned_action(
    tmp_path: Path,
) -> None:
    """描述模式不得注册任务，且参数应直达项目内 shadow 包装器。"""
    repository = tmp_path / "repository with spaces"
    runtime = tmp_path / "runtime with spaces"
    execution = tmp_path / "execution with spaces"
    scripts = repository / "scripts"
    scripts.mkdir(parents=True)
    runtime.mkdir()
    execution.mkdir()
    (scripts / "run_frozen_shadow_task.ps1").write_text(
        "exit 0\n", encoding="utf-8",
    )
    register = Path("scripts/register_frozen_shadow_task.ps1").resolve()
    result = subprocess.run(
        [
            str(POWERSHELL), "-NoProfile", "-File", str(register),
            "-PlanId", PLAN_ID,
            "-StartUtc", "2026-08-24T00:00:00Z",
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
    assert definition["task_name"] == "guvolu-frozen-forward-cccccccccccc"
    assert definition["execute"] == "powershell.exe"
    assert definition["working_directory"] == str(repository.resolve())
    assert str((scripts / "run_frozen_shadow_task.ps1").resolve()) in arguments
    assert f'-PlanId "{PLAN_ID}"' in arguments
    assert f'-RuntimeRoot "{runtime.resolve()}"' in arguments
    assert f'-ExecutionRepository "{execution.resolve()}"' in arguments
    assert arguments.endswith(" -NoPaper")
    assert "ops-alt" not in arguments
    assert definition["no_paper"] is True
    assert definition["minute_offset"] == 25
    first_run = datetime.fromisoformat(definition["first_run_local"])
    assert (first_run.hour, first_run.minute) == (9, 25)
    assert first_run.utcoffset() == timedelta(hours=9)
    assert definition["start_when_available"] is True
    assert definition["allow_start_on_batteries"] is True
    assert definition["wake_to_run"] is True
    assert definition["execution_time_limit_minutes"] == 45
    assert definition["restart_count"] == 3
    assert definition["restart_interval_minutes"] == 5


@pytest.mark.skipif(POWERSHELL is None, reason="需要 Windows PowerShell")
def test_shadow_registration_accepts_explicit_minute_offset(
    tmp_path: Path,
) -> None:
    """延迟偏移可显式调整，但只能落在一个小时内。"""
    repository = tmp_path / "repository"
    runtime = tmp_path / "runtime"
    execution = tmp_path / "execution"
    (repository / "scripts").mkdir(parents=True)
    runtime.mkdir()
    execution.mkdir()
    (repository / "scripts" / "run_frozen_shadow_task.ps1").write_text(
        "exit 0\n", encoding="utf-8",
    )
    register = Path("scripts/register_frozen_shadow_task.ps1").resolve()
    result = subprocess.run(
        [
            str(POWERSHELL), "-NoProfile", "-File", str(register),
            "-PlanId", PLAN_ID,
            "-StartUtc", "2026-08-24T00:00:00Z",
            "-EndUtc", "2026-12-02T00:00:00Z",
            "-RuntimeRoot", str(runtime),
            "-ExecutionRepository", str(execution),
            "-Repository", str(repository),
            "-MinuteOffset", "35", "-DescribeOnly",
        ],
        check=False,
        capture_output=True,
        text=True,
        encoding="utf-8",
    )

    assert result.returncode == 0, result.stderr
    definition = json.loads(result.stdout)
    assert definition["minute_offset"] == 35
    first_run = datetime.fromisoformat(definition["first_run_local"])
    assert (first_run.hour, first_run.minute) == (9, 35)
    assert first_run.utcoffset() == timedelta(hours=9)


@pytest.mark.skipif(POWERSHELL is None, reason="需要 Windows PowerShell")
def test_shadow_registration_rejects_malformed_plan_without_side_effects(
    tmp_path: Path,
) -> None:
    """计划身份在任何路径解析或任务注册前必须通过白名单。"""
    register = Path("scripts/register_frozen_shadow_task.ps1").resolve()
    result = subprocess.run(
        [
            str(POWERSHELL), "-NoProfile", "-File", str(register),
            "-PlanId", PLAN_ID + " --help",
            "-StartUtc", "2026-08-24T00:00:00Z",
            "-EndUtc", "2026-12-02T00:00:00Z",
            "-RuntimeRoot", str(tmp_path / "missing-runtime"),
            "-ExecutionRepository", str(tmp_path / "missing-execution"),
            "-DescribeOnly",
        ],
        check=False,
        capture_output=True,
        text=True,
        encoding="utf-8",
    )
    assert result.returncode != 0
    assert "ParameterArgumentValidationError" in result.stderr


@pytest.mark.skipif(POWERSHELL is None, reason="需要 Windows PowerShell")
def test_shadow_registration_rejects_unaligned_start(tmp_path: Path) -> None:
    """小时偏移必须以整点 vintage 边界为锚，不能静默漂移。"""
    repository = tmp_path / "repository"
    runtime = tmp_path / "runtime"
    execution = tmp_path / "execution"
    (repository / "scripts").mkdir(parents=True)
    runtime.mkdir()
    execution.mkdir()
    (repository / "scripts" / "run_frozen_shadow_task.ps1").write_text(
        "exit 0\n", encoding="utf-8",
    )
    register = Path("scripts/register_frozen_shadow_task.ps1").resolve()
    result = subprocess.run(
        [
            str(POWERSHELL), "-NoProfile", "-File", str(register),
            "-PlanId", PLAN_ID,
            "-StartUtc", "2026-08-24T00:30:00Z",
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
    )

    assert result.returncode != 0
    assert "exact UTC hour" in result.stderr

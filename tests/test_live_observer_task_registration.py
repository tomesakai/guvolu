"""live 伴随观察进程计划任务注册脚本的离线校验。

注册脚本本身由维护者按上膛协议执行（执行链设计第 14 节）；
本测试只用描述模式验证定义，不注册任何任务（C-14 口径）。
"""
from __future__ import annotations

import json
import shutil
import subprocess
from pathlib import Path

import pytest

REPO = Path(__file__).resolve().parents[1]
POWERSHELL = shutil.which("powershell.exe")


@pytest.mark.skipif(POWERSHELL is None, reason="需要 Windows PowerShell")
def test_observer_registration_pins_execution_repository(
    tmp_path: Path,
) -> None:
    """观察进程必须从执行仓启动，守护任务不限时且忽略重复实例。"""
    execution = tmp_path / "execution with spaces"
    scripts = execution / "scripts"
    scripts.mkdir(parents=True)
    (scripts / "run_live_observer.ps1").write_text(
        "exit 0\n", encoding="utf-8",
    )
    register = REPO / "scripts" / "register_live_observer_task.ps1"
    result = subprocess.run(
        [
            str(POWERSHELL), "-NoProfile", "-File", str(register),
            "-ExecutionRepository", str(execution),
            "-IntervalSeconds", "30", "-DescribeOnly",
        ],
        check=False,
        capture_output=True,
        text=True,
        encoding="utf-8",
    )
    assert result.returncode == 0, result.stderr
    definition = json.loads(result.stdout)
    assert definition["task_names"] == [
        "guvolu-live-observer-logon", "guvolu-live-observer-guard",
    ]
    arguments = definition["arguments"]
    assert str((scripts / "run_live_observer.ps1").resolve()) in arguments
    assert f'-Repository "{execution.resolve()}"' in arguments
    assert arguments.endswith("-IntervalSeconds 30")
    assert definition["working_directory"] == str(execution.resolve())
    assert definition["multiple_instances"] == "IgnoreNew"
    assert definition["execution_time_limit_minutes"] == 0


@pytest.mark.skipif(POWERSHELL is None, reason="需要 Windows PowerShell")
def test_observer_registration_rejects_missing_runner(tmp_path: Path) -> None:
    """执行仓缺少包装脚本时响亮失败，不注册任务。"""
    register = REPO / "scripts" / "register_live_observer_task.ps1"
    result = subprocess.run(
        [
            str(POWERSHELL), "-NoProfile", "-File", str(register),
            "-ExecutionRepository", str(tmp_path), "-DescribeOnly",
        ],
        check=False,
        capture_output=True,
        text=True,
        encoding="utf-8",
        errors="replace",
    )
    assert result.returncode != 0
    assert "run_live_observer.ps1" in (result.stderr + result.stdout)

"""holdout 预检任务必须直接绑定受版本控制的包装器。"""
from __future__ import annotations

import json
import shutil
import subprocess
from pathlib import Path

import pytest


POWERSHELL = shutil.which("powershell.exe")
VINTAGE_ID = "holdout-vintage-" + "a" * 64


def _repository(tmp_path: Path) -> tuple[Path, Path]:
    """建立含受版本控制包装器形状的临时仓库。"""
    repository = tmp_path / "repository with spaces"
    runtime = tmp_path / "runtime with spaces"
    scripts = repository / "scripts"
    scripts.mkdir(parents=True)
    runtime.mkdir()
    (scripts / "run_holdout_preflight_task.ps1").write_text(
        "exit 0\n", encoding="utf-8",
    )
    return repository, runtime


@pytest.mark.skipif(POWERSHELL is None, reason="需要 Windows PowerShell")
def test_preflight_registration_describes_direct_versioned_action(
    tmp_path: Path,
) -> None:
    """描述模式应输出完整定义且不注册任务。"""
    repository, runtime = _repository(tmp_path)
    register = Path("scripts/register_holdout_preflight_task.ps1").resolve()
    result = subprocess.run(
        [
            str(POWERSHELL), "-NoProfile", "-File", str(register),
            "-Repository", str(repository),
            "-RuntimeRoot", str(runtime),
            "-VintageId", VINTAGE_ID,
            "-DailyAt", "07:25",
            "-DescribeOnly",
        ],
        check=False,
        capture_output=True,
        text=True,
        encoding="utf-8",
    )
    assert result.returncode == 0, result.stderr
    definition = json.loads(result.stdout)
    arguments = definition["arguments"]
    runner = repository / "scripts" / "run_holdout_preflight_task.ps1"
    assert definition["task_name"] == "guvolu-holdout-preflight"
    assert definition["execute"] == "powershell.exe"
    assert definition["working_directory"] == str(repository.resolve())
    assert str(runner.resolve()) in arguments
    assert f'-Repository "{repository.resolve()}"' in arguments
    assert f'-RuntimeRoot "{runtime.resolve()}"' in arguments
    assert f'-VintageId "{VINTAGE_ID}"' in arguments
    assert definition["daily_at_local"] == "07:25"
    assert definition["local_time_zone"]
    assert definition["vintage_id"] == VINTAGE_ID
    assert definition["multiple_instances"] == "IgnoreNew"
    assert definition["start_when_available"] is True
    assert definition["allow_start_on_batteries"] is True
    assert definition["dont_stop_if_going_on_batteries"] is True
    assert definition["wake_to_run"] is True
    assert definition["execution_time_limit_minutes"] == 30
    assert definition["restart_count"] == 0
    assert definition["restart_interval_minutes"] == 0
    assert "ops-alt" not in arguments


@pytest.mark.skipif(POWERSHELL is None, reason="需要 Windows PowerShell")
def test_preflight_registration_defaults_runtime_and_local_time(
    tmp_path: Path,
) -> None:
    """可选参数省略时应使用仓库运行根和本地 09:35。"""
    repository, _runtime = _repository(tmp_path)
    register = Path("scripts/register_holdout_preflight_task.ps1").resolve()
    result = subprocess.run(
        [
            str(POWERSHELL), "-NoProfile", "-File", str(register),
            "-Repository", str(repository), "-DescribeOnly",
        ],
        check=False,
        capture_output=True,
        text=True,
        encoding="utf-8",
    )
    assert result.returncode == 0, result.stderr
    definition = json.loads(result.stdout)
    arguments = definition["arguments"]
    assert definition["daily_at_local"] == "09:35"
    assert definition["vintage_id"] is None
    assert f'-RuntimeRoot "{repository.resolve()}"' in arguments
    assert "-VintageId" not in arguments


@pytest.mark.skipif(POWERSHELL is None, reason="需要 Windows PowerShell")
def test_preflight_registration_rejects_malformed_vintage_first(
    tmp_path: Path,
) -> None:
    """非法封存段身份必须在路径解析与注册前被拒绝。"""
    register = Path("scripts/register_holdout_preflight_task.ps1").resolve()
    result = subprocess.run(
        [
            str(POWERSHELL), "-NoProfile", "-File", str(register),
            "-Repository", str(tmp_path / "missing-repository"),
            "-RuntimeRoot", str(tmp_path / "missing-runtime"),
            "-VintageId", VINTAGE_ID + " --help",
            "-DescribeOnly",
        ],
        check=False,
        capture_output=True,
        text=True,
        encoding="utf-8",
    )
    assert result.returncode != 0
    assert "canonical holdout vintage identifier" in result.stderr

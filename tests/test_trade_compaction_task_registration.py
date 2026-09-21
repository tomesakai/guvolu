"""逐笔实时段每日合并任务注册脚本的离线校验。

只用描述模式验证定义，不注册任何任务（C-14 口径）。
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
def test_compaction_task_definition(tmp_path: Path) -> None:
    """任务每日一次、忽略重复实例、时刻避开冻结前向链起跑分钟。"""
    repository = tmp_path / "repo with spaces"
    scripts = repository / "scripts"
    scripts.mkdir(parents=True)
    (scripts / "run_trade_compaction.ps1").write_text("exit 0\n", encoding="utf-8")
    register = REPO / "scripts" / "register_trade_compaction_task.ps1"
    result = subprocess.run(
        [
            str(POWERSHELL), "-NoProfile", "-File", str(register),
            "-Repository", str(repository), "-DescribeOnly",
        ],
        check=False, capture_output=True, text=True, encoding="utf-8",
        errors="replace",
    )
    assert result.returncode == 0, result.stderr
    definition = json.loads(result.stdout)
    assert definition["task_name"] == "guvolu-trade-compaction"
    arguments = definition["arguments"]
    assert str((scripts / "run_trade_compaction.ps1").resolve()) in arguments
    assert f'-Repository "{repository.resolve()}"' in arguments
    assert definition["multiple_instances"] == "IgnoreNew"
    assert definition["execution_time_limit_minutes"] == 60
    minute = int(str(definition["at"]).split(":")[1])
    # 避开每小时链起跑时段
    assert 50 <= minute <= 59 or minute <= 10


@pytest.mark.skipif(POWERSHELL is None, reason="需要 Windows PowerShell")
def test_compaction_task_rejects_missing_runner(tmp_path: Path) -> None:
    """仓库缺少启动器时响亮失败，不注册任务。"""
    register = REPO / "scripts" / "register_trade_compaction_task.ps1"
    result = subprocess.run(
        [
            str(POWERSHELL), "-NoProfile", "-File", str(register),
            "-Repository", str(tmp_path), "-DescribeOnly",
        ],
        check=False, capture_output=True, text=True, encoding="utf-8",
        errors="replace",
    )
    assert result.returncode != 0

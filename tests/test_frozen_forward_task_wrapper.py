"""Windows PowerShell 冻结前向任务包装器的进程与日志合同。"""
from __future__ import annotations

import json
import os
import shutil
import subprocess
import sys
from pathlib import Path

import pytest


POWERSHELL = shutil.which("powershell.exe")
VALID_FAILURE_PLAN = "frozen-forward-plan-" + "a" * 64
VALID_SUCCESS_PLAN = "frozen-forward-plan-" + "b" * 64


def _fake_repository(tmp_path: Path) -> Path:
    """构造含空格路径和可启动 venv launcher 的最小仓库。"""
    root = tmp_path / "repository with spaces"
    scripts = root / "scripts"
    python_directory = root / ".venv" / "Scripts"
    scripts.mkdir(parents=True)
    python_directory.mkdir(parents=True)
    python_target = python_directory / "python.exe"
    try:
        os.link(sys.executable, python_target)
    except OSError:
        shutil.copy2(sys.executable, python_target)
    source_venv = Path(sys.executable).resolve().parents[1]
    shutil.copy2(source_venv / "pyvenv.cfg", root / ".venv" / "pyvenv.cfg")
    (scripts / "manage_frozen_forward.py").write_text(
        """from __future__ import annotations
import argparse
import sys

parser = argparse.ArgumentParser()
parser.add_argument("--root")
commands = parser.add_subparsers(dest="command", required=True)
predict = commands.add_parser("predict")
predict.add_argument("plan_id")
predict.add_argument("--registry")
arguments = parser.parse_args()
if arguments.plan_id.endswith("a" * 64):
    print("标准输出")
    print("中文错误输出", file=sys.stderr)
    raise SystemExit(7)
print("成功输出")
""",
        encoding="utf-8",
    )
    return root


def _run_wrapper(
    wrapper: Path,
    root: Path,
    plan_id: str,
) -> subprocess.CompletedProcess[str]:
    """通过计划任务实际使用的 Windows PowerShell 5.1 执行包装器。"""
    return subprocess.run(
        [
            str(POWERSHELL),
            "-NoProfile",
            "-File",
            str(wrapper),
            "-PlanId",
            plan_id,
            "-Repository",
            str(root),
            "-Registry",
            str(root / "data with spaces" / "governance.sqlite3"),
        ],
        check=False,
        capture_output=True,
        text=True,
        encoding="utf-8",
    )


@pytest.mark.skipif(POWERSHELL is None, reason="需要 Windows PowerShell")
def test_frozen_forward_task_wrapper_preserves_process_and_log_contract(
    tmp_path: Path,
) -> None:
    """失败、成功和畸形参数均不得破坏审计与退出码合同。"""
    root = _fake_repository(tmp_path)
    wrapper = Path("scripts/run_frozen_forward_task.ps1").resolve()
    log_path = root / "logs" / "research" / "frozen-forward" / "task.jsonl"

    failed = _run_wrapper(wrapper, root, VALID_FAILURE_PLAN)
    assert failed.returncode == 7
    failure = json.loads(log_path.read_text(encoding="utf-8").splitlines()[-1])
    assert failure["exit_code"] == 7
    assert failure["prediction_exit_code"] == 7
    assert failure["wrapper_error"] is None
    assert "标准输出" in failure["output"]
    assert "中文错误输出" in failure["output"]
    assert not tuple(log_path.parent.glob(".*.tmp"))

    succeeded = _run_wrapper(wrapper, root, VALID_SUCCESS_PLAN)
    assert succeeded.returncode == 0
    success = json.loads(log_path.read_text(encoding="utf-8").splitlines()[-1])
    assert success["exit_code"] == 0
    assert success["prediction_exit_code"] == 0
    assert success["wrapper_error"] is None
    assert success["output"] == "成功输出"
    assert not tuple(log_path.parent.glob(".*.tmp"))

    lines_before = log_path.read_text(encoding="utf-8").splitlines()
    malformed = _run_wrapper(wrapper, root, VALID_SUCCESS_PLAN + " --help")
    assert malformed.returncode != 0
    assert "ParameterArgumentValidationError" in malformed.stderr
    assert log_path.read_text(encoding="utf-8").splitlines() == lines_before

    register = Path("scripts/register_frozen_forward_task.ps1").resolve()
    malformed_registration = subprocess.run(
        [
            str(POWERSHELL),
            "-NoProfile",
            "-File",
            str(register),
            "-PlanId",
            VALID_SUCCESS_PLAN + " --help",
            "-StartUtc",
            "2026-08-21T00:00:00Z",
            "-EndUtc",
            "2026-11-29T00:00:00Z",
        ],
        check=False,
        capture_output=True,
        text=True,
        encoding="utf-8",
    )
    assert malformed_registration.returncode != 0
    assert "ParameterArgumentValidationError" in malformed_registration.stderr

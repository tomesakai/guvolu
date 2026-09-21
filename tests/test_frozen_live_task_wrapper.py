"""冻结 live 任务包装脚本的离线校验：留痕、透传与超时整树终止。

用假仓库与假运行脚本驱动，不触任何真实运行根与场所（C-14 口径）。
"""
from __future__ import annotations

import json
import shutil
import subprocess
import sys
import time
from pathlib import Path

import pytest

REPO = Path(__file__).resolve().parents[1]
POWERSHELL = shutil.which("powershell.exe")
WRAPPER = REPO / "scripts" / "run_frozen_live_task.ps1"
PLAN = "frozen-forward-plan-" + "a" * 64


def _fake_repository(tmp_path: Path, runner_body: str) -> Path:
    repository = tmp_path / "repo with spaces"
    (repository / "scripts").mkdir(parents=True)
    (repository / "scripts" / "run_frozen_live.py").write_text(
        runner_body, encoding="utf-8",
    )
    return repository


def _run_wrapper(
    repository: Path, tmp_path: Path, *extra: str,
) -> tuple[int, list[dict[str, object]]]:
    runtime = tmp_path / "runtime"
    execution = tmp_path / "execution"
    runtime.mkdir(exist_ok=True)
    execution.mkdir(exist_ok=True)
    result = subprocess.run(
        [
            str(POWERSHELL), "-NoProfile", "-NonInteractive",
            "-ExecutionPolicy", "Bypass", "-File", str(WRAPPER),
            "-PlanId", PLAN, "-Repository", str(repository),
            "-RuntimeRoot", str(runtime),
            "-ExecutionRepository", str(execution),
            "-Python", sys.executable, *extra,
        ],
        check=False, capture_output=True, text=True, encoding="utf-8",
        cwd=repository,
    )
    log = repository / "logs" / "research" / "frozen-forward" / "live-scheduler.jsonl"
    rows = [
        json.loads(line)
        for line in log.read_text(encoding="utf-8-sig").splitlines()
        if line.strip()
    ]
    return result.returncode, rows


@pytest.mark.skipif(POWERSHELL is None, reason="需要 Windows PowerShell")
def test_wrapper_records_start_and_completion(tmp_path: Path) -> None:
    """正常轮次先写开始记录，再写带退出码与中文输出的完成记录。"""
    repository = _fake_repository(
        tmp_path,
        "import json, sys\n"
        "print(json.dumps({'argv': sys.argv[1:], 'note': '冻结预测过期'},"
        " ensure_ascii=False))\n"
        "print('标准错误也保留', file=sys.stderr)\n"
        "raise SystemExit(1)\n",
    )
    code, rows = _run_wrapper(
        repository, tmp_path, "-MarketId", "mkt__gmo__eth__r0",
        "-Symbol", "ETH", "-TargetConfig", "config/paper_executor_eth.json",
    )
    assert code == 1
    assert [row.get("phase") for row in rows] == ["started", None]
    started, completed = rows
    assert started["started_at"] == completed["started_at"]
    assert completed["exit_code"] == 1
    assert completed["timed_out"] is False
    output = str(completed["output"])
    assert "标准错误也保留" in output
    payload = json.loads(output.splitlines()[0])
    assert payload["note"] == "冻结预测过期"
    argv = payload["argv"]
    assert argv[argv.index("--repository") + 1] == str(repository.resolve())
    assert argv[argv.index("--plan-id") + 1] == PLAN
    assert argv[argv.index("--symbol") + 1] == "ETH"
    assert argv[argv.index("--target-config") + 1] == (
        "config/paper_executor_eth.json"
    )


@pytest.mark.skipif(POWERSHELL is None, reason="需要 Windows PowerShell")
def test_wrapper_kills_process_tree_on_timeout(tmp_path: Path) -> None:
    """超时轮次连同孙进程一起终止，并留下超时完成记录。"""
    marker = tmp_path / "grandchild.pid"
    repository = _fake_repository(
        tmp_path,
        "import subprocess, sys, time\n"
        "child = subprocess.Popen([sys.executable, '-c',"
        " 'import time; time.sleep(120)'])\n"
        f"open(r'{marker}', 'w').write(str(child.pid))\n"
        "time.sleep(120)\n",
    )
    began = time.monotonic()
    code, rows = _run_wrapper(
        repository, tmp_path, "-RoundTimeoutSeconds", "3",
    )
    assert time.monotonic() - began < 60
    assert code == 4
    completed = rows[-1]
    assert completed["exit_code"] == 4
    assert completed["timed_out"] is True
    assert "整树终止" in str(completed["output"])
    grandchild = int(marker.read_text())
    probe = subprocess.run(
        ["tasklist", "/FI", f"PID eq {grandchild}", "/NH"],
        check=False, capture_output=True, text=True,
        encoding="mbcs", errors="replace",
    )
    assert str(grandchild) not in probe.stdout


@pytest.mark.skipif(POWERSHELL is None, reason="需要 Windows PowerShell")
def test_wrapper_records_unreachable_runtime(tmp_path: Path) -> None:
    """运行根不可达时不启动进程，但仍留下失败记录。"""
    repository = _fake_repository(tmp_path, "raise SystemExit(0)\n")
    execution = tmp_path / "execution"
    execution.mkdir()
    result = subprocess.run(
        [
            str(POWERSHELL), "-NoProfile", "-NonInteractive",
            "-ExecutionPolicy", "Bypass", "-File", str(WRAPPER),
            "-PlanId", PLAN, "-Repository", str(repository),
            "-RuntimeRoot", str(tmp_path / "missing"),
            "-ExecutionRepository", str(execution),
            "-Python", sys.executable,
        ],
        check=False, capture_output=True, text=True, encoding="utf-8",
    )
    assert result.returncode == 3
    log = repository / "logs" / "research" / "frozen-forward" / "live-scheduler.jsonl"
    rows = [
        json.loads(line)
        for line in log.read_text(encoding="utf-8-sig").splitlines()
        if line.strip()
    ]
    assert rows[-1]["exit_code"] == 3
    assert rows[-1]["resolved_runtime_root"] is None

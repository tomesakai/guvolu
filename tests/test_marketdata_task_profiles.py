"""市场数据守护配置的静态部署合同。"""
from __future__ import annotations

import json
import os
import shutil
import subprocess
import sys
import time
from pathlib import Path

import pytest


POWERSHELL = shutil.which("powershell.exe")
RUNNERS = (
    "run_l2_collector.ps1",
    "run_trade_collector.ps1",
    "run_l2_materializer.ps1",
    "run_trade_materializer.ps1",
    "run_book_state_materializer.ps1",
    "run_orderflow_tile_watcher.ps1",
)
MODULES = (
    "l2_capture",
    "trade_capture",
    "l2_materialize",
    "trade_realtime_materialize",
    "book_state_materialize",
    "orderflow_tile_materialize",
)


def test_forward_minimal_profile_preserves_raw_and_required_trade_path() -> None:
    """最小配置不得缩减原始采集，且只保留逐笔物化。"""
    script = Path("scripts/start_marketdata_pipeline.ps1").read_text(encoding="utf-8")

    for name in (
        "l2-gmo-btc",
        "l2-bitbank-btc-jpy",
        "l2-bitflyer-btc-jpy",
        "trade-gmo-btc",
        "trade-bitbank-btc-jpy",
        "trade-bitflyer-btc-jpy",
    ):
        assert f"Name = '{name}'" in script
    assert "[ValidateSet('Full', 'ForwardMinimal')]" in script
    assert "Where-Object { $_.Name -eq 'trade-realtime-materializer' }" in script
    assert "Where-Object { $_.Name -ne 'trade-realtime-materializer' }" in script
    assert "Stop-Process -Id $Process.ProcessId -Force" in script
    assert "$DataRoot = Join-Path $RepoRoot 'data'" in script
    assert "$RunnerRoot = $PSScriptRoot" in script
    assert "Set-Location -LiteralPath $RepoRoot" in script
    assert "-match $DataRootPattern" in script
    assert "-match $RepositoryPattern" in script
    assert '-File `"$RunnerPath`" -Repository `"$RepoRoot`"' in script
    assert "Start-Sleep -Milliseconds 200" in script
    assert "failed to pause PID=" in script

    for name in RUNNERS:
        runner = Path("scripts", name).read_text(encoding="utf-8")
        assert "[string]$Repository = ''" in runner
        assert "(Resolve-Path -LiteralPath $Repository).Path" in runner
        assert "$DataRoot = Join-Path $RepoRoot 'data'" in runner
        assert "--data-root $DataRoot" in runner


def test_task_registration_pins_profile_and_resolved_repository() -> None:
    """任务动作必须显式传递受限配置和仓库绝对路径。"""
    script = Path("scripts/register_marketdata_tasks.ps1").read_text(
        encoding="utf-8"
    )

    assert "[ValidateSet('Full', 'ForwardMinimal')]" in script
    assert "(Resolve-Path -LiteralPath $Repository).Path" in script
    assert '"-Profile $Profile -Repository `"$RepoRoot`""' in script


def _make_probe_repository(tmp_path: Path) -> tuple[Path, Path, Path]:
    repository = tmp_path / "repository with spaces"
    scripts = tmp_path / "launcher with spaces" / "scripts"
    probe = tmp_path / "probe package"
    log = tmp_path / "probe logs"
    scripts.mkdir(parents=True)
    log.mkdir()
    (repository / "data").mkdir(parents=True)
    subprocess.run(
        [
            "cmd.exe",
            "/d",
            "/c",
            "mklink",
            "/J",
            str(repository / ".venv"),
            str(Path(sys.executable).resolve().parents[1]),
        ],
        check=True,
        capture_output=True,
        text=True,
    )
    shutil.copy2("scripts/start_marketdata_pipeline.ps1", scripts)
    for name in RUNNERS:
        shutil.copy2(Path("scripts", name), scripts)
    package = probe / "guvolu" / "data"
    package.mkdir(parents=True)
    (probe / "guvolu" / "__init__.py").write_text("", encoding="utf-8")
    (package / "__init__.py").write_text("", encoding="utf-8")
    module_source = """\
import json
import os
import sys
import time
from pathlib import Path

record = {
    "module": __spec__.name,
    "argv": sys.argv[1:],
    "cwd": os.getcwd(),
}
directory = Path(os.environ["GUVOLU_PROCESS_PROBE_LOG"])
directory.mkdir(parents=True, exist_ok=True)
(directory / f"{os.getpid()}.json").write_text(json.dumps(record), encoding="utf-8")
if "watch" in sys.argv and __spec__.name.endswith("l2_materialize"):
    time.sleep(60)
"""
    for name in MODULES:
        (package / f"{name}.py").write_text(module_source, encoding="utf-8")
    return repository, scripts, log


def _probe_environment(probe: Path, log: Path) -> dict[str, str]:
    environment = os.environ.copy()
    environment["PYTHONPATH"] = str(probe)
    environment["GUVOLU_PROCESS_PROBE_LOG"] = str(log)
    return environment


def _read_probe(log: Path) -> list[dict[str, object]]:
    rows: list[dict[str, object]] = []
    for path in log.glob("*.json"):
        rows.append(json.loads(path.read_text(encoding="utf-8")))
    return rows


@pytest.mark.skipif(
    os.name != "nt" or POWERSHELL is None,
    reason="需要 Windows PowerShell 5.1 与目录联接",
)
def test_forward_minimal_executes_quoted_repository_scoped_processes(
    tmp_path: Path,
) -> None:
    """真实 PS5.1 启动空格路径，并只暂停同一 data-root 的派生进程。"""
    repository, scripts, log = _make_probe_repository(tmp_path)
    probe = tmp_path / "probe package"
    environment = _probe_environment(probe, log)
    python = repository / ".venv" / "Scripts" / "python.exe"
    same_root = subprocess.Popen(
        [
            python,
            "-m",
            "guvolu.data.l2_materialize",
            "--data-root",
            str(repository / "data"),
            "watch",
        ],
        env=environment,
    )
    other_root = subprocess.Popen(
        [
            python,
            "-m",
            "guvolu.data.l2_materialize",
            "--data-root",
            str(repository / "data-other"),
            "watch",
        ],
        env=environment,
    )
    try:
        deadline = time.monotonic() + 10
        while len(_read_probe(log)) < 2 and time.monotonic() < deadline:
            time.sleep(0.05)
        completed = subprocess.run(
            [
                str(POWERSHELL),
                "-NoProfile",
                "-NonInteractive",
                "-ExecutionPolicy",
                "Bypass",
                "-File",
                str(scripts / "start_marketdata_pipeline.ps1"),
                "-WindowStyle",
                "Hidden",
                "-Profile",
                "ForwardMinimal",
                "-Repository",
                str(repository),
            ],
            check=False,
            capture_output=True,
            text=True,
            encoding="utf-8",
            env=environment,
            timeout=30,
        )
        assert completed.returncode == 0, completed.stderr
        deadline = time.monotonic() + 10
        while time.monotonic() < deadline:
            rows = _read_probe(log)
            records = [row for row in rows if "record" in row["argv"]]
            trade_watch = [
                row
                for row in rows
                if row["module"].endswith("trade_realtime_materialize")
                and "watch" in row["argv"]
            ]
            if len(records) >= 6 and trade_watch:
                break
            time.sleep(0.05)
        assert same_root.wait(timeout=5) != 0
        assert other_root.poll() is None
        assert len(records) == 6
        assert len(trade_watch) == 1
        expected_root = str(repository / "data")
        for row in [*records, *trade_watch]:
            argv = row["argv"]
            assert argv[argv.index("--data-root") + 1] == expected_root
            assert Path(str(row["cwd"])) == repository
        derived = {
            str(row["module"])
            for row in rows
            if "watch" in row["argv"]
        }
        assert "guvolu.data.book_state_materialize" not in derived
        assert "guvolu.data.orderflow_tile_materialize" not in derived
    finally:
        for process in (same_root, other_root):
            if process.poll() is None:
                process.terminate()
                process.wait(timeout=5)

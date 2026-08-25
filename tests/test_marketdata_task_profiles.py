"""市场数据守护配置的静态部署合同。"""
from __future__ import annotations

import json
import os
import shutil
import subprocess
import sys
import time
from datetime import UTC, datetime
from pathlib import Path
from typing import TypedDict, cast

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


class ProbeRecord(TypedDict):
    pid: int
    module: str
    argv: list[str]
    cwd: str
    recorded_at_ns: int


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
    assert "function Test-RepositoryPythonProcess" in script
    assert "function Test-RepositoryMaterializerRunnerProcess" in script
    assert "-Left $Roots[0] -Right $DataRoot" in script
    assert "-Left $Repositories[0] -Right $RepoRoot" in script
    assert '-File `"$RunnerPath`" -Repository `"$RepoRoot`"' in script
    assert "Start-Sleep -Milliseconds 200" in script
    assert "failed to pause PID=" in script
    assert "[Nullable[int]]$L2LatestSealedSegmentsPerStream = $null" in script
    assert "L2LatestRunOnly and L2LatestSealedSegmentsPerStream" in script
    assert " -LatestSealedSegmentsPerStream " in script
    assert "L2 input selection cannot be used with ForwardMinimal" in script
    assert "function Assert-L2MaterializerSelection" in script
    assert "existing process selection differs" in script
    assert "Get-RepositoryL2MaterializerProcess" in script
    assert "l2_materializer_process_contract.ps1" in script
    assert script.index(
        "$L2OwnerTruth = Start-OrConfirm-L2Owner"
    ) < script.index("# Recover only stale crash tails")
    assert script.index(
        "$L2SuppressionLock = Stop-AndConfirm-L2OwnerReleased"
    ) < script.index("# Recover only stale crash tails")
    assert "Global\\guvolu-l2-launch-" in script
    assert "Wait-L2OwnerTruth" in script
    assert "l2-materializer-owner.json" in script
    assert "ExecutablePath = [string]$Record.executable_path" in script

    for name in RUNNERS:
        runner = Path("scripts", name).read_text(encoding="utf-8")
        assert "[string]$Repository = ''" in runner
        assert "(Resolve-Path -LiteralPath $Repository).Path" in runner
        assert "$DataRoot = Join-Path $RepoRoot 'data'" in runner
        assert "--data-root $DataRoot" in runner

    l2_runner = Path("scripts/run_l2_materializer.ps1").read_text(
        encoding="utf-8"
    )
    assert "[Nullable[int]]$LatestSealedSegmentsPerStream = $null" in l2_runner
    assert "LatestRunOnly and LatestSealedSegmentsPerStream" in l2_runner
    assert "--latest-sealed-segments-per-stream" in l2_runner


def test_task_registration_pins_profile_and_resolved_repository() -> None:
    """任务动作必须显式传递受限配置和仓库绝对路径。"""
    script = Path("scripts/register_marketdata_tasks.ps1").read_text(
        encoding="utf-8"
    )

    assert "[ValidateSet('Full', 'ForwardMinimal')]" in script
    assert "(Resolve-Path -LiteralPath $Repository).Path" in script
    assert '"-Profile $Profile -Repository `"$RepoRoot`""' in script


@pytest.mark.skipif(
    POWERSHELL is None,
    reason="需要 Windows PowerShell 5.1 参数绑定",
)
@pytest.mark.parametrize(
    ("script", "arguments", "message"),
    (
        (
            "run_l2_materializer.ps1",
            ("-LatestRunOnly", "-LatestSealedSegmentsPerStream", "1"),
            "mutually exclusive",
        ),
        (
            "start_marketdata_pipeline.ps1",
            (
                "-Profile", "ForwardMinimal",
                "-L2LatestSealedSegmentsPerStream", "1",
            ),
            "cannot be used with ForwardMinimal",
        ),
    ),
)
def test_l2_bounded_modes_fail_before_repository_or_process_side_effects(
    tmp_path: Path,
    script: str,
    arguments: tuple[str, ...],
    message: str,
) -> None:
    """非法模式须在路径解析、recover 和进程操作前失败。"""
    missing_repository = tmp_path / "must-not-be-resolved"
    completed = subprocess.run(
        [
            str(POWERSHELL),
            "-NoProfile",
            "-NonInteractive",
            "-ExecutionPolicy",
            "Bypass",
            "-File",
            str(Path("scripts", script).resolve()),
            "-Repository",
            str(missing_repository),
            *arguments,
        ],
        check=False,
        capture_output=True,
        text=True,
        encoding="utf-8",
        timeout=10,
    )
    assert completed.returncode != 0
    assert message in (completed.stdout + completed.stderr)
    assert not missing_repository.exists()


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
    shutil.copy2("scripts/l2_materializer_process_contract.ps1", scripts)
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
import uuid
from datetime import UTC, datetime
from pathlib import Path

owner_stream = None
owner_path = None
owner_nonce = None
is_l2_watch = (
    "watch" in sys.argv and __spec__.name.endswith("l2_materialize")
)
if is_l2_watch:
    import msvcrt

    data_index = sys.argv.index("--data-root") + 1
    data_root = Path(sys.argv[data_index]).resolve()
    owner_directory = data_root / ".locks"
    owner_directory.mkdir(parents=True, exist_ok=True)
    lock_path = owner_directory / "l2-materializer-owner.lock"
    owner_path = owner_directory / "l2-materializer-owner.json"
    owner_stream = lock_path.open("a+b")
    owner_stream.seek(0, os.SEEK_END)
    if owner_stream.tell() == 0:
        owner_stream.write(b"\\0")
        owner_stream.flush()
    owner_stream.seek(0)
    try:
        msvcrt.locking(owner_stream.fileno(), msvcrt.LK_NBLCK, 1)
    except OSError:
        print("L2 watch singleton is already owned", file=sys.stderr)
        owner_stream.close()
        raise SystemExit(73)
    try:
        owner_path.unlink()
    except FileNotFoundError:
        pass
    if "--latest-run-only" in sys.argv:
        selection = "latest_run"
    elif "--latest-sealed-segments-per-stream" in sys.argv:
        limit_index = sys.argv.index(
            "--latest-sealed-segments-per-stream"
        ) + 1
        selection = f"latest_sealed_per_stream:{sys.argv[limit_index]}"
    else:
        selection = "all"
    owner_nonce = uuid.uuid4().hex
    owner = {
        "schema_version": 1,
        "pid": os.getpid(),
        "selection": selection,
        "data_root": str(data_root),
        "executable_path": str(Path(
            getattr(sys, "_base_executable", sys.executable)
        ).resolve()),
        "started_at": datetime.now(UTC).isoformat(),
        "nonce": owner_nonce,
    }
    temporary = owner_path.with_name(f".owner-{os.getpid()}.tmp")
    temporary.write_text(
        json.dumps(owner, sort_keys=True) + "\\n", encoding="utf-8",
    )
    os.replace(temporary, owner_path)

record = {
    "pid": os.getpid(),
    "module": __spec__.name,
    "argv": sys.argv[1:],
    "cwd": os.getcwd(),
    "recorded_at_ns": time.time_ns(),
}
directory = Path(os.environ["GUVOLU_PROCESS_PROBE_LOG"])
directory.mkdir(parents=True, exist_ok=True)
(directory / f"{os.getpid()}.json").write_text(json.dumps(record), encoding="utf-8")
try:
    hold_non_l2 = (
        os.environ.get("GUVOLU_PROCESS_PROBE_HOLD_NON_L2") == "1"
        and not is_l2_watch
        and ("record" in sys.argv or "watch" in sys.argv)
    )
    if is_l2_watch or hold_non_l2:
        release = os.environ.get("GUVOLU_PROCESS_PROBE_RELEASE")
        if release:
            while not Path(release).exists():
                time.sleep(0.05)
        else:
            time.sleep(60)
finally:
    if owner_stream is not None:
        try:
            if owner_path is not None and owner_path.exists():
                current = json.loads(owner_path.read_text(encoding="utf-8"))
                if current.get("nonce") == owner_nonce:
                    owner_path.unlink()
        finally:
            owner_stream.seek(0)
            msvcrt.locking(owner_stream.fileno(), msvcrt.LK_UNLCK, 1)
            owner_stream.close()
"""
    for name in MODULES:
        (package / f"{name}.py").write_text(module_source, encoding="utf-8")
    return repository, scripts, log


def _probe_environment(probe: Path, log: Path) -> dict[str, str]:
    environment = os.environ.copy()
    environment["PYTHONPATH"] = str(probe)
    environment["GUVOLU_PROCESS_PROBE_LOG"] = str(log)
    return environment


def _pid_is_running(pid: int) -> bool:
    completed = subprocess.run(
        ["tasklist.exe", "/FI", f"PID eq {pid}", "/FO", "CSV", "/NH"],
        check=False,
        capture_output=True,
        text=True,
        encoding="utf-8",
    )
    return f'"{pid}"' in completed.stdout


def _read_probe(log: Path) -> list[ProbeRecord]:
    rows: list[ProbeRecord] = []
    for path in log.glob("*.json"):
        loaded = json.loads(path.read_text(encoding="utf-8"))
        assert isinstance(loaded, dict)
        module = loaded.get("module")
        raw_argv = loaded.get("argv")
        cwd = loaded.get("cwd")
        recorded_at_ns = loaded.get("recorded_at_ns")
        pid = loaded.get("pid")
        assert isinstance(pid, int)
        assert not isinstance(pid, bool)
        assert isinstance(module, str)
        assert isinstance(raw_argv, list)
        assert all(isinstance(argument, str) for argument in raw_argv)
        assert isinstance(cwd, str)
        assert isinstance(recorded_at_ns, int)
        assert not isinstance(recorded_at_ns, bool)
        rows.append({
            "pid": pid,
            "module": module,
            "argv": cast(list[str], raw_argv),
            "cwd": cwd,
            "recorded_at_ns": recorded_at_ns,
        })
    return rows


def _run_l2_process_contract(
    tmp_path: Path,
    *,
    process_name: str,
    command_line: str,
    repository: Path,
    runner: Path,
    expected_python: Path | None = None,
    executable_path: Path | None = None,
) -> subprocess.CompletedProcess[str]:
    expected_python = expected_python or Path(sys.executable).resolve()
    executable_path = executable_path or expected_python
    (repository / "data").mkdir(parents=True, exist_ok=True)
    runner.parent.mkdir(parents=True, exist_ok=True)
    runner.touch(exist_ok=True)
    probe = tmp_path / "process-contract-probe.ps1"
    probe.write_text(
        """\
param(
    [string]$Contract,
    [string]$ProcessName,
    [string]$Repository,
    [string]$DataRoot,
    [string]$Runner,
    [string]$ExpectedPython,
    [string]$ExecutablePath
)
$ErrorActionPreference = 'Stop'
. $Contract
$Result = Get-L2MaterializerProcessContract `
    -ProcessName $ProcessName `
    -CommandLine $env:GUVOLU_TEST_COMMAND_LINE `
    -ProcessId 42 `
    -RepositoryRoot $Repository `
    -DataRoot $DataRoot `
    -RunnerPath $Runner `
    -ExpectedPythonPath $ExpectedPython `
    -ExecutablePath $ExecutablePath
$Result | ConvertTo-Json -Compress
""",
        encoding="utf-8",
    )
    environment = os.environ.copy()
    environment["GUVOLU_TEST_COMMAND_LINE"] = command_line
    return subprocess.run(
        [
            str(POWERSHELL),
            "-NoProfile",
            "-NonInteractive",
            "-ExecutionPolicy",
            "Bypass",
            "-File",
            str(probe),
            "-Contract",
            str(Path("scripts/l2_materializer_process_contract.ps1").resolve()),
            "-ProcessName",
            process_name,
            "-Repository",
            str(repository),
            "-DataRoot",
            str(repository / "data"),
            "-Runner",
            str(runner),
            "-ExpectedPython",
            str(expected_python),
            "-ExecutablePath",
            str(executable_path),
        ],
        check=False,
        capture_output=True,
        text=True,
        encoding="utf-8",
        env=environment,
        timeout=10,
    )


def _run_marketdata_process_predicate(
    tmp_path: Path,
    *,
    process_name: str,
    command_line: str,
    repository: Path,
    runner_root: Path,
    runner: str,
) -> subprocess.CompletedProcess[str]:
    (repository / "data").mkdir(parents=True, exist_ok=True)
    runner_root.mkdir(parents=True, exist_ok=True)
    (runner_root / runner).touch(exist_ok=True)
    probe = tmp_path / "marketdata-process-predicate-probe.ps1"
    probe.write_text(
        """\
param(
    [string]$StartScript,
    [string]$Contract,
    [string]$ProcessName,
    [string]$Repository,
    [string]$RunnerRoot,
    [string]$Runner
)
$ErrorActionPreference = 'Stop'
. $Contract
$Tokens = $null
$Errors = $null
$Ast = [System.Management.Automation.Language.Parser]::ParseFile(
    $StartScript,
    [ref]$Tokens,
    [ref]$Errors
)
if ($Errors.Count -gt 0) {
    throw 'start script AST is invalid'
}
$Names = @(
    'Test-MarketdataCommandLineTokenHint',
    'Test-RepositoryPythonProcess',
    'Test-RepositoryMaterializerRunnerProcess'
)
foreach ($Name in $Names) {
    $Definitions = @($Ast.FindAll({
        param($Node)
        $Node -is [System.Management.Automation.Language.FunctionDefinitionAst] `
            -and $Node.Name -eq $Name
    }, $true))
    if ($Definitions.Count -ne 1) {
        throw "function definition is ambiguous: $Name"
    }
    Invoke-Expression ([string]$Definitions[0].Extent.Text)
}
$RepoRoot = $Repository
$DataRoot = Join-Path $Repository 'data'
$Process = [pscustomobject]@{
    Name = $ProcessName
    ProcessId = 42
    CommandLine = $env:GUVOLU_TEST_COMMAND_LINE
}
if ($ProcessName -eq 'python.exe') {
    $Result = Test-RepositoryPythonProcess `
        -Process $Process `
        -Module 'guvolu.data.book_state_materialize' `
        -Command 'watch'
} else {
    $Result = Test-RepositoryMaterializerRunnerProcess `
        -Process $Process `
        -Materializer @{
            Module = 'guvolu.data.book_state_materialize'
            Runner = $Runner
        }
}
$Result | ConvertTo-Json -Compress
""",
        encoding="utf-8",
    )
    environment = os.environ.copy()
    environment["GUVOLU_TEST_COMMAND_LINE"] = command_line
    return subprocess.run(
        [
            str(POWERSHELL), "-NoProfile", "-NonInteractive",
            "-ExecutionPolicy", "Bypass", "-File", str(probe),
            "-StartScript",
            str(Path("scripts/start_marketdata_pipeline.ps1").resolve()),
            "-Contract",
            str(Path("scripts/l2_materializer_process_contract.ps1").resolve()),
            "-ProcessName", process_name,
            "-Repository", str(repository),
            "-RunnerRoot", str(runner_root),
            "-Runner", runner,
        ],
        check=False,
        capture_output=True,
        text=True,
        encoding="utf-8",
        env=environment,
        timeout=10,
    )


@pytest.mark.skipif(
    POWERSHELL is None,
    reason="需要 Windows PowerShell 5.1 token parser",
)
@pytest.mark.parametrize(
    ("kind", "command", "selection"),
    (
        (
            "python.exe",
            "'{python}' "
            "-m guvolu.data.l2_materialize "
            "--data-root '{data}' --latest-run-only watch",
            "latest_run",
        ),
        (
            "python.exe",
            "python.exe -m guvolu.data.l2_materialize "
            '--data-root="{data}" watch '
            "--latest-sealed-segments-per-stream=7",
            "latest_sealed_per_stream:7",
        ),
        (
            "powershell.exe",
            "powershell.exe -NoProfile -File '{runner}' "
            "-LatestSealedSegmentsPerStream 9 "
            "-Repository '{repository}'",
            "latest_sealed_per_stream:9",
        ),
    ),
)
def test_l2_process_contract_handles_tail_position_and_quotes(
    tmp_path: Path,
    kind: str,
    command: str,
    selection: str,
) -> None:
    repository = tmp_path / "repository with spaces"
    runner = tmp_path / "launcher with spaces" / "run_l2_materializer.ps1"
    completed = _run_l2_process_contract(
        tmp_path,
        process_name=kind,
        command_line=command.format(
            python=Path(sys.executable).resolve(),
            data=repository / "data",
            runner=runner,
            repository=repository,
        ),
        repository=repository,
        runner=runner,
    )
    assert completed.returncode == 0, completed.stderr
    result = json.loads(completed.stdout)
    assert result["Selection"] == selection
    assert result["Kind"] == (
        "python" if kind == "python.exe" else "runner"
    )


@pytest.mark.skipif(
    POWERSHELL is None,
    reason="需要 Windows PowerShell 5.1 token parser",
)
@pytest.mark.parametrize(
    ("kind", "command", "message"),
    (
        (
            "python.exe",
            "python.exe -m guvolu.data.l2_materialize "
            "--data-root '{data}' watch --latest-run-only "
            "--latest-run-only",
            "repeated",
        ),
        (
            "python.exe",
            "python.exe -m guvolu.data.l2_materialize "
            "--data-root '{data}' watch "
            "--latest-sealed-segments-per-stream 0",
            "positive integer",
        ),
        (
            "python.exe",
            "python.exe -m guvolu.data.l2_materialize "
            "--data-root '{data}' --data-root '{data}' watch",
            "data-root identity is ambiguous",
        ),
        (
            "powershell.exe",
            "powershell.exe -File '{runner}' "
            "-Repository '{repository}' -LatestRunOnly:$false",
            "opaque",
        ),
        (
            "powershell.exe",
            "powershell.exe -File '{runner}' "
            "-Repository '{repository}' "
            "-LatestSealedSegmentsPerStream 2 "
            "-LatestSealedSegmentsPerStream 3",
            "repeated",
        ),
        (
            "python.exe",
            "python.exe -c 'print(1)' -m guvolu.data.l2_materialize "
            "--data-root '{data}' watch",
            "interpreter entry is opaque",
        ),
        (
            "python.exe",
            "python.exe other.py -m guvolu.data.l2_materialize "
            "--data-root '{data}' watch",
            "interpreter entry is opaque",
        ),
        (
            "python.exe",
            "python.exe - -m guvolu.data.l2_materialize "
            "--data-root '{data}' watch",
            "opaque",
        ),
        (
            "python.exe",
            "python.exe -X dev -m guvolu.data.l2_materialize "
            "--data-root '{data}' watch",
            "interpreter entry is opaque",
        ),
        (
            "python.exe",
            "python.exe -m guvolu.data.l2_materialize "
            "--data-root '{data}' watch "
            "-m guvolu.data.l2_materialize",
            "module/command identity is ambiguous",
        ),
        (
            "python.exe",
            "python.exe -m guvolu.data.l2_materialize "
            "--data-root '{data}' watch --latest-run",
            "opaque",
        ),
        (
            "python.exe",
            "python.exe -m guvolu.data.l2_materialize "
            "--data-ro '{data}' watch",
            "opaque",
        ),
        (
            "powershell.exe",
            "powershell.exe -File '{runner}' "
            "-Repository '{repository}' -LatestRun",
            "opaque",
        ),
        (
            "powershell.exe",
            "powershell.exe -File '{runner}' "
            "-Repo '{repository}' -LatestRunOnly",
            "opaque",
        ),
        (
            "powershell.exe",
            "powershell.exe -File '{runner}' -LatestRunOnly",
            "repository identity is ambiguous",
        ),
        (
            "powershell.exe",
            "powershell.exe -Command noop -File '{runner}' "
            "-Repository '{repository}'",
            "interpreter entry is opaque",
        ),
    ),
)
def test_l2_process_contract_rejects_repeated_or_opaque_arguments(
    tmp_path: Path,
    kind: str,
    command: str,
    message: str,
) -> None:
    repository = tmp_path / "repository with spaces"
    runner = tmp_path / "launcher with spaces" / "run_l2_materializer.ps1"
    completed = _run_l2_process_contract(
        tmp_path,
        process_name=kind,
        command_line=command.format(
            python=Path(sys.executable).resolve(),
            data=repository / "data",
            runner=runner,
            repository=repository,
        ),
        repository=repository,
        runner=runner,
    )
    assert completed.returncode != 0
    assert message in (completed.stdout + completed.stderr)


@pytest.mark.skipif(
    os.name != "nt" or POWERSHELL is None,
    reason="需要 Windows 物理路径 identity 与目录联接",
)
@pytest.mark.parametrize("kind", ("python.exe", "powershell.exe"))
def test_l2_process_contract_recognizes_physical_path_aliases(
    tmp_path: Path,
    kind: str,
) -> None:
    repository = tmp_path / "physical repository"
    runner = repository / "scripts" / "run_l2_materializer.ps1"
    (repository / "data").mkdir(parents=True)
    runner.parent.mkdir(parents=True)
    runner.touch()
    alias = tmp_path / "repository alias"
    subprocess.run(
        [
            "cmd.exe", "/d", "/c", "mklink", "/J",
            str(alias), str(repository),
        ],
        check=True,
        capture_output=True,
        text=True,
    )
    command = (
        "python.exe -m guvolu.data.l2_materialize "
        f"--data-root '{alias / 'data'}' watch"
        if kind == "python.exe" else
        "powershell.exe -File "
        f"'{alias / 'scripts' / 'run_l2_materializer.ps1'}' "
        f"-Repository '{alias}'"
    )

    completed = _run_l2_process_contract(
        tmp_path,
        process_name=kind,
        command_line=command,
        repository=repository,
        runner=runner,
    )

    assert completed.returncode == 0, completed.stderr
    assert json.loads(completed.stdout)["Selection"] == "all"


@pytest.mark.skipif(
    os.name != "nt" or POWERSHELL is None,
    reason="需要 Windows PowerShell 5.1 进程 token 合同",
)
@pytest.mark.parametrize(
    ("process_name", "command_template", "message"),
    (
        (
            "python.exe",
            "python.exe other.py -m guvolu.data.book_state_materialize "
            "--data-root '{data}' watch",
            "Python interpreter entry is opaque",
        ),
        (
            "python.exe",
            "python.exe -c noop -m guvolu.data.book_state_materialize "
            "--data-root '{data}' watch",
            "Python interpreter entry is opaque",
        ),
        (
            "powershell.exe",
            "powershell.exe -Command noop -File '{runner}' "
            "-Repository '{repository}'",
            "PowerShell interpreter entry is opaque",
        ),
    ),
)
def test_marketdata_process_predicates_reject_opaque_interpreter_entries(
    tmp_path: Path,
    process_name: str,
    command_template: str,
    message: str,
) -> None:
    repository = tmp_path / "repository with spaces"
    runner_root = tmp_path / "launcher with spaces" / "scripts"
    runner_name = "run_book_state_materializer.ps1"
    completed = _run_marketdata_process_predicate(
        tmp_path,
        process_name=process_name,
        command_line=command_template.format(
            data=repository / "data",
            runner=runner_root / runner_name,
            repository=repository,
        ),
        repository=repository,
        runner_root=runner_root,
        runner=runner_name,
    )
    assert completed.returncode != 0
    assert message in (completed.stdout + completed.stderr)


@pytest.mark.skipif(
    os.name != "nt" or POWERSHELL is None,
    reason="需要 Windows PowerShell 5.1 与目录联接",
)
def test_python_l2_selection_conflict_fails_before_recovery_or_collectors(
    tmp_path: Path,
) -> None:
    repository, scripts, log = _make_probe_repository(tmp_path)
    probe = tmp_path / "probe package"
    environment = _probe_environment(probe, log)
    python = repository / ".venv" / "Scripts" / "python.exe"
    watcher = subprocess.Popen(
        [
            python,
            "-m",
            "guvolu.data.l2_materialize",
            "--data-root",
            str(repository / "data"),
            "--latest-run-only",
            "watch",
        ],
        env=environment,
    )
    try:
        deadline = time.monotonic() + 10
        while not _read_probe(log) and time.monotonic() < deadline:
            time.sleep(0.05)
        assert len(_read_probe(log)) == 1
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
                "-Repository",
                str(repository),
            ],
            check=False,
            capture_output=True,
            text=True,
            encoding="utf-8",
            env=environment,
            timeout=40,
        )
        assert completed.returncode != 0
        assert "existing process selection differs" in (
            completed.stdout + completed.stderr
        )
        time.sleep(0.2)
        assert len(_read_probe(log)) == 1
    finally:
        if watcher.poll() is None:
            watcher.terminate()
            watcher.wait(timeout=5)


@pytest.mark.skipif(
    os.name != "nt" or POWERSHELL is None,
    reason="需要 Windows PowerShell 5.1 与目录联接",
)
def test_runner_only_l2_conflict_fails_before_recovery_or_collectors(
    tmp_path: Path,
) -> None:
    repository, scripts, log = _make_probe_repository(tmp_path)
    probe = tmp_path / "probe package"
    environment = _probe_environment(probe, log)
    sentinel = tmp_path / "runner-started.txt"
    environment["GUVOLU_RUNNER_SENTINEL"] = str(sentinel)
    runner = scripts / "run_l2_materializer.ps1"
    runner.write_text(
        """\
param(
    [string]$Repository = '',
    [switch]$LatestRunOnly,
    [Nullable[int]]$LatestSealedSegmentsPerStream = $null
)
[System.IO.File]::WriteAllText(
    $env:GUVOLU_RUNNER_SENTINEL,
    [string]$PID
)
while ($true) { Start-Sleep -Seconds 1 }
""",
        encoding="utf-8",
    )
    parent = subprocess.Popen(
        [
            str(POWERSHELL),
            "-NoProfile",
            "-NonInteractive",
            "-ExecutionPolicy",
            "Bypass",
            "-File",
            str(runner),
            "-Repository",
            str(repository),
            "-LatestRunOnly",
        ],
        env=environment,
    )
    try:
        deadline = time.monotonic() + 10
        while not sentinel.exists() and time.monotonic() < deadline:
            time.sleep(0.05)
        assert sentinel.exists()
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
                "-Repository",
                str(repository),
            ],
            check=False,
            capture_output=True,
            text=True,
            encoding="utf-8",
            env=environment,
            timeout=20,
        )
        assert completed.returncode != 0
        assert "existing process selection differs" in (
            completed.stdout + completed.stderr
        )
        assert _read_probe(log) == []
    finally:
        if parent.poll() is None:
            parent.terminate()
            parent.wait(timeout=5)


@pytest.mark.skipif(
    os.name != "nt" or POWERSHELL is None,
    reason="需要 Windows PowerShell 5.1",
)
def test_public_l2_runner_rejects_abbreviated_switch_before_python(
    tmp_path: Path,
) -> None:
    repository, scripts, log = _make_probe_repository(tmp_path)
    environment = _probe_environment(tmp_path / "probe package", log)

    completed = subprocess.run(
        [
            str(POWERSHELL),
            "-NoProfile",
            "-NonInteractive",
            "-ExecutionPolicy",
            "Bypass",
            "-File",
            str(scripts / "run_l2_materializer.ps1"),
            "-Repository",
            str(repository),
            "-LatestRun",
        ],
        check=False,
        capture_output=True,
        text=True,
        encoding="utf-8",
        env=environment,
        timeout=10,
    )

    assert completed.returncode != 0
    assert "opaque form" in (completed.stdout + completed.stderr)
    assert _read_probe(log) == []


@pytest.mark.skipif(
    os.name != "nt" or POWERSHELL is None,
    reason="需要 Windows PowerShell 5.1 与命名 mutex",
)
def test_concurrent_full_launchers_share_one_confirmed_l2_owner(
    tmp_path: Path,
) -> None:
    repository, scripts, log = _make_probe_repository(tmp_path)
    environment = _probe_environment(tmp_path / "probe package", log)
    command = [
        str(POWERSHELL),
        "-NoProfile",
        "-NonInteractive",
        "-ExecutionPolicy",
        "Bypass",
        "-File",
        str(scripts / "start_marketdata_pipeline.ps1"),
        "-WindowStyle",
        "Hidden",
        "-Repository",
        str(repository),
    ]
    first = subprocess.Popen(
        command, env=environment,
        stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL,
    )
    second = subprocess.Popen(
        command, env=environment,
        stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL,
    )
    owner_path = (
        repository / "data/.locks/l2-materializer-owner.json"
    )
    owner_pid: int | None = None
    try:
        assert first.wait(timeout=60) == 0
        assert second.wait(timeout=60) == 0
        deadline = time.monotonic() + 10
        while not owner_path.exists() and time.monotonic() < deadline:
            time.sleep(0.05)
        owner = json.loads(owner_path.read_text(encoding="utf-8"))
        owner_pid = int(owner["pid"])
        assert owner["selection"] == "all"
        rows = _read_probe(log)
        l2_watch = [
            row for row in rows
            if row["module"].endswith("l2_materialize")
            and "watch" in row["argv"]
        ]
        recoveries = [
            row for row in rows if "recover" in row["argv"]
        ]
        assert len(l2_watch) == 1
        assert recoveries
        assert int(l2_watch[0]["recorded_at_ns"]) < min(
            int(row["recorded_at_ns"]) for row in recoveries
        )
    finally:
        for launcher in (first, second):
            if launcher.poll() is None:
                launcher.terminate()
                launcher.wait(timeout=5)
        if owner_pid is not None:
            subprocess.run(
                ["taskkill.exe", "/PID", str(owner_pid), "/T", "/F"],
                check=False,
                capture_output=True,
                text=True,
            )


@pytest.mark.skipif(
    os.name != "nt" or POWERSHELL is None,
    reason="需要 Windows PowerShell 5.1 与目录联接",
)
def test_full_junction_alias_reentry_reuses_all_marketdata_processes(
    tmp_path: Path,
) -> None:
    """物理仓库已启动后，经 junction 重入不得复制六采集与三派生进程。"""
    repository, scripts, log = _make_probe_repository(tmp_path)
    alias = tmp_path / "repository junction alias"
    subprocess.run(
        ["cmd.exe", "/d", "/c", "mklink", "/J", str(alias), str(repository)],
        check=True,
        capture_output=True,
        text=True,
    )
    release = tmp_path / "release-held-probes"
    environment = _probe_environment(tmp_path / "probe package", log)
    environment["GUVOLU_PROCESS_PROBE_HOLD_NON_L2"] = "1"
    environment["GUVOLU_PROCESS_PROBE_RELEASE"] = str(release)

    def launch(target: Path) -> subprocess.CompletedProcess[str]:
        return subprocess.run(
            [
                str(POWERSHELL), "-NoProfile", "-NonInteractive",
                "-ExecutionPolicy", "Bypass", "-File",
                str(scripts / "start_marketdata_pipeline.ps1"),
                "-WindowStyle", "Hidden", "-Repository", str(target),
            ],
            check=False,
            capture_output=True,
            text=True,
            encoding="utf-8",
            env=environment,
            timeout=60,
        )

    held_pids: set[int] = set()
    try:
        first = launch(repository)
        assert first.returncode == 0, first.stdout + first.stderr
        deadline = time.monotonic() + 20
        held: list[ProbeRecord] = []
        while time.monotonic() < deadline:
            held = [
                row for row in _read_probe(log)
                if "record" in row["argv"] or "watch" in row["argv"]
            ]
            if len(held) == 10:
                break
            time.sleep(0.05)
        assert len(held) == 10
        held_pids = {row["pid"] for row in held}

        second = launch(alias)
        assert second.returncode == 0, second.stdout + second.stderr
        time.sleep(0.25)
        after = [
            row for row in _read_probe(log)
            if "record" in row["argv"] or "watch" in row["argv"]
        ]
        assert len(after) == 10
        assert {row["pid"] for row in after} == held_pids
        assert sum("record" in row["argv"] for row in after) == 6
        assert sum("watch" in row["argv"] for row in after) == 4
    finally:
        release.touch(exist_ok=True)
        time.sleep(0.25)
        for pid in held_pids:
            subprocess.run(
                ["taskkill.exe", "/PID", str(pid), "/T", "/F"],
                check=False,
                capture_output=True,
                text=True,
            )


@pytest.mark.skipif(
    os.name != "nt" or POWERSHELL is None,
    reason="需要 Windows PowerShell 5.1 与目录联接",
)
def test_forward_minimal_stops_materializers_through_repo_alias(
    tmp_path: Path,
) -> None:
    """ForwardMinimal 必须经物理路径识别 alias 下的 runner 与 Python。"""
    repository, scripts, log = _make_probe_repository(tmp_path)
    alias = tmp_path / "repository junction alias"
    subprocess.run(
        ["cmd.exe", "/d", "/c", "mklink", "/J", str(alias), str(repository)],
        check=True,
        capture_output=True,
        text=True,
    )
    launcher_alias = tmp_path / "launcher junction alias"
    subprocess.run(
        [
            "cmd.exe", "/d", "/c", "mklink", "/J",
            str(launcher_alias), str(scripts.parent),
        ],
        check=True,
        capture_output=True,
        text=True,
    )
    environment = _probe_environment(tmp_path / "probe package", log)
    holder_environment = environment.copy()
    holder_environment["GUVOLU_PROCESS_PROBE_HOLD_NON_L2"] = "1"
    sleeping_runner = scripts / "run_book_state_materializer.ps1"
    sleeping_runner.write_text(
        """\
param(
    [string]$Repository = '',
    [int]$IntervalSeconds = 300
)
Start-Sleep -Seconds 60
""",
        encoding="utf-8",
    )
    runner_invocation = launcher_alias / "scripts" / sleeping_runner.name
    runner = subprocess.Popen(
        [
            str(POWERSHELL), "-NoProfile", "-NonInteractive",
            "-ExecutionPolicy", "Bypass", "-File", str(runner_invocation),
            "-Repository", str(repository), "-IntervalSeconds", "300",
        ],
        env=holder_environment,
        stdout=subprocess.DEVNULL,
        stderr=subprocess.DEVNULL,
    )
    python_watch = subprocess.Popen(
        [
            repository / ".venv/Scripts/python.exe",
            "-m", "guvolu.data.orderflow_tile_materialize",
            "--data-root", str(repository / "data"), "watch",
        ],
        env=holder_environment,
        stdout=subprocess.DEVNULL,
        stderr=subprocess.DEVNULL,
    )
    processes = (runner, python_watch)
    child_pids: set[int] = set()
    try:
        deadline = time.monotonic() + 10
        held: list[ProbeRecord] = []
        while time.monotonic() < deadline:
            held = [
                row for row in _read_probe(log)
                if row["module"].endswith("orderflow_tile_materialize")
                and "watch" in row["argv"]
            ]
            if len(held) == 1 and runner.poll() is None:
                break
            time.sleep(0.05)
        assert len(held) == 1
        assert runner.poll() is None
        child_pids = {row["pid"] for row in held}

        completed = subprocess.run(
            [
                str(POWERSHELL), "-NoProfile", "-NonInteractive",
                "-ExecutionPolicy", "Bypass", "-File",
                str(scripts / "start_marketdata_pipeline.ps1"),
                "-WindowStyle", "Hidden", "-Profile", "ForwardMinimal",
                "-Repository", str(alias),
            ],
            check=False,
            capture_output=True,
            text=True,
            encoding="utf-8",
            env=environment,
            timeout=45,
        )

        assert completed.returncode == 0, completed.stdout + completed.stderr
        for process in processes:
            process.wait(timeout=10)
        deadline = time.monotonic() + 5
        while (
            any(_pid_is_running(pid) for pid in child_pids)
            and time.monotonic() < deadline
        ):
            time.sleep(0.05)
        assert not any(_pid_is_running(pid) for pid in child_pids)
        assert any("recover" in row["argv"] for row in _read_probe(log))
    finally:
        for process in processes:
            if process.poll() is None:
                process.terminate()
                process.wait(timeout=5)
        for pid in child_pids:
            subprocess.run(
                ["taskkill.exe", "/PID", str(pid), "/T", "/F"],
                check=False,
                capture_output=True,
                text=True,
            )


@pytest.mark.skipif(
    os.name != "nt" or POWERSHELL is None,
    reason="需要 Windows PowerShell 5.1 与 owner byte lock",
)
def test_locked_owner_executable_mismatch_fails_before_recovery(
    tmp_path: Path,
) -> None:
    repository, scripts, log = _make_probe_repository(tmp_path)
    environment = _probe_environment(tmp_path / "probe package", log)
    python = repository / ".venv/Scripts/python.exe"
    watcher = subprocess.Popen(
        [
            python, "-m", "guvolu.data.l2_materialize",
            "--data-root", str(repository / "data"), "watch",
        ],
        env=environment,
    )
    owner_path = repository / "data/.locks/l2-materializer-owner.json"
    try:
        deadline = time.monotonic() + 10
        while not owner_path.exists() and time.monotonic() < deadline:
            time.sleep(0.05)
        owner = json.loads(owner_path.read_text(encoding="utf-8"))
        owner["executable_path"] = str(
            Path(os.environ["SystemRoot"]) / "System32/cmd.exe"
        )
        owner_path.write_text(json.dumps(owner), encoding="utf-8")

        completed = subprocess.run(
            [
                str(POWERSHELL), "-NoProfile", "-NonInteractive",
                "-ExecutionPolicy", "Bypass", "-File",
                str(scripts / "start_marketdata_pipeline.ps1"),
                "-WindowStyle", "Hidden", "-Repository", str(repository),
            ],
            check=False,
            capture_output=True,
            text=True,
            encoding="utf-8",
            env=environment,
            timeout=30,
        )

        assert completed.returncode != 0
        assert "locked owner executable differs" in (
            completed.stdout + completed.stderr
        )
        assert len(_read_probe(log)) == 1
    finally:
        if watcher.poll() is None:
            watcher.terminate()
            watcher.wait(timeout=5)


@pytest.mark.skipif(
    os.name != "nt" or POWERSHELL is None,
    reason="需要 Windows PowerShell 5.1 与 owner byte lock",
)
def test_forward_minimal_retries_when_direct_watch_wins_suppression_race(
    tmp_path: Path,
) -> None:
    """空闲观察后抢锁的直连 watch 必须被重查停止，才能进入 recover。"""
    repository, scripts, log = _make_probe_repository(tmp_path)
    environment = _probe_environment(tmp_path / "probe package", log)
    ready = tmp_path / "suppression-ready"
    proceed = tmp_path / "suppression-proceed"
    environment["GUVOLU_SUPPRESSION_READY"] = str(ready)
    environment["GUVOLU_SUPPRESSION_PROCEED"] = str(proceed)
    launcher_path = scripts / "start_marketdata_pipeline.ps1"
    launcher_source = launcher_path.read_text(encoding="utf-8")
    needle = "            $Suppression = Try-Enter-L2OwnerSuppression\n"
    barrier = """\
            [System.IO.File]::WriteAllText(
                $env:GUVOLU_SUPPRESSION_READY,
                'ready'
            )
            while (-not (
                Test-Path -LiteralPath $env:GUVOLU_SUPPRESSION_PROCEED
            )) {
                Start-Sleep -Milliseconds 10
            }
"""
    assert needle in launcher_source
    launcher_path.write_text(
        launcher_source.replace(needle, barrier + needle, 1),
        encoding="utf-8",
    )
    launcher = subprocess.Popen(
        [
            str(POWERSHELL), "-NoProfile", "-NonInteractive",
            "-ExecutionPolicy", "Bypass", "-File", str(launcher_path),
            "-WindowStyle", "Hidden", "-Profile", "ForwardMinimal",
            "-Repository", str(repository),
        ],
        env=environment,
        stdout=subprocess.PIPE,
        stderr=subprocess.PIPE,
        text=True,
        encoding="utf-8",
    )
    watcher: subprocess.Popen[bytes] | None = None
    owner_path = repository / "data/.locks/l2-materializer-owner.json"
    try:
        deadline = time.monotonic() + 10
        while not ready.exists() and time.monotonic() < deadline:
            if launcher.poll() is not None:
                break
            time.sleep(0.05)
        assert ready.exists()

        started_watcher = subprocess.Popen(
            [
                repository / ".venv/Scripts/python.exe",
                "-m", "guvolu.data.l2_materialize",
                "--data-root", str(repository / "data"), "watch",
            ],
            env=environment,
            stdout=subprocess.DEVNULL,
            stderr=subprocess.DEVNULL,
        )
        watcher = started_watcher
        deadline = time.monotonic() + 10
        while not owner_path.exists() and time.monotonic() < deadline:
            if started_watcher.poll() is not None:
                break
            time.sleep(0.05)
        assert owner_path.exists()
        owner = json.loads(owner_path.read_text(encoding="utf-8"))
        owner_pid = int(owner["pid"])
        assert owner_pid > 0

        proceed.write_text("go", encoding="utf-8")
        stdout, stderr = launcher.communicate(timeout=45)
        assert launcher.returncode == 0, stdout + stderr
        assert started_watcher.wait(timeout=10) != 0
        assert not owner_path.exists()
        rows = _read_probe(log)
        assert any(
            row["pid"] == owner_pid
            and row["module"].endswith("l2_materialize")
            and "watch" in row["argv"]
            for row in rows
        )
        assert any("recover" in row["argv"] for row in rows)
    finally:
        proceed.touch(exist_ok=True)
        if launcher.poll() is None:
            launcher.terminate()
            launcher.wait(timeout=5)
        if watcher is not None and watcher.poll() is None:
            watcher.terminate()
            watcher.wait(timeout=5)


@pytest.mark.skipif(
    os.name != "nt" or POWERSHELL is None,
    reason="需要 Windows PowerShell 5.1 与 owner byte lock",
)
def test_forward_minimal_clears_unlocked_stale_owner_before_recovery(
    tmp_path: Path,
) -> None:
    repository, scripts, log = _make_probe_repository(tmp_path)
    environment = _probe_environment(tmp_path / "probe package", log)
    owner_path = repository / "data/.locks/l2-materializer-owner.json"
    owner_path.parent.mkdir(parents=True, exist_ok=True)
    owner_path.write_text('{"stale":true}', encoding="utf-8")

    completed = subprocess.run(
        [
            str(POWERSHELL), "-NoProfile", "-NonInteractive",
            "-ExecutionPolicy", "Bypass", "-File",
            str(scripts / "start_marketdata_pipeline.ps1"),
            "-WindowStyle", "Hidden", "-Profile", "ForwardMinimal",
            "-Repository", str(repository),
        ],
        check=False,
        capture_output=True,
        text=True,
        encoding="utf-8",
        env=environment,
        timeout=30,
    )

    assert completed.returncode == 0, completed.stderr
    assert not owner_path.exists()
    assert any("recover" in row["argv"] for row in _read_probe(log))


@pytest.mark.skipif(
    os.name != "nt" or POWERSHELL is None,
    reason="需要 Windows PowerShell 5.1 与 owner byte lock",
)
def test_full_replaces_unlocked_stale_owner_before_handshake(
    tmp_path: Path,
) -> None:
    repository, scripts, log = _make_probe_repository(tmp_path)
    environment = _probe_environment(tmp_path / "probe package", log)
    owner_path = repository / "data/.locks/l2-materializer-owner.json"
    owner_path.parent.mkdir(parents=True, exist_ok=True)
    owner_path.write_text(
        json.dumps({
            "schema_version": 1,
            "pid": 2147483647,
            "selection": "latest_run",
            "data_root": str(repository / "data"),
            "executable_path": str(repository / ".venv/Scripts/python.exe"),
            "started_at": datetime.now(UTC).isoformat(),
            "nonce": "0" * 32,
        }),
        encoding="utf-8",
    )
    owner_pid: int | None = None
    try:
        completed = subprocess.run(
            [
                str(POWERSHELL), "-NoProfile", "-NonInteractive",
                "-ExecutionPolicy", "Bypass", "-File",
                str(scripts / "start_marketdata_pipeline.ps1"),
                "-WindowStyle", "Hidden", "-Repository", str(repository),
            ],
            check=False,
            capture_output=True,
            text=True,
            encoding="utf-8",
            env=environment,
            timeout=45,
        )

        assert completed.returncode == 0, completed.stderr
        owner = json.loads(owner_path.read_text(encoding="utf-8"))
        owner_pid = int(owner["pid"])
        assert owner_pid != 2147483647
        assert owner["selection"] == "all"
        assert any("recover" in row["argv"] for row in _read_probe(log))
    finally:
        if owner_pid is not None:
            subprocess.run(
                ["taskkill.exe", "/PID", str(owner_pid), "/T", "/F"],
                check=False,
                capture_output=True,
                text=True,
            )


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
            "--latest-sealed-segments-per-stream",
            "3",
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
        owner_path = repository / "data/.locks/l2-materializer-owner.json"
        assert not owner_path.exists()
        import msvcrt

        with (
            repository / "data/.locks/l2-materializer-owner.lock"
        ).open("r+b") as owner_lock:
            owner_lock.seek(0)
            msvcrt.locking(owner_lock.fileno(), msvcrt.LK_NBLCK, 1)
            owner_lock.seek(0)
            msvcrt.locking(owner_lock.fileno(), msvcrt.LK_UNLCK, 1)
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

"""冻结 shadow 调度必须绑定封存代码树，并与活数据根严格分离。"""
from __future__ import annotations

import base64
import hashlib
import json
import os
import shutil
import subprocess
import sys
from datetime import datetime
from pathlib import Path

import pytest


POWERSHELL = shutil.which("powershell.exe")
GIT = shutil.which("git.exe") or shutil.which("git")
PLAN_ID = "frozen-forward-plan-" + "c" * 64
AVAILABLE = POWERSHELL is not None and GIT is not None


def _sha256(path: Path) -> str:
    """计算注册证据使用的文件 SHA256。"""
    return hashlib.sha256(path.read_bytes()).hexdigest()


def _venv_tree_sha256(root: Path) -> str:
    """复算包装器与 runner 共用的规范 venv tree identity。"""
    material = bytearray()
    for path in sorted(
        (candidate for candidate in root.rglob("*") if candidate.is_file()),
        key=lambda candidate: candidate.relative_to(root).as_posix(),
    ):
        relative = path.relative_to(root).as_posix()
        content = path.read_bytes()
        material.extend(relative.encode("utf-8"))
        material.extend(b"\0")
        material.extend(str(len(content)).encode("ascii"))
        material.extend(b"\0")
        material.extend(hashlib.sha256(content).hexdigest().encode("ascii"))
        material.extend(b"\n")
    return hashlib.sha256(material).hexdigest()


def _system32_powershell() -> Path:
    """返回计划任务唯一允许的绝对 Windows PowerShell。"""
    return (
        Path(os.environ["SystemRoot"])
        / "System32"
        / "WindowsPowerShell"
        / "v1.0"
        / "powershell.exe"
    ).resolve()


def _git(repository: Path, *arguments: str) -> str:
    """在临时代码仓执行确定性的本地 Git 命令。"""
    assert GIT is not None
    result = subprocess.run(
        [GIT, "-C", str(repository), *arguments],
        check=False,
        capture_output=True,
        text=True,
        encoding="utf-8",
    )
    assert result.returncode == 0, result.stderr
    return result.stdout.strip()


def _code_checkout(
    tmp_path: Path,
    *,
    detach: bool = True,
) -> tuple[Path, str]:
    """建立含真实注册器/包装器的 tracked-clean 临时代码仓。"""
    code_root = tmp_path / "detached code with spaces"
    scripts = code_root / "scripts"
    (code_root / "src").mkdir(parents=True)
    scripts.mkdir()
    for name in (
        "register_frozen_shadow_task.ps1",
        "run_frozen_shadow_task.ps1",
    ):
        shutil.copy2(Path("scripts") / name, scripts / name)
    (scripts / "run_frozen_shadow.py").write_text(
        """
from __future__ import annotations
import json
import os
import sys
from pathlib import Path

arguments = sys.argv[1:]
data_root = Path(arguments[arguments.index("--repository") + 1])
(data_root / "wrapper-capture.json").write_text(
    json.dumps(
        {
            "argv": arguments,
            "cwd": os.getcwd(),
            "pythonpath": os.environ.get("PYTHONPATH"),
        }
    ),
    encoding="utf-8",
)
""".lstrip(),
        encoding="utf-8",
    )
    _git(code_root, "init", "--quiet")
    _git(code_root, "config", "core.autocrlf", "false")
    _git(code_root, "config", "user.email", "shadow-test@example.invalid")
    _git(code_root, "config", "user.name", "Shadow Test")
    _git(code_root, "add", "scripts", "src")
    _git(code_root, "commit", "--quiet", "-m", "frozen shadow code")
    head = _git(code_root, "rev-parse", "HEAD")
    if detach:
        _git(code_root, "checkout", "--quiet", "--detach", head)
    # 刷新 Windows Git 状态缓存
    _git(code_root, "status", "--porcelain=v1", "--untracked-files=all")
    assert _git(
        code_root,
        "status",
        "--porcelain=v1",
        "--untracked-files=all",
    ) == ""
    return code_root, head


def _roots(tmp_path: Path) -> tuple[Path, Path, Path]:
    """建立彼此独立的活数据、运行与执行根。"""
    data_root = tmp_path / "live data with spaces"
    runtime = tmp_path / "runtime with spaces"
    execution = tmp_path / "execution with spaces"
    for path in (data_root, runtime, execution):
        path.mkdir()
    venv = execution / ".venv"
    (venv / "Scripts").mkdir(parents=True)
    (venv / "Lib" / "site-packages").mkdir(parents=True)
    (venv / "Scripts" / "python.exe").write_bytes(b"fixture-python")
    (venv / "pyvenv.cfg").write_text("home = fixture\n", encoding="ascii")
    return data_root, runtime, execution


def _registration_command(
    code_root: Path,
    head: str,
    data_root: Path,
    runtime: Path,
    execution: Path,
    *,
    minute_offset: int = 25,
    describe_only: bool = True,
    no_paper: bool = True,
) -> list[str]:
    """生成不含任何隐式代码根参数的注册器命令。"""
    assert POWERSHELL is not None
    assert GIT is not None
    command = [
        POWERSHELL,
        "-NoProfile",
        "-File",
        str(code_root / "scripts" / "register_frozen_shadow_task.ps1"),
        "-PlanId",
        PLAN_ID,
        "-StartUtc",
        "2026-08-24T00:00:00Z",
        "-EndUtc",
        "2026-12-02T00:00:00Z",
        "-RuntimeRoot",
        str(runtime),
        "-ExecutionRepository",
        str(execution),
        "-Repository",
        str(data_root),
        "-PythonExecutable",
        str(Path(sys.executable).resolve()),
        "-GitExecutable",
        str(Path(GIT).resolve()),
        "-ExpectedCodeHead",
        head,
        "-MinuteOffset",
        str(minute_offset),
    ]
    if no_paper:
        command.append("-NoPaper")
    if describe_only:
        command.append("-DescribeOnly")
    return command


def _run_describe(
    code_root: Path,
    head: str,
    data_root: Path,
    runtime: Path,
    execution: Path,
    *,
    minute_offset: int = 25,
) -> subprocess.CompletedProcess[str]:
    """运行 DescribeOnly；该路径绝不应接触任务计划程序。"""
    return subprocess.run(
        _registration_command(
            code_root,
            head,
            data_root,
            runtime,
            execution,
            minute_offset=minute_offset,
        ),
        check=False,
        capture_output=True,
        text=True,
        encoding="utf-8",
    )


def _wrapper_command(
    code_root: Path,
    head: str,
    data_root: Path,
    runtime: Path,
    execution: Path,
    *,
    no_paper: bool = True,
) -> list[str]:
    """生成带全部注册身份的直接包装器命令。"""
    assert POWERSHELL is not None
    assert GIT is not None
    wrapper = code_root / "scripts" / "run_frozen_shadow_task.ps1"
    python = Path(sys.executable).resolve()
    git = Path(GIT).resolve()
    command = [
        POWERSHELL,
        "-NoProfile",
        "-File",
        str(wrapper),
        "-PlanId",
        PLAN_ID,
        "-Repository",
        str(data_root),
        "-RuntimeRoot",
        str(runtime),
        "-ExecutionRepository",
        str(execution),
        "-PythonExecutable",
        str(python),
        "-ExpectedPythonSha256",
        _sha256(python),
        "-GitExecutable",
        str(git),
        "-ExpectedGitSha256",
        _sha256(git),
        "-ExpectedExecutionEnvironmentTreeSha256",
        _venv_tree_sha256(execution / ".venv"),
        "-ExpectedCodeHead",
        head,
        "-ExpectedWrapperSha256",
        _sha256(wrapper),
    ]
    if no_paper:
        command.append("-NoPaper")
    return command


def _decode_bootstrap(arguments: str) -> str:
    """解码 action 中的 UTF-16LE PowerShell bootstrap。"""
    marker = "-EncodedCommand "
    assert marker in arguments
    encoded = arguments.split(marker, maxsplit=1)[1]
    return base64.b64decode(encoded).decode("utf-16le")


def _ps_literal(value: str | Path) -> str:
    """为测试 harness 构造无插值 PowerShell 字面量。"""
    return "'" + str(value).replace("'", "''") + "'"


def _mock_registration_harness(
    harness_path: Path,
    *,
    code_root: Path,
    head: str,
    data_root: Path,
    runtime: Path,
    execution: Path,
    calls_path: Path,
    xml_mismatch: bool = False,
    object_mismatch: bool = False,
    disable_fails: bool = False,
) -> None:
    """写入纯内存 ScheduledTasks mock，绝不操作系统任务。"""
    assert GIT is not None
    harness = rf"""
$ErrorActionPreference = 'Stop'
$global:ShadowMockCalls = [System.Collections.Generic.List[string]]::new()
$global:ShadowMockRegistered = $false
$global:ShadowMockXmlMismatch = ${str(xml_mismatch).lower()}
$global:ShadowMockObjectMismatch = ${str(object_mismatch).lower()}
$global:ShadowMockDisableFails = ${str(disable_fails).lower()}

function New-ScheduledTaskAction {{
    [CmdletBinding()]
    param([string]$Execute, [string]$Argument, [string]$WorkingDirectory)
    $global:ShadowMockCalls.Add('NewAction') | Out-Null
    [pscustomobject]@{{
        Execute = $Execute
        Arguments = $Argument
        WorkingDirectory = $WorkingDirectory
    }}
}}
function New-ScheduledTaskTrigger {{
    [CmdletBinding()]
    param(
        [switch]$Once,
        [datetime]$At,
        [timespan]$RepetitionInterval,
        [timespan]$RepetitionDuration
    )
    $global:ShadowMockCalls.Add('NewTrigger') | Out-Null
    [pscustomobject]@{{
        StartBoundary = $At
        Enabled = $true
        Repetition = [pscustomobject]@{{
            Interval = $RepetitionInterval
            Duration = $RepetitionDuration
        }}
    }}
}}
function New-ScheduledTaskPrincipal {{
    [CmdletBinding()]
    param([string]$UserId, [string]$LogonType, [string]$RunLevel)
    $global:ShadowMockCalls.Add('NewPrincipal') | Out-Null
    [pscustomobject]@{{
        UserId = $UserId
        LogonType = $LogonType
        RunLevel = $RunLevel
    }}
}}
function New-ScheduledTaskSettingsSet {{
    [CmdletBinding()]
    param(
        [string]$MultipleInstances,
        [switch]$StartWhenAvailable,
        [switch]$AllowStartIfOnBatteries,
        [switch]$DontStopIfGoingOnBatteries,
        [switch]$WakeToRun,
        [timespan]$ExecutionTimeLimit,
        [int]$RestartCount,
        [timespan]$RestartInterval,
        [switch]$Hidden,
        [switch]$Disable
    )
    $global:ShadowMockCalls.Add('NewSettings') | Out-Null
    [pscustomobject]@{{
        MultipleInstances = $MultipleInstances
        StartWhenAvailable = [bool]$StartWhenAvailable
        DisallowStartIfOnBatteries = -not [bool]$AllowStartIfOnBatteries
        StopIfGoingOnBatteries = -not [bool]$DontStopIfGoingOnBatteries
        WakeToRun = [bool]$WakeToRun
        ExecutionTimeLimit = $ExecutionTimeLimit
        RestartCount = $RestartCount
        RestartInterval = $RestartInterval
        Hidden = [bool]$Hidden
        Enabled = -not [bool]$Disable
    }}
}}
function Register-ScheduledTask {{
    [CmdletBinding()]
    param(
        [string]$TaskName,
        [object]$Action,
        [object]$Trigger,
        [object]$Principal,
        [object]$Settings,
        [switch]$Force
    )
    $global:ShadowMockCalls.Add('Register') | Out-Null
    $global:ShadowMockRegistered = $true
    $global:ShadowMockTaskName = $TaskName
    $global:ShadowMockAction = $Action
    $global:ShadowMockTrigger = $Trigger
    $global:ShadowMockPrincipal = $Principal
    $global:ShadowMockSettings = $Settings
}}
function Get-ScheduledTask {{
    [CmdletBinding()]
    param([string]$TaskName)
    if (-not $global:ShadowMockRegistered) {{
        $global:ShadowMockCalls.Add('GetExisting') | Out-Null
        return $null
    }}
    $global:ShadowMockCalls.Add('GetReadback') | Out-Null
    $WorkingDirectory = if ($global:ShadowMockObjectMismatch) {{
        'C:\contract-mismatch'
    }} else {{
        $global:ShadowMockAction.WorkingDirectory
    }}
    [pscustomobject]@{{
        TaskName = $global:ShadowMockTaskName
        State = 'Disabled'
        Actions = @([pscustomobject]@{{
            Execute = $global:ShadowMockAction.Execute
            Arguments = $global:ShadowMockAction.Arguments
            WorkingDirectory = $WorkingDirectory
        }})
        Triggers = @($global:ShadowMockTrigger)
        Principal = $global:ShadowMockPrincipal
        Settings = $global:ShadowMockSettings
    }}
}}
function Export-ScheduledTask {{
    [CmdletBinding()]
    param([string]$TaskName)
    $global:ShadowMockCalls.Add('Export') | Out-Null
    $Escape = {{
        param([string]$Value)
        [System.Security.SecurityElement]::Escape($Value)
    }}
    $WorkingDirectory = if ($global:ShadowMockXmlMismatch) {{
        'C:\xml-contract-mismatch'
    }} else {{
        $global:ShadowMockAction.WorkingDirectory
    }}
    $StartBoundary = $global:ShadowMockTrigger.StartBoundary.ToString('yyyy-MM-ddTHH:mm:ss')
    $Interval = [System.Xml.XmlConvert]::ToString(
        [timespan]$global:ShadowMockTrigger.Repetition.Interval
    )
    $Duration = [System.Xml.XmlConvert]::ToString(
        [timespan]$global:ShadowMockTrigger.Repetition.Duration
    )
    $ExecutionLimit = [System.Xml.XmlConvert]::ToString(
        [timespan]$global:ShadowMockSettings.ExecutionTimeLimit
    )
    $RestartInterval = [System.Xml.XmlConvert]::ToString(
        [timespan]$global:ShadowMockSettings.RestartInterval
    )
    $Command = & $Escape $global:ShadowMockAction.Execute
    $Arguments = & $Escape $global:ShadowMockAction.Arguments
    $Working = & $Escape $WorkingDirectory
    $User = & $Escape $global:ShadowMockPrincipal.UserId
    @"
<?xml version="1.0" encoding="UTF-16"?>
<Task xmlns="http://schemas.microsoft.com/windows/2004/02/mit/task">
  <Triggers><TimeTrigger>
    <StartBoundary>$StartBoundary</StartBoundary><Enabled>true</Enabled>
    <Repetition><Interval>$Interval</Interval><Duration>$Duration</Duration></Repetition>
  </TimeTrigger></Triggers>
  <Principals><Principal>
    <UserId>$User</UserId><LogonType>InteractiveToken</LogonType>
    <RunLevel>LeastPrivilege</RunLevel>
  </Principal></Principals>
  <Settings>
    <MultipleInstancesPolicy>IgnoreNew</MultipleInstancesPolicy>
    <DisallowStartIfOnBatteries>false</DisallowStartIfOnBatteries>
    <StopIfGoingOnBatteries>false</StopIfGoingOnBatteries>
    <StartWhenAvailable>true</StartWhenAvailable><WakeToRun>true</WakeToRun>
    <Enabled>false</Enabled><Hidden>true</Hidden>
    <ExecutionTimeLimit>$ExecutionLimit</ExecutionTimeLimit>
    <RestartOnFailure><Interval>$RestartInterval</Interval><Count>3</Count></RestartOnFailure>
  </Settings>
  <Actions><Exec>
    <Command>$Command</Command><Arguments>$Arguments</Arguments>
    <WorkingDirectory>$Working</WorkingDirectory>
  </Exec></Actions>
</Task>
"@
}}
function Disable-ScheduledTask {{
    [CmdletBinding()]
    param([string]$TaskName)
    $global:ShadowMockCalls.Add('Disable') | Out-Null
    if ($global:ShadowMockDisableFails) {{ throw 'synthetic disable failure' }}
}}
function Stop-ScheduledTask {{
    [CmdletBinding()]
    param([string]$TaskName)
    $global:ShadowMockCalls.Add('Stop') | Out-Null
}}

$InvokeArguments = @{{
    PlanId = { _ps_literal(PLAN_ID) }
    StartUtc = [datetime]'2026-08-24T00:00:00Z'
    EndUtc = [datetime]'2026-12-02T00:00:00Z'
    RuntimeRoot = { _ps_literal(runtime) }
    ExecutionRepository = { _ps_literal(execution) }
    Repository = { _ps_literal(data_root) }
    PythonExecutable = { _ps_literal(Path(sys.executable).resolve()) }
    GitExecutable = { _ps_literal(Path(GIT).resolve()) }
    ExpectedCodeHead = { _ps_literal(head) }
    NoPaper = $true
}}
$Succeeded = $false
$Failure = $null
try {{
    & { _ps_literal(code_root / 'scripts' / 'register_frozen_shadow_task.ps1') } `
        @InvokeArguments | Out-Null
    $Succeeded = $true
}} catch {{
    $Failure = $_
}} finally {{
    $global:ShadowMockCalls | ConvertTo-Json -Compress | Set-Content `
        -LiteralPath { _ps_literal(calls_path) } -Encoding UTF8
}}
if ($Succeeded) {{ exit 0 }}
[Console]::Error.WriteLine($Failure.Exception.Message)
exit 17
"""
    harness_path.write_text(harness, encoding="utf-8")


@pytest.mark.skipif(not AVAILABLE, reason="需要 Windows PowerShell 与 Git")
def test_shadow_registration_describes_separated_pinned_action(
    tmp_path: Path,
) -> None:
    """描述应绑定 detached 代码树，同时仅把 Repository 当活数据根。"""
    assert GIT is not None
    code_root, head = _code_checkout(tmp_path)
    data_root, runtime, execution = _roots(tmp_path)

    result = _run_describe(code_root, head, data_root, runtime, execution)

    assert result.returncode == 0, result.stderr
    definition = json.loads(result.stdout)
    assert definition["task_name"] == "guvolu-frozen-forward-cccccccccccc"
    assert Path(definition["execute"]).resolve() == _system32_powershell()
    assert definition["powershell_sha256"] == _sha256(_system32_powershell())
    assert definition["working_directory"] == str(code_root.resolve())
    assert definition["task_runner"] == str(
        (code_root / "scripts" / "run_frozen_shadow_task.ps1").resolve()
    )
    assert definition["code_root"] == str(code_root.resolve())
    assert definition["data_root"] == str(data_root.resolve())
    assert definition["runtime_root"] == str(runtime.resolve())
    assert definition["execution_repository"] == str(execution.resolve())
    assert definition["python_executable"] == str(Path(sys.executable).resolve())
    assert definition["python_sha256"] == _sha256(Path(sys.executable).resolve())
    assert definition["git_executable"] == str(Path(GIT).resolve())
    assert definition["git_sha256"] == _sha256(Path(GIT).resolve())
    venv = execution / ".venv"
    assert definition["execution_environment_tree_sha256"] == (
        _venv_tree_sha256(venv)
    )
    assert definition["execution_environment_file_count"] == 2
    assert definition["python_base_runtime_attestation"] == "unbound-partial"
    assert definition["paper_fill_cost_provenance"] == "unbound"
    assert definition["paper_capable"] is False
    assert definition["expected_code_head"] == head
    assert definition["actual_code_head"] == head
    assert definition["enabled"] is False
    assert definition["no_paper"] is True
    assert definition["principal_logon_type"] == "Interactive"
    assert definition["principal_run_level"] == "Limited"
    assert definition["unattended_coverage_capable"] is False
    assert "stores no credentials" in definition["coverage_limit"]
    assert "enables no task" in definition["coverage_limit"]
    assert definition["multiple_instances"] == "IgnoreNew"
    assert definition["start_when_available"] is True
    assert definition["allow_start_on_batteries"] is True
    assert definition["dont_stop_if_going_on_batteries"] is True
    assert definition["wake_to_run"] is True
    assert definition["hidden"] is True
    assert definition["execution_time_limit_minutes"] == 45
    assert definition["restart_count"] == 3
    assert definition["restart_interval_minutes"] == 5
    assert definition["repetition_interval"] == "PT1H"
    assert definition["arguments"].startswith(
        "-NoProfile -NonInteractive -ExecutionPolicy Bypass "
    )

    wrapper = code_root / "scripts" / "run_frozen_shadow_task.ps1"
    wrapper_sha256 = hashlib.sha256(wrapper.read_bytes()).hexdigest()
    assert definition["wrapper_sha256"] == wrapper_sha256
    bootstrap = _decode_bootstrap(definition["arguments"])
    assert str(wrapper.resolve()) in bootstrap
    assert wrapper_sha256 in bootstrap
    assert f"-Repository '{data_root.resolve()}'" in bootstrap
    assert f"-PythonExecutable '{Path(sys.executable).resolve()}'" in bootstrap
    assert f"-GitExecutable '{Path(GIT).resolve()}'" in bootstrap
    assert "-ExpectedPythonSha256 '" in bootstrap
    assert "-ExpectedGitSha256 '" in bootstrap
    assert "-ExpectedExecutionEnvironmentTreeSha256 '" in bootstrap
    assert f"-ExpectedCodeHead '{head}'" in bootstrap
    assert " -NoPaper" in bootstrap
    assert "[System.IO.File]::Open($wrapper" in bootstrap
    assert "[System.Security.Cryptography.SHA256]::Create()" in bootstrap


@pytest.mark.skipif(not AVAILABLE, reason="需要 Windows PowerShell 与 Git")
def test_shadow_registration_describe_calls_no_scheduled_task_command(
    tmp_path: Path,
) -> None:
    """所有 ScheduledTasks 命令均用抛错 mock 覆盖，DescribeOnly 仍须成功。"""
    code_root, head = _code_checkout(tmp_path)
    data_root, runtime, execution = _roots(tmp_path)
    harness = tmp_path / "describe-with-throwing-mocks.ps1"
    command = _registration_command(
        code_root,
        head,
        data_root,
        runtime,
        execution,
    )
    invocation_parts = ["& " + _ps_literal(command[3])]
    for item in command[4:]:
        invocation_parts.append(item if item.startswith("-") else _ps_literal(item))
    invocation = " ".join(invocation_parts)
    harness.write_text(
        "\n".join(
            [
                "$ErrorActionPreference = 'Stop'",
                *[
                    f"function {name} {{ throw '{name} must not run' }}"
                    for name in (
                        "Get-ScheduledTask",
                        "New-ScheduledTaskAction",
                        "New-ScheduledTaskTrigger",
                        "New-ScheduledTaskPrincipal",
                        "New-ScheduledTaskSettingsSet",
                        "Register-ScheduledTask",
                        "Export-ScheduledTask",
                        "Disable-ScheduledTask",
                        "Stop-ScheduledTask",
                    )
                ],
                invocation,
            ]
        ),
        encoding="utf-8",
    )
    assert POWERSHELL is not None

    result = subprocess.run(
        [POWERSHELL, "-NoProfile", "-File", str(harness)],
        check=False,
        capture_output=True,
        text=True,
        encoding="utf-8",
    )

    assert result.returncode == 0, result.stderr
    assert json.loads(result.stdout)["enabled"] is False


@pytest.mark.skipif(not AVAILABLE, reason="需要 Windows PowerShell 与 Git")
def test_shadow_registration_sanitizes_inherited_git_redirection(
    tmp_path: Path,
) -> None:
    """GIT_* 污染不得重定向代码根、索引或配置身份。"""
    code_root, head = _code_checkout(tmp_path)
    data_root, runtime, execution = _roots(tmp_path)
    hostile_environment = dict(os.environ)
    hostile_environment.update({
        "GIT_DIR": str(tmp_path / "bogus-git-dir"),
        "GIT_WORK_TREE": str(tmp_path / "bogus-work-tree"),
        "GIT_INDEX_FILE": str(tmp_path / "bogus-index"),
        "GIT_CONFIG_COUNT": "1",
        "GIT_CONFIG_KEY_0": "core.fsmonitor",
        "GIT_CONFIG_VALUE_0": "malicious-helper",
    })

    result = subprocess.run(
        _registration_command(
            code_root,
            head,
            data_root,
            runtime,
            execution,
        ),
        env=hostile_environment,
        check=False,
        capture_output=True,
        text=True,
        encoding="utf-8",
    )

    assert result.returncode == 0, result.stderr
    definition = json.loads(result.stdout)
    assert definition["actual_code_head"] == head
    assert Path(definition["code_root"]).resolve() == code_root.resolve()


@pytest.mark.skipif(not AVAILABLE, reason="需要 Windows PowerShell 与 Git")
def test_shadow_registration_forces_no_paper_even_without_caller_switch(
    tmp_path: Path,
) -> None:
    """注册接口不能解除成本 provenance 门禁。"""
    code_root, head = _code_checkout(tmp_path)
    data_root, runtime, execution = _roots(tmp_path)
    command = _registration_command(
        code_root,
        head,
        data_root,
        runtime,
        execution,
        no_paper=False,
    )

    result = subprocess.run(
        command,
        check=False,
        capture_output=True,
        text=True,
        encoding="utf-8",
    )

    assert result.returncode == 0, result.stderr
    definition = json.loads(result.stdout)
    assert definition["no_paper"] is True
    assert definition["paper_capable"] is False
    assert " -NoPaper" in _decode_bootstrap(definition["arguments"])


@pytest.mark.skipif(not AVAILABLE, reason="需要 Windows PowerShell 与 Git")
def test_shadow_action_bootstrap_runs_exact_wrapper(tmp_path: Path) -> None:
    """未漂移时 action bootstrap 应进入同一包装器并保留退出码。"""
    code_root, head = _code_checkout(tmp_path)
    data_root, runtime, execution = _roots(tmp_path)
    described = _run_describe(code_root, head, data_root, runtime, execution)
    assert described.returncode == 0, described.stderr
    definition = json.loads(described.stdout)
    assert POWERSHELL is not None

    result = subprocess.run(
        [definition["execute"], *definition["arguments"].split()],
        check=False,
        capture_output=True,
        text=True,
        encoding="utf-8",
        cwd=tmp_path,
    )

    assert result.returncode == 0, result.stderr
    capture = json.loads((data_root / "wrapper-capture.json").read_text())
    assert Path(capture["cwd"]).resolve() == code_root.resolve()
    assert capture["pythonpath"] is None


@pytest.mark.skipif(not AVAILABLE, reason="需要 Windows PowerShell 与 Git")
def test_shadow_action_bootstrap_rejects_wrapper_drift(tmp_path: Path) -> None:
    """调度 action 自身先验 SHA，包装器被替换时不能进入 Python runner。"""
    code_root, head = _code_checkout(tmp_path)
    data_root, runtime, execution = _roots(tmp_path)
    described = _run_describe(code_root, head, data_root, runtime, execution)
    assert described.returncode == 0, described.stderr
    definition = json.loads(described.stdout)
    wrapper = code_root / "scripts" / "run_frozen_shadow_task.ps1"
    wrapper.write_text("exit 0\n", encoding="ascii")
    assert POWERSHELL is not None

    result = subprocess.run(
        [definition["execute"], *definition["arguments"].split()],
        check=False,
        capture_output=True,
        text=True,
        encoding="utf-8",
        cwd=code_root,
    )

    assert result.returncode != 0
    assert "wrapper SHA256 mismatch" in result.stderr
    assert not (data_root / "wrapper-capture.json").exists()


@pytest.mark.skipif(not AVAILABLE, reason="需要 Windows PowerShell 与 Git")
def test_shadow_action_uses_absolute_system32_powershell_under_path_attack(
    tmp_path: Path,
) -> None:
    """工作目录伪造 powershell.exe 也不得先于受验 bootstrap 执行。"""
    code_root, head = _code_checkout(tmp_path)
    data_root, runtime, execution = _roots(tmp_path)
    described = _run_describe(code_root, head, data_root, runtime, execution)
    assert described.returncode == 0, described.stderr
    definition = json.loads(described.stdout)
    (code_root / "powershell.exe").write_bytes(b"not-a-Windows-executable")

    result = subprocess.run(
        [definition["execute"], *definition["arguments"].split()],
        check=False,
        capture_output=True,
        text=True,
        encoding="utf-8",
        cwd=code_root,
    )

    assert result.returncode == 3
    log = data_root / "logs/research/frozen-forward/shadow-scheduler.jsonl"
    record = json.loads(log.read_text(encoding="utf-8").splitlines()[-1])
    assert "completely clean" in record["output"]
    assert not (data_root / "wrapper-capture.json").exists()


@pytest.mark.skipif(not AVAILABLE, reason="需要 Windows PowerShell 与 Git")
def test_shadow_wrapper_ignores_python_environment_startup_injection(
    tmp_path: Path,
) -> None:
    """真实启动须忽略 PYTHONPATH、sitecustomize 与 PYTHONHOME 注入。"""
    code_root, head = _code_checkout(tmp_path)
    data_root, runtime, execution = _roots(tmp_path)
    malicious = tmp_path / "malicious python path"
    malicious.mkdir()
    marker = tmp_path / "sitecustomize-ran"
    (malicious / "sitecustomize.py").write_text(
        f"from pathlib import Path\nPath({str(marker)!r}).write_text('ran')\n",
        encoding="utf-8",
    )
    control_environment = dict(os.environ)
    control_environment["PYTHONPATH"] = str(malicious)
    control = subprocess.run(
        [sys.executable, "-c", "pass"],
        env=control_environment,
        check=False,
        capture_output=True,
        text=True,
        encoding="utf-8",
    )
    assert control.returncode == 0, control.stderr
    assert marker.is_file()
    marker.unlink()
    hostile_environment = dict(control_environment)
    hostile_environment["PYTHONHOME"] = str(malicious)
    hostile_environment["PYTHONSTARTUP"] = str(malicious / "sitecustomize.py")

    result = subprocess.run(
        _wrapper_command(code_root, head, data_root, runtime, execution),
        env=hostile_environment,
        check=False,
        capture_output=True,
        text=True,
        encoding="utf-8",
        cwd=tmp_path,
    )

    assert result.returncode == 0, result.stderr
    assert not marker.exists()
    capture = json.loads((data_root / "wrapper-capture.json").read_text())
    assert capture["pythonpath"] is None


@pytest.mark.skipif(not AVAILABLE, reason="需要 Windows PowerShell 与 Git")
def test_shadow_wrapper_refuses_paper_even_if_switch_is_omitted(
    tmp_path: Path,
) -> None:
    """成本 provenance 未绑定时包装器必须在进入 runner 前拒绝 paper。"""
    code_root, head = _code_checkout(tmp_path)
    data_root, runtime, execution = _roots(tmp_path)

    result = subprocess.run(
        _wrapper_command(
            code_root,
            head,
            data_root,
            runtime,
            execution,
            no_paper=False,
        ),
        check=False,
        capture_output=True,
        text=True,
        encoding="utf-8",
    )

    assert result.returncode == 3
    assert not (data_root / "wrapper-capture.json").exists()
    log = data_root / "logs/research/frozen-forward/shadow-scheduler.jsonl"
    record = json.loads(log.read_text(encoding="utf-8").splitlines()[-1])
    assert "paper is disabled until fill cost provenance is bound" in record["output"]
    assert record["python_base_runtime_attestation"] == "unbound-partial"
    assert record["paper_fill_cost_provenance"] == "unbound"


@pytest.mark.skipif(not AVAILABLE, reason="需要 Windows PowerShell 与 Git")
def test_shadow_action_rejects_execution_venv_pth_drift(tmp_path: Path) -> None:
    """注册后的 .pth/site-packages 增量必须在 Python 启动前失败。"""
    code_root, head = _code_checkout(tmp_path)
    data_root, runtime, execution = _roots(tmp_path)
    described = _run_describe(code_root, head, data_root, runtime, execution)
    assert described.returncode == 0, described.stderr
    definition = json.loads(described.stdout)
    (execution / ".venv/Lib/site-packages/injected.pth").write_text(
        "import sys; raise SystemExit('pth injection')\n",
        encoding="utf-8",
    )

    result = subprocess.run(
        [definition["execute"], *definition["arguments"].split()],
        check=False,
        capture_output=True,
        text=True,
        encoding="utf-8",
    )

    assert result.returncode == 3
    assert not (data_root / "wrapper-capture.json").exists()
    log = data_root / "logs/research/frozen-forward/shadow-scheduler.jsonl"
    record = json.loads(log.read_text(encoding="utf-8").splitlines()[-1])
    assert "execution venv tree SHA256" in record["output"]


@pytest.mark.skipif(not AVAILABLE, reason="需要 Windows PowerShell 与 Git")
def test_shadow_wrapper_rejects_prepositioned_log_junction(
    tmp_path: Path,
) -> None:
    """预置日志 junction 不得把调度证据写出 DataRoot。"""
    code_root, head = _code_checkout(tmp_path)
    data_root, runtime, execution = _roots(tmp_path)
    outside = tmp_path / "outside-log-target"
    outside.mkdir()
    logs = data_root / "logs"
    assert POWERSHELL is not None
    junction = subprocess.run(
        [
            POWERSHELL,
            "-NoProfile",
            "-Command",
            (
                "New-Item -ItemType Junction -Path "
                + _ps_literal(logs)
                + " -Target "
                + _ps_literal(outside)
                + " -ErrorAction Stop | Out-Null"
            ),
        ],
        check=False,
        capture_output=True,
        text=True,
        encoding="utf-8",
    )
    assert junction.returncode == 0, junction.stderr

    result = subprocess.run(
        _wrapper_command(code_root, head, data_root, runtime, execution),
        check=False,
        capture_output=True,
        text=True,
        encoding="utf-8",
    )

    assert result.returncode != 0
    assert "managed directory is not a physical directory" in result.stderr
    assert list(outside.iterdir()) == []


@pytest.mark.skipif(not AVAILABLE, reason="需要 Windows PowerShell 与 Git")
def test_shadow_wrapper_uses_code_for_runner_cwd_and_pythonpath(
    tmp_path: Path,
) -> None:
    """包装器只从 CodeRoot 运行代码，数据与调度日志只写 DataRoot。"""
    code_root, head = _code_checkout(tmp_path)
    data_root, runtime, execution = _roots(tmp_path)
    wrapper = code_root / "scripts" / "run_frozen_shadow_task.ps1"
    wrapper_sha256 = hashlib.sha256(wrapper.read_bytes()).hexdigest()

    result = subprocess.run(
        _wrapper_command(code_root, head, data_root, runtime, execution),
        check=False,
        capture_output=True,
        text=True,
        encoding="utf-8",
        cwd=tmp_path,
    )

    assert result.returncode == 0, result.stderr
    capture = json.loads((data_root / "wrapper-capture.json").read_text())
    assert Path(capture["cwd"]).resolve() == code_root.resolve()
    assert capture["pythonpath"] is None
    arguments = capture["argv"]
    assert arguments[arguments.index("--repository") + 1] == str(
        data_root.resolve()
    )
    assert arguments[arguments.index("--runtime-root") + 1] == str(
        runtime.resolve()
    )
    assert arguments[arguments.index("--execution-repository") + 1] == str(
        execution.resolve()
    )
    assert "--no-paper" in arguments
    log_path = data_root / "logs/research/frozen-forward/shadow-scheduler.jsonl"
    assert log_path.is_file()
    record = json.loads(log_path.read_text(encoding="utf-8-sig").splitlines()[-1])
    assert record["code_root"] == str(code_root.resolve())
    assert record["resolved_data_root"] == str(data_root.resolve())
    assert record["actual_code_head"] == head
    assert record["actual_wrapper_sha256"] == wrapper_sha256
    assert record["exit_code"] == 0
    assert not (code_root / "logs").exists()


@pytest.mark.skipif(not AVAILABLE, reason="需要 Windows PowerShell 与 Git")
@pytest.mark.parametrize(
    ("mutation", "message"),
    [
        ("attached", "detached HEAD"),
        ("dirty", "completely clean"),
        ("untracked", "completely clean"),
        ("ignored", "ignored injection"),
        ("head", "ExpectedCodeHead"),
        ("same-root", "distinct, non-overlapping roots"),
        ("nested-root", "distinct, non-overlapping roots"),
        ("junction-root", "distinct, non-overlapping roots"),
    ],
)
def test_shadow_registration_rejects_unfrozen_code_or_mixed_roots(
    tmp_path: Path,
    mutation: str,
    message: str,
) -> None:
    """附着/脏/错误 HEAD 代码树及混用数据根都必须在注册前失败。"""
    code_root, head = _code_checkout(tmp_path, detach=mutation != "attached")
    data_root, runtime, execution = _roots(tmp_path)
    expected_head = head
    if mutation == "dirty":
        (code_root / "scripts" / "run_frozen_shadow.py").write_text(
            "raise SystemExit(9)\n",
            encoding="utf-8",
        )
    elif mutation == "untracked":
        (code_root / "src" / "sitecustomize.py").write_text(
            "raise SystemExit('untracked injection')\n",
            encoding="utf-8",
        )
    elif mutation == "ignored":
        (code_root / ".git" / "info" / "exclude").write_text(
            "src/sitecustomize.py\n",
            encoding="utf-8",
        )
        (code_root / "src" / "sitecustomize.py").write_text(
            "raise SystemExit('ignored injection')\n",
            encoding="utf-8",
        )
    elif mutation == "head":
        expected_head = "0" * 40
    elif mutation == "same-root":
        data_root = code_root
    elif mutation == "nested-root":
        data_root = code_root / "live-data"
        data_root.mkdir()
    elif mutation == "junction-root":
        data_root = tmp_path / "data-junction"
        assert POWERSHELL is not None
        junction = subprocess.run(
            [
                POWERSHELL,
                "-NoProfile",
                "-Command",
                (
                    "New-Item -ItemType Junction -Path "
                    + _ps_literal(data_root)
                    + " -Target "
                    + _ps_literal(code_root)
                    + " -ErrorAction Stop | Out-Null"
                ),
            ],
            check=False,
            capture_output=True,
            text=True,
            encoding="utf-8",
        )
        assert junction.returncode == 0, junction.stderr

    result = _run_describe(
        code_root,
        expected_head,
        data_root,
        runtime,
        execution,
    )

    assert result.returncode != 0
    assert message in result.stderr


@pytest.mark.skipif(not AVAILABLE, reason="需要 Windows PowerShell 与 Git")
def test_shadow_registration_accepts_explicit_minute_offset(
    tmp_path: Path,
) -> None:
    """延迟偏移可调整，但仍锚定同一整点 vintage。"""
    code_root, head = _code_checkout(tmp_path)
    data_root, runtime, execution = _roots(tmp_path)

    result = _run_describe(
        code_root,
        head,
        data_root,
        runtime,
        execution,
        minute_offset=35,
    )

    assert result.returncode == 0, result.stderr
    definition = json.loads(result.stdout)
    assert definition["minute_offset"] == 35
    first_run = datetime.fromisoformat(definition["first_run_local"])
    assert first_run.minute == 35


@pytest.mark.skipif(not AVAILABLE, reason="需要 Windows PowerShell 与 Git")
def test_shadow_registration_rejects_malformed_plan_before_paths(
    tmp_path: Path,
) -> None:
    """计划身份参数绑定必须先于代码、数据与任务系统访问。"""
    code_root, head = _code_checkout(tmp_path)
    assert POWERSHELL is not None
    command = _registration_command(
        code_root,
        head,
        tmp_path / "missing-data",
        tmp_path / "missing-runtime",
        tmp_path / "missing-execution",
    )
    command[command.index(PLAN_ID)] = PLAN_ID + " --help"

    result = subprocess.run(
        command,
        check=False,
        capture_output=True,
        text=True,
        encoding="utf-8",
    )

    assert result.returncode != 0
    assert "ParameterArgumentValidationError" in result.stderr


@pytest.mark.skipif(not AVAILABLE, reason="需要 Windows PowerShell 与 Git")
def test_shadow_registration_rejects_unaligned_start(tmp_path: Path) -> None:
    """开始时间必须与冻结预测的整点边界对齐。"""
    code_root, head = _code_checkout(tmp_path)
    data_root, runtime, execution = _roots(tmp_path)
    command = _registration_command(
        code_root,
        head,
        data_root,
        runtime,
        execution,
    )
    command[command.index("2026-08-24T00:00:00Z")] = "2026-08-24T00:30:00Z"
    assert POWERSHELL is not None

    result = subprocess.run(
        command,
        check=False,
        capture_output=True,
        text=True,
        encoding="utf-8",
    )

    assert result.returncode != 0
    assert "exact UTC hour" in result.stderr


@pytest.mark.skipif(not AVAILABLE, reason="需要 Windows PowerShell 与 Git")
def test_shadow_registration_reads_back_object_and_exported_xml(
    tmp_path: Path,
) -> None:
    """内存 mock 必须完整经历对象与 Export XML 双重读回且不清理。"""
    code_root, head = _code_checkout(tmp_path)
    data_root, runtime, execution = _roots(tmp_path)
    harness = tmp_path / "registration-success.ps1"
    calls_path = tmp_path / "success-calls.json"
    _mock_registration_harness(
        harness,
        code_root=code_root,
        head=head,
        data_root=data_root,
        runtime=runtime,
        execution=execution,
        calls_path=calls_path,
    )
    assert POWERSHELL is not None

    result = subprocess.run(
        [POWERSHELL, "-NoProfile", "-File", str(harness)],
        check=False,
        capture_output=True,
        text=True,
        encoding="utf-8",
    )

    assert result.returncode == 0, result.stderr
    calls = json.loads(calls_path.read_text(encoding="utf-8-sig"))
    assert calls == [
        "GetExisting",
        "NewAction",
        "NewTrigger",
        "NewPrincipal",
        "NewSettings",
        "Register",
        "GetReadback",
        "Export",
    ]


@pytest.mark.skipif(not AVAILABLE, reason="需要 Windows PowerShell 与 Git")
@pytest.mark.parametrize("mismatch", ["object", "xml"])
def test_shadow_registration_mismatch_disables_then_stops_and_fails(
    tmp_path: Path,
    mismatch: str,
) -> None:
    """任一读回层失配都必须按 Disable、Stop 顺序补偿，且最终仍失败。"""
    code_root, head = _code_checkout(tmp_path)
    data_root, runtime, execution = _roots(tmp_path)
    harness = tmp_path / f"registration-{mismatch}-mismatch.ps1"
    calls_path = tmp_path / f"{mismatch}-mismatch-calls.json"
    _mock_registration_harness(
        harness,
        code_root=code_root,
        head=head,
        data_root=data_root,
        runtime=runtime,
        execution=execution,
        calls_path=calls_path,
        object_mismatch=mismatch == "object",
        xml_mismatch=mismatch == "xml",
    )
    assert POWERSHELL is not None

    result = subprocess.run(
        [POWERSHELL, "-NoProfile", "-File", str(harness)],
        check=False,
        capture_output=True,
        text=True,
        encoding="utf-8",
    )

    assert result.returncode == 17
    assert "post-registration task contract mismatch" in result.stderr
    calls = json.loads(calls_path.read_text(encoding="utf-8-sig"))
    assert calls[-2:] == ["Disable", "Stop"]
    assert calls.index("Export") < calls.index("Disable")


@pytest.mark.skipif(not AVAILABLE, reason="需要 Windows PowerShell 与 Git")
def test_shadow_registration_cleanup_failure_preserves_original_mismatch(
    tmp_path: Path,
) -> None:
    """Disable 失败不得阻断 Stop，也不得覆盖原始读回失败。"""
    code_root, head = _code_checkout(tmp_path)
    data_root, runtime, execution = _roots(tmp_path)
    harness = tmp_path / "registration-cleanup-failure.ps1"
    calls_path = tmp_path / "cleanup-failure-calls.json"
    _mock_registration_harness(
        harness,
        code_root=code_root,
        head=head,
        data_root=data_root,
        runtime=runtime,
        execution=execution,
        calls_path=calls_path,
        object_mismatch=True,
        disable_fails=True,
    )
    assert POWERSHELL is not None

    result = subprocess.run(
        [POWERSHELL, "-NoProfile", "-File", str(harness)],
        check=False,
        capture_output=True,
        text=True,
        encoding="utf-8",
    )

    assert result.returncode == 17
    assert "post-registration task contract mismatch" in result.stderr
    calls = json.loads(calls_path.read_text(encoding="utf-8-sig"))
    assert calls[-2:] == ["Disable", "Stop"]


def test_shadow_registration_source_orders_describe_and_cleanup_guards() -> None:
    """静态守卫补充验证 Describe 边界、默认禁用与失败补偿顺序。"""
    source = Path("scripts/register_frozen_shadow_task.ps1").read_text(
        encoding="utf-8",
    )
    assert "$CodeRoot = Resolve-PhysicalDirectoryPath" in source
    assert "Join-Path $PSScriptRoot \"..\"" in source
    assert "-Hidden -Disable" in source
    assert "unattended_coverage_capable = $false" in source
    assert source.index("if ($DescribeOnly)") < source.index(
        "$Existing = Get-ScheduledTask",
    )
    assert source.index("Register-ScheduledTask") < source.index(
        "$Registered = Get-ScheduledTask",
    )
    assert source.index("$Registered = Get-ScheduledTask") < source.index(
        "Export-ScheduledTask",
    )
    assert source.index("Disable-ScheduledTask") < source.index(
        "Stop-ScheduledTask",
    )

param(
    [Parameter(Mandatory = $true)]
    [string]$PlanId,
    [Parameter(Mandatory = $true)]
    [string]$Repository,
    [Parameter(Mandatory = $true)]
    [string]$RuntimeRoot,
    [Parameter(Mandatory = $true)]
    [string]$ExecutionRepository,
    [switch]$NoPaper,
    [string]$MarketId = "",
    [string]$Symbol = "",
    [string]$TargetConfig = "",
    # 必须短于计划任务自身的 55 分钟时限，见下方超时说明。
    [ValidateRange(1, 3240)]
    [int]$RoundTimeoutSeconds = 3000,
    [string]$Python = ""
)

$ErrorActionPreference = "Stop"
$Root = (Resolve-Path -LiteralPath $Repository).Path
if (-not $Python) {
    $Python = Join-Path $Root ".venv\Scripts\python.exe"
}
$Runner = Join-Path $Root "scripts\run_frozen_live.py"
$LogDirectory = Join-Path $Root "logs\research\frozen-forward"
$LogPath = Join-Path $LogDirectory "live-scheduler.jsonl"
New-Item -ItemType Directory -Path $LogDirectory -Force | Out-Null
$StartedAt = [datetime]::UtcNow.ToString("o")
$Runtime = $null
$Execution = $null
$ExitCode = 3
$TimedOut = $false
$Output = @()

function Format-NativeArgument([string]$Value) {
    # 按 Windows 命令行规则加引号：引号前与结尾的反斜杠成对。
    if ($Value -ne "" -and $Value -notmatch '[\s"]') {
        return $Value
    }
    $Escaped = $Value -replace '(\\*)"', '$1$1\"'
    $Escaped = $Escaped -replace '(\\+)$', '$1$1'
    return '"' + $Escaped + '"'
}

# -NoPaper 透传为 --no-paper。
$ExtraArguments = @()
if ($NoPaper) {
    $ExtraArguments += "--no-paper"
}
# 第二市场参数按需透传，缺省沿用脚本内 BTC 缺省值。
if ($MarketId) {
    $ExtraArguments += @("--market-id", $MarketId)
}
if ($Symbol) {
    $ExtraArguments += @("--symbol", $Symbol)
}
if ($TargetConfig) {
    $ExtraArguments += @("--target-config", $TargetConfig)
}
# 轮次开始先留痕：异常中断的轮次没有完成记录，观察进程据此识别。
$StartRecord = [ordered]@{
    phase = "started"
    started_at = $StartedAt
    plan_id = $PlanId
    market_id = $MarketId
    symbol = $Symbol
}
Add-Content -LiteralPath $LogPath -Encoding UTF8 `
    -Value ($StartRecord | ConvertTo-Json -Compress)
try {
    # 运行根不可达也必须留下调度记录。
    $Runtime = (Resolve-Path -LiteralPath $RuntimeRoot -ErrorAction Stop).Path
    if (-not $Runtime) {
        throw "运行根不可达: $RuntimeRoot"
    }
    $Execution = (Resolve-Path -LiteralPath $ExecutionRepository -ErrorAction Stop).Path
    if (-not $Execution) {
        throw "执行仓不可达: $ExecutionRepository"
    }
    $NativeArguments = @(
        $Runner, "--repository", $Root, "--runtime-root", $Runtime,
        "--execution-repository", $Execution, "--plan-id", $PlanId
    ) + $ExtraArguments
    # 自带超时并整树终止：计划任务到时限只杀本脚本，Python 子进程会成为
    # 孤儿继续占用写锁与内存并与下一轮重叠（2026-09-21 实测）。
    $Info = New-Object System.Diagnostics.ProcessStartInfo
    $Info.FileName = $Python
    $Info.Arguments = (
        $NativeArguments | ForEach-Object { Format-NativeArgument ([string]$_) }
    ) -join " "
    $Info.WorkingDirectory = (Get-Location).Path
    $Info.UseShellExecute = $false
    $Info.CreateNoWindow = $true
    $Info.RedirectStandardOutput = $true
    $Info.RedirectStandardError = $true
    $Info.StandardOutputEncoding = [System.Text.Encoding]::UTF8
    $Info.StandardErrorEncoding = [System.Text.Encoding]::UTF8
    $Info.EnvironmentVariables["PYTHONUTF8"] = "1"
    $Info.EnvironmentVariables["PYTHONIOENCODING"] = "utf-8"
    $Process = [System.Diagnostics.Process]::Start($Info)
    # 两路异步读取，避免管道写满互锁。
    $StdOut = $Process.StandardOutput.ReadToEndAsync()
    $StdErr = $Process.StandardError.ReadToEndAsync()
    if ($Process.WaitForExit($RoundTimeoutSeconds * 1000)) {
        $Process.WaitForExit()
        $ExitCode = $Process.ExitCode
    } else {
        $TimedOut = $true
        & taskkill.exe /PID $Process.Id /T /F 2>&1 | Out-Null
        $Process.WaitForExit(15000) | Out-Null
        $ExitCode = 4
    }
    $Captured = @()
    foreach ($Reader in @($StdOut, $StdErr)) {
        if ($Reader.Wait(15000) -and $Reader.Result) {
            $Captured += $Reader.Result.TrimEnd()
        }
    }
    if ($TimedOut) {
        $Captured += "轮次超过 $RoundTimeoutSeconds 秒，包装脚本已整树终止"
    }
    $Output = $Captured
} catch {
    $Output = @($_.Exception.Message)
    $ExitCode = 3
}
$Record = [ordered]@{
    started_at = $StartedAt
    completed_at = [datetime]::UtcNow.ToString("o")
    plan_id = $PlanId
    runtime_root = $RuntimeRoot
    resolved_runtime_root = $Runtime
    execution_repository = $ExecutionRepository
    no_paper = [bool]$NoPaper
    market_id = $MarketId
    symbol = $Symbol
    target_config = $TargetConfig
    exit_code = $ExitCode
    timed_out = $TimedOut
    output = ($Output -join "`n")
}
Add-Content -LiteralPath $LogPath -Encoding UTF8 `
    -Value ($Record | ConvertTo-Json -Compress)
exit $ExitCode

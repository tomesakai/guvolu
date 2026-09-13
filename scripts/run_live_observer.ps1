param(
    [int]$IntervalSeconds = 60,
    [string]$Repository,
    [string]$SchedulerLog = ""
)

$ErrorActionPreference = 'Stop'
$RepoRoot = if ($Repository) {
    (Resolve-Path $Repository).Path
} else {
    (Resolve-Path (Join-Path $PSScriptRoot '..')).Path
}
$PythonPath = Join-Path $RepoRoot '.venv\Scripts\python.exe'
$LogDirectory = Join-Path $RepoRoot 'logs'
$LogPath = Join-Path $LogDirectory 'live-observer.log'

New-Item -ItemType Directory -Force -Path $LogDirectory | Out-Null
try {
    $Host.UI.RawUI.WindowTitle = 'guvolu live-observer'
} catch {
    # 无交互宿主缺 RawUI，忽略。
}
$WindowPlacementPath = Join-Path $PSScriptRoot 'window_placement.ps1'
if (-not $env:GUVOLU_WINDOW_PLACED -and (Test-Path -LiteralPath $WindowPlacementPath)) {
    try {
        . $WindowPlacementPath
        Move-OwnWindowToSecondary
    } catch {
        Write-Warning "[window-placement] skipped: $($_.Exception.Message)"
    }
}
Set-Location -LiteralPath $RepoRoot
function Write-VisibleLog {
    process {
        $Line = [string]$_
        Write-Host $Line
        $Line | Out-File -LiteralPath $LogPath -Append -Encoding utf8
    }
}
"$(Get-Date -Format o) guvolu live-observer started; interval=${IntervalSeconds}s; scheduler-log=${SchedulerLog}." |
    Write-VisibleLog
# 调度日志在主仓 logs 下，由注册脚本以绝对路径传入
$ObserverArguments = @('-m', 'guvolu.execution.live_observer', '--interval-seconds', $IntervalSeconds)
if ($SchedulerLog) {
    $ObserverArguments += @('--scheduler-log', $SchedulerLog)
}
& $PythonPath @ObserverArguments 2>&1 |
    Write-VisibleLog
exit $LASTEXITCODE

param(
    [int]$IntervalSeconds = 300,
    [string]$Repository = ''
)

# 独立 L2 质量刷新循环包装，从物化热循环分离质量遥测。
$ErrorActionPreference = 'Stop'
$RepoRoot = if ($Repository) {
    (Resolve-Path -LiteralPath $Repository).Path
} else {
    (Resolve-Path (Join-Path $PSScriptRoot '..')).Path
}
$PythonPath = Join-Path $RepoRoot '.venv\Scripts\python.exe'
$DataRoot = Join-Path $RepoRoot 'data'
$LogDirectory = Join-Path $RepoRoot 'logs'
$LogPath = Join-Path $LogDirectory 'quality-watcher.log'

New-Item -ItemType Directory -Force -Path $LogDirectory | Out-Null
try {
    $Host.UI.RawUI.WindowTitle = 'guvolu quality-watcher'
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
"$(Get-Date -Format o) guvolu quality-watcher started; interval=${IntervalSeconds}s." |
    Write-VisibleLog
& $PythonPath -m guvolu.data.quality_watcher --data-root $DataRoot `
    watch --interval-seconds $IntervalSeconds 2>&1 |
    Write-VisibleLog

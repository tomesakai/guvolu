param(
    [int]$IntervalSeconds = 60,
    [string]$Repository
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
Set-Location -LiteralPath $RepoRoot
function Write-VisibleLog {
    process {
        $Line = [string]$_
        Write-Host $Line
        $Line | Out-File -LiteralPath $LogPath -Append -Encoding utf8
    }
}
"$(Get-Date -Format o) guvolu live-observer started; interval=${IntervalSeconds}s." |
    Write-VisibleLog
& $PythonPath -m guvolu.execution.live_observer `
    --interval-seconds $IntervalSeconds 2>&1 |
    Write-VisibleLog
exit $LASTEXITCODE

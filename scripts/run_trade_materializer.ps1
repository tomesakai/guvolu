param(
    [int]$IntervalSeconds = 300
)

$ErrorActionPreference = 'Stop'
$RepoRoot = (Resolve-Path (Join-Path $PSScriptRoot '..')).Path
$PythonPath = Join-Path $RepoRoot '.venv\Scripts\python.exe'
$LogDirectory = Join-Path $RepoRoot 'logs'
$LogPath = Join-Path $LogDirectory 'trade-realtime-materializer.log'

New-Item -ItemType Directory -Force -Path $LogDirectory | Out-Null
try {
    $Host.UI.RawUI.WindowTitle = 'guvolu trade-realtime-materializer'
} catch {
    # C-04
}
Set-Location -LiteralPath $RepoRoot
Start-Transcript -Path $LogPath -Append | Out-Null
try {
    Write-Host "guvolu trade-realtime-materializer started; interval=${IntervalSeconds}s."
    & $PythonPath -m guvolu.data.trade_realtime_materialize watch `
        --interval-seconds $IntervalSeconds
} finally {
    Write-Host 'guvolu trade-realtime-materializer exited.'
    Stop-Transcript | Out-Null
}

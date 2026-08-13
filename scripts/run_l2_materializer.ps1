param(
    [int]$IntervalSeconds = 300
)

$ErrorActionPreference = 'Stop'
$RepoRoot = (Resolve-Path (Join-Path $PSScriptRoot '..')).Path
$PythonPath = Join-Path $RepoRoot '.venv\Scripts\python.exe'
$LogDirectory = Join-Path $RepoRoot 'logs'
$LogPath = Join-Path $LogDirectory 'l2-materializer.log'

New-Item -ItemType Directory -Force -Path $LogDirectory | Out-Null
try {
    $Host.UI.RawUI.WindowTitle = 'guvolu l2-materializer'
} catch {
    # RawUI is optional in non-interactive hosts.
}
Set-Location -LiteralPath $RepoRoot
Start-Transcript -Path $LogPath -Append | Out-Null
try {
    Write-Host "guvolu l2-materializer started; interval=${IntervalSeconds}s."
    & $PythonPath -m guvolu.data.l2_materialize watch `
        --interval-seconds $IntervalSeconds
} finally {
    Write-Host 'guvolu l2-materializer exited.'
    Stop-Transcript | Out-Null
}

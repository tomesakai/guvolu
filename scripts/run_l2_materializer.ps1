param(
    [int]$IntervalSeconds = 300,
    [string]$Repository = '',
    [switch]$LatestRunOnly
)

$ErrorActionPreference = 'Stop'
$RepoRoot = if ($Repository) {
    (Resolve-Path -LiteralPath $Repository).Path
} else {
    (Resolve-Path (Join-Path $PSScriptRoot '..')).Path
}
$PythonPath = Join-Path $RepoRoot '.venv\Scripts\python.exe'
$DataRoot = Join-Path $RepoRoot 'data'
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
    $Arguments = @('watch', '--interval-seconds', [string]$IntervalSeconds)
    if ($LatestRunOnly) {
        $Arguments += '--latest-run-only'
    }
    & $PythonPath -m guvolu.data.l2_materialize --data-root $DataRoot @Arguments
} finally {
    Write-Host 'guvolu l2-materializer exited.'
    Stop-Transcript | Out-Null
}

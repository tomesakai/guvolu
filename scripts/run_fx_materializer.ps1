param(
    [int]$IntervalSeconds = 300,
    [string]$Repository = '',
    [switch]$VerifyAllHashes
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
$LogPath = Join-Path $LogDirectory 'fx-rate-materializer.log'

New-Item -ItemType Directory -Force -Path $LogDirectory | Out-Null
try {
    $Host.UI.RawUI.WindowTitle = 'guvolu fx-rate-materializer'
} catch {
    # C-04
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
Start-Transcript -Path $LogPath -Append | Out-Null
try {
    Write-Host "guvolu fx-rate-materializer started; interval=${IntervalSeconds}s."
    $Arguments = @('watch', '--interval-seconds', [string]$IntervalSeconds)
    if ($VerifyAllHashes) {
        $Arguments += '--verify-all-hashes'
    }
    & $PythonPath -m guvolu.data.fx_materialize `
        --data-root $DataRoot @Arguments
} finally {
    Write-Host 'guvolu fx-rate-materializer exited.'
    Stop-Transcript | Out-Null
}

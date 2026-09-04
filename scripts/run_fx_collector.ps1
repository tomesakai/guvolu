param(
    [string[]]$Symbols = @('USD_JPY'),
    [ValidateRange(10, 86400)]
    [int]$IntervalSeconds = 60,
    [string]$Name = 'fx-gmo-usd-jpy',
    [string]$Repository = ''
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
$LogPath = Join-Path $LogDirectory "$Name.log"

New-Item -ItemType Directory -Force -Path $LogDirectory | Out-Null
try {
    $Host.UI.RawUI.WindowTitle = "guvolu $Name"
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
$SymbolArguments = @()
foreach ($Symbol in $Symbols) {
    $SymbolArguments += @('--symbol', $Symbol)
}
Set-Location -LiteralPath $RepoRoot
Start-Transcript -Path $LogPath -Append | Out-Null
try {
    Write-Host "guvolu $Name started; fx_rate segments print rows/bytes/SHA-256."
    & $PythonPath -m guvolu.data.fx_capture --data-root $DataRoot record `
        @SymbolArguments --minutes 0 --interval-seconds $IntervalSeconds `
        --segment-seconds 300 --segment-max-mib 32
} finally {
    Write-Host "guvolu $Name exited."
    Stop-Transcript | Out-Null
}

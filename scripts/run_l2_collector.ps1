param(
    [Parameter(Mandatory = $true)]
    [ValidateSet('gmo', 'bitbank', 'bitflyer')]
    [string]$Venue,

    [Parameter(Mandatory = $true)]
    [string]$Symbol,

    [Parameter(Mandatory = $true)]
    [string]$Name,

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
    # RawUI is optional in non-interactive hosts.
}
Set-Location -LiteralPath $RepoRoot
Start-Transcript -Path $LogPath -Append | Out-Null
try {
    Write-Host "guvolu $Name started; sealed segments print rows/bytes/SHA-256."
    & $PythonPath -m guvolu.data.l2_capture --data-root $DataRoot record `
        --venue $Venue --symbol $Symbol --minutes 0 `
        --segment-seconds 300 --segment-max-mib 128
} finally {
    Write-Host "guvolu $Name exited."
    Stop-Transcript | Out-Null
}

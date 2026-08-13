param(
    [Parameter(Mandatory = $true)]
    [string]$MarketId,
    [Parameter(Mandatory = $true)]
    [string]$Hour,
    [ValidateSet('1s', '5s', '1min')]
    [string]$Bucket = '5s'
)

$ErrorActionPreference = 'Stop'
$RepoRoot = (Resolve-Path (Join-Path $PSScriptRoot '..')).Path
$PythonPath = Join-Path $RepoRoot '.venv\Scripts\python.exe'
$LogDirectory = Join-Path $RepoRoot 'logs'
$SafeMarket = $MarketId -replace '[^a-zA-Z0-9_-]', '_'
$SafeHour = $Hour -replace '[^a-zA-Z0-9_-]', '_'
$LogPath = Join-Path $LogDirectory "orderflow-tile-$SafeMarket-$SafeHour-$Bucket.log"

New-Item -ItemType Directory -Force -Path $LogDirectory | Out-Null
try { $Host.UI.RawUI.WindowTitle = "guvolu OFL tile $MarketId $Hour" } catch {}
Set-Location -LiteralPath $RepoRoot

function Write-VisibleLog {
    process {
        $Line = [string]$_
        Write-Host $Line
        $Line | Out-File -LiteralPath $LogPath -Append -Encoding utf8
    }
}

"$(Get-Date -Format o) OFL tile started: $MarketId $Hour $Bucket" | Write-VisibleLog
try {
    & $PythonPath -m guvolu.data.orderflow_tile_materialize hour `
        --market-id $MarketId --hour $Hour --bucket $Bucket 2>&1 |
        Write-VisibleLog
    if ($LASTEXITCODE -ne 0) { throw "OFL tile exited with code $LASTEXITCODE" }
} catch {
    "$(Get-Date -Format o) ERROR: $($_.Exception.Message)" | Write-VisibleLog
    throw
} finally {
    "$(Get-Date -Format o) OFL tile finished: $MarketId $Hour $Bucket" |
        Write-VisibleLog
}

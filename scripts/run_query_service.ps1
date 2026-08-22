param()

$ErrorActionPreference = 'Stop'
$RepoRoot = (Resolve-Path (Join-Path $PSScriptRoot '..')).Path
$PythonPath = Join-Path $RepoRoot '.venv\Scripts\python.exe'
$LogDirectory = Join-Path $RepoRoot 'logs'
$LogPath = Join-Path $LogDirectory 'query-service.log'

New-Item -ItemType Directory -Force -Path $LogDirectory | Out-Null
try { $Host.UI.RawUI.WindowTitle = 'guvolu query-service' } catch {}
Set-Location -LiteralPath $RepoRoot

function Write-VisibleLog {
    process {
        $Line = [string]$_
        Write-Host $Line
        $Line | Out-File -LiteralPath $LogPath -Append -Encoding utf8
    }
}

"$(Get-Date -Format o) guvolu query-service started." | Write-VisibleLog
try {
    & $PythonPath -m guvolu.ui.query_service 2>&1 | Write-VisibleLog
    $NativeExitCode = $LASTEXITCODE
    if ($NativeExitCode -ne 0) {
        throw "query-service exited with code $NativeExitCode."
    }
} finally {
    "$(Get-Date -Format o) guvolu query-service exited." | Write-VisibleLog
}

param(
    [int]$IntervalSeconds = 300,
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
$LogPath = Join-Path $LogDirectory 'book-state-materializer-progress.log'

New-Item -ItemType Directory -Force -Path $LogDirectory | Out-Null
try {
    $Host.UI.RawUI.WindowTitle = 'guvolu book-state-materializer'
} catch {
    # RawUI is optional in non-interactive hosts.
}
Set-Location -LiteralPath $RepoRoot
function Write-VisibleLog {
    process {
        $Line = [string]$_
        Write-Host $Line
        $Line | Out-File -LiteralPath $LogPath -Append -Encoding utf8
    }
}
try {
    "$(Get-Date -Format o) guvolu book-state-materializer started; interval=${IntervalSeconds}s." |
        Write-VisibleLog
    & $PythonPath -m guvolu.data.book_state_materialize `
        --data-root $DataRoot watch `
        --poll-seconds $IntervalSeconds 2>&1 |
        Write-VisibleLog
} finally {
    "$(Get-Date -Format o) guvolu book-state-materializer exited." |
        Write-VisibleLog
}

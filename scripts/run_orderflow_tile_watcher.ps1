param([int]$IntervalSeconds = 300)

$ErrorActionPreference = 'Stop'
$RepoRoot = (Resolve-Path (Join-Path $PSScriptRoot '..')).Path
$PythonPath = Join-Path $RepoRoot '.venv\Scripts\python.exe'
$LogDirectory = Join-Path $RepoRoot 'logs'
$LogPath = Join-Path $LogDirectory 'orderflow-tile-watcher.log'
New-Item -ItemType Directory -Force -Path $LogDirectory | Out-Null
try { $Host.UI.RawUI.WindowTitle = 'guvolu OFL tile watcher' } catch {}
Set-Location -LiteralPath $RepoRoot

function Write-VisibleLog {
    process {
        $Line = [string]$_
        Write-Host $Line
        $Line | Out-File -LiteralPath $LogPath -Append -Encoding utf8
    }
}

"$(Get-Date -Format o) OFL tile watcher started; interval=${IntervalSeconds}s." |
    Write-VisibleLog
[System.Management.Automation.ActionPreference]$PreviousErrorActionPreference =
    [System.Management.Automation.ActionPreference]$ErrorActionPreference
try {
    try {
        # 保留完整原生错误输出。
        $ErrorActionPreference =
            [System.Management.Automation.ActionPreference]::Continue
        & $PythonPath -m guvolu.data.orderflow_tile_materialize watch `
            --bucket 5s --poll-seconds $IntervalSeconds 2>&1 | Write-VisibleLog
        $NativeExitCode = $LASTEXITCODE
    } finally {
        $ErrorActionPreference = $PreviousErrorActionPreference
    }
    if ($NativeExitCode -ne 0) {
        throw "OFL tile watcher native process exited with code $NativeExitCode."
    }
} catch {
    "$(Get-Date -Format o) ERROR: $($_.Exception.Message)" | Write-VisibleLog
    throw
} finally {
    "$(Get-Date -Format o) OFL tile watcher exited." | Write-VisibleLog
}

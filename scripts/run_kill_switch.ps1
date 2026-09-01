param(
    [string]$EnvFile
)

$ErrorActionPreference = 'Stop'
$RepoRoot = (Resolve-Path (Join-Path $PSScriptRoot '..')).Path
$PythonPath = Join-Path $RepoRoot '.venv\Scripts\python.exe'
$LogDirectory = Join-Path $RepoRoot 'logs'
$LogPath = Join-Path $LogDirectory 'kill-switch.log'

New-Item -ItemType Directory -Force -Path $LogDirectory | Out-Null
Set-Location -LiteralPath $RepoRoot

function Write-VisibleLog {
    process {
        $Line = [string]$_
        Write-Host $Line
        $Line | Out-File -LiteralPath $LogPath -Append -Encoding utf8
    }
}

$KillSwitchArgs = @('-m', 'guvolu.ops.kill_switch')
if ($EnvFile) {
    $KillSwitchArgs += @('--env-file', $EnvFile)
}
"$(Get-Date -Format o) guvolu kill-switch invoked." | Write-VisibleLog
& $PythonPath @KillSwitchArgs 2>&1 | Write-VisibleLog
$NativeExitCode = $LASTEXITCODE
"$(Get-Date -Format o) guvolu kill-switch exited with code $NativeExitCode." |
    Write-VisibleLog
exit $NativeExitCode

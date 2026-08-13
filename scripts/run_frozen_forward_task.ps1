param(
    [Parameter(Mandatory = $true)]
    [string]$PlanId,
    [Parameter(Mandatory = $true)]
    [string]$Repository,
    [Parameter(Mandatory = $true)]
    [string]$Registry
)

$ErrorActionPreference = "Stop"
$Root = (Resolve-Path -LiteralPath $Repository).Path
$Python = Join-Path $Root ".venv\Scripts\python.exe"
$Runner = Join-Path $Root "scripts\manage_frozen_forward.py"
$LogDirectory = Join-Path $Root "logs\research\frozen-forward"
$LogPath = Join-Path $LogDirectory "task.jsonl"
New-Item -ItemType Directory -Path $LogDirectory -Force | Out-Null
$StartedAt = [datetime]::UtcNow.ToString("o")
$Output = & $Python $Runner --root $Root predict $PlanId --registry $Registry 2>&1
$ExitCode = $LASTEXITCODE
$Record = [ordered]@{
    started_at = $StartedAt
    completed_at = [datetime]::UtcNow.ToString("o")
    plan_id = $PlanId
    exit_code = $ExitCode
    output = ($Output -join "`n")
}
Add-Content -LiteralPath $LogPath -Value ($Record | ConvertTo-Json -Compress)
if ($ExitCode -ne 0) {
    exit $ExitCode
}

param(
    [Parameter(Mandatory = $true)]
    [string]$PlanId,
    [Parameter(Mandatory = $true)]
    [string]$Repository,
    [Parameter(Mandatory = $true)]
    [string]$RuntimeRoot,
    [Parameter(Mandatory = $true)]
    [string]$ExecutionRepository
)

$ErrorActionPreference = "Stop"
$Root = (Resolve-Path -LiteralPath $Repository).Path
$Runtime = (Resolve-Path -LiteralPath $RuntimeRoot).Path
$Execution = (Resolve-Path -LiteralPath $ExecutionRepository).Path
$Python = Join-Path $Root ".venv\Scripts\python.exe"
$Runner = Join-Path $Root "scripts\run_frozen_shadow.py"
$LogDirectory = Join-Path $Root "logs\research\frozen-forward"
$LogPath = Join-Path $LogDirectory "shadow-scheduler.jsonl"
New-Item -ItemType Directory -Path $LogDirectory -Force | Out-Null
$StartedAt = [datetime]::UtcNow.ToString("o")
[System.Management.Automation.ActionPreference]$PreviousErrorActionPreference =
    [System.Management.Automation.ActionPreference]$ErrorActionPreference
try {
    # 完整保留原生进程输出，任务日志另存调度层结果。
    $ErrorActionPreference =
        [System.Management.Automation.ActionPreference]::Continue
    $Output = & $Python $Runner --repository $Root `
        --runtime-root $Runtime `
        --execution-repository $Execution `
        --plan-id $PlanId 2>&1
    $ExitCode = $LASTEXITCODE
} finally {
    $ErrorActionPreference = $PreviousErrorActionPreference
}
$Record = [ordered]@{
    started_at = $StartedAt
    completed_at = [datetime]::UtcNow.ToString("o")
    plan_id = $PlanId
    runtime_root = $Runtime
    execution_repository = $Execution
    exit_code = $ExitCode
    output = ($Output -join "`n")
}
Add-Content -LiteralPath $LogPath -Value ($Record | ConvertTo-Json -Compress)
exit $ExitCode

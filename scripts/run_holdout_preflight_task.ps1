param(
    [Parameter(Mandatory = $true)]
    [string]$Repository,
    [Parameter(Mandatory = $true)]
    [string]$RuntimeRoot,
    [string]$VintageId = ""
)

$ErrorActionPreference = "Stop"
$Root = (Resolve-Path -LiteralPath $Repository).Path
$Python = Join-Path $Root ".venv\Scripts\python.exe"
$Runner = Join-Path $Root "scripts\preflight_holdout.py"
$LogDirectory = Join-Path $Root "logs\research\frozen-forward\preflight"
$LogPath = Join-Path $Root "logs\research\frozen-forward\preflight-scheduler.jsonl"
New-Item -ItemType Directory -Path $LogDirectory -Force | Out-Null
$StartedAt = [datetime]::UtcNow
$JsonOutput = Join-Path $LogDirectory ("preflight-" + $StartedAt.ToString("yyyyMMddTHHmmssZ") + ".json")
$ExitCode = 3
$Output = @()
$Runtime = $null
[System.Management.Automation.ActionPreference]$PreviousErrorActionPreference =
    [System.Management.Automation.ActionPreference]$ErrorActionPreference
try {
    # 运行根不可达也必须留下调度记录。
    $Runtime = (Resolve-Path -LiteralPath $RuntimeRoot -ErrorAction Stop).Path
    $Arguments = @("--root", $Runtime, "--json-output", $JsonOutput)
    if ($VintageId -ne "") {
        $Arguments += @("--vintage-id", $VintageId)
    }
    # 完整保留原生进程输出。
    $ErrorActionPreference =
        [System.Management.Automation.ActionPreference]::Continue
    $Output = & $Python $Runner @Arguments 2>&1
    $ExitCode = $LASTEXITCODE
} catch {
    $Output = @($_.Exception.Message)
    $ExitCode = 3
} finally {
    $ErrorActionPreference = $PreviousErrorActionPreference
}
$Record = [ordered]@{
    started_at = $StartedAt.ToString("o")
    completed_at = [datetime]::UtcNow.ToString("o")
    runtime_root = $RuntimeRoot
    resolved_runtime_root = $Runtime
    json_output = $JsonOutput
    exit_code = $ExitCode
    output = ($Output -join "`n")
}
Add-Content -LiteralPath $LogPath -Value ($Record | ConvertTo-Json -Compress)
exit $ExitCode

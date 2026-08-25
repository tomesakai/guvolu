param(
    [string]$Repository = "",
    [string]$RuntimeRoot = "",
    [string]$VintageId = "",
    [ValidatePattern('^(?:[01][0-9]|2[0-3]):[0-5][0-9]$')]
    [string]$DailyAt = "09:35",
    [switch]$DescribeOnly
)

$ErrorActionPreference = "Stop"
if ($VintageId -and $VintageId -notmatch '^holdout-vintage-[0-9a-f]{64}$') {
    throw "VintageId must be a canonical holdout vintage identifier"
}
$RepoRoot = if ($Repository) {
    (Resolve-Path -LiteralPath $Repository).Path
} else {
    (Resolve-Path (Join-Path $PSScriptRoot "..")).Path
}
$Runtime = if ($RuntimeRoot) {
    (Resolve-Path -LiteralPath $RuntimeRoot).Path
} else {
    $RepoRoot
}
$TaskRunner = Join-Path $RepoRoot "scripts\run_holdout_preflight_task.ps1"
if (-not (Test-Path -LiteralPath $TaskRunner -PathType Leaf)) {
    throw "Holdout preflight task runner does not exist: $TaskRunner"
}
$Time = [datetime]::ParseExact(
    $DailyAt,
    "HH:mm",
    [System.Globalization.CultureInfo]::InvariantCulture
)
$FirstRunLocal = [datetime]::Today.Add($Time.TimeOfDay)
$TaskName = "guvolu-holdout-preflight"
$Arguments = (
    '-NoProfile -NonInteractive -ExecutionPolicy Bypass -WindowStyle Hidden ' +
    "-File `"$TaskRunner`" -Repository `"$RepoRoot`" " +
    "-RuntimeRoot `"$Runtime`""
)
if ($VintageId) {
    $Arguments += " -VintageId `"$VintageId`""
}
$Definition = [ordered]@{
    task_name = $TaskName
    execute = "powershell.exe"
    arguments = $Arguments
    working_directory = $RepoRoot
    daily_at_local = $FirstRunLocal.ToString("HH:mm")
    local_time_zone = [System.TimeZoneInfo]::Local.Id
    vintage_id = if ($VintageId) { $VintageId } else { $null }
    multiple_instances = "IgnoreNew"
    start_when_available = $true
    allow_start_on_batteries = $true
    dont_stop_if_going_on_batteries = $true
    wake_to_run = $true
    execution_time_limit_minutes = 30
    restart_count = 0
    restart_interval_minutes = 0
}
if ($DescribeOnly) {
    [pscustomobject]$Definition | ConvertTo-Json -Compress
    exit 0
}
$Action = New-ScheduledTaskAction -Execute $Definition.execute `
    -Argument $Definition.arguments -WorkingDirectory $Definition.working_directory
$Trigger = New-ScheduledTaskTrigger -Daily -At $FirstRunLocal -DaysInterval 1
$Principal = New-ScheduledTaskPrincipal `
    -UserId ([System.Security.Principal.WindowsIdentity]::GetCurrent().Name) `
    -LogonType Interactive -RunLevel Limited
$Settings = New-ScheduledTaskSettingsSet -MultipleInstances IgnoreNew `
    -StartWhenAvailable -AllowStartIfOnBatteries `
    -DontStopIfGoingOnBatteries -WakeToRun `
    -ExecutionTimeLimit (New-TimeSpan -Minutes 30) `
    -Hidden
Register-ScheduledTask -TaskName $TaskName -Action $Action -Trigger $Trigger `
    -Principal $Principal -Settings $Settings -Force | Out-Null
Get-ScheduledTask -TaskName $TaskName | Select-Object `
    TaskName, State, @{Name = "DailyAtLocal"; Expression = { $DailyAt }}, `
    @{Name = "LocalTimeZone"; Expression = { $Definition.local_time_zone }}

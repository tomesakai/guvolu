param(
    [Parameter(Mandatory = $true)]
    [ValidatePattern('^frozen-forward-plan-[0-9a-f]{64}$')]
    [string]$PlanId,
    [Parameter(Mandatory = $true)]
    [datetime]$StartUtc,
    [Parameter(Mandatory = $true)]
    [datetime]$EndUtc,
    [Parameter(Mandatory = $true)]
    [string]$RuntimeRoot,
    [Parameter(Mandatory = $true)]
    [string]$ExecutionRepository,
    [string]$Repository = "",
    [ValidateRange(1, 59)]
    [int]$MinuteOffset = 25,
    [switch]$NoPaper,
    [switch]$DescribeOnly
)

$ErrorActionPreference = "Stop"
$RepoRoot = if ($Repository) {
    (Resolve-Path -LiteralPath $Repository).Path
} else {
    (Resolve-Path (Join-Path $PSScriptRoot "..")).Path
}
$Runtime = (Resolve-Path -LiteralPath $RuntimeRoot).Path
$Execution = (Resolve-Path -LiteralPath $ExecutionRepository).Path
$TaskRunner = Join-Path $RepoRoot "scripts\run_frozen_shadow_task.ps1"
if (-not (Test-Path -LiteralPath $TaskRunner -PathType Leaf)) {
    throw "冻结 shadow 任务脚本不存在: $TaskRunner"
}
$Start = $StartUtc.ToUniversalTime()
$End = $EndUtc.ToUniversalTime()
if ($Start -ge $End) {
    throw "任务区间必须满足 StartUtc < EndUtc"
}
if (($Start.Ticks % [TimeSpan]::TicksPerHour) -ne 0) {
    throw "StartUtc must align to an exact UTC hour"
}
$FirstRunUtc = $Start.AddMinutes($MinuteOffset)
$Duration = $End - $FirstRunUtc
if ($Duration.TotalHours -lt 1) {
    throw "任务区间不足一小时"
}
$LocalZone = [System.TimeZoneInfo]::Local
$FirstRunLocal = [System.TimeZoneInfo]::ConvertTimeFromUtc($FirstRunUtc, $LocalZone)
$TaskSuffix = $PlanId.Substring([Math]::Max(0, $PlanId.Length - 12))
$TaskName = "guvolu-frozen-forward-$TaskSuffix"
$Arguments = (
    '-NoProfile -NonInteractive -ExecutionPolicy Bypass -WindowStyle Hidden ' +
    "-File `"$TaskRunner`" -PlanId `"$PlanId`" " +
    "-Repository `"$RepoRoot`" -RuntimeRoot `"$Runtime`" " +
    "-ExecutionRepository `"$Execution`""
)
if ($NoPaper) {
    $Arguments += " -NoPaper"
}
$Definition = [ordered]@{
    task_name = $TaskName
    execute = "powershell.exe"
    arguments = $Arguments
    working_directory = $RepoRoot
    first_run_local = $FirstRunLocal.ToString("o")
    end_utc = $End.ToString("o")
    no_paper = [bool]$NoPaper
    minute_offset = $MinuteOffset
    start_when_available = $true
    allow_start_on_batteries = $true
    wake_to_run = $true
    execution_time_limit_minutes = 45
    restart_count = 3
    restart_interval_minutes = 5
}
if ($DescribeOnly) {
    [pscustomobject]$Definition | ConvertTo-Json -Compress
    exit 0
}
$Action = New-ScheduledTaskAction -Execute $Definition.execute `
    -Argument $Definition.arguments -WorkingDirectory $Definition.working_directory
$Trigger = New-ScheduledTaskTrigger -Once -At $FirstRunLocal `
    -RepetitionInterval (New-TimeSpan -Hours 1) `
    -RepetitionDuration $Duration
$Principal = New-ScheduledTaskPrincipal `
    -UserId ([System.Security.Principal.WindowsIdentity]::GetCurrent().Name) `
    -LogonType Interactive -RunLevel Limited
$Settings = New-ScheduledTaskSettingsSet -MultipleInstances IgnoreNew `
    -StartWhenAvailable -AllowStartIfOnBatteries `
    -DontStopIfGoingOnBatteries -WakeToRun `
    -ExecutionTimeLimit (New-TimeSpan -Minutes 45) `
    -RestartCount $Definition.restart_count `
    -RestartInterval (New-TimeSpan -Minutes $Definition.restart_interval_minutes) `
    -Hidden
Register-ScheduledTask -TaskName $TaskName -Action $Action -Trigger $Trigger `
    -Principal $Principal -Settings $Settings -Force | Out-Null
Get-ScheduledTask -TaskName $TaskName | Select-Object `
    TaskName, State, @{Name = "FirstRunLocal"; Expression = { $FirstRunLocal }}, `
    @{Name = "EndUtc"; Expression = { $End.ToString("o") }}

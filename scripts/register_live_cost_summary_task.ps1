param(
    [string]$Repository = "",
    [string]$ExecutionRepository = "C:\Users\wu_zh\dev\guvolu-exec",
    [ValidateSet("Sunday", "Monday", "Tuesday", "Wednesday", "Thursday", "Friday", "Saturday")]
    [string]$DayOfWeek = "Sunday",
    [string]$At = "07:00",
    [switch]$DescribeOnly
)
# 登记每周一次的 live 成交成本汇总任务（只读，零写）。
# 由维护者运行；-DescribeOnly 只打印任务定义不登记。
$ErrorActionPreference = "Stop"
$RepoRoot = if ($Repository) {
    (Resolve-Path -LiteralPath $Repository).Path
} else {
    (Resolve-Path (Join-Path $PSScriptRoot "..")).Path
}
$Runner = Join-Path $RepoRoot "scripts\run_live_cost_summary.ps1"
if (-not (Test-Path -LiteralPath $Runner -PathType Leaf)) {
    throw "启动器不存在: $Runner"
}
$Execution = (Resolve-Path -LiteralPath $ExecutionRepository).Path
$TaskName = "guvolu-live-cost-summary"
$Definition = [ordered]@{
    task_name = $TaskName
    execute = "powershell.exe"
    arguments = (
        '-NoProfile -NonInteractive -ExecutionPolicy Bypass -WindowStyle Hidden ' +
        "-File `"$Runner`" -Repository `"$RepoRoot`" -ExecutionRepository `"$Execution`""
    )
    working_directory = $RepoRoot
    day_of_week = $DayOfWeek
    at = $At
    execution_time_limit_minutes = 30
}
if ($DescribeOnly) {
    [pscustomobject]$Definition | ConvertTo-Json -Compress
    exit 0
}
$CurrentUser = [System.Security.Principal.WindowsIdentity]::GetCurrent().Name
$Principal = New-ScheduledTaskPrincipal -UserId $CurrentUser `
    -LogonType Interactive -RunLevel Limited
$Settings = New-ScheduledTaskSettingsSet -MultipleInstances IgnoreNew `
    -StartWhenAvailable -AllowStartIfOnBatteries -DontStopIfGoingOnBatteries `
    -ExecutionTimeLimit (New-TimeSpan -Minutes 30) -Hidden
$Action = New-ScheduledTaskAction -Execute $Definition.execute `
    -Argument $Definition.arguments -WorkingDirectory $Definition.working_directory
$Trigger = New-ScheduledTaskTrigger -Weekly -DaysOfWeek $DayOfWeek -At $At
Register-ScheduledTask -TaskName $TaskName -Action $Action -Trigger $Trigger `
    -Principal $Principal -Settings $Settings `
    -Description "Weekly read-only summary of live fill costs (guvolu-exec)." -Force | Out-Null
Get-ScheduledTask -TaskName $TaskName | Select-Object TaskName, State, TaskPath

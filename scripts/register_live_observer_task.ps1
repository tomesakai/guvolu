param(
    [Parameter(Mandatory = $true)]
    [string]$ExecutionRepository,
    [ValidateRange(10, 600)]
    [int]$IntervalSeconds = 60,
    [ValidateRange(1, 60)]
    [int]$GuardMinutes = 5,
    [switch]$DescribeOnly
)
# 上膛协议（执行链设计第 14 节）：本脚本由维护者亲自执行注册，
# 代理只交付脚本文件，不代行注册与首次启动（T-04、A-01）。
# 观察进程必须从执行仓启动，账本、心跳与观察记录才与 live
# 执行器落在同一数据根；登录触发拉起，守护触发按 IgnoreNew
# 只在进程不在时重新拉起。
$ErrorActionPreference = "Stop"
$Execution = (Resolve-Path -LiteralPath $ExecutionRepository).Path
$Runner = Join-Path $Execution "scripts\run_live_observer.ps1"
if (-not (Test-Path -LiteralPath $Runner -PathType Leaf)) {
    throw "观察进程包装脚本不存在: $Runner"
}
$TaskNames = @("guvolu-live-observer-logon", "guvolu-live-observer-guard")
$Arguments = (
    '-NoProfile -NonInteractive -ExecutionPolicy Bypass -WindowStyle Hidden ' +
    "-File `"$Runner`" -Repository `"$Execution`" " +
    "-IntervalSeconds $IntervalSeconds"
)
$Definition = [ordered]@{
    task_names = $TaskNames
    execute = "powershell.exe"
    arguments = $Arguments
    working_directory = $Execution
    interval_seconds = $IntervalSeconds
    guard_minutes = $GuardMinutes
    multiple_instances = "IgnoreNew"
    execution_time_limit_minutes = 0
}
if ($DescribeOnly) {
    [pscustomobject]$Definition | ConvertTo-Json -Compress
    exit 0
}
$CurrentUser = [System.Security.Principal.WindowsIdentity]::GetCurrent().Name
$Action = New-ScheduledTaskAction -Execute $Definition.execute `
    -Argument $Definition.arguments -WorkingDirectory $Definition.working_directory
$Principal = New-ScheduledTaskPrincipal -UserId $CurrentUser `
    -LogonType Interactive -RunLevel Limited
# 时限为零即不限时，观察循环常驻
$Settings = New-ScheduledTaskSettingsSet -MultipleInstances IgnoreNew `
    -StartWhenAvailable -AllowStartIfOnBatteries -DontStopIfGoingOnBatteries `
    -ExecutionTimeLimit (New-TimeSpan -Seconds 0) -Hidden
$LogonTrigger = New-ScheduledTaskTrigger -AtLogOn -User $CurrentUser
Register-ScheduledTask -TaskName $TaskNames[0] -Action $Action `
    -Trigger $LogonTrigger -Principal $Principal -Settings $Settings `
    -Description "Start the guvolu live observer at logon." -Force | Out-Null
$GuardTrigger = New-ScheduledTaskTrigger -Once -At (Get-Date).AddMinutes(1) `
    -RepetitionInterval (New-TimeSpan -Minutes $GuardMinutes)
Register-ScheduledTask -TaskName $TaskNames[1] -Action $Action `
    -Trigger $GuardTrigger -Principal $Principal -Settings $Settings `
    -Description "Restart the guvolu live observer when it is not running." `
    -Force | Out-Null
Get-ScheduledTask -TaskName "guvolu-live-observer-*" |
    Select-Object TaskName, State, TaskPath

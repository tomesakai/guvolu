param(
    [string]$Repository = "",
    [string]$At = "10:53",
    [switch]$DescribeOnly
)
# 登记每日一次的逐笔实时段合并任务（TBD-40）。时刻避开每小时第 12 分与
# 第 30 分起跑的冻结前向链，也在 03:16 起约一小时的每日补漏之后；放在
# UTC 日结束（09:00 JST）加一小时宽限之后，当日即可合并前一 UTC 日。
$ErrorActionPreference = "Stop"
$RepoRoot = if ($Repository) {
    (Resolve-Path -LiteralPath $Repository).Path
} else {
    (Resolve-Path (Join-Path $PSScriptRoot "..")).Path
}
$Runner = Join-Path $RepoRoot "scripts\run_trade_compaction.ps1"
if (-not (Test-Path -LiteralPath $Runner -PathType Leaf)) {
    throw "启动器不存在: $Runner"
}
$TaskName = "guvolu-trade-compaction"
$Definition = [ordered]@{
    task_name = $TaskName
    execute = "powershell.exe"
    arguments = (
        '-NoProfile -NonInteractive -ExecutionPolicy Bypass -WindowStyle Hidden ' +
        "-File `"$Runner`" -Repository `"$RepoRoot`""
    )
    working_directory = $RepoRoot
    at = $At
    multiple_instances = "IgnoreNew"
    execution_time_limit_minutes = 60
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
    -ExecutionTimeLimit (New-TimeSpan -Minutes 60) -Hidden
$Action = New-ScheduledTaskAction -Execute $Definition.execute `
    -Argument $Definition.arguments -WorkingDirectory $Definition.working_directory
$Trigger = New-ScheduledTaskTrigger -Daily -At $At
Register-ScheduledTask -TaskName $TaskName -Action $Action -Trigger $Trigger `
    -Principal $Principal -Settings $Settings `
    -Description "Daily merge of realtime trade segment heads into day partitions." -Force | Out-Null
Get-ScheduledTask -TaskName $TaskName | Select-Object TaskName, State, TaskPath

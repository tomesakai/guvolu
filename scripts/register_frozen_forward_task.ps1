param(
    [Parameter(Mandatory = $true)]
    [string]$PlanId,
    [Parameter(Mandatory = $true)]
    [datetime]$StartUtc,
    [Parameter(Mandatory = $true)]
    [datetime]$EndUtc,
    [string]$Registry = "data/research/governance.sqlite3"
)

$ErrorActionPreference = "Stop"
$Repository = (Resolve-Path (Join-Path $PSScriptRoot "..")).Path
$Python = Join-Path $Repository ".venv\Scripts\python.exe"
$Runner = Join-Path $Repository "scripts\manage_frozen_forward.py"
$RegistryPath = if ([System.IO.Path]::IsPathRooted($Registry)) {
    $Registry
} else {
    Join-Path $Repository $Registry
}
if (-not (Test-Path -LiteralPath $Python -PathType Leaf)) {
    throw "Python 运行环境不存在: $Python"
}
if (-not (Test-Path -LiteralPath $Runner -PathType Leaf)) {
    throw "冻结前向脚本不存在: $Runner"
}
$Start = $StartUtc.ToUniversalTime()
$End = $EndUtc.ToUniversalTime()
if ($Start -ge $End) {
    throw "任务区间必须满足 StartUtc < EndUtc"
}
$FirstRunUtc = $Start.AddMinutes(10)
$Duration = $End - $FirstRunUtc
if ($Duration.TotalHours -lt 1) {
    throw "任务区间不足一小时"
}
$LocalZone = [System.TimeZoneInfo]::Local
$FirstRunLocal = [System.TimeZoneInfo]::ConvertTimeFromUtc($FirstRunUtc, $LocalZone)
$TaskSuffix = $PlanId.Substring([Math]::Max(0, $PlanId.Length - 12))
$TaskName = "guvolu-frozen-forward-$TaskSuffix"
$Arguments = '"{0}" --root "{1}" predict "{2}" --registry "{3}"' -f `
    $Runner, $Repository, $PlanId, $RegistryPath
$Action = New-ScheduledTaskAction -Execute $Python -Argument $Arguments `
    -WorkingDirectory $Repository
$Trigger = New-ScheduledTaskTrigger -Once -At $FirstRunLocal `
    -RepetitionInterval (New-TimeSpan -Hours 1) `
    -RepetitionDuration $Duration
$Principal = New-ScheduledTaskPrincipal -UserId ([System.Security.Principal.WindowsIdentity]::GetCurrent().Name) `
    -LogonType Interactive -RunLevel Limited
$Settings = New-ScheduledTaskSettingsSet -MultipleInstances IgnoreNew `
    -StartWhenAvailable -ExecutionTimeLimit (New-TimeSpan -Minutes 45) `
    -Hidden
if (Get-ScheduledTask -TaskName $TaskName -ErrorAction SilentlyContinue) {
    Unregister-ScheduledTask -TaskName $TaskName -Confirm:$false
}
Register-ScheduledTask -TaskName $TaskName -Action $Action -Trigger $Trigger `
    -Principal $Principal -Settings $Settings | Out-Null
Get-ScheduledTask -TaskName $TaskName | Select-Object `
    TaskName, State, @{Name = "FirstRunLocal"; Expression = { $FirstRunLocal }}, `
    @{Name = "EndUtc"; Expression = { $End.ToString("o") }}

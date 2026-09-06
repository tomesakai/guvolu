param(
    [string]$Repository = "",
    [ValidateRange(10, 3600)]
    [int]$CollectorIntervalSeconds = 60,
    [ValidateRange(60, 3600)]
    [int]$MaterializerIntervalSeconds = 300,
    [ValidateRange(1, 60)]
    [int]$GuardMinutes = 5,
    [switch]$DescribeOnly
)
# 汇率腿常驻（TBD-32，runtime-ops 第 5 节）：采集器与物化器各一条任务，
# 登录触发拉起，守护触发每 GuardMinutes 分钟按 IgnoreNew 只在进程不在
# 时重新拉起；不纳入 start_marketdata_pipeline 清单，独立于 L2 单写边界。
$ErrorActionPreference = "Stop"
$RepoRoot = if ($Repository) {
    (Resolve-Path -LiteralPath $Repository).Path
} else {
    (Resolve-Path (Join-Path $PSScriptRoot "..")).Path
}
$Tasks = @(
    @{
        Name = "guvolu-fx-collector"
        Runner = Join-Path $RepoRoot "scripts\run_fx_collector.ps1"
        Extra = "-IntervalSeconds $CollectorIntervalSeconds"
        Description = "Poll GMO forex ticker into raw v3 segments (fx_rate)."
    },
    @{
        Name = "guvolu-fx-materializer"
        Runner = Join-Path $RepoRoot "scripts\run_fx_materializer.ps1"
        Extra = "-IntervalSeconds $MaterializerIntervalSeconds"
        Description = "Materialize sealed fx_rate segments to Parquet."
    }
)
$Definitions = @()
foreach ($Task in $Tasks) {
    if (-not (Test-Path -LiteralPath $Task.Runner -PathType Leaf)) {
        throw "启动器不存在: $($Task.Runner)"
    }
    $Definitions += [ordered]@{
        task_name = $Task.Name
        execute = "powershell.exe"
        arguments = (
            '-NoProfile -NonInteractive -ExecutionPolicy Bypass -WindowStyle Hidden ' +
            "-File `"$($Task.Runner)`" -Repository `"$RepoRoot`" $($Task.Extra)"
        )
        working_directory = $RepoRoot
        guard_minutes = $GuardMinutes
        multiple_instances = "IgnoreNew"
        execution_time_limit_minutes = 0
    }
}
if ($DescribeOnly) {
    $Definitions | ForEach-Object { [pscustomobject]$_ | ConvertTo-Json -Compress }
    exit 0
}
$CurrentUser = [System.Security.Principal.WindowsIdentity]::GetCurrent().Name
$Principal = New-ScheduledTaskPrincipal -UserId $CurrentUser `
    -LogonType Interactive -RunLevel Limited
# 时限为零即不限时，采集与物化循环常驻
$Settings = New-ScheduledTaskSettingsSet -MultipleInstances IgnoreNew `
    -StartWhenAvailable -AllowStartIfOnBatteries -DontStopIfGoingOnBatteries `
    -ExecutionTimeLimit (New-TimeSpan -Seconds 0) -Hidden
foreach ($Definition in $Definitions) {
    $Action = New-ScheduledTaskAction -Execute $Definition.execute `
        -Argument $Definition.arguments -WorkingDirectory $Definition.working_directory
    $Triggers = @(
        (New-ScheduledTaskTrigger -AtLogOn -User $CurrentUser),
        (New-ScheduledTaskTrigger -Once -At (Get-Date).AddMinutes(1) `
            -RepetitionInterval (New-TimeSpan -Minutes $GuardMinutes))
    )
    $Task = $Tasks | Where-Object { $_.Name -eq $Definition.task_name }
    Register-ScheduledTask -TaskName $Definition.task_name -Action $Action `
        -Trigger $Triggers -Principal $Principal -Settings $Settings `
        -Description $Task.Description -Force | Out-Null
}
Get-ScheduledTask -TaskName "guvolu-fx-*" | Select-Object TaskName, State, TaskPath

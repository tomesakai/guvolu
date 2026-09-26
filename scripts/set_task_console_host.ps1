param(
    [Parameter(Mandatory = $true)]
    [string[]]$TaskName,
    [ValidateSet("headless", "console")]
    [string]$ConsoleHost = "headless",
    [switch]$DescribeOnly
)
# 改写已登记计划任务的控制台宿主（2026-09-26 快照）。
# 默认终端委托给 Windows Terminal 时，每次隐藏启动 powershell.exe 都会
# 在其进程内泄漏约 2 MB；每五分钟一次的守护任务每天泄漏逾 1 GB。
# headless 以 conhost.exe --headless 承载，不经委托、不泄漏；代价是
# 任务计划程序拿不到子进程退出码，故只用于不依赖退出码的守护任务。
# console 还原为直接启动 powershell.exe。触发器、主体与设置保持不变。
$ErrorActionPreference = "Stop"
# -File 调用无法传数组，逗号连写的任务名在此拆开。
$TaskName = @($TaskName | ForEach-Object { $_ -split "," } |
    ForEach-Object { $_.Trim() } | Where-Object { $_ })
$Prefix = "--headless powershell.exe "
$Results = @()
foreach ($Name in $TaskName) {
    $Task = Get-ScheduledTask -TaskName $Name -ErrorAction SilentlyContinue
    if (-not $Task) {
        throw "计划任务不存在: $Name"
    }
    if ($Task.Actions.Count -ne 1) {
        throw "只支持单一动作的任务: $Name"
    }
    $Current = $Task.Actions[0]
    $Execute = Split-Path -Leaf $Current.Execute
    $Arguments = [string]$Current.Arguments
    if ($ConsoleHost -eq "headless") {
        if ($Execute -ieq "conhost.exe" -and $Arguments.StartsWith($Prefix)) {
            $NewExecute = $Execute
            $NewArguments = $Arguments
        } elseif ($Execute -ieq "powershell.exe") {
            $NewExecute = "conhost.exe"
            $NewArguments = $Prefix + $Arguments
        } else {
            throw "动作不是 powershell.exe，拒绝改写: $Name ($Execute)"
        }
    } else {
        if ($Execute -ieq "powershell.exe") {
            $NewExecute = $Execute
            $NewArguments = $Arguments
        } elseif ($Execute -ieq "conhost.exe" -and $Arguments.StartsWith($Prefix)) {
            $NewExecute = "powershell.exe"
            $NewArguments = $Arguments.Substring($Prefix.Length)
        } else {
            throw "动作不是 headless 宿主，拒绝改写: $Name ($Execute)"
        }
    }
    $Changed = ($NewExecute -ne $Execute) -or ($NewArguments -ne $Arguments)
    $Results += [pscustomobject][ordered]@{
        task_name = $Name
        execute = $NewExecute
        arguments = $NewArguments
        changed = $Changed
    }
    if ($DescribeOnly -or -not $Changed) {
        continue
    }
    $Action = if ($Current.WorkingDirectory) {
        New-ScheduledTaskAction -Execute $NewExecute -Argument $NewArguments `
            -WorkingDirectory $Current.WorkingDirectory
    } else {
        New-ScheduledTaskAction -Execute $NewExecute -Argument $NewArguments
    }
    Set-ScheduledTask -TaskName $Name -Action $Action | Out-Null
}
$Results | ConvertTo-Json -Compress

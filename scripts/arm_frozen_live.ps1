param(
    [switch]$Disarm
)

# 上膛与解除武装的单命令入口（执行链设计第 14 节）。
# 上膛：注销 shadow 任务并注册 -live 每小时任务。
# 解除：注销 -live 任务并恢复 shadow 任务。
# 本脚本由维护者亲自运行（A-01），参数与当前冻结计划绑定。
$ErrorActionPreference = 'Stop'
$RepoRoot = (Resolve-Path (Join-Path $PSScriptRoot '..')).Path
$PlanId = 'frozen-forward-plan-c6981780abe962d23b6f788e7f526a228584ff66b9113fbdc368f8981e8826b4'
$Suffix = $PlanId.Substring($PlanId.Length - 12)
$ShadowTask = "guvolu-frozen-forward-$Suffix"
$LiveTask = "guvolu-frozen-forward-$Suffix-live"
$Common = @{
    PlanId              = $PlanId
    RuntimeRoot         = 'D:\dev\guvolu-frozen-runtime-356b45e'
    ExecutionRepository = 'C:\Users\wu_zh\dev\guvolu-exec'
    Repository          = $RepoRoot
    MinuteOffset        = 12
}

if ($Disarm) {
    if (Get-ScheduledTask -TaskName $LiveTask -ErrorAction SilentlyContinue) {
        Unregister-ScheduledTask -TaskName $LiveTask -Confirm:$false
        Write-Host "removed $LiveTask"
    }
    & (Join-Path $PSScriptRoot 'register_frozen_shadow_task.ps1') @Common `
        -StartUtc '2026-08-24T00:00:00Z' -EndUtc '2026-12-02T00:00:00Z' | Out-Null
    Write-Host "disarmed: shadow task restored"
} else {
    if (Get-ScheduledTask -TaskName $ShadowTask -ErrorAction SilentlyContinue) {
        Unregister-ScheduledTask -TaskName $ShadowTask -Confirm:$false
        Write-Host "removed $ShadowTask"
    }
    & (Join-Path $PSScriptRoot 'register_frozen_live_task.ps1') @Common `
        -StartUtc '2026-08-24T00:00:00Z' -EndUtc '2026-10-01T00:00:00Z' | Out-Null
    Write-Host "armed: live task registered"
}

Get-ScheduledTask -TaskName "guvolu-frozen-forward-*" |
    ForEach-Object {
        $info = $_ | Get-ScheduledTaskInfo
        "{0}: {1} next={2}" -f $_.TaskName, $_.State, $info.NextRunTime
    }

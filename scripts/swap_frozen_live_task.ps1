param(
    [Parameter(Mandatory = $true)]
    [ValidatePattern('^frozen-forward-plan-[0-9a-f]{64}$')]
    [string]$OldPlanId,
    [Parameter(Mandatory = $true)]
    [ValidatePattern('^frozen-forward-plan-[0-9a-f]{64}$')]
    [string]$PlanId,
    [Parameter(Mandatory = $true)]
    [string]$RuntimeRoot,
    [Parameter(Mandatory = $true)]
    [datetime]$StartUtc,
    [Parameter(Mandatory = $true)]
    [datetime]$EndUtc,
    [string]$ExecutionRepository = "C:\Users\wu_zh\dev\guvolu-exec",
    [ValidateRange(1, 59)]
    [int]$MinuteOffset = 30,
    [string]$MarketId = "",
    [string]$Symbol = "",
    [string]$TargetConfig = "",
    [switch]$Rehearsal
)
# 换计划（执行链设计第 14 节）：注销旧计划的 -live 任务，再按新计划注册。
# 由维护者亲自运行（A-01）；-Rehearsal 只描述新任务，不注销不注册。
$ErrorActionPreference = "Stop"
$RepoRoot = (Resolve-Path (Join-Path $PSScriptRoot "..")).Path
$Runtime = (Resolve-Path -LiteralPath $RuntimeRoot).Path
$Execution = (Resolve-Path -LiteralPath $ExecutionRepository).Path
$OldTask = "guvolu-frozen-forward-$($OldPlanId.Substring($OldPlanId.Length - 12))-live"
$NewTask = "guvolu-frozen-forward-$($PlanId.Substring($PlanId.Length - 12))-live"
if ($OldTask -eq $NewTask) {
    throw "新旧计划相同，无需换任务"
}
$PlanFile = Get-ChildItem -LiteralPath (Join-Path $Runtime "reports\strategy-research\frozen-forward") `
    -Recurse -Filter "plan.json" -ErrorAction SilentlyContinue |
    Where-Object { $_.Directory.Name -eq $PlanId } | Select-Object -First 1
if (-not $PlanFile) {
    throw "运行根内找不到新计划 $PlanId"
}
$Mode = if ($Rehearsal) { "rehearsal" } else { "swap" }
Write-Host "[$Mode] 旧任务 $OldTask -> 新任务 $NewTask；运行根 $Runtime"

$Existing = Get-ScheduledTask -TaskName $OldTask -ErrorAction SilentlyContinue
if ($Existing) {
    if ($Existing.State -eq "Running") {
        throw "旧任务正在运行，等本轮结束再换"
    }
    if ($Rehearsal) {
        Write-Host "[$Mode] 将注销 $OldTask"
    } else {
        Unregister-ScheduledTask -TaskName $OldTask -Confirm:$false
        Write-Host "[$Mode] 已注销 $OldTask"
    }
} else {
    Write-Host "[$Mode] 旧任务不存在，跳过注销"
}

$RegisterArguments = @(
    "-PlanId", $PlanId,
    "-StartUtc", $StartUtc.ToUniversalTime().ToString("o"),
    "-EndUtc", $EndUtc.ToUniversalTime().ToString("o"),
    "-RuntimeRoot", $Runtime,
    "-ExecutionRepository", $Execution,
    "-Repository", $RepoRoot,
    "-MinuteOffset", $MinuteOffset
)
if ($MarketId) { $RegisterArguments += @("-MarketId", $MarketId) }
if ($Symbol) { $RegisterArguments += @("-Symbol", $Symbol) }
if ($TargetConfig) { $RegisterArguments += @("-TargetConfig", $TargetConfig) }
if ($Rehearsal) { $RegisterArguments += "-DescribeOnly" }
if (-not $Rehearsal -and (Get-ScheduledTask -TaskName $NewTask -ErrorAction SilentlyContinue)) {
    Write-Host "[$Mode] 任务 $NewTask 已存在，跳过注册"
} else {
    & powershell -NoProfile -ExecutionPolicy Bypass -File (Join-Path $RepoRoot "scripts\register_frozen_live_task.ps1") @RegisterArguments
    if ($LASTEXITCODE -ne 0) {
        throw "注册任务失败，退出码 $LASTEXITCODE"
    }
}
Get-ScheduledTask | Where-Object { $_.TaskName -like "guvolu-frozen-forward-*-live" } |
    Select-Object TaskName, State | Format-Table -AutoSize

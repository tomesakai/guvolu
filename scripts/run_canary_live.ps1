param(
    [string]$ExecutionRepository = "C:\Users\wu_zh\dev\guvolu-exec",
    [string]$Size = "0.00002",
    [int]$MaxWaitSeconds = 120,
    [switch]$Rehearsal
)
# 最小实盘 canary 单命令入口（T-12 第三级，执行链设计第 13 节）。
# 由维护者在交互终端亲自运行（A-01）；live 模式只注入本次子进程
# （T-04），二次确认由 canary 自身在终端完成（X-02）。
# 先决：服务 OPEN、观察进程心跳新鲜、避开每小时 -live 链路窗口
# （第 10 至 25 分钟同品种在途锁会冲突）。-Rehearsal 不注入 live，
# 只走到模式拒绝，用于核对环境。
$ErrorActionPreference = "Stop"
$Execution = (Resolve-Path -LiteralPath $ExecutionRepository).Path
$Python = Join-Path $Execution ".venv\Scripts\python.exe"
$Runner = Join-Path $Execution "scripts\run_live_canary.py"
if (-not (Test-Path -LiteralPath $Runner -PathType Leaf)) { throw "缺少 $Runner" }
$Minute = (Get-Date).ToUniversalTime().Minute
if (-not $Rehearsal -and $Minute -ge 10 -and $Minute -le 25) {
    throw "当前为每小时 -live 链路窗口（UTC 第 10 至 25 分钟），请稍后再运行"
}
$Heartbeat = Join-Path $Execution "data\execution\live\observer_heartbeat.json"
if (Test-Path -LiteralPath $Heartbeat) {
    $Beat = Get-Content -LiteralPath $Heartbeat -Raw | ConvertFrom-Json
    $Age = ([datetime]::UtcNow - [datetime]::Parse($Beat.at).ToUniversalTime()).TotalSeconds
    if ($Age -gt 180) { Write-Warning "观察进程心跳已 $([int]$Age) 秒未更新" }
    else { Write-Host "观察进程心跳 $([int]$Age) 秒前，状态 $($Beat.status)" }
} else {
    Write-Warning "未见观察进程心跳文件"
}
$Ledger = Join-Path $Execution "data\execution\canary\intent_ledger.jsonl"
$Reports = Join-Path $Execution "data\execution\canary"
Write-Host "执行仓: $Execution"
Write-Host "数量: $Size BTC；等待窗口: $MaxWaitSeconds 秒；报告目录: $Reports"
Push-Location $Execution
try {
    $env:PYTHONPATH = Join-Path $Execution "src"
    $env:PYTHONIOENCODING = "utf-8"
    if ($Rehearsal) {
        Remove-Item Env:GUVOLU_MODE -ErrorAction SilentlyContinue
        Write-Host "彩排：不注入 live，预期以模式拒绝退出"
    } else {
        # live 只注入本子进程（T-04）
        $env:GUVOLU_MODE = "live"
    }
    & $Python $Runner --size $Size --max-wait-seconds $MaxWaitSeconds `
        --ledger $Ledger --report-directory $Reports
    $Code = $LASTEXITCODE
} finally {
    Remove-Item Env:GUVOLU_MODE -ErrorAction SilentlyContinue
    Pop-Location
}
Write-Host "canary 退出码 $Code（0 终态、1 未终态待人工复核、2 拒绝）"
exit $Code

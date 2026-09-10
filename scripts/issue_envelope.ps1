param(
    [string]$Draft = "config\authorization_envelope.draft-2.json",
    [string]$ExecutionRepository = "C:\Users\wu_zh\dev\guvolu-exec",
    [switch]$DryRun
)
# 签发授权信封（执行链设计第 14 节）：由维护者亲自运行（A-01）。
# 步骤：校验草案；把草案写为正式信封并提交主仓；执行仓快进到主仓
# 提交（执行器与观察进程从执行仓读信封与代码）；以执行仓密钥复核新
# 身份未熔断；重启观察进程使其装载新信封；核对 -live 任务状态。
# 熔断解除只能换封：新字节即新身份、新状态，旧状态文件不改写。
$ErrorActionPreference = "Stop"
$RepoRoot = (Resolve-Path (Join-Path $PSScriptRoot "..")).Path
$Execution = (Resolve-Path -LiteralPath $ExecutionRepository).Path
$Python = Join-Path $RepoRoot ".venv\Scripts\python.exe"
$ExecPython = Join-Path $Execution ".venv\Scripts\python.exe"
$DraftPath = Join-Path $RepoRoot $Draft
$Target = Join-Path $RepoRoot "config\authorization_envelope.json"
$Verifier = "scripts\verify_envelope.py"
if (-not (Test-Path -LiteralPath $DraftPath -PathType Leaf)) {
    throw "草案不存在: $DraftPath"
}
$MainDirty = git -C $RepoRoot status --porcelain
if ($MainDirty) {
    throw "主仓工作树不干净，先提交或清理:`n$MainDirty"
}
$ExecDirty = git -C $Execution status --porcelain
if ($ExecDirty) {
    throw "执行仓工作树不干净:`n$ExecDirty"
}
Write-Host "== 1/6 校验草案（主仓）"
# 校验器按当前目录找 .env 与脚本，须在主仓内执行（与第 4 步同）
Push-Location $RepoRoot
try {
    & $Python $Verifier --envelope $DraftPath
    if ($LASTEXITCODE -ne 0) { throw "草案校验失败" }
} finally {
    Pop-Location
}
$Sha = (Get-FileHash -LiteralPath $DraftPath -Algorithm SHA256).Hash.ToLowerInvariant()
$Sha12 = $Sha.Substring(0, 12)
if ($DryRun) {
    Write-Host "dry-run: 将签发 $Sha12，执行仓将快进到 $(git -C $RepoRoot rev-parse --short HEAD)"
    exit 0
}
Write-Host "== 2/6 写入正式信封并提交主仓"
Copy-Item -LiteralPath $DraftPath -Destination $Target -Force
git -C $RepoRoot add config/authorization_envelope.json
git -C $RepoRoot -c core.quotepath=false commit -q -m "chore(execution): 签发信封 $Sha12`n`n草案 $Draft 转正式信封，熔断解除即换封（第 14 节）。"
if ($LASTEXITCODE -ne 0) { throw "主仓提交失败" }
$Head = git -C $RepoRoot rev-parse HEAD
Write-Host "== 3/6 执行仓快进到 $($Head.Substring(0, 8))"
git -C $Execution merge --ff-only $Head | Out-Null
if ($LASTEXITCODE -ne 0) { throw "执行仓无法快进，人工处置" }
$ExecSha = (Get-FileHash -LiteralPath (Join-Path $Execution "config\authorization_envelope.json") `
    -Algorithm SHA256).Hash.ToLowerInvariant()
if ($ExecSha -ne $Sha) { throw "执行仓信封散列与主仓不一致" }
Write-Host "== 4/6 以执行仓复核新身份"
Push-Location $Execution
try {
    & $ExecPython $Verifier
    if ($LASTEXITCODE -ne 0) { throw "执行仓信封复核失败（身份已熔断或配置错误）" }
} finally {
    Pop-Location
}
Write-Host "== 5/6 重启观察进程以装载新信封"
$StopMarker = Join-Path $Execution "data\execution\live\observer.stop"
$Observer = Get-CimInstance Win32_Process |
    Where-Object { $_.CommandLine -match "live_observer" -and $_.CommandLine -match [regex]::Escape($Execution) }
if ($Observer) {
    New-Item -ItemType File -Path $StopMarker -Force | Out-Null
    $Deadline = (Get-Date).AddSeconds(150)
    while ((Get-Date) -lt $Deadline) {
        Start-Sleep -Seconds 5
        $Alive = Get-CimInstance Win32_Process |
            Where-Object { $_.CommandLine -match "live_observer" -and $_.CommandLine -match [regex]::Escape($Execution) }
        if (-not $Alive) { break }
    }
    Remove-Item -LiteralPath $StopMarker -Force -ErrorAction SilentlyContinue
    if ($Alive) { throw "观察进程未在时限内退出，人工处置" }
    Write-Host "旧观察进程已退出"
} else {
    Write-Host "无运行中的观察进程"
}
if (Get-ScheduledTask -TaskName "guvolu-live-observer-guard" -ErrorAction SilentlyContinue) {
    Start-ScheduledTask -TaskName "guvolu-live-observer-guard"
    Write-Host "已触发观察进程守护任务"
} else {
    Write-Warning "未注册观察进程守护任务，请先运行 register_live_observer_task.ps1"
}
Write-Host "== 6/6 任务状态"
Get-ScheduledTask | Where-Object { $_.TaskName -like "guvolu-frozen-forward-*-live" -or $_.TaskName -like "guvolu-live-observer-*" } |
    ForEach-Object {
        $Info = $_ | Get-ScheduledTaskInfo
        "{0}: {1} last={2} next={3}" -f $_.TaskName, $_.State, $Info.LastTaskResult, $Info.NextRunTime
    }
Write-Host "签发完成: $Sha12；下一轮 -live 任务将装载新信封。"

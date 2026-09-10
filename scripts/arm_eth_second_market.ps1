param(
    [string]$ExecutionRepository = "C:\Users\wu_zh\dev\guvolu-exec",
    [string]$RuntimeRoot = "D:\dev\guvolu-frozen-runtime-eth-b-a42aac6",
    [ValidatePattern('^frozen-forward-plan-[0-9a-f]{64}$')]
    [string]$PlanId = "frozen-forward-plan-01d6ae93e571d537017be6f525d3ecddb62ff04d346e93a86f85421394425b3f",
    [string]$Draft = "config\authorization_envelope.draft-8.json",
    [datetime]$StartUtc = "2026-09-05T00:00:00Z",
    [datetime]$EndUtc = "2026-12-31T00:00:00Z",
    [ValidateRange(1, 59)]
    [int]$MinuteOffset = 30,
    [string]$MarketId = "mkt__gmo__eth__r0",
    [string]$Symbol = "ETH",
    [string]$TargetConfig = "config/paper_executor_eth.json",
    [switch]$Rehearsal
)
# 上膛第二市场（执行链设计第 14 节）：由维护者亲自运行（A-01）。
# 三步：执行仓 .env 白名单补品种；签发两品种信封；注册第二市场 -live 任务。
# -Rehearsal 只校验不落地：.env 不改，信封走 -DryRun，任务走 -DescribeOnly。
$ErrorActionPreference = "Stop"
$RepoRoot = (Resolve-Path (Join-Path $PSScriptRoot "..")).Path
$Execution = (Resolve-Path -LiteralPath $ExecutionRepository).Path
$Runtime = (Resolve-Path -LiteralPath $RuntimeRoot).Path
$DraftPath = Join-Path $RepoRoot $Draft
$PlanPath = Get-ChildItem -LiteralPath (Join-Path $Runtime "reports\strategy-research\frozen-forward") `
    -Recurse -Filter "plan.json" -ErrorAction SilentlyContinue |
    Where-Object { $_.Directory.Name -eq $PlanId } | Select-Object -First 1
$Mode = if ($Rehearsal) { "rehearsal" } else { "arm" }

# 0. 前置校验：草案含品种、计划在运行根内、目标配置在两仓一致、主仓干净。
if (-not (Test-Path -LiteralPath $DraftPath -PathType Leaf)) {
    throw "草案不存在: $DraftPath"
}
$DraftSymbols = @((Get-Content -LiteralPath $DraftPath -Raw | ConvertFrom-Json).symbols)
if ($DraftSymbols -notcontains $Symbol) {
    throw "草案品种 [$($DraftSymbols -join ',')] 不含 $Symbol"
}
if (-not $PlanPath) {
    throw "运行根内找不到计划 $PlanId"
}
$MainConfig = Join-Path $RepoRoot ($TargetConfig -replace '/', '\')
if (-not (Test-Path -LiteralPath $MainConfig -PathType Leaf)) {
    throw "目标配置不存在: $MainConfig"
}
$MainDirty = git -C $RepoRoot status --porcelain
if ($MainDirty -and -not $Rehearsal) {
    throw "主仓有未提交改动，签发信封前须清空"
}
if ($MainDirty) {
    Write-Warning "主仓有未提交改动，正式上膛前须清空"
}
Write-Host "[$Mode] 草案 $Draft 品种 $($DraftSymbols -join ',')；计划 $($PlanId.Substring($PlanId.Length - 12))；运行根 $Runtime"

# 1. 执行仓 .env 白名单：只改这一键，不打印其他行（G-01）。
$EnvPath = Join-Path $Execution ".env"
if (-not (Test-Path -LiteralPath $EnvPath -PathType Leaf)) {
    throw "执行仓 .env 不存在: $EnvPath"
}
$Key = "GUVOLU_SPOT_WHITELIST"
$Lines = @(Get-Content -LiteralPath $EnvPath -Encoding UTF8)
$Existing = @($Lines | Where-Object { $_ -match "^\s*$Key\s*=" })
$Current = @()
if ($Existing.Count -gt 0) {
    $Current = @(($Existing[0] -split "=", 2)[1].Trim().Trim('"') -split "," |
        ForEach-Object { $_.Trim() } | Where-Object { $_ })
}
$Wanted = @($Current)
foreach ($Item in $DraftSymbols) {
    if ($Wanted -notcontains $Item) { $Wanted += $Item }
}
if ($Wanted.Count -eq 0) { $Wanted = @("BTC") }
$Desired = "$Key=$($Wanted -join ',')"
if ($Existing.Count -gt 0 -and $Existing[0].Trim() -eq $Desired) {
    Write-Host "[$Mode] 白名单已含 $($Wanted -join ',')，不改 .env"
} else {
    if ($Existing.Count -gt 0) {
        $Lines = @($Lines | ForEach-Object { if ($_ -match "^\s*$Key\s*=") { $Desired } else { $_ } })
    } else {
        $Lines += $Desired
    }
    if ($Rehearsal) {
        Write-Host "[$Mode] 将写入 .env: $Desired"
    } else {
        # 无 BOM UTF-8：配置解析按 utf-8 读，BOM 会污染首键。
        $Encoding = New-Object System.Text.UTF8Encoding($false)
        [System.IO.File]::WriteAllLines($EnvPath, [string[]]$Lines, $Encoding)
        Write-Host "[$Mode] 已写入 .env: $Desired"
    }
}

# 2. 签发信封（校验、提交主仓、快进执行仓、重启观察进程）。
# 主仓校验器读主仓 .env，白名单改以进程环境变量传入（环境优先于文件）。
$env:GUVOLU_SPOT_WHITELIST = ($Wanted -join ",")
$IssueArguments = @("-Draft", $Draft, "-ExecutionRepository", $Execution)
if ($Rehearsal) { $IssueArguments += "-DryRun" }
& powershell -NoProfile -ExecutionPolicy Bypass -File (Join-Path $RepoRoot "scripts\issue_envelope.ps1") @IssueArguments
if ($LASTEXITCODE -ne 0) {
    throw "签发信封失败，退出码 $LASTEXITCODE；任务未注册"
}

# 3. 注册第二市场 -live 任务；已存在则跳过，不重复注册。
$TaskName = "guvolu-frozen-forward-$($PlanId.Substring($PlanId.Length - 12))-live"
$RegisterArguments = @(
    "-PlanId", $PlanId,
    "-StartUtc", $StartUtc.ToUniversalTime().ToString("o"),
    "-EndUtc", $EndUtc.ToUniversalTime().ToString("o"),
    "-RuntimeRoot", $Runtime,
    "-ExecutionRepository", $Execution,
    "-Repository", $RepoRoot,
    "-MarketId", $MarketId,
    "-Symbol", $Symbol,
    "-TargetConfig", $TargetConfig,
    "-MinuteOffset", $MinuteOffset
)
if ($Rehearsal) { $RegisterArguments += "-DescribeOnly" }
if (-not $Rehearsal -and (Get-ScheduledTask -TaskName $TaskName -ErrorAction SilentlyContinue)) {
    Write-Host "[$Mode] 任务 $TaskName 已存在，跳过注册"
} else {
    & powershell -NoProfile -ExecutionPolicy Bypass -File (Join-Path $RepoRoot "scripts\register_frozen_live_task.ps1") @RegisterArguments
    if ($LASTEXITCODE -ne 0) {
        throw "注册任务失败，退出码 $LASTEXITCODE"
    }
}

# 收尾：只报状态，不打印密钥。
$Active = Join-Path $Execution "config\authorization_envelope.json"
$ActiveSymbols = @((Get-Content -LiteralPath $Active -Raw | ConvertFrom-Json).symbols)
Write-Host "[$Mode] 执行仓信封品种: $($ActiveSymbols -join ',')；白名单: $Desired"
Get-ScheduledTask | Where-Object { $_.TaskName -like "guvolu-frozen-forward-*-live" } |
    Select-Object TaskName, State | Format-Table -AutoSize

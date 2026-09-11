param(
    [string]$ExecutionRepository = "C:\Users\wu_zh\dev\guvolu-exec",
    [string]$Repository = ""
)

# 每周汇总 live 实测成交成本（执行链设计第 14 节）：只读扫描执行仓账本，
# 以 READ_ONLY 密钥取成交明细，报告落在执行仓 cost-summary 目录。零写。
$ErrorActionPreference = "Stop"
$RepoRoot = if ($Repository) {
    (Resolve-Path -LiteralPath $Repository).Path
} else {
    (Resolve-Path (Join-Path $PSScriptRoot "..")).Path
}
$Execution = (Resolve-Path -LiteralPath $ExecutionRepository).Path
$Python = Join-Path $Execution ".venv\Scripts\python.exe"
$Script = Join-Path $Execution "scripts\summarize_live_costs.py"
$LogDirectory = Join-Path $RepoRoot "logs"
$LogPath = Join-Path $LogDirectory "live-cost-summary.log"
New-Item -ItemType Directory -Force -Path $LogDirectory | Out-Null
if (-not (Test-Path -LiteralPath $Script -PathType Leaf)) {
    throw "汇总脚本不存在: $Script"
}
"$(Get-Date -Format o) cost summary start" | Out-File -LiteralPath $LogPath -Append -Encoding utf8
Push-Location $Execution
try {
    $Output = & $Python $Script --repository $Execution 2>&1
    $ExitCode = $LASTEXITCODE
} finally {
    Pop-Location
}
($Output -join "`n") | Out-File -LiteralPath $LogPath -Append -Encoding utf8
"$(Get-Date -Format o) cost summary exit $ExitCode" | Out-File -LiteralPath $LogPath -Append -Encoding utf8
exit $ExitCode

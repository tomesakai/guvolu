param(
    [string]$Repository = "",
    [string[]]$Markets = @(
        "mkt__gmo__btc__r0", "mkt__gmo__eth__r0", "mkt__gmo__xrp__r0",
        "mkt__gmo__sol__r0", "mkt__gmo__doge__r0",
        "mkt__bitbank__btc_jpy__r0", "mkt__bitflyer__btc_jpy__r0"
    ),
    [ValidateRange(1, 5)]
    [int]$AttemptsPerMarket = 3
)

# 逐笔实时段按日合并（TBD-40）：已过当日的五分钟段头合并为 day/YYYY-MM-DD，
# 先撤销零行段头。生产补漏任务运行自冻结运维副本，没有合并步；段头只增不减
# 会让每小时冻结前向链逐日变慢（2026-09-21 实测每市场 4,400 个头、单轮 45 分钟）。
$ErrorActionPreference = "Stop"
$RepoRoot = if ($Repository) {
    (Resolve-Path -LiteralPath $Repository).Path
} else {
    (Resolve-Path (Join-Path $PSScriptRoot "..")).Path
}
$Python = Join-Path $RepoRoot ".venv\Scripts\python.exe"
$DataRoot = Join-Path $RepoRoot "data"
$LogDirectory = Join-Path $RepoRoot "logs"
$LogPath = Join-Path $LogDirectory "trade-compaction.log"
New-Item -ItemType Directory -Force -Path $LogDirectory | Out-Null
$env:PYTHONIOENCODING = "utf-8"
$Failed = 0
"$(Get-Date -Format o) trade compaction start" | Out-File -LiteralPath $LogPath -Append -Encoding utf8
foreach ($Market in $Markets) {
    # 写锁被物化器占用时单次等待 120 秒即超时，逐市场重试。
    foreach ($Attempt in 1..$AttemptsPerMarket) {
        $Output = & $Python -m guvolu.data.trade_realtime_compact `
            --data-root $DataRoot --market-id $Market 2>&1
        $Code = $LASTEXITCODE
        $Summary = ($Output | Select-Object -Last 1)
        if ($Summary -and $Summary.ToString().Length -gt 300) {
            $Summary = $Summary.ToString().Substring(0, 300)
        }
        "$(Get-Date -Format o) $Market attempt $Attempt exit $Code $Summary" |
            Out-File -LiteralPath $LogPath -Append -Encoding utf8
        if ($Code -eq 0) { break }
    }
    if ($Code -ne 0) { $Failed += 1 }
}
"$(Get-Date -Format o) trade compaction end failed=$Failed" | Out-File -LiteralPath $LogPath -Append -Encoding utf8
exit $Failed

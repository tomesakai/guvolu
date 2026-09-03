param(
    [Parameter(Mandatory = $true)]
    [string]$Repository
)

# 每日全量补漏：常驻 watcher 为增量模式时，补齐超出
# 最新窗口的封口段并推进 bitbank 市场状态事实。
$ErrorActionPreference = 'Stop'
$RepoRoot = (Resolve-Path -LiteralPath $Repository).Path
$Python = Join-Path $RepoRoot '.venv\Scripts\python.exe'
$DataRoot = Join-Path $RepoRoot 'data'
$LogDir = Join-Path $RepoRoot 'logs'
# 中文输出按 UTF-8，避免转录乱码
$env:PYTHONIOENCODING = 'utf-8'
New-Item -ItemType Directory -Force -Path $LogDir | Out-Null
Start-Transcript -Path (Join-Path $LogDir 'daily-materializer-catchup.log') `
    -Append | Out-Null
try {
    & $Python -m guvolu.data.trade_realtime_materialize `
        --data-root $DataRoot all | Out-Null
    if ($LASTEXITCODE -ne 0) { throw "trade catchup exit $LASTEXITCODE" }
    Write-Host 'trade catchup done'
    # 已过当日的实时段按日合并（TBD-40）
    $CompactMarkets = @(
        'mkt__gmo__btc__r0', 'mkt__gmo__eth__r0', 'mkt__gmo__xrp__r0',
        'mkt__gmo__sol__r0', 'mkt__gmo__doge__r0',
        'mkt__bitbank__btc_jpy__r0', 'mkt__bitflyer__btc_jpy__r0'
    )
    foreach ($Market in $CompactMarkets) {
        & $Python -m guvolu.data.trade_realtime_compact `
            --data-root $DataRoot --market-id $Market | Out-Null
        if ($LASTEXITCODE -ne 0) { throw "trade compaction exit $LASTEXITCODE ($Market)" }
    }
    Write-Host 'trade compaction done'
    & $Python -m guvolu.data.l2_materialize --data-root $DataRoot all | Out-Null
    if ($LASTEXITCODE -ne 0) { throw "l2 catchup exit $LASTEXITCODE" }
    Write-Host 'l2 catchup done'
    & $Python -m guvolu.data.market_status_materialize `
        --data-root $DataRoot all | Out-Null
    if ($LASTEXITCODE -ne 0) { throw "market-status catchup exit $LASTEXITCODE" }
    Write-Host 'market-status catchup done'
} finally {
    Stop-Transcript | Out-Null
}

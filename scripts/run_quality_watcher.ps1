param(
    [int]$IntervalSeconds = 300,
    [string]$Repository = ''
)

# 独立 L2 质量刷新循环包装，从物化热循环分离质量遥测。
$ErrorActionPreference = 'Stop'
$RepoRoot = if ($Repository) {
    (Resolve-Path -LiteralPath $Repository).Path
} else {
    (Resolve-Path (Join-Path $PSScriptRoot '..')).Path
}
$PythonPath = Join-Path $RepoRoot '.venv\Scripts\python.exe'
$DataRoot = Join-Path $RepoRoot 'data'
Write-Host "guvolu quality-watcher started; interval=${IntervalSeconds}s."
& $PythonPath -m guvolu.data.quality_watcher --data-root $DataRoot `
    watch --interval-seconds $IntervalSeconds

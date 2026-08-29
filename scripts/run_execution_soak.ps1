# 浸泡进程包装：设定编码与模块路径后前台运行，Ctrl+C 直达进程。
# 其余参数原样转交 scripts/run_execution_soak.py，用法见执行链设计第 12 节。
param(
    [string]$Repository = (Split-Path -Parent $PSScriptRoot),
    [string]$Python = "",
    [Parameter(ValueFromRemainingArguments = $true)]
    [string[]]$SoakArgs
)

$ErrorActionPreference = "Stop"
$Utf8NoBom = [System.Text.UTF8Encoding]::new($false)
[Console]::OutputEncoding = $Utf8NoBom
$OutputEncoding = $Utf8NoBom
$env:PYTHONUTF8 = "1"
$Root = (Resolve-Path -LiteralPath $Repository).Path
if ([string]::IsNullOrWhiteSpace($Python)) {
    $Python = Join-Path $Root ".venv\Scripts\python.exe"
}
if (-not (Test-Path -LiteralPath $Python)) {
    Write-Error "找不到 Python 解释器: $Python（可用 -Python 指定）"
}
$Runner = Join-Path $Root "scripts\run_execution_soak.py"
$env:PYTHONPATH = Join-Path $Root "src"
Push-Location $Root
try {
    & $Python $Runner @SoakArgs
    exit $LASTEXITCODE
}
finally {
    Pop-Location
}

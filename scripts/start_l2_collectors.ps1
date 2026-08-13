param(
    [ValidateSet('Normal', 'Hidden')]
    [string]$WindowStyle = 'Normal'
)

$ErrorActionPreference = 'Stop'
$RepoRoot = (Resolve-Path (Join-Path $PSScriptRoot '..')).Path
$PythonPath = Join-Path $RepoRoot '.venv\Scripts\python.exe'

if (-not (Test-Path -LiteralPath $PythonPath -PathType Leaf)) {
    throw "Project Python runtime is missing: $PythonPath"
}

$Collectors = @(
    @{ Name = 'l2-gmo-btc'; Venue = 'gmo'; Symbol = 'BTC' },
    @{ Name = 'l2-bitbank-btc-jpy'; Venue = 'bitbank'; Symbol = 'btc_jpy' },
    @{ Name = 'l2-bitflyer-btc-jpy'; Venue = 'bitflyer'; Symbol = 'BTC_JPY' }
)

foreach ($Collector in $Collectors) {
    $Venue = $Collector.Venue
    $Symbol = $Collector.Symbol
    $CommandTail = (
        "-m guvolu.data.l2_capture record --venue $Venue --symbol $Symbol " +
        '--minutes 0 --segment-seconds 300 --segment-max-mib 128'
    )
    $Existing = @(
        Get-CimInstance Win32_Process -Filter "Name='python.exe'" |
            Where-Object { $_.CommandLine -like "*$CommandTail*" }
    )
    if ($Existing.Count -gt 0) {
        $Pids = ($Existing.ProcessId -join ',')
        Write-Host "[$($Collector.Name)] already running PID=$Pids"
        continue
    }

    $RunnerPath = Join-Path $PSScriptRoot 'run_l2_collector.ps1'
    $Started = Start-Process -FilePath 'powershell.exe' -ArgumentList @(
        '-NoProfile', '-NoExit', '-ExecutionPolicy', 'Bypass',
        '-File', $RunnerPath,
        '-Venue', $Venue,
        '-Symbol', $Symbol,
        '-Name', $Collector.Name
    ) -WorkingDirectory $RepoRoot -WindowStyle $WindowStyle -PassThru
    Write-Host "[$($Collector.Name)] window started PID=$($Started.Id)"
}

$MaterializerTail = '-m guvolu.data.l2_materialize watch --interval-seconds 300'
$ExistingMaterializer = @(
    Get-CimInstance Win32_Process -Filter "Name='python.exe'" |
        Where-Object { $_.CommandLine -like "*$MaterializerTail*" }
)
if ($ExistingMaterializer.Count -gt 0) {
    $Pids = ($ExistingMaterializer.ProcessId -join ',')
    Write-Host "[l2-materializer] already running PID=$Pids"
} else {
    $MaterializerRunner = Join-Path $PSScriptRoot 'run_l2_materializer.ps1'
    $Started = Start-Process -FilePath 'powershell.exe' -ArgumentList @(
        '-NoProfile', '-NoExit', '-ExecutionPolicy', 'Bypass',
        '-File', $MaterializerRunner, '-IntervalSeconds', '300'
    ) -WorkingDirectory $RepoRoot -WindowStyle $WindowStyle -PassThru
    Write-Host "[l2-materializer] window started PID=$($Started.Id)"
}

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
# 窗口安置：隐藏启动后无激活显示到副屏
# 窗口安置由启动方负责
$env:GUVOLU_WINDOW_PLACED = '1'
$WindowPlacementLoaded = $false
$WindowPlacementPath = Join-Path $PSScriptRoot 'window_placement.ps1'
if ($WindowStyle -eq 'Normal' -and (Test-Path -LiteralPath $WindowPlacementPath)) {
    try {
        . $WindowPlacementPath
        $WindowPlacementLoaded = $true
    } catch {
        Write-Warning "[window-placement] helper unavailable: $($_.Exception.Message)"
    }
}
$LaunchWindowStyle = if ($WindowStyle -eq 'Normal' -and $WindowPlacementLoaded) {
    'Hidden'
} else {
    $WindowStyle
}
function Move-StartedWindow {
    param([Parameter(Mandatory = $true)][int]$ProcessId)
    if ($WindowStyle -eq 'Normal' -and $WindowPlacementLoaded) {
        Move-StartedWindowToSecondary -ProcessId $ProcessId
    }
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
    ) -WorkingDirectory $RepoRoot -WindowStyle $LaunchWindowStyle -PassThru
    Move-StartedWindow -ProcessId $Started.Id
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
    ) -WorkingDirectory $RepoRoot -WindowStyle $LaunchWindowStyle -PassThru
    Move-StartedWindow -ProcessId $Started.Id
    Write-Host "[l2-materializer] window started PID=$($Started.Id)"
}

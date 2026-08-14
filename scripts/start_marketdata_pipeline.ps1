param(
    [ValidateSet('Normal', 'Hidden')]
    [string]$WindowStyle = 'Normal',
    [ValidateSet('Full', 'ForwardMinimal')]
    [string]$Profile = 'Full',
    [string]$Repository = ''
)

$ErrorActionPreference = 'Stop'
$RepoRoot = if ($Repository) {
    (Resolve-Path -LiteralPath $Repository).Path
} else {
    (Resolve-Path (Join-Path $PSScriptRoot '..')).Path
}
$PythonPath = Join-Path $RepoRoot '.venv\Scripts\python.exe'
$DataRoot = Join-Path $RepoRoot 'data'
$RunnerRoot = $PSScriptRoot
$DataRootPattern = (
    '--data-root\s+"?' + [regex]::Escape($DataRoot) + '"?(?:\s|$)'
)
$RepositoryPattern = (
    '-Repository\s+"?' + [regex]::Escape($RepoRoot) + '"?(?:\s|$)'
)

if (-not (Test-Path -LiteralPath $PythonPath -PathType Leaf)) {
    throw "Project Python runtime is missing: $PythonPath"
}
Set-Location -LiteralPath $RepoRoot

function Get-RepositoryPythonProcess {
    param(
        [Parameter(Mandatory = $true)]
        [string]$Module,
        [Parameter(Mandatory = $true)]
        [string]$Command
    )
    Get-CimInstance Win32_Process -Filter "Name='python.exe'" |
        Where-Object {
            $_.CommandLine -and
            $_.CommandLine -like "*-m $Module* $Command *" -and
            $_.CommandLine -match $DataRootPattern
        }
}

function Get-RepositoryMaterializerProcess {
    param(
        [Parameter(Mandatory = $true)]
        [hashtable]$Materializer
    )
    Get-CimInstance Win32_Process |
        Where-Object {
            $_.CommandLine -and (
                (
                    $_.Name -eq 'python.exe' -and
                    $_.CommandLine -like (
                        "*-m $($Materializer.Module)* watch*"
                    ) -and
                    $_.CommandLine -match $DataRootPattern
                ) -or (
                    $_.Name -eq 'powershell.exe' -and
                    $_.CommandLine -like "*$($Materializer.Runner)*" -and
                    $_.CommandLine -match $RepositoryPattern
                )
            )
        }
}

# Recover only stale crash tails; a fresh checkpoint protects sparse live runs.
& $PythonPath -m guvolu.data.l2_capture --data-root $DataRoot `
    recover --older-minutes 60
& $PythonPath -m guvolu.data.trade_capture --data-root $DataRoot `
    recover --older-minutes 60

$Collectors = @(
    @{ Name = 'l2-gmo-btc'; Module = 'guvolu.data.l2_capture'; Runner = 'run_l2_collector.ps1'; Venue = 'gmo'; Symbol = 'BTC'; MaxMiB = 128 },
    @{ Name = 'l2-bitbank-btc-jpy'; Module = 'guvolu.data.l2_capture'; Runner = 'run_l2_collector.ps1'; Venue = 'bitbank'; Symbol = 'btc_jpy'; MaxMiB = 128 },
    @{ Name = 'l2-bitflyer-btc-jpy'; Module = 'guvolu.data.l2_capture'; Runner = 'run_l2_collector.ps1'; Venue = 'bitflyer'; Symbol = 'BTC_JPY'; MaxMiB = 128 },
    @{ Name = 'trade-gmo-btc'; Module = 'guvolu.data.trade_capture'; Runner = 'run_trade_collector.ps1'; Venue = 'gmo'; Symbol = 'BTC'; MaxMiB = 32 },
    @{ Name = 'trade-bitbank-btc-jpy'; Module = 'guvolu.data.trade_capture'; Runner = 'run_trade_collector.ps1'; Venue = 'bitbank'; Symbol = 'btc_jpy'; MaxMiB = 32 },
    @{ Name = 'trade-bitflyer-btc-jpy'; Module = 'guvolu.data.trade_capture'; Runner = 'run_trade_collector.ps1'; Venue = 'bitflyer'; Symbol = 'BTC_JPY'; MaxMiB = 32 }
)

foreach ($Collector in $Collectors) {
    $Venue = $Collector.Venue
    $Symbol = $Collector.Symbol
    $Existing = @(
        Get-RepositoryPythonProcess `
            -Module $Collector.Module -Command 'record' |
            Where-Object {
                $_.CommandLine -like "*--venue $Venue*" -and
                $_.CommandLine -like "*--symbol $Symbol*"
            }
    )
    if ($Existing.Count -gt 0) {
        Write-Host "[$($Collector.Name)] already running PID=$(($Existing.ProcessId -join ','))"
        continue
    }
    $RunnerPath = Join-Path $RunnerRoot $Collector.Runner
    $NoExit = if ($WindowStyle -eq 'Normal') { ' -NoExit' } else { '' }
    $Arguments = (
        "-NoProfile$NoExit -ExecutionPolicy Bypass " +
        "-File `"$RunnerPath`" -Repository `"$RepoRoot`" " +
        "-Venue $Venue -Symbol $Symbol -Name $($Collector.Name)"
    )
    $Started = Start-Process -FilePath 'powershell.exe' `
        -ArgumentList $Arguments -WorkingDirectory $RepoRoot `
        -WindowStyle $WindowStyle -PassThru
    Write-Host "[$($Collector.Name)] window started PID=$($Started.Id)"
}

$Materializers = @(
    @{ Name = 'l2-materializer'; Module = 'guvolu.data.l2_materialize'; Runner = 'run_l2_materializer.ps1' },
    @{ Name = 'trade-realtime-materializer'; Module = 'guvolu.data.trade_realtime_materialize'; Runner = 'run_trade_materializer.ps1' },
    @{ Name = 'book-state-materializer'; Module = 'guvolu.data.book_state_materialize'; Runner = 'run_book_state_materializer.ps1' },
    @{ Name = 'orderflow-tile-watcher'; Module = 'guvolu.data.orderflow_tile_materialize'; Runner = 'run_orderflow_tile_watcher.ps1' }
)
if ($Profile -eq 'ForwardMinimal') {
    $PausedMaterializers = @(
        $Materializers |
            Where-Object { $_.Name -ne 'trade-realtime-materializer' }
    )
    foreach ($Materializer in $PausedMaterializers) {
        $Existing = @(Get-RepositoryMaterializerProcess $Materializer)
        foreach ($Process in $Existing) {
            Stop-Process -Id $Process.ProcessId -Force -ErrorAction SilentlyContinue
        }
        if ($Existing.Count -gt 0) {
            Start-Sleep -Milliseconds 200
            $Remaining = @(Get-RepositoryMaterializerProcess $Materializer)
            if ($Remaining.Count -gt 0) {
                throw (
                    "[$($Materializer.Name)] failed to pause PID=" +
                    ($Remaining.ProcessId -join ',')
                )
            }
        }
        if ($Existing.Count -gt 0) {
            Write-Host (
                "[$($Materializer.Name)] paused for ForwardMinimal " +
                "PID=$(($Existing.ProcessId -join ','))"
            )
        }
    }
    $Materializers = @(
        $Materializers |
            Where-Object { $_.Name -eq 'trade-realtime-materializer' }
    )
}
foreach ($Materializer in $Materializers) {
    $Existing = @(Get-RepositoryPythonProcess `
        -Module $Materializer.Module -Command 'watch')
    if ($Existing.Count -gt 0) {
        Write-Host "[$($Materializer.Name)] already running PID=$(($Existing.ProcessId -join ','))"
        continue
    }
    $RunnerPath = Join-Path $RunnerRoot $Materializer.Runner
    $NoExit = if ($WindowStyle -eq 'Normal') { ' -NoExit' } else { '' }
    $Arguments = (
        "-NoProfile$NoExit -ExecutionPolicy Bypass " +
        "-File `"$RunnerPath`" -Repository `"$RepoRoot`" " +
        '-IntervalSeconds 300'
    )
    $Started = Start-Process -FilePath 'powershell.exe' `
        -ArgumentList $Arguments -WorkingDirectory $RepoRoot `
        -WindowStyle $WindowStyle -PassThru
    Write-Host "[$($Materializer.Name)] window started PID=$($Started.Id)"
}

param(
    [string]$Symbol = 'BTC-USDT',
    [string]$FromDay = '2026-07-12',
    [string]$ToDay = '2026-08-10',
    [ValidateRange(5, 3600)]
    [int]$IntervalSeconds = 15,
    [switch]$Once
)

$ErrorActionPreference = 'Stop'
$RepoRoot = (Resolve-Path (Join-Path $PSScriptRoot '..')).Path
$PythonPath = Join-Path $RepoRoot '.venv\Scripts\python.exe'

if (-not (Test-Path -LiteralPath $PythonPath -PathType Leaf)) {
    throw "Project Python runtime is missing: $PythonPath"
}

function Format-ProgressBar {
    param(
        [int]$Completed,
        [int]$Total,
        [int]$Width = 32
    )
    if ($Total -le 0) {
        return ('-' * $Width)
    }
    $Filled = [Math]::Min($Width, [Math]::Floor($Completed * $Width / $Total))
    return ('#' * $Filled) + ('-' * ($Width - $Filled))
}

function Get-LastCompleteLine {
    param([string]$Path)
    $Stream = [IO.File]::Open(
        $Path,
        [IO.FileMode]::Open,
        [IO.FileAccess]::Read,
        [IO.FileShare]::ReadWrite
    )
    try {
        $ReadLength = [int][Math]::Min([long]65536, [long]$Stream.Length)
        if ($ReadLength -le 0) {
            return $null
        }
        [void]$Stream.Seek(-$ReadLength, [IO.SeekOrigin]::End)
        $Buffer = New-Object byte[] $ReadLength
        $Read = $Stream.Read($Buffer, 0, $ReadLength)
        $Text = [Text.Encoding]::UTF8.GetString($Buffer, 0, $Read)
        $Lines = $Text -split "`n"
        if ($Lines.Count -lt 2) {
            return $null
        }
        # The final fragment may still be owned by the writer.  Use the last
        # newline-terminated record and trim only its CR terminator.
        $Index = $Lines.Count - 2
        return $Lines[$Index].TrimEnd("`r")
    } finally {
        $Stream.Dispose()
    }
}

try {
    $Host.UI.RawUI.WindowTitle = "guvolu OKX L2 hot backfill: $Symbol"
} catch {
    # RawUI is optional in non-interactive hosts.
}

Set-Location -LiteralPath $RepoRoot
$FrameHeaders = 0..32 | ForEach-Object { "c$_" }
$PreviousFramePath = $null
$PreviousSourceRow = $null
$PreviousSampleAt = $null
do {
    $PlanText = (& $PythonPath -m guvolu.data.okx_l2_backfill `
        --data-root data status --symbol $Symbol `
        --from-day $FromDay --to-day $ToDay 2>&1 | Out-String)
    if ($LASTEXITCODE -ne 0) {
        throw "Backfill plan failed:`n$PlanText"
    }
    $Plan = $PlanText | ConvertFrom-Json
    $Processes = @(
        Get-CimInstance Win32_Process -Filter "Name='python.exe'" |
            Where-Object {
                $_.CommandLine -like '*guvolu.data.okx_l2_backfill*run*' -and
                $_.CommandLine -like "*--symbol $Symbol*" -and
                $_.CommandLine -like "*--from-day $FromDay*" -and
                $_.CommandLine -like "*--to-day $ToDay*"
            }
    )
    $Downloaded = [int]$Plan.active_days + [int]$Plan.sealed_days
    $Materialized = [int]$Plan.active_days
    $Total = [int]$Plan.total_days
    $DownloadPercent = if ($Total -gt 0) { 100 * $Downloaded / $Total } else { 0 }
    $MaterializePercent = if ($Total -gt 0) { 100 * $Materialized / $Total } else { 0 }
    $Drive = Get-PSDrive -Name ([IO.Path]::GetPathRoot($RepoRoot).Substring(0, 1))
    $SampleAt = Get-Date
    $Live = $null
    $MaterializedRoot = Join-Path $RepoRoot (
        'data\materialized\book_l2\schema_version=2\' +
        'normalization_version=book-l2-normalization-v2\venue_id=okx\' +
        "market_id=$($Plan.market_id)"
    )
    $TempFrame = if (Test-Path -LiteralPath $MaterializedRoot) {
        Get-ChildItem -LiteralPath $MaterializedRoot -Recurse -File `
            -Filter '.okx-l2-*.frames.csv' |
            Sort-Object LastWriteTime -Descending |
            Select-Object -First 1
    }
    if ($null -ne $TempFrame -and $TempFrame.Length -gt 0) {
        $LastLine = Get-LastCompleteLine -Path $TempFrame.FullName
        if ($LastLine) {
            $Parsed = $LastLine | ConvertFrom-Csv -Header $FrameHeaders
            $SourceRow = [long]$Parsed.c30
            $EventAt = [DateTimeOffset]::Parse($Parsed.c12)
            $DayStart = [DateTimeOffset]::new(
                $EventAt.Year, $EventAt.Month, $EventAt.Day,
                0, 0, 0, [TimeSpan]::Zero
            )
            $CoveragePercent = [Math]::Min(
                100,
                [Math]::Max(0, 100 * ($EventAt - $DayStart).TotalDays)
            )
            $RowRate = $null
            if (
                $PreviousFramePath -eq $TempFrame.FullName -and
                $null -ne $PreviousSourceRow -and
                $null -ne $PreviousSampleAt
            ) {
                $Elapsed = ($SampleAt - $PreviousSampleAt).TotalSeconds
                if ($Elapsed -gt 0) {
                    $RowRate = ($SourceRow - $PreviousSourceRow) / $Elapsed
                }
            }
            $TempLevelPath = $TempFrame.FullName.Replace('.frames.csv', '.levels.csv')
            $TempBytes = $TempFrame.Length
            if (Test-Path -LiteralPath $TempLevelPath -PathType Leaf) {
                $TempBytes += (Get-Item -LiteralPath $TempLevelPath).Length
            }
            $Live = [pscustomobject]@{
                Day = $TempFrame.Directory.Name.Replace('event_day=', '')
                SourceRow = $SourceRow
                EventTime = $EventAt
                CoveragePercent = $CoveragePercent
                RowRate = $RowRate
                TempGiB = $TempBytes / 1GB
            }
            $PreviousFramePath = $TempFrame.FullName
            $PreviousSourceRow = $SourceRow
            $PreviousSampleAt = $SampleAt
        }
    }

    if (-not $Once) {
        Clear-Host
    }
    Write-Host "guvolu OKX L2 hot backfill" -ForegroundColor Cyan
    Write-Host "$(Get-Date -Format 'yyyy-MM-dd HH:mm:ss zzz')  $Symbol  $FromDay .. $ToDay"
    Write-Host "market_id: $($Plan.market_id)"
    Write-Host ''
    Write-Host ("download  [{0}] {1,2}/{2} ({3,5:N1}%)" -f `
        (Format-ProgressBar $Downloaded $Total), $Downloaded, $Total, $DownloadPercent)
    Write-Host ("material  [{0}] {1,2}/{2} ({3,5:N1}%)" -f `
        (Format-ProgressBar $Materialized $Total), $Materialized, $Total, $MaterializePercent)
    Write-Host ''
    Write-Host ("active={0}  sealed={1}  pending={2}  blocked={3}" -f `
        $Plan.active_days, $Plan.sealed_days, $Plan.pending_days, $Plan.blocked_days)
    Write-Host ("active_frames={0:N0}  estimated_remaining={1:N3} GiB" -f `
        $Plan.active_frames, $Plan.estimated_additional_gib)
    Write-Host ("disk_free={0:N2} GiB  worker_pid={1}" -f `
        ($Drive.Free / 1GB), (($Processes.ProcessId | Sort-Object -Unique) -join ','))
    if ($null -ne $Live) {
        $RateText = if ($null -eq $Live.RowRate) {
            'sampling'
        } else {
            "{0:N0} rows/s" -f $Live.RowRate
        }
        Write-Host ("live_day={0}  source_row={1:N0}  event_day={2:N1}%  rate={3}  temp={4:N2} GiB" -f `
            $Live.Day, $Live.SourceRow, $Live.CoveragePercent, $RateText, $Live.TempGiB) `
            -ForegroundColor Cyan
    }

    if ([int]$Plan.blocked_days -gt 0) {
        Write-Host "BLOCKED: $($Plan.blocked | ConvertTo-Json -Compress)" -ForegroundColor Red
    } elseif ([int]$Plan.sealed_days -gt 0) {
        Write-Host 'stage: sealed raw exists; normalizing, auditing, then promoting the partition head' `
            -ForegroundColor Yellow
    } elseif ($Downloaded -lt $Total) {
        Write-Host 'stage: requesting/downloading the next sealed daily artifact' -ForegroundColor Yellow
    } elseif ($Materialized -eq $Total) {
        Write-Host 'stage: complete' -ForegroundColor Green
    }

    $Activity = "OKX L2 materialization $Materialized/$Total"
    Write-Progress -Activity $Activity `
        -Status "downloaded=$Downloaded sealed=$($Plan.sealed_days) pending=$($Plan.pending_days)" `
        -PercentComplete $MaterializePercent

    if ($Once -or $Processes.Count -eq 0 -or $Materialized -eq $Total) {
        break
    }
    Start-Sleep -Seconds $IntervalSeconds
} while ($true)

Write-Progress -Activity 'OKX L2 materialization' -Completed
if (-not $Once) {
    if ($Materialized -eq $Total) {
        Write-Host 'Backfill completed and all daily heads are active.' -ForegroundColor Green
    } elseif ($Processes.Count -eq 0) {
        Write-Host 'Backfill worker is not running; inspect the run log before resuming.' `
            -ForegroundColor Red
    }
}

param(
    [int]$IntervalSeconds = 300,
    [string]$Repository = '',
    [switch]$LatestRunOnly,
    [ValidateRange(1, 2147483647)]
    [Nullable[int]]$LatestSealedSegmentsPerStream = $null
)

$ErrorActionPreference = 'Stop'
$KnownInvocationOptions = @(
    '-Repository',
    '-LatestRunOnly',
    '-LatestSealedSegmentsPerStream',
    '-IntervalSeconds'
)
$InvocationOptionCounts = @{}
foreach ($Name in $KnownInvocationOptions) {
    $InvocationOptionCounts[$Name] = 0
}
foreach ($RawArgument in [System.Environment]::GetCommandLineArgs()) {
    $Token = [string]$RawArgument
    if (-not $Token.StartsWith('-')) {
        continue
    }
    $DelimiterIndices = @($Token.IndexOf('='), $Token.IndexOf(':')) |
        Where-Object { $_ -gt 0 }
    $DelimiterIndex = if ($DelimiterIndices.Count -gt 0) {
        ($DelimiterIndices | Measure-Object -Minimum).Minimum
    } else {
        -1
    }
    $Base = if ($DelimiterIndex -gt 0) {
        $Token.Substring(0, $DelimiterIndex)
    } else {
        $Token
    }
    foreach ($Name in $KnownInvocationOptions) {
        if ($Base -ieq $Name) {
            if ($Token -ine $Name) {
                throw "L2 runner option has an opaque form: $Token"
            }
            $InvocationOptionCounts[$Name] += 1
            continue
        }
        if (
            $Name.StartsWith(
                $Base,
                [System.StringComparison]::OrdinalIgnoreCase
            ) -or
            $Base.StartsWith(
                $Name,
                [System.StringComparison]::OrdinalIgnoreCase
            )
        ) {
            throw "L2 runner option has an opaque form: $Token"
        }
    }
}
foreach ($Name in $KnownInvocationOptions) {
    if ($InvocationOptionCounts[$Name] -gt 1) {
        throw "L2 runner option is repeated: $Name"
    }
}
if ($LatestRunOnly -and $null -ne $LatestSealedSegmentsPerStream) {
    throw (
        'LatestRunOnly and LatestSealedSegmentsPerStream are mutually exclusive.'
    )
}
$RepoRoot = if ($Repository) {
    (Resolve-Path -LiteralPath $Repository).Path
} else {
    (Resolve-Path (Join-Path $PSScriptRoot '..')).Path
}
$PythonPath = Join-Path $RepoRoot '.venv\Scripts\python.exe'
$DataRoot = Join-Path $RepoRoot 'data'
$LogDirectory = Join-Path $RepoRoot 'logs'
$LogPath = Join-Path $LogDirectory 'l2-materializer.log'

New-Item -ItemType Directory -Force -Path $LogDirectory | Out-Null
try {
    $Host.UI.RawUI.WindowTitle = 'guvolu l2-materializer'
} catch {
    # RawUI is optional in non-interactive hosts.
}
Set-Location -LiteralPath $RepoRoot
Start-Transcript -Path $LogPath -Append | Out-Null
try {
    Write-Host "guvolu l2-materializer started; interval=${IntervalSeconds}s."
    # 质量由独立进程刷新，不占物化热循环
    $Arguments = @(
        'watch', '--interval-seconds', [string]$IntervalSeconds, '--no-quality'
    )
    if ($LatestRunOnly) {
        $Arguments += '--latest-run-only'
    }
    if ($null -ne $LatestSealedSegmentsPerStream) {
        $Arguments += @(
            '--latest-sealed-segments-per-stream',
            [string]$LatestSealedSegmentsPerStream
        )
    }
    & $PythonPath -m guvolu.data.l2_materialize --data-root $DataRoot @Arguments
} finally {
    Write-Host 'guvolu l2-materializer exited.'
    Stop-Transcript | Out-Null
}

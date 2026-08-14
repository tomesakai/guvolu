param(
    [Parameter(Mandatory = $true)]
    [string]$PlanId,
    [Parameter(Mandatory = $true)]
    [string]$Repository,
    [Parameter(Mandatory = $true)]
    [string]$Registry
)

$ErrorActionPreference = "Stop"
$Utf8NoBom = [System.Text.UTF8Encoding]::new($false)
[Console]::OutputEncoding = $Utf8NoBom
$OutputEncoding = $Utf8NoBom
$env:PYTHONUTF8 = "1"
$Root = (Resolve-Path -LiteralPath $Repository).Path
$Python = Join-Path $Root ".venv\Scripts\python.exe"
$Runner = Join-Path $Root "scripts\manage_frozen_forward.py"
$LogDirectory = Join-Path $Root "logs\research\frozen-forward"
$LogPath = Join-Path $LogDirectory "task.jsonl"
New-Item -ItemType Directory -Path $LogDirectory -Force | Out-Null
$StartedAt = [datetime]::UtcNow.ToString("o")
$RunToken = [guid]::NewGuid().ToString("N")
$StdoutPath = Join-Path $LogDirectory ("." + $RunToken + ".stdout.tmp")
$StderrPath = Join-Path $LogDirectory ("." + $RunToken + ".stderr.tmp")
$ExitCode = 1
$Output = ""
try {
    $Arguments = @(
        ('"' + $Runner + '"'),
        "--root",
        ('"' + $Root + '"'),
        "predict",
        $PlanId,
        "--registry",
        ('"' + $Registry + '"')
    )
    $Process = Start-Process `
        -FilePath $Python `
        -ArgumentList $Arguments `
        -RedirectStandardOutput $StdoutPath `
        -RedirectStandardError $StderrPath `
        -WindowStyle Hidden `
        -Wait `
        -PassThru
    $ExitCode = $Process.ExitCode
    $Streams = @()
    $Stdout = Get-Content -LiteralPath $StdoutPath -Raw -Encoding UTF8
    $Stderr = Get-Content -LiteralPath $StderrPath -Raw -Encoding UTF8
    if (-not [string]::IsNullOrWhiteSpace($Stdout)) {
        $Streams += $Stdout.TrimEnd()
    }
    if (-not [string]::IsNullOrWhiteSpace($Stderr)) {
        $Streams += $Stderr.TrimEnd()
    }
    $Output = $Streams -join "`n"
}
catch {
    $ExitCode = 1
    $Output = $_.Exception.ToString()
}
finally {
    Remove-Item -LiteralPath $StdoutPath -Force -ErrorAction SilentlyContinue
    Remove-Item -LiteralPath $StderrPath -Force -ErrorAction SilentlyContinue
}
$Record = [ordered]@{
    started_at = $StartedAt
    completed_at = [datetime]::UtcNow.ToString("o")
    plan_id = $PlanId
    exit_code = $ExitCode
    output = $Output
}
$Json = $Record | ConvertTo-Json -Compress
[System.IO.File]::AppendAllText(
    $LogPath,
    $Json + [Environment]::NewLine,
    $Utf8NoBom
)
if ($ExitCode -ne 0) {
    exit $ExitCode
}

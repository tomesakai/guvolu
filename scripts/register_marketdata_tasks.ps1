param(
    [switch]$Unregister,
    [ValidateSet('Full', 'ForwardMinimal')]
    [string]$Profile = 'Full',
    [string]$Repository = '',
    [ValidateRange(1, 2147483647)]
    [Nullable[int]]$L2LatestSealedSegmentsPerStream = $null,
    [ValidateRange(1, 2147483647)]
    [Nullable[int]]$TradeLatestSealedSegmentsPerStream = $null
)

$ErrorActionPreference = 'Stop'
$RepoRoot = if ($Repository) {
    (Resolve-Path -LiteralPath $Repository).Path
} else {
    (Resolve-Path (Join-Path $PSScriptRoot '..')).Path
}
$StartScript = Join-Path $PSScriptRoot 'start_marketdata_pipeline.ps1'
$CurrentUser = [System.Security.Principal.WindowsIdentity]::GetCurrent().Name
$TaskNames = @('guvolu-marketdata-logon', 'guvolu-marketdata-guard')
$ObsoleteTaskNames = @('guvolu-api-logon', 'guvolu-api-guard')

foreach ($TaskName in $ObsoleteTaskNames) {
    if (Get-ScheduledTask -TaskName $TaskName -ErrorAction SilentlyContinue) {
        Unregister-ScheduledTask -TaskName $TaskName -Confirm:$false
        Write-Host "removed obsolete task $TaskName"
    }
}

if ($Unregister) {
    foreach ($TaskName in $TaskNames) {
        if (Get-ScheduledTask -TaskName $TaskName -ErrorAction SilentlyContinue) {
            Unregister-ScheduledTask -TaskName $TaskName -Confirm:$false
            Write-Host "removed task $TaskName"
        }
    }
    exit 0
}

$Argument = (
    '-NoProfile -NonInteractive -ExecutionPolicy Bypass -WindowStyle Hidden ' +
    "-File `"$StartScript`" -WindowStyle Hidden " +
    "-Profile $Profile -Repository `"$RepoRoot`""
)
if ($null -ne $L2LatestSealedSegmentsPerStream) {
    $Argument += (
        ' -L2LatestSealedSegmentsPerStream ' +
        [string]$L2LatestSealedSegmentsPerStream
    )
}
if ($null -ne $TradeLatestSealedSegmentsPerStream) {
    $Argument += (
        ' -TradeLatestSealedSegmentsPerStream ' +
        [string]$TradeLatestSealedSegmentsPerStream
    )
}
$Action = New-ScheduledTaskAction -Execute 'powershell.exe' `
    -Argument $Argument -WorkingDirectory $RepoRoot
$Principal = New-ScheduledTaskPrincipal -UserId $CurrentUser `
    -LogonType Interactive -RunLevel Limited
$Settings = New-ScheduledTaskSettingsSet -MultipleInstances IgnoreNew `
    -StartWhenAvailable -ExecutionTimeLimit (New-TimeSpan -Minutes 4)

$LogonTrigger = New-ScheduledTaskTrigger -AtLogOn -User $CurrentUser
Register-ScheduledTask -TaskName $TaskNames[0] -Action $Action `
    -Trigger $LogonTrigger -Principal $Principal -Settings $Settings `
    -Description 'Start the guvolu public market-data pipeline at logon.' `
    -Force | Out-Null

$GuardTrigger = New-ScheduledTaskTrigger -Once -At (Get-Date).AddMinutes(1) `
    -RepetitionInterval (New-TimeSpan -Minutes 5)
Register-ScheduledTask -TaskName $TaskNames[1] -Action $Action `
    -Trigger $GuardTrigger -Principal $Principal -Settings $Settings `
    -Description 'Idempotently guard guvolu public market-data writers.' `
    -Force | Out-Null

Get-ScheduledTask -TaskName 'guvolu-marketdata-*' |
    Select-Object TaskName, State, TaskPath

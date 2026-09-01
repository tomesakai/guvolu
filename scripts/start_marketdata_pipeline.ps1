param(
    [ValidateSet('Normal', 'Hidden')]
    [string]$WindowStyle = 'Normal',
    [ValidateSet('Full', 'ForwardMinimal')]
    [string]$Profile = 'Full',
    [string]$Repository = '',
    [switch]$L2LatestRunOnly,
    [ValidateRange(1, 2147483647)]
    [Nullable[int]]$L2LatestSealedSegmentsPerStream = $null,
    [ValidateRange(1, 2147483647)]
    [Nullable[int]]$TradeLatestSealedSegmentsPerStream = $null
)

$ErrorActionPreference = 'Stop'
if ($L2LatestRunOnly -and $null -ne $L2LatestSealedSegmentsPerStream) {
    throw (
        'L2LatestRunOnly and L2LatestSealedSegmentsPerStream are mutually exclusive.'
    )
}
if (
    $Profile -eq 'ForwardMinimal' -and (
        $L2LatestRunOnly -or
        $null -ne $L2LatestSealedSegmentsPerStream -or
        $null -ne $TradeLatestSealedSegmentsPerStream
    )
) {
    throw 'Input selection cannot be used with ForwardMinimal.'
}
$RepoRoot = if ($Repository) {
    (Resolve-Path -LiteralPath $Repository).Path
} else {
    (Resolve-Path (Join-Path $PSScriptRoot '..')).Path
}
$PythonPath = Join-Path $RepoRoot '.venv\Scripts\python.exe'
$PyVenvConfigPath = Join-Path $RepoRoot '.venv\pyvenv.cfg'
$DataRoot = Join-Path $RepoRoot 'data'
$RunnerRoot = $PSScriptRoot
$L2RunnerPath = Join-Path $RunnerRoot 'run_l2_materializer.ps1'
. (Join-Path $PSScriptRoot 'l2_materializer_process_contract.ps1')
$L2OwnerDirectory = Join-Path $DataRoot '.locks'
$L2OwnerLockPath = Join-Path `
    $L2OwnerDirectory 'l2-materializer-owner.lock'
$L2OwnerRecordPath = Join-Path `
    $L2OwnerDirectory 'l2-materializer-owner.json'

if (-not (Test-Path -LiteralPath $PythonPath -PathType Leaf)) {
    throw "Project Python runtime is missing: $PythonPath"
}
if (-not (Test-Path -LiteralPath $PyVenvConfigPath -PathType Leaf)) {
    throw "Project Python runtime config is missing: $PyVenvConfigPath"
}
$PythonHomeMatches = @(
    Get-Content -LiteralPath $PyVenvConfigPath |
        Where-Object { $_ -match '^(?i:home)\s*=\s*(.+?)\s*$' }
)
if ($PythonHomeMatches.Count -ne 1) {
    throw "Project Python runtime home is ambiguous: $PyVenvConfigPath"
}
$PythonHome = [regex]::Match(
    $PythonHomeMatches[0],
    '^(?i:home)\s*=\s*(.+?)\s*$'
).Groups[1].Value
if (-not [System.IO.Path]::IsPathRooted($PythonHome)) {
    throw "Project Python runtime home is not absolute: $PyVenvConfigPath"
}
$PythonBasePath = Join-Path $PythonHome 'python.exe'
if (-not (Test-Path -LiteralPath $PythonBasePath -PathType Leaf)) {
    throw "Project Python base runtime is missing: $PythonBasePath"
}
Set-Location -LiteralPath $RepoRoot

function Test-MarketdataCommandLineTokenHint {
    param(
        [Parameter(Mandatory = $true)]
        [string]$CommandLine,
        [Parameter(Mandatory = $true)]
        [string]$Value
    )
    return $CommandLine -match (
        '(?i)(?:^|[\s"''])' + [regex]::Escape($Value) +
        '(?:$|[\s"''])'
    )
}

function Test-RepositoryPythonProcess {
    param(
        [Parameter(Mandatory = $true)]
        [object]$Process,
        [Parameter(Mandatory = $true)]
        [string]$Module,
        [Parameter(Mandatory = $true)]
        [string]$Command
    )
    $CommandLine = [string]$Process.CommandLine
    if (
        -not (Test-MarketdataCommandLineTokenHint $CommandLine $Module) -or
        -not (Test-MarketdataCommandLineTokenHint $CommandLine $Command)
    ) {
        return $false
    }
    try {
        $Tokens = @(
            ConvertTo-L2ProcessCommandTokens -CommandLine $CommandLine
        )
        $ModuleIndices = @()
        $ModuleSwitchIndices = @()
        $CommandIndices = @()
        for ($Index = 0; $Index -lt $Tokens.Count; $Index += 1) {
            if ($Tokens[$Index] -ieq $Module) {
                $ModuleIndices += $Index
            }
            if ($Tokens[$Index] -ieq '-m') {
                $ModuleSwitchIndices += $Index
            }
            if ($Tokens[$Index] -ieq $Command) {
                $CommandIndices += $Index
            }
        }
        if (
            $ModuleIndices.Count -ne 1 -or
            $ModuleSwitchIndices.Count -ne 1 -or
            $ModuleSwitchIndices[0] -lt 1 -or
            $ModuleIndices[0] -ne ($ModuleSwitchIndices[0] + 1) -or
            $CommandIndices.Count -ne 1 -or
            $CommandIndices[0] -le $ModuleIndices[0]
        ) {
            throw 'Python module/command identity is ambiguous.'
        }
        if (
            [System.IO.Path]::GetFileName([string]$Tokens[0]) -ine
            [string]$Process.Name
        ) {
            throw 'Python interpreter identity is opaque.'
        }
        $AllowedInterpreterFlags = @(
            '-B', '-E', '-I', '-O', '-OO', '-P', '-q', '-s', '-S',
            '-u', '-v'
        )
        for (
            $Index = 1;
            $Index -lt $ModuleSwitchIndices[0];
            $Index += 1
        ) {
            if ($Tokens[$Index] -cnotin $AllowedInterpreterFlags) {
                throw (
                    'Python interpreter entry is opaque: ' +
                    [string]$Tokens[$Index]
                )
            }
        }
        $ModuleArguments = @(
            $Tokens[($ModuleIndices[0] + 1)..($Tokens.Count - 1)]
        )
        $Roots = @(
            Get-L2OptionValues `
                -Tokens $ModuleArguments `
                -Name '--data-root' -AllowEquals
        )
        if ($Roots.Count -ne 1) {
            throw 'Python data-root identity is ambiguous.'
        }
        if (-not [System.IO.Path]::IsPathRooted($Roots[0])) {
            throw 'Python data-root is not an absolute path.'
        }
        return (
            Test-L2CanonicalPathEqual `
                -Left $Roots[0] -Right $DataRoot
        )
    } catch {
        throw (
            '[marketdata-process] existing Python process is opaque; ' +
            'refusing pipeline side effects. PID=' +
            [string]$Process.ProcessId + '; ' + $_.Exception.Message
        )
    }
}

function Get-RepositoryPythonProcess {
    param(
        [Parameter(Mandatory = $true)]
        [string]$Module,
        [Parameter(Mandatory = $true)]
        [string]$Command
    )
    foreach ($Process in @(
        Get-CimInstance Win32_Process -Filter "Name='python.exe'"
    )) {
        if (
            $Process.CommandLine -and
            (Test-RepositoryPythonProcess `
                -Process $Process -Module $Module -Command $Command)
        ) {
            $Process
        }
    }
}

function Test-RepositoryMaterializerRunnerProcess {
    param(
        [Parameter(Mandatory = $true)]
        [object]$Process,
        [Parameter(Mandatory = $true)]
        [hashtable]$Materializer
    )
    $CommandLine = [string]$Process.CommandLine
    $RunnerPath = Join-Path $RunnerRoot $Materializer.Runner
    $RunnerName = [System.IO.Path]::GetFileName($RunnerPath)
    if (
        $CommandLine.IndexOf(
            $RunnerName,
            [System.StringComparison]::OrdinalIgnoreCase
        ) -lt 0
    ) {
        return $false
    }
    try {
        $Tokens = @(
            ConvertTo-L2ProcessCommandTokens -CommandLine $CommandLine
        )
        $RunnerIndices = @()
        for ($Index = 0; $Index -lt $Tokens.Count; $Index += 1) {
            if (
                [System.IO.Path]::GetFileName([string]$Tokens[$Index]) -ieq
                $RunnerName
            ) {
                $RunnerIndices += $Index
            }
        }
        if ($RunnerIndices.Count -ne 1) {
            throw 'PowerShell runner path identity is ambiguous.'
        }
        $RunnerIndex = $RunnerIndices[0]
        Assert-L2PowerShellFileEntry `
            -Tokens $Tokens `
            -ProcessName ([string]$Process.Name) `
            -FilePathIndex $RunnerIndex
        if (-not [System.IO.Path]::IsPathRooted($Tokens[$RunnerIndex])) {
            throw 'PowerShell runner path is not absolute.'
        }
        if (-not (
            Test-L2CanonicalPathEqual $Tokens[$RunnerIndex] $RunnerPath
        )) {
            return $false
        }
        $Repositories = @(
            Get-L2OptionValues -Tokens $Tokens -Name '-Repository'
        )
        if ($Repositories.Count -ne 1) {
            throw 'PowerShell repository identity is ambiguous.'
        }
        if (-not [System.IO.Path]::IsPathRooted($Repositories[0])) {
            throw 'PowerShell repository is not an absolute path.'
        }
        return (
            Test-L2CanonicalPathEqual `
                -Left $Repositories[0] -Right $RepoRoot
        )
    } catch {
        throw (
            '[marketdata-process] existing runner process is opaque; ' +
            'refusing pipeline side effects. PID=' +
            [string]$Process.ProcessId + '; ' + $_.Exception.Message
        )
    }
}

function Get-RepositoryMaterializerProcess {
    param(
        [Parameter(Mandatory = $true)]
        [hashtable]$Materializer
    )
    foreach ($Process in @(Get-CimInstance Win32_Process)) {
        if (-not $Process.CommandLine) {
            continue
        }
        if (
            $Process.Name -eq 'python.exe' -and
            (Test-RepositoryPythonProcess `
                -Process $Process `
                -Module $Materializer.Module -Command 'watch')
        ) {
            $Process
        } elseif (
            $Process.Name -eq 'powershell.exe' -and
            (Test-RepositoryMaterializerRunnerProcess `
                -Process $Process -Materializer $Materializer)
        ) {
            $Process
        }
    }
}

function Assert-L2MaterializerSelection {
    param(
        [Parameter(Mandatory = $true)]
        [object[]]$Processes
    )
    $Expected = if ($L2LatestRunOnly) {
        'latest_run'
    } elseif ($null -ne $L2LatestSealedSegmentsPerStream) {
        'latest_sealed_per_stream:' +
            [string]$L2LatestSealedSegmentsPerStream
    } else {
        'all'
    }
    foreach ($Process in $Processes) {
        if ([string]$Process.Selection -ne $Expected) {
            throw (
                '[l2-materializer] existing process selection differs; ' +
                'refusing to claim the requested bounded mode. PID=' +
                [string]$Process.ProcessId + '; kind=' +
                [string]$Process.Kind + '; actual=' +
                [string]$Process.Selection + '; expected=' + $Expected
            )
        }
    }
}

function Get-RepositoryL2MaterializerProcess {
    foreach ($Process in @(Get-CimInstance Win32_Process)) {
        if (-not $Process.CommandLine) {
            continue
        }
        $Contract = Get-L2MaterializerProcessContract `
            -ProcessName ([string]$Process.Name) `
            -CommandLine ([string]$Process.CommandLine) `
            -ProcessId ([int]$Process.ProcessId) `
            -RepositoryRoot $RepoRoot `
            -DataRoot $DataRoot `
            -RunnerPath $L2RunnerPath `
            -ExpectedPythonPath $PythonPath `
            -ExpectedPythonBasePath $PythonBasePath `
            -ExecutablePath ([string]$Process.ExecutablePath)
        if ($null -ne $Contract) {
            $Contract
        }
    }
}

function Get-ExpectedL2Selection {
    if ($L2LatestRunOnly) {
        return 'latest_run'
    }
    if ($null -ne $L2LatestSealedSegmentsPerStream) {
        return (
            'latest_sealed_per_stream:' +
            [string]$L2LatestSealedSegmentsPerStream
        )
    }
    return 'all'
}

function Test-L2ExactJsonInteger {
    param([object]$Value)
    return (
        $Value -is [byte] -or
        $Value -is [sbyte] -or
        $Value -is [int16] -or
        $Value -is [uint16] -or
        $Value -is [int32] -or
        $Value -is [uint32] -or
        $Value -is [int64] -or
        $Value -is [uint64]
    )
}

function Test-L2OwnerLockHeld {
    New-Item -ItemType Directory -Force `
        -Path $L2OwnerDirectory | Out-Null
    $Stream = [System.IO.File]::Open(
        $L2OwnerLockPath,
        [System.IO.FileMode]::OpenOrCreate,
        [System.IO.FileAccess]::ReadWrite,
        [System.IO.FileShare]::ReadWrite
    )
    try {
        if ($Stream.Length -eq 0) {
            $Stream.SetLength(1)
            $Stream.Flush($true)
        }
        try {
            $Stream.Lock(0, 1)
        } catch [System.IO.IOException] {
            return $true
        }
        try {
            return $false
        } finally {
            $Stream.Unlock(0, 1)
        }
    } finally {
        $Stream.Dispose()
    }
}

function Try-Enter-L2OwnerSuppression {
    New-Item -ItemType Directory -Force `
        -Path $L2OwnerDirectory | Out-Null
    $Stream = [System.IO.File]::Open(
        $L2OwnerLockPath,
        [System.IO.FileMode]::OpenOrCreate,
        [System.IO.FileAccess]::ReadWrite,
        [System.IO.FileShare]::ReadWrite
    )
    $LockAcquired = $false
    $KeepOpen = $false
    try {
        if ($Stream.Length -eq 0) {
            $Stream.SetLength(1)
            $Stream.Flush($true)
        }
        try {
            $Stream.Lock(0, 1)
            $LockAcquired = $true
        } catch [System.IO.IOException] {
            return $null
        }
        if (Test-Path -LiteralPath $L2OwnerRecordPath -PathType Leaf) {
            Remove-Item -LiteralPath $L2OwnerRecordPath -Force
        }
        $KeepOpen = $true
        return $Stream
    } finally {
        if (-not $KeepOpen) {
            if ($LockAcquired) {
                try {
                    $Stream.Unlock(0, 1)
                } finally {
                    $Stream.Dispose()
                }
            } else {
                $Stream.Dispose()
            }
        }
    }
}

function Get-L2OwnerTruth {
    if (-not (Test-L2OwnerLockHeld)) {
        return $null
    }
    if (-not (
        Test-Path -LiteralPath $L2OwnerRecordPath -PathType Leaf
    )) {
        return $null
    }
    try {
        $First = [System.IO.File]::ReadAllBytes($L2OwnerRecordPath)
        $Second = [System.IO.File]::ReadAllBytes($L2OwnerRecordPath)
    } catch {
        return $null
    }
    if (
        [System.Convert]::ToBase64String($First) -cne
        [System.Convert]::ToBase64String($Second)
    ) {
        return $null
    }
    try {
        $Utf8 = New-Object System.Text.UTF8Encoding($false, $true)
        $Text = $Utf8.GetString($First)
        $Record = $Text | ConvertFrom-Json -ErrorAction Stop
    } catch {
        throw '[l2-materializer] locked owner record is invalid JSON.'
    }
    $Required = @(
        'schema_version', 'pid', 'selection', 'data_root',
        'executable_path', 'started_at', 'nonce'
    )
    $Names = @($Record.PSObject.Properties.Name)
    if (
        $Names.Count -ne $Required.Count -or
        @($Required | Where-Object { $_ -notin $Names }).Count -gt 0
    ) {
        throw '[l2-materializer] locked owner record shape is invalid.'
    }
    if (
        -not (Test-L2ExactJsonInteger $Record.schema_version) -or
        [int64]$Record.schema_version -ne 1 -or
        -not (Test-L2ExactJsonInteger $Record.pid) -or
        [int64]$Record.pid -le 0 -or
        [int64]$Record.pid -gt [int]::MaxValue
    ) {
        throw '[l2-materializer] locked owner record integers are invalid.'
    }
    if (
        $Record.selection -isnot [string] -or
        [string]$Record.selection -notmatch (
            '^(?:all|latest_run|latest_sealed_per_stream:' +
            '[1-9][0-9]*)$'
        ) -or
        $Record.data_root -isnot [string] -or
        -not [System.IO.Path]::IsPathRooted([string]$Record.data_root) -or
        $Record.executable_path -isnot [string] -or
        -not [System.IO.Path]::IsPathRooted(
            [string]$Record.executable_path
        ) -or
        $Record.started_at -isnot [string] -or
        [string]$Record.started_at -notmatch '\+00:00$' -or
        $Record.nonce -isnot [string] -or
        [string]$Record.nonce -notmatch '^[0-9a-f]{32}$'
    ) {
        throw '[l2-materializer] locked owner record values are invalid.'
    }
    $StartedAt = [datetimeoffset]::MinValue
    if (-not [datetimeoffset]::TryParse(
        [string]$Record.started_at,
        [System.Globalization.CultureInfo]::InvariantCulture,
        [System.Globalization.DateTimeStyles]::RoundtripKind,
        [ref]$StartedAt
    )) {
        throw '[l2-materializer] locked owner started_at is invalid.'
    }
    if (-not (Test-L2CanonicalPathEqual $Record.data_root $DataRoot)) {
        throw '[l2-materializer] locked owner data-root differs.'
    }
    if (
        -not (Test-L2CanonicalPathEqual `
            $Record.executable_path $PythonPath
        ) -and
        -not (Test-L2CanonicalPathEqual `
            $Record.executable_path $PythonBasePath
        )
    ) {
        throw '[l2-materializer] locked owner executable differs.'
    }
    $OwnerPid = [int]$Record.pid
    $Process = Get-CimInstance Win32_Process `
        -Filter "ProcessId=$OwnerPid" -ErrorAction Stop
    if ($null -eq $Process -or -not $Process.CommandLine) {
        throw '[l2-materializer] locked owner PID is not live.'
    }
    if (-not (
        Test-L2CanonicalPathEqual `
            $Record.executable_path $Process.ExecutablePath
    )) {
        throw '[l2-materializer] locked owner executable truth differs.'
    }
    try {
        $ProcessStartedAt = [datetimeoffset]$Process.CreationDate
    } catch {
        throw '[l2-materializer] locked owner process start is opaque.'
    }
    if (
        $StartedAt -lt $ProcessStartedAt.AddSeconds(-1) -or
        $StartedAt -gt [datetimeoffset]::UtcNow.AddSeconds(5)
    ) {
        throw '[l2-materializer] locked owner start truth differs.'
    }
    $Contract = Get-L2MaterializerProcessContract `
        -ProcessName ([string]$Process.Name) `
        -CommandLine ([string]$Process.CommandLine) `
        -ProcessId ([int]$Process.ProcessId) `
        -RepositoryRoot $RepoRoot `
        -DataRoot $DataRoot `
        -RunnerPath $L2RunnerPath `
        -ExpectedPythonPath $PythonPath `
        -ExpectedPythonBasePath $PythonBasePath `
        -ExecutablePath ([string]$Process.ExecutablePath)
    if (
        $null -eq $Contract -or
        [string]$Contract.Kind -ne 'python' -or
        [string]$Contract.Selection -ne [string]$Record.selection
    ) {
        throw '[l2-materializer] locked owner process truth differs.'
    }
    return [pscustomobject]@{
        ProcessId = $OwnerPid
        Selection = [string]$Record.selection
        ExecutablePath = [string]$Record.executable_path
        StartedAt = [string]$Record.started_at
        Nonce = [string]$Record.nonce
        ParentProcessId = [int]$Process.ParentProcessId
        Contract = $Contract
    }
}

function Wait-L2OwnerTruth {
    param(
        [Parameter(Mandatory = $true)]
        [string]$ExpectedSelection,
        [int]$TimeoutSeconds = 15
    )
    $Deadline = [datetime]::UtcNow.AddSeconds($TimeoutSeconds)
    do {
        $Truth = Get-L2OwnerTruth
        if ($null -ne $Truth) {
            if ([string]$Truth.Selection -ne $ExpectedSelection) {
                throw (
                    '[l2-materializer] owner selection differs; actual=' +
                    [string]$Truth.Selection + '; expected=' +
                    $ExpectedSelection
                )
            }
            $Processes = @(Get-RepositoryL2MaterializerProcess)
            Assert-L2MaterializerSelection -Processes $Processes
            $PythonProcesses = @(
                $Processes | Where-Object { $_.Kind -eq 'python' }
            )
            $OwnerProcesses = @(
                $PythonProcesses | Where-Object {
                    $_.ProcessId -eq $Truth.ProcessId
                }
            )
            $Redirectors = @(
                $PythonProcesses | Where-Object {
                    $_.ProcessId -ne $Truth.ProcessId
                }
            )
            if ($OwnerProcesses.Count -ne 1) {
                throw '[l2-materializer] owner process confirmation is not unique.'
            }
            $OwnerUsesProjectLauncher = Test-L2CanonicalPathEqual `
                $OwnerProcesses[0].ExecutablePath $PythonPath
            if (
                -not $OwnerUsesProjectLauncher -and
                $Redirectors.Count -eq 0
            ) {
                throw '[l2-materializer] base Python owner lacks project launcher.'
            }
            if ($Redirectors.Count -gt 0) {
                if (
                    $Redirectors.Count -ne 1 -or
                    $Redirectors[0].ProcessId -ne $Truth.ParentProcessId -or
                    -not (
                        Test-L2CanonicalPathEqual `
                            $Redirectors[0].ExecutablePath $PythonPath
                    ) -or
                    $Redirectors[0].ArgumentSignature -cne
                    $OwnerProcesses[0].ArgumentSignature
                ) {
                    throw '[l2-materializer] additional Python writer is present.'
                }
            }
            return $Truth
        }
        Start-Sleep -Milliseconds 50
    } while ([datetime]::UtcNow -lt $Deadline)
    throw '[l2-materializer] timed out waiting for owner handshake.'
}

function Start-OrConfirm-L2Owner {
    $Expected = Get-ExpectedL2Selection
    $Existing = @(Get-RepositoryL2MaterializerProcess)
    if ($Existing.Count -gt 0) {
        Assert-L2MaterializerSelection -Processes $Existing
        return Wait-L2OwnerTruth -ExpectedSelection $Expected
    }
    if (Test-L2OwnerLockHeld) {
        return Wait-L2OwnerTruth -ExpectedSelection $Expected
    }
    $NoExit = if ($WindowStyle -eq 'Normal') { ' -NoExit' } else { '' }
    $Arguments = (
        "-NoProfile$NoExit -ExecutionPolicy Bypass " +
        "-File `"$L2RunnerPath`" -Repository `"$RepoRoot`" " +
        '-IntervalSeconds 300'
    )
    if ($L2LatestRunOnly) {
        $Arguments += ' -LatestRunOnly'
    }
    if ($null -ne $L2LatestSealedSegmentsPerStream) {
        $Arguments += (
            ' -LatestSealedSegmentsPerStream ' +
            [string]$L2LatestSealedSegmentsPerStream
        )
    }
    $Started = Start-Process -FilePath 'powershell.exe' `
        -ArgumentList $Arguments -WorkingDirectory $RepoRoot `
        -WindowStyle $WindowStyle -PassThru
    try {
        $Truth = Wait-L2OwnerTruth -ExpectedSelection $Expected
    } catch {
        if (-not $Started.HasExited) {
            Stop-Process -Id $Started.Id -Force -ErrorAction SilentlyContinue
        }
        throw
    }
    Write-Host (
        '[l2-materializer] owner confirmed PID=' +
        [string]$Truth.ProcessId + '; selection=' +
        [string]$Truth.Selection
    )
    return $Truth
}

function Stop-AndConfirm-L2OwnerReleased {
    $Deadline = [datetime]::UtcNow.AddSeconds(15)
    do {
        $Current = @(Get-RepositoryL2MaterializerProcess)
        $Held = Test-L2OwnerLockHeld
        if ($Held) {
            $Truth = Get-L2OwnerTruth
            if ($null -eq $Truth) {
                Start-Sleep -Milliseconds 50
                continue
            }
            $Current = @(Get-RepositoryL2MaterializerProcess)
            if (@(
                $Current | Where-Object {
                    $_.Kind -eq 'python' -and
                    $_.ProcessId -eq $Truth.ProcessId
                }
            ).Count -ne 1) {
                throw '[l2-materializer] held owner process is not unique.'
            }
        }
        foreach ($Process in $Current) {
            Stop-Process -Id $Process.ProcessId -Force `
                -ErrorAction SilentlyContinue
        }
        if ($Current.Count -eq 0 -and -not $Held) {
            $Suppression = Try-Enter-L2OwnerSuppression
            if ($null -ne $Suppression) {
                return $Suppression
            }
        }
        Start-Sleep -Milliseconds 50
    } while ([datetime]::UtcNow -lt $Deadline)
    throw '[l2-materializer] failed to release singleton owner.'
}

function New-L2LauncherMutex {
    $Descriptor = Get-L2PhysicalPathDescriptor -Path $DataRoot
    $Scope = ([string]$Descriptor.ResolvedPath).ToLowerInvariant()
    $Sha = [System.Security.Cryptography.SHA256]::Create()
    try {
        $Digest = $Sha.ComputeHash(
            [System.Text.Encoding]::UTF8.GetBytes($Scope)
        )
    } finally {
        $Sha.Dispose()
    }
    $Hex = -join ($Digest | ForEach-Object { $_.ToString('x2') })
    return [System.Threading.Mutex]::new(
        $false, ('Global\guvolu-l2-launch-' + $Hex)
    )
}

$L2LauncherMutex = New-L2LauncherMutex
$L2LauncherMutexAcquired = $false
$L2SuppressionLock = $null
try {
    try {
        $L2LauncherMutexAcquired = $L2LauncherMutex.WaitOne(
            [timespan]::FromSeconds(120)
        )
    } catch [System.Threading.AbandonedMutexException] {
        $L2LauncherMutexAcquired = $true
    }
    if (-not $L2LauncherMutexAcquired) {
        throw '[l2-materializer] timed out waiting for launcher mutex.'
    }
    if ($Profile -eq 'ForwardMinimal') {
        $L2SuppressionLock = Stop-AndConfirm-L2OwnerReleased
        Write-Host '[l2-materializer] owner released for ForwardMinimal.'
    } else {
        $L2OwnerTruth = Start-OrConfirm-L2Owner
    }

# Recover only stale crash tails; a fresh checkpoint protects sparse live runs.
& $PythonPath -m guvolu.data.l2_capture --data-root $DataRoot `
    recover --older-minutes 60
& $PythonPath -m guvolu.data.trade_capture --data-root $DataRoot `
    recover --older-minutes 60

if ($Profile -eq 'Full') {
    $L2OwnerTruth = Wait-L2OwnerTruth `
        -ExpectedSelection (Get-ExpectedL2Selection) `
        -TimeoutSeconds 2
}

$Collectors = @(
    @{ Name = 'l2-gmo-btc'; Module = 'guvolu.data.l2_capture'; Runner = 'run_l2_collector.ps1'; Venue = 'gmo'; Symbol = 'BTC'; MaxMiB = 128 },
    @{ Name = 'l2-bitbank-btc-jpy'; Module = 'guvolu.data.l2_capture'; Runner = 'run_l2_collector.ps1'; Venue = 'bitbank'; Symbol = 'btc_jpy'; MaxMiB = 128 },
    @{ Name = 'l2-bitflyer-btc-jpy'; Module = 'guvolu.data.l2_capture'; Runner = 'run_l2_collector.ps1'; Venue = 'bitflyer'; Symbol = 'BTC_JPY'; MaxMiB = 128 },
    @{ Name = 'trade-gmo-btc'; Module = 'guvolu.data.trade_capture'; Runner = 'run_trade_collector.ps1'; Venue = 'gmo'; Symbol = 'BTC'; MaxMiB = 32 },
    @{ Name = 'trade-bitbank-btc-jpy'; Module = 'guvolu.data.trade_capture'; Runner = 'run_trade_collector.ps1'; Venue = 'bitbank'; Symbol = 'btc_jpy'; MaxMiB = 32 },
    @{ Name = 'trade-bitflyer-btc-jpy'; Module = 'guvolu.data.trade_capture'; Runner = 'run_trade_collector.ps1'; Venue = 'bitflyer'; Symbol = 'BTC_JPY'; MaxMiB = 32 }
)

foreach ($Collector in $Collectors) {
    if ($Profile -eq 'Full') {
        $L2OwnerTruth = Wait-L2OwnerTruth `
            -ExpectedSelection (Get-ExpectedL2Selection) `
            -TimeoutSeconds 2
    }
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
    @{ Name = 'trade-realtime-materializer'; Module = 'guvolu.data.trade_realtime_materialize'; Runner = 'run_trade_materializer.ps1' },
    @{ Name = 'book-state-materializer'; Module = 'guvolu.data.book_state_materialize'; Runner = 'run_book_state_materializer.ps1' },
    @{ Name = 'orderflow-tile-watcher'; Module = 'guvolu.data.orderflow_tile_materialize'; Runner = 'run_orderflow_tile_watcher.ps1' },
    @{ Name = 'quality-watcher'; Module = 'guvolu.data.quality_watcher'; Runner = 'run_quality_watcher.ps1' }
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
    if (
        $Materializer.Name -eq 'trade-realtime-materializer' -and
        $null -ne $TradeLatestSealedSegmentsPerStream
    ) {
        $Arguments += (
            ' -LatestSealedSegmentsPerStream ' +
            [string]$TradeLatestSealedSegmentsPerStream
        )
    }
    $Started = Start-Process -FilePath 'powershell.exe' `
        -ArgumentList $Arguments -WorkingDirectory $RepoRoot `
        -WindowStyle $WindowStyle -PassThru
    Write-Host "[$($Materializer.Name)] window started PID=$($Started.Id)"
}

$QueryTail = '-m guvolu.ui.query_service'
$ExistingQuery = @(
    Get-CimInstance Win32_Process -Filter "Name='python.exe'" |
        Where-Object { $_.CommandLine -like "*$QueryTail*" }
)
if ($ExistingQuery.Count -gt 0) {
    Write-Host "[query-service] already running PID=$(($ExistingQuery.ProcessId -join ','))"
} else {
    $QueryRunner = Join-Path $PSScriptRoot 'run_query_service.ps1'
    $QueryArguments = @(
        '-NoProfile', '-ExecutionPolicy', 'Bypass', '-File', $QueryRunner
    )
    if ($WindowStyle -eq 'Normal') {
        $QueryArguments = @('-NoProfile', '-NoExit') +
            $QueryArguments[1..($QueryArguments.Count - 1)]
    }
    $QueryStarted = Start-Process -FilePath 'powershell.exe' `
        -ArgumentList $QueryArguments -WorkingDirectory $RepoRoot `
        -WindowStyle $WindowStyle -PassThru
    Write-Host "[query-service] window started PID=$($QueryStarted.Id)"
}
if ($Profile -eq 'Full') {
    $L2OwnerTruth = Wait-L2OwnerTruth `
        -ExpectedSelection (Get-ExpectedL2Selection) `
        -TimeoutSeconds 2
}
} finally {
    if ($null -ne $L2SuppressionLock) {
        try {
            $L2SuppressionLock.Unlock(0, 1)
        } finally {
            $L2SuppressionLock.Dispose()
        }
    }
    if ($L2LauncherMutexAcquired) {
        $L2LauncherMutex.ReleaseMutex()
    }
    $L2LauncherMutex.Dispose()
}

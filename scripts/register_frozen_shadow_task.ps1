param(
    [Parameter(Mandatory = $true)]
    [ValidatePattern('^frozen-forward-plan-[0-9a-f]{64}$')]
    [string]$PlanId,
    [Parameter(Mandatory = $true)]
    [datetime]$StartUtc,
    [Parameter(Mandatory = $true)]
    [datetime]$EndUtc,
    [Parameter(Mandatory = $true)]
    [string]$RuntimeRoot,
    [Parameter(Mandatory = $true)]
    [string]$ExecutionRepository,
    [Parameter(Mandatory = $true)]
    [string]$Repository,
    [Parameter(Mandatory = $true)]
    [string]$PythonExecutable,
    [Parameter(Mandatory = $true)]
    [string]$GitExecutable,
    [Parameter(Mandatory = $true)]
    [ValidatePattern('^[0-9a-f]{40,64}$')]
    [string]$ExpectedCodeHead,
    [ValidateRange(1, 59)]
    [int]$MinuteOffset = 25,
    [switch]$NoPaper,
    [switch]$DescribeOnly
)

$ErrorActionPreference = "Stop"

function Resolve-FileSystemPath {
    param(
        [Parameter(Mandatory = $true)]
        [string]$LiteralPath,
        [Parameter(Mandatory = $true)]
        [string]$Label,
        [switch]$Leaf
    )

    $Resolved = Resolve-Path -LiteralPath $LiteralPath -ErrorAction Stop
    if ($Resolved.Provider.Name -ne "FileSystem") {
        throw "$Label must use the FileSystem provider: $LiteralPath"
    }
    $Path = [System.IO.Path]::GetFullPath($Resolved.ProviderPath)
    $ExpectedType = if ($Leaf) { "Leaf" } else { "Container" }
    if (-not (Test-Path -LiteralPath $Path -PathType $ExpectedType)) {
        throw "$Label has the wrong path type: $Path"
    }
    return $Path
}

function Test-SamePath {
    param(
        [Parameter(Mandatory = $true)]
        [string]$Left,
        [Parameter(Mandatory = $true)]
        [string]$Right
    )

    return [System.StringComparer]::OrdinalIgnoreCase.Equals(
        $Left.TrimEnd('\', '/'),
        $Right.TrimEnd('\', '/')
    )
}

function Test-OverlappingRoot {
    param(
        [Parameter(Mandatory = $true)]
        [string]$Left,
        [Parameter(Mandatory = $true)]
        [string]$Right
    )

    $LeftRoot = $Left.TrimEnd('\', '/')
    $RightRoot = $Right.TrimEnd('\', '/')
    if (Test-SamePath -Left $LeftRoot -Right $RightRoot) {
        return $true
    }
    $Comparison = [System.StringComparison]::OrdinalIgnoreCase
    return (
        $LeftRoot.StartsWith($RightRoot + [System.IO.Path]::DirectorySeparatorChar, $Comparison) -or
        $RightRoot.StartsWith($LeftRoot + [System.IO.Path]::DirectorySeparatorChar, $Comparison)
    )
}

function Resolve-PhysicalDirectoryPath {
    param(
        [Parameter(Mandatory = $true)]
        [string]$LiteralPath,
        [Parameter(Mandatory = $true)]
        [string]$Label
    )

    $Candidate = Resolve-FileSystemPath -LiteralPath $LiteralPath -Label $Label
    for ($Pass = 0; $Pass -lt 64; $Pass += 1) {
        $Root = [System.IO.Path]::GetPathRoot($Candidate)
        $Current = $Root
        $Parts = $Candidate.Substring($Root.Length).Split(
            [char[]]@('\', '/'),
            [System.StringSplitOptions]::RemoveEmptyEntries
        )
        $Changed = $false
        for ($Index = 0; $Index -lt $Parts.Count; $Index += 1) {
            $Current = Join-Path $Current $Parts[$Index]
            $Item = Get-Item -LiteralPath $Current -Force -ErrorAction Stop
            if (($Item.Attributes -band [System.IO.FileAttributes]::ReparsePoint) -eq 0) {
                continue
            }
            $Targets = @($Item.Target | Where-Object { -not [string]::IsNullOrWhiteSpace($_) })
            if ($Targets.Count -ne 1) {
                throw "$Label has an ambiguous reparse target: $Current"
            }
            $Target = [string]$Targets[0]
            if (-not [System.IO.Path]::IsPathRooted($Target)) {
                $Target = Join-Path $Item.Parent.FullName $Target
            }
            $Candidate = Resolve-FileSystemPath -LiteralPath $Target -Label $Label
            for ($Remaining = $Index + 1; $Remaining -lt $Parts.Count; $Remaining += 1) {
                $Candidate = Join-Path $Candidate $Parts[$Remaining]
            }
            $Candidate = Resolve-FileSystemPath -LiteralPath $Candidate -Label $Label
            $Changed = $true
            break
        }
        if (-not $Changed) {
            return [System.IO.Path]::GetFullPath($Current)
        }
    }
    throw "$Label contains a reparse cycle"
}

function Get-FileSha256 {
    param(
        [Parameter(Mandatory = $true)]
        [string]$LiteralPath
    )

    $Stream = [System.IO.File]::Open(
        $LiteralPath,
        [System.IO.FileMode]::Open,
        [System.IO.FileAccess]::Read,
        [System.IO.FileShare]::Read
    )
    $Hasher = [System.Security.Cryptography.SHA256]::Create()
    try {
        $Hash = $Hasher.ComputeHash($Stream)
        return ([System.BitConverter]::ToString($Hash)).Replace("-", "").ToLowerInvariant()
    } finally {
        $Hasher.Dispose()
        $Stream.Dispose()
    }
}

function Get-VenvTreeManifest {
    param(
        [Parameter(Mandatory = $true)]
        [string]$VenvRoot
    )

    $Root = Resolve-PhysicalDirectoryPath -LiteralPath $VenvRoot -Label "execution venv"
    $Directories = @(Get-ChildItem -LiteralPath $Root -Directory -Recurse -Force)
    foreach ($Directory in $Directories) {
        if (($Directory.Attributes -band [System.IO.FileAttributes]::ReparsePoint) -ne 0) {
            throw "execution venv contains a reparse directory: $($Directory.FullName)"
        }
    }
    $Files = @(Get-ChildItem -LiteralPath $Root -File -Recurse -Force)
    if ($Files.Count -le 0 -or $Files.Count -gt 10000) {
        throw "execution venv file count is outside the governed range: $($Files.Count)"
    }
    $ByRelative = @{}
    $Relatives = [string[]]::new($Files.Count)
    for ($Index = 0; $Index -lt $Files.Count; $Index += 1) {
        $File = $Files[$Index]
        if (($File.Attributes -band [System.IO.FileAttributes]::ReparsePoint) -ne 0) {
            throw "execution venv contains a reparse file: $($File.FullName)"
        }
        $Relative = $File.FullName.Substring($Root.Length).TrimStart('\', '/').Replace('\', '/')
        $Relatives[$Index] = $Relative
        $ByRelative[$Relative] = $File
    }
    [Array]::Sort($Relatives, [System.StringComparer]::Ordinal)
    $Material = [System.IO.MemoryStream]::new()
    $TotalBytes = [int64]0
    try {
        foreach ($Relative in $Relatives) {
            $File = $ByRelative[$Relative]
            $Size = [int64]$File.Length
            $TotalBytes += $Size
            if ($TotalBytes -gt 1000000000) {
                throw "execution venv bytes exceed the governed limit"
            }
            $Sha256 = Get-FileSha256 -LiteralPath $File.FullName
            $Line = "$Relative`0$Size`0$Sha256`n"
            $Bytes = [System.Text.Encoding]::UTF8.GetBytes($Line)
            $Material.Write($Bytes, 0, $Bytes.Length)
        }
        $Material.Position = 0
        $Hasher = [System.Security.Cryptography.SHA256]::Create()
        try {
            $Hash = $Hasher.ComputeHash($Material)
            $TreeSha256 = ([System.BitConverter]::ToString($Hash)).Replace("-", "").ToLowerInvariant()
        } finally {
            $Hasher.Dispose()
        }
    } finally {
        $Material.Dispose()
    }
    return [pscustomobject]@{
        FileCount = $Files.Count
        TotalBytes = $TotalBytes
        TreeSha256 = $TreeSha256
    }
}

function Invoke-CodeGit {
    param(
        [Parameter(Mandatory = $true)]
        [string]$CodeRoot,
        [Parameter(Mandatory = $true)]
        [string]$GitExecutable,
        [Parameter(Mandatory = $true)]
        [string[]]$Arguments,
        [Parameter(Mandatory = $true)]
        [string]$Label,
        [switch]$AllowExitCodeOne
    )

    $SavedGitEnvironment = @{}
    foreach ($Variable in @(Get-ChildItem Env: | Where-Object { $_.Name -like 'GIT_*' })) {
        $SavedGitEnvironment[$Variable.Name] = $Variable.Value
        Remove-Item -LiteralPath ("Env:" + $Variable.Name)
    }
    $env:GIT_CONFIG_NOSYSTEM = "1"
    $env:GIT_CONFIG_GLOBAL = "NUL"
    $env:GIT_CONFIG_SYSTEM = "NUL"
    $env:GIT_CONFIG_COUNT = "0"
    $env:GIT_OPTIONAL_LOCKS = "0"
    $env:GIT_TERMINAL_PROMPT = "0"
    try {
        $Output = @(& $GitExecutable `
            -c core.fsmonitor=false -c core.untrackedCache=false `
            -C $CodeRoot @Arguments 2>&1)
        $ExitCode = $LASTEXITCODE
    } finally {
        foreach ($Variable in @(Get-ChildItem Env: | Where-Object { $_.Name -like 'GIT_*' })) {
            Remove-Item -LiteralPath ("Env:" + $Variable.Name)
        }
        foreach ($Name in $SavedGitEnvironment.Keys) {
            Set-Item -LiteralPath ("Env:" + $Name) -Value $SavedGitEnvironment[$Name]
        }
    }
    $script:ShadowGitExitCode = $ExitCode
    if ($ExitCode -ne 0 -and -not ($AllowExitCodeOne -and $ExitCode -eq 1)) {
        $Detail = (($Output | ForEach-Object { [string]$_ }) -join "`n").Trim()
        throw "$Label failed ($ExitCode): $Detail"
    }
    return (($Output | ForEach-Object { [string]$_ }) -join "`n").Trim()
}

function Assert-CodeCheckout {
    param(
        [Parameter(Mandatory = $true)]
        [string]$CodeRoot,
        [Parameter(Mandatory = $true)]
        [string]$GitExecutable,
        [Parameter(Mandatory = $true)]
        [string]$ExpectedHead
    )

    $TopLevelText = Invoke-CodeGit -CodeRoot $CodeRoot `
        -GitExecutable $GitExecutable `
        -Arguments @("rev-parse", "--show-toplevel") -Label "read code repository root"
    $TopLevel = Resolve-PhysicalDirectoryPath `
        -LiteralPath $TopLevelText -Label "code repository root"
    if (-not (Test-SamePath -Left $CodeRoot -Right $TopLevel)) {
        throw "CodeRoot must exactly equal the Git repository root: $CodeRoot != $TopLevel"
    }
    $ActualHead = Invoke-CodeGit -CodeRoot $CodeRoot `
        -GitExecutable $GitExecutable `
        -Arguments @("rev-parse", "--verify", "HEAD") -Label "read code HEAD"
    if ($ActualHead -cne $ExpectedHead) {
        throw "code HEAD does not match ExpectedCodeHead: $ActualHead != $ExpectedHead"
    }

    $SymbolicOutput = Invoke-CodeGit -CodeRoot $CodeRoot `
        -GitExecutable $GitExecutable `
        -Arguments @("symbolic-ref", "-q", "HEAD") `
        -Label "read code symbolic HEAD" -AllowExitCodeOne
    $SymbolicExitCode = $script:ShadowGitExitCode
    if ($SymbolicExitCode -eq 0) {
        $Branch = (($SymbolicOutput | ForEach-Object { [string]$_ }) -join "`n").Trim()
        throw "CodeRoot must use detached HEAD; currently attached to: $Branch"
    }
    if ($SymbolicExitCode -ne 1) {
        $Detail = (($SymbolicOutput | ForEach-Object { [string]$_ }) -join "`n").Trim()
        throw "could not verify CodeRoot detached HEAD ($SymbolicExitCode): $Detail"
    }

    $TrackedChanges = Invoke-CodeGit -CodeRoot $CodeRoot `
        -GitExecutable $GitExecutable `
        -Arguments @("status", "--porcelain=v1", "--untracked-files=all") `
        -Label "read code repository status"
    if ($TrackedChanges.Length -ne 0) {
        throw "CodeRoot must be completely clean: $TrackedChanges"
    }
    $Ignored = Invoke-CodeGit -CodeRoot $CodeRoot `
        -GitExecutable $GitExecutable `
        -Arguments @(
            "ls-files", "--others", "--ignored", "--exclude-standard",
            "--", "src", "scripts"
        ) -Label "read ignored code paths"
    if ($Ignored.Length -ne 0) {
        throw "CodeRoot code paths contain ignored injection files: $Ignored"
    }
    return $ActualHead
}

function ConvertTo-PowerShellLiteral {
    param(
        [Parameter(Mandatory = $true)]
        [AllowEmptyString()]
        [string]$Value
    )

    return "'" + $Value.Replace("'", "''") + "'"
}

function Get-TextSha256 {
    param(
        [Parameter(Mandatory = $true)]
        [string]$Text
    )

    $Hasher = [System.Security.Cryptography.SHA256]::Create()
    try {
        $Bytes = [System.Text.Encoding]::Unicode.GetBytes($Text)
        $Hash = $Hasher.ComputeHash($Bytes)
        return ([System.BitConverter]::ToString($Hash)).Replace("-", "").ToLowerInvariant()
    } finally {
        $Hasher.Dispose()
    }
}

function ConvertTo-IsoDuration {
    param([AllowNull()][object]$Value)

    if ($null -eq $Value) {
        return "<null>"
    }
    if ($Value -is [TimeSpan]) {
        return [System.Xml.XmlConvert]::ToString([TimeSpan]$Value)
    }
    $Text = [string]$Value
    try {
        return [System.Xml.XmlConvert]::ToString(
            [System.Xml.XmlConvert]::ToTimeSpan($Text)
        )
    } catch {
        return $Text
    }
}

function ConvertTo-LocalSecond {
    param([AllowNull()][object]$Value)

    if ($null -eq $Value) {
        return "<null>"
    }
    try {
        return ([datetime]::Parse([string]$Value)).ToString("yyyy-MM-ddTHH:mm:ss")
    } catch {
        return [string]$Value
    }
}

function Add-ContractMismatch {
    param(
        [Parameter(Mandatory = $true)]
        [AllowEmptyCollection()]
        [System.Collections.Generic.List[string]]$Mismatches,
        [Parameter(Mandatory = $true)]
        [string]$Name,
        [AllowNull()][object]$Expected,
        [AllowNull()][object]$Actual,
        [switch]$IgnoreCase
    )

    $ExpectedText = if ($null -eq $Expected) { "<null>" } else { [string]$Expected }
    $ActualText = if ($null -eq $Actual) { "<null>" } else { [string]$Actual }
    $Equal = if ($IgnoreCase) {
        [System.StringComparer]::OrdinalIgnoreCase.Equals($ExpectedText, $ActualText)
    } else {
        [System.StringComparer]::Ordinal.Equals($ExpectedText, $ActualText)
    }
    if (-not $Equal) {
        $Mismatches.Add("$Name expected=$ExpectedText actual=$ActualText") | Out-Null
    }
}

function Add-BooleanMismatch {
    param(
        [Parameter(Mandatory = $true)]
        [AllowEmptyCollection()]
        [System.Collections.Generic.List[string]]$Mismatches,
        [Parameter(Mandatory = $true)]
        [string]$Name,
        [Parameter(Mandatory = $true)]
        [bool]$Expected,
        [AllowNull()][object]$Actual
    )

    if ($null -eq $Actual -or [bool]$Actual -ne $Expected) {
        $Mismatches.Add("$Name expected=$Expected actual=$Actual") | Out-Null
    }
}

function Get-XmlText {
    param(
        [Parameter(Mandatory = $true)]
        [xml]$Document,
        [Parameter(Mandatory = $true)]
        [System.Xml.XmlNamespaceManager]$NamespaceManager,
        [Parameter(Mandatory = $true)]
        [string]$XPath
    )

    $Node = $Document.SelectSingleNode($XPath, $NamespaceManager)
    if ($null -eq $Node) {
        return $null
    }
    return [string]$Node.InnerText
}

function Test-ExpectedPrincipalIdentity {
    param(
        [AllowNull()][object]$Actual,
        [Parameter(Mandatory = $true)]
        [string]$ExpectedName,
        [Parameter(Mandatory = $true)]
        [string]$ExpectedSid
    )

    if ($null -eq $Actual) {
        return $false
    }
    $Text = [string]$Actual
    return (
        [System.StringComparer]::OrdinalIgnoreCase.Equals($Text, $ExpectedName) -or
        [System.StringComparer]::OrdinalIgnoreCase.Equals($Text, $ExpectedSid)
    )
}

# CodeRoot comes only from this script; Repository denotes the live data root.
$CodeRoot = Resolve-PhysicalDirectoryPath `
    -LiteralPath (Join-Path $PSScriptRoot "..") -Label "CodeRoot"
$DataRoot = Resolve-PhysicalDirectoryPath -LiteralPath $Repository -Label "DataRoot"
if (Test-OverlappingRoot -Left $CodeRoot -Right $DataRoot) {
    throw "CodeRoot and DataRoot must be distinct, non-overlapping roots"
}
$Runtime = Resolve-FileSystemPath -LiteralPath $RuntimeRoot -Label "runtime root"
$Execution = Resolve-FileSystemPath `
    -LiteralPath $ExecutionRepository -Label "execution repository"
$Python = Resolve-FileSystemPath `
    -LiteralPath $PythonExecutable -Label "PythonExecutable" -Leaf
$Git = Resolve-FileSystemPath `
    -LiteralPath $GitExecutable -Label "GitExecutable" -Leaf
$WindowsPowerShell = Resolve-FileSystemPath `
    -LiteralPath (Join-Path $env:SystemRoot `
        "System32\WindowsPowerShell\v1.0\powershell.exe") `
    -Label "System32 Windows PowerShell" -Leaf
$TaskRunner = Resolve-FileSystemPath `
    -LiteralPath (Join-Path $CodeRoot "scripts\run_frozen_shadow_task.ps1") `
    -Label "frozen shadow task wrapper" -Leaf
$ActualCodeHead = Assert-CodeCheckout `
    -CodeRoot $CodeRoot -GitExecutable $Git -ExpectedHead $ExpectedCodeHead
$WrapperSha256 = Get-FileSha256 -LiteralPath $TaskRunner
$PythonSha256 = Get-FileSha256 -LiteralPath $Python
$GitSha256 = Get-FileSha256 -LiteralPath $Git
$PowerShellSha256 = Get-FileSha256 -LiteralPath $WindowsPowerShell
$ExecutionVenvManifest = Get-VenvTreeManifest `
    -VenvRoot (Join-Path $Execution ".venv")

$Start = $StartUtc.ToUniversalTime()
$End = $EndUtc.ToUniversalTime()
if ($Start -ge $End) {
    throw "task interval must satisfy StartUtc < EndUtc"
}
if (($Start.Ticks % [TimeSpan]::TicksPerHour) -ne 0) {
    throw "StartUtc must align to an exact UTC hour"
}
$FirstRunUtc = $Start.AddMinutes($MinuteOffset)
$Duration = $End - $FirstRunUtc
if ($Duration.TotalHours -lt 1) {
    throw "task interval is shorter than one hour"
}
$LocalZone = [System.TimeZoneInfo]::Local
$FirstRunLocal = [System.TimeZoneInfo]::ConvertTimeFromUtc($FirstRunUtc, $LocalZone)
$TaskSuffix = $PlanId.Substring([Math]::Max(0, $PlanId.Length - 12))
$TaskName = "guvolu-frozen-forward-$TaskSuffix"

$WrapperInvocation = @(
    "& " + (ConvertTo-PowerShellLiteral $TaskRunner),
    "-PlanId " + (ConvertTo-PowerShellLiteral $PlanId),
    "-Repository " + (ConvertTo-PowerShellLiteral $DataRoot),
    "-RuntimeRoot " + (ConvertTo-PowerShellLiteral $Runtime),
    "-ExecutionRepository " + (ConvertTo-PowerShellLiteral $Execution),
    "-PythonExecutable " + (ConvertTo-PowerShellLiteral $Python),
    "-ExpectedPythonSha256 " + (ConvertTo-PowerShellLiteral $PythonSha256),
    "-GitExecutable " + (ConvertTo-PowerShellLiteral $Git),
    "-ExpectedGitSha256 " + (ConvertTo-PowerShellLiteral $GitSha256),
    "-ExpectedExecutionEnvironmentTreeSha256 " + (
        ConvertTo-PowerShellLiteral $ExecutionVenvManifest.TreeSha256
    ),
    "-ExpectedCodeHead " + (ConvertTo-PowerShellLiteral $ExpectedCodeHead),
    "-ExpectedWrapperSha256 " + (ConvertTo-PowerShellLiteral $WrapperSha256)
) -join " "
$WrapperInvocation += " -NoPaper"
$Bootstrap = @(
    '$ErrorActionPreference = ''Stop''',
    '$wrapper = ' + (ConvertTo-PowerShellLiteral $TaskRunner),
    '$expectedWrapperSha256 = ' + (ConvertTo-PowerShellLiteral $WrapperSha256),
    'if (-not (Test-Path -LiteralPath $wrapper -PathType Leaf)) { ' +
        'throw "frozen shadow task wrapper does not exist: $wrapper" }',
    '$stream = [System.IO.File]::Open($wrapper, ' +
        '[System.IO.FileMode]::Open, [System.IO.FileAccess]::Read, ' +
        '[System.IO.FileShare]::Read)',
    '$hasher = [System.Security.Cryptography.SHA256]::Create()',
    '$wrapperExitCode = 3',
    'try {',
    '    $hash = $hasher.ComputeHash($stream)',
    '    $actualWrapperSha256 = ([System.BitConverter]::ToString($hash)).' +
        'Replace("-", "").ToLowerInvariant()',
    '    if ($actualWrapperSha256 -cne $expectedWrapperSha256) { ' +
        'throw "frozen shadow task wrapper SHA256 mismatch" }',
    '    ' + $WrapperInvocation,
    '    $wrapperExitCode = $LASTEXITCODE',
    '} finally { $hasher.Dispose(); $stream.Dispose() }',
    'exit [int]$wrapperExitCode'
) -join "`r`n"
$EncodedBootstrap = [Convert]::ToBase64String(
    [System.Text.Encoding]::Unicode.GetBytes($Bootstrap)
)
$ActionArguments = (
    '-NoProfile -NonInteractive -ExecutionPolicy Bypass -WindowStyle Hidden ' +
    "-EncodedCommand $EncodedBootstrap"
)

$Identity = [System.Security.Principal.WindowsIdentity]::GetCurrent()
$PrincipalUserId = $Identity.Name
$PrincipalSid = $Identity.User.Value
$Definition = [ordered]@{
    task_name = $TaskName
    execute = $WindowsPowerShell
    powershell_sha256 = $PowerShellSha256
    arguments = $ActionArguments
    working_directory = $CodeRoot
    task_runner = $TaskRunner
    wrapper_sha256 = $WrapperSha256
    bootstrap_sha256 = Get-TextSha256 -Text $Bootstrap
    code_root = $CodeRoot
    data_root = $DataRoot
    runtime_root = $Runtime
    execution_repository = $Execution
    python_executable = $Python
    python_sha256 = $PythonSha256
    git_executable = $Git
    git_sha256 = $GitSha256
    execution_environment_tree_sha256 = $ExecutionVenvManifest.TreeSha256
    execution_environment_file_count = $ExecutionVenvManifest.FileCount
    execution_environment_total_bytes = $ExecutionVenvManifest.TotalBytes
    python_base_runtime_attestation = "unbound-partial"
    paper_fill_cost_provenance = "unbound"
    paper_capable = $false
    expected_code_head = $ExpectedCodeHead
    actual_code_head = $ActualCodeHead
    first_run_local = $FirstRunLocal.ToString("o")
    end_utc = $End.ToString("o")
    repetition_interval = "PT1H"
    repetition_duration = ConvertTo-IsoDuration $Duration
    no_paper = $true
    enabled = $false
    minute_offset = $MinuteOffset
    multiple_instances = "IgnoreNew"
    start_when_available = $true
    allow_start_on_batteries = $true
    dont_stop_if_going_on_batteries = $true
    wake_to_run = $true
    hidden = $true
    execution_time_limit_minutes = 45
    restart_count = 3
    restart_interval_minutes = 5
    principal_user_id = $PrincipalUserId
    principal_logon_type = "Interactive"
    principal_run_level = "Limited"
    unattended_coverage_capable = $false
    coverage_limit = (
        "Interactive logon covers only periods with an active user session; " +
        "the registrar stores no credentials, enables no task, and claims no unattended coverage."
    )
}
if ($DescribeOnly) {
    [pscustomobject]$Definition | ConvertTo-Json -Compress
    exit 0
}

# No ScheduledTasks cmdlet may run above this DescribeOnly boundary.
$Existing = Get-ScheduledTask -TaskName $TaskName -ErrorAction SilentlyContinue
if ($null -ne $Existing -and (
    $Existing.Settings.Enabled -ne $false -or
    [string]$Existing.State -ne "Disabled"
)) {
    throw "refusing to replace a task not in Disabled/Enabled=False state: $TaskName"
}

$Action = New-ScheduledTaskAction -Execute $Definition.execute `
    -Argument $Definition.arguments -WorkingDirectory $Definition.working_directory
$Trigger = New-ScheduledTaskTrigger -Once -At $FirstRunLocal `
    -RepetitionInterval (New-TimeSpan -Hours 1) `
    -RepetitionDuration $Duration
$Principal = New-ScheduledTaskPrincipal -UserId $PrincipalUserId `
    -LogonType Interactive -RunLevel Limited
$Settings = New-ScheduledTaskSettingsSet -MultipleInstances IgnoreNew `
    -StartWhenAvailable -AllowStartIfOnBatteries `
    -DontStopIfGoingOnBatteries -WakeToRun `
    -ExecutionTimeLimit (New-TimeSpan -Minutes 45) `
    -RestartCount $Definition.restart_count `
    -RestartInterval (New-TimeSpan -Minutes $Definition.restart_interval_minutes) `
    -Hidden -Disable

$RegistrationAttempted = $false
try {
    # Narrow the pre-registration drift window; every task run checks again.
    $null = Assert-CodeCheckout `
        -CodeRoot $CodeRoot -GitExecutable $Git -ExpectedHead $ExpectedCodeHead
    $PreRegisterWrapperSha256 = Get-FileSha256 -LiteralPath $TaskRunner
    if ($PreRegisterWrapperSha256 -cne $WrapperSha256) {
        throw "task wrapper SHA256 drifted before registration"
    }
    if ((Get-FileSha256 -LiteralPath $Python) -cne $PythonSha256) {
        throw "PythonExecutable drifted before registration"
    }
    if ((Get-FileSha256 -LiteralPath $Git) -cne $GitSha256) {
        throw "GitExecutable drifted before registration"
    }
    if ((Get-FileSha256 -LiteralPath $WindowsPowerShell) -cne $PowerShellSha256) {
        throw "System32 Windows PowerShell drifted before registration"
    }
    $PreRegisterEnvironment = Get-VenvTreeManifest `
        -VenvRoot (Join-Path $Execution ".venv")
    if ($PreRegisterEnvironment.TreeSha256 -cne $ExecutionVenvManifest.TreeSha256) {
        throw "execution venv drifted before registration"
    }

    $RegistrationAttempted = $true
    Register-ScheduledTask -TaskName $TaskName -Action $Action -Trigger $Trigger `
        -Principal $Principal -Settings $Settings -Force | Out-Null

    $Registered = Get-ScheduledTask -TaskName $TaskName -ErrorAction Stop
    $ExportText = [string](
        Export-ScheduledTask -TaskName $TaskName -ErrorAction Stop
    )
    [xml]$ExportXml = $ExportText
    $NamespaceUri = $ExportXml.DocumentElement.NamespaceURI
    if ([string]::IsNullOrWhiteSpace($NamespaceUri)) {
        throw "exported task XML lacks a namespace"
    }
    $NamespaceManager = New-Object System.Xml.XmlNamespaceManager(
        $ExportXml.NameTable
    )
    $NamespaceManager.AddNamespace("t", $NamespaceUri)

    $Mismatches = [System.Collections.Generic.List[string]]::new()
    Add-ContractMismatch $Mismatches "TaskName" $TaskName $Registered.TaskName
    Add-ContractMismatch $Mismatches "State" "Disabled" $Registered.State
    Add-BooleanMismatch $Mismatches "Settings.Enabled" $false `
        $Registered.Settings.Enabled

    $Actions = @($Registered.Actions)
    Add-ContractMismatch $Mismatches "Actions.Count" 1 $Actions.Count
    if ($Actions.Count -eq 1) {
        Add-ContractMismatch $Mismatches "Action.Execute" `
            $Definition.execute $Actions[0].Execute
        Add-ContractMismatch $Mismatches "Action.Arguments" `
            $Definition.arguments $Actions[0].Arguments
        Add-ContractMismatch $Mismatches "Action.WorkingDirectory" `
            $Definition.working_directory $Actions[0].WorkingDirectory -IgnoreCase
    }

    $Triggers = @($Registered.Triggers)
    Add-ContractMismatch $Mismatches "Triggers.Count" 1 $Triggers.Count
    if ($Triggers.Count -eq 1) {
        Add-ContractMismatch $Mismatches "Trigger.StartBoundary" `
            (ConvertTo-LocalSecond $FirstRunLocal) `
            (ConvertTo-LocalSecond $Triggers[0].StartBoundary)
        Add-ContractMismatch $Mismatches "Trigger.Repetition.Interval" "PT1H" `
            (ConvertTo-IsoDuration $Triggers[0].Repetition.Interval)
        Add-ContractMismatch $Mismatches "Trigger.Repetition.Duration" `
            (ConvertTo-IsoDuration $Duration) `
            (ConvertTo-IsoDuration $Triggers[0].Repetition.Duration)
        Add-BooleanMismatch $Mismatches "Trigger.Enabled" $true $Triggers[0].Enabled
    }

    if (-not (Test-ExpectedPrincipalIdentity -Actual $Registered.Principal.UserId `
        -ExpectedName $PrincipalUserId -ExpectedSid $PrincipalSid)) {
        $Mismatches.Add(
            "Principal.UserId is not the expected Windows identity: $($Registered.Principal.UserId)"
        ) | Out-Null
    }
    Add-ContractMismatch $Mismatches "Principal.LogonType" "Interactive" `
        $Registered.Principal.LogonType -IgnoreCase
    Add-ContractMismatch $Mismatches "Principal.RunLevel" "Limited" `
        $Registered.Principal.RunLevel -IgnoreCase

    Add-ContractMismatch $Mismatches "Settings.MultipleInstances" "IgnoreNew" `
        $Registered.Settings.MultipleInstances -IgnoreCase
    Add-BooleanMismatch $Mismatches "Settings.StartWhenAvailable" $true `
        $Registered.Settings.StartWhenAvailable
    Add-BooleanMismatch $Mismatches "Settings.DisallowStartIfOnBatteries" $false `
        $Registered.Settings.DisallowStartIfOnBatteries
    Add-BooleanMismatch $Mismatches "Settings.StopIfGoingOnBatteries" $false `
        $Registered.Settings.StopIfGoingOnBatteries
    Add-BooleanMismatch $Mismatches "Settings.WakeToRun" $true `
        $Registered.Settings.WakeToRun
    Add-BooleanMismatch $Mismatches "Settings.Hidden" $true `
        $Registered.Settings.Hidden
    Add-ContractMismatch $Mismatches "Settings.ExecutionTimeLimit" "PT45M" `
        (ConvertTo-IsoDuration $Registered.Settings.ExecutionTimeLimit)
    Add-ContractMismatch $Mismatches "Settings.RestartCount" 3 `
        $Registered.Settings.RestartCount
    Add-ContractMismatch $Mismatches "Settings.RestartInterval" "PT5M" `
        (ConvertTo-IsoDuration $Registered.Settings.RestartInterval)

    $XmlActions = @($ExportXml.SelectNodes(
        "/t:Task/t:Actions/t:Exec", $NamespaceManager
    ))
    $XmlTriggers = @($ExportXml.SelectNodes(
        "/t:Task/t:Triggers/t:TimeTrigger", $NamespaceManager
    ))
    Add-ContractMismatch $Mismatches "XML Actions.Count" 1 $XmlActions.Count
    Add-ContractMismatch $Mismatches "XML Triggers.Count" 1 $XmlTriggers.Count
    Add-ContractMismatch $Mismatches "XML Action.Command" $Definition.execute `
        (Get-XmlText $ExportXml $NamespaceManager "/t:Task/t:Actions/t:Exec/t:Command")
    Add-ContractMismatch $Mismatches "XML Action.Arguments" $Definition.arguments `
        (Get-XmlText $ExportXml $NamespaceManager "/t:Task/t:Actions/t:Exec/t:Arguments")
    Add-ContractMismatch $Mismatches "XML Action.WorkingDirectory" `
        $Definition.working_directory `
        (Get-XmlText $ExportXml $NamespaceManager "/t:Task/t:Actions/t:Exec/t:WorkingDirectory") `
        -IgnoreCase
    Add-ContractMismatch $Mismatches "XML Trigger.StartBoundary" `
        (ConvertTo-LocalSecond $FirstRunLocal) `
        (ConvertTo-LocalSecond (Get-XmlText $ExportXml $NamespaceManager `
            "/t:Task/t:Triggers/t:TimeTrigger/t:StartBoundary"))
    Add-ContractMismatch $Mismatches "XML Trigger.Enabled" "true" `
        (Get-XmlText $ExportXml $NamespaceManager `
            "/t:Task/t:Triggers/t:TimeTrigger/t:Enabled") -IgnoreCase
    Add-ContractMismatch $Mismatches "XML Trigger.Repetition.Interval" "PT1H" `
        (ConvertTo-IsoDuration (Get-XmlText $ExportXml $NamespaceManager `
            "/t:Task/t:Triggers/t:TimeTrigger/t:Repetition/t:Interval"))
    Add-ContractMismatch $Mismatches "XML Trigger.Repetition.Duration" `
        (ConvertTo-IsoDuration $Duration) `
        (ConvertTo-IsoDuration (Get-XmlText $ExportXml $NamespaceManager `
            "/t:Task/t:Triggers/t:TimeTrigger/t:Repetition/t:Duration"))

    $XmlPrincipalUser = Get-XmlText $ExportXml $NamespaceManager `
        "/t:Task/t:Principals/t:Principal/t:UserId"
    if (-not (Test-ExpectedPrincipalIdentity -Actual $XmlPrincipalUser `
        -ExpectedName $PrincipalUserId -ExpectedSid $PrincipalSid)) {
        $Mismatches.Add(
            "XML Principal.UserId is not the expected Windows identity: $XmlPrincipalUser"
        ) | Out-Null
    }
    Add-ContractMismatch $Mismatches "XML Principal.LogonType" "InteractiveToken" `
        (Get-XmlText $ExportXml $NamespaceManager `
            "/t:Task/t:Principals/t:Principal/t:LogonType") -IgnoreCase
    Add-ContractMismatch $Mismatches "XML Principal.RunLevel" "LeastPrivilege" `
        (Get-XmlText $ExportXml $NamespaceManager `
            "/t:Task/t:Principals/t:Principal/t:RunLevel") -IgnoreCase

    $XmlSettings = [ordered]@{
        "MultipleInstancesPolicy" = "IgnoreNew"
        "StartWhenAvailable" = "true"
        "DisallowStartIfOnBatteries" = "false"
        "StopIfGoingOnBatteries" = "false"
        "WakeToRun" = "true"
        "Enabled" = "false"
        "Hidden" = "true"
        "ExecutionTimeLimit" = "PT45M"
    }
    foreach ($SettingName in $XmlSettings.Keys) {
        Add-ContractMismatch $Mismatches "XML Settings.$SettingName" `
            $XmlSettings[$SettingName] `
            (Get-XmlText $ExportXml $NamespaceManager `
                "/t:Task/t:Settings/t:$SettingName") -IgnoreCase
    }
    Add-ContractMismatch $Mismatches "XML Settings.RestartOnFailure.Interval" `
        "PT5M" (Get-XmlText $ExportXml $NamespaceManager `
            "/t:Task/t:Settings/t:RestartOnFailure/t:Interval")
    Add-ContractMismatch $Mismatches "XML Settings.RestartOnFailure.Count" `
        "3" (Get-XmlText $ExportXml $NamespaceManager `
            "/t:Task/t:Settings/t:RestartOnFailure/t:Count")

    $null = Assert-CodeCheckout `
        -CodeRoot $CodeRoot -GitExecutable $Git -ExpectedHead $ExpectedCodeHead
    $PostReadbackWrapperSha256 = Get-FileSha256 -LiteralPath $TaskRunner
    Add-ContractMismatch $Mismatches "PostReadback.WrapperSha256" `
        $WrapperSha256 $PostReadbackWrapperSha256
    Add-ContractMismatch $Mismatches "PostReadback.PythonSha256" `
        $PythonSha256 (Get-FileSha256 -LiteralPath $Python)
    Add-ContractMismatch $Mismatches "PostReadback.GitSha256" `
        $GitSha256 (Get-FileSha256 -LiteralPath $Git)
    Add-ContractMismatch $Mismatches "PostReadback.PowerShellSha256" `
        $PowerShellSha256 (Get-FileSha256 -LiteralPath $WindowsPowerShell)
    $PostReadbackEnvironment = Get-VenvTreeManifest `
        -VenvRoot (Join-Path $Execution ".venv")
    Add-ContractMismatch $Mismatches "PostReadback.ExecutionVenvTreeSha256" `
        $ExecutionVenvManifest.TreeSha256 $PostReadbackEnvironment.TreeSha256

    if ($Mismatches.Count -ne 0) {
        throw "post-registration task contract mismatch: $($Mismatches -join '; ')"
    }

    [pscustomobject]@{
        TaskName = $Registered.TaskName
        State = [string]$Registered.State
        Enabled = [bool]$Registered.Settings.Enabled
        Verified = $true
        FirstRunLocal = $FirstRunLocal
        EndUtc = $End.ToString("o")
        ExportXmlSha256 = Get-TextSha256 -Text $ExportText
    }
} catch {
    $RegistrationFailure = $_
    if ($RegistrationAttempted) {
        # Attempt both compensations and preserve the original contract failure.
        try {
            Disable-ScheduledTask -TaskName $TaskName -ErrorAction Stop | Out-Null
        } catch {
            Write-Warning "post-registration Disable cleanup failed: $($_.Exception.Message)"
        }
        try {
            Stop-ScheduledTask -TaskName $TaskName -ErrorAction Stop | Out-Null
        } catch {
            Write-Warning "post-registration Stop cleanup failed: $($_.Exception.Message)"
        }
    }
    throw $RegistrationFailure
}

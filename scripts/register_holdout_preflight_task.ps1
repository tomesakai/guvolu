param(
    [Parameter(Mandatory = $true)]
    [string]$Repository,
    [Parameter(Mandatory = $true)]
    [string]$RuntimeRoot,
    [Parameter(Mandatory = $true)]
    [string]$PythonExecutable,
    [Parameter(Mandatory = $true)]
    [string]$GitExecutable,
    [Parameter(Mandatory = $true)]
    [ValidatePattern('^(?:[0-9a-f]{40}|[0-9a-f]{64})$')]
    [string]$ExpectedCodeHead,
    [string]$VintageId = "",
    [ValidatePattern('^(?:[01][0-9]|2[0-3]):[0-5][0-9]$')]
    [string]$DailyAt = "09:35",
    [switch]$DescribeOnly
)

Set-StrictMode -Version Latest
$ErrorActionPreference = "Stop"

function Get-NormalizedPath {
    param(
        [Parameter(Mandatory = $true)][string]$Path,
        [Parameter(Mandatory = $true)][string]$Description,
        [switch]$Leaf
    )
    $Resolved = (Resolve-Path -LiteralPath $Path -ErrorAction Stop).Path
    $ExpectedType = if ($Leaf) { "Leaf" } else { "Container" }
    if (-not (Test-Path -LiteralPath $Resolved -PathType $ExpectedType)) {
        throw "$Description is not a $ExpectedType path: $Path"
    }
    return [System.IO.Path]::GetFullPath($Resolved).TrimEnd(
        [System.IO.Path]::DirectorySeparatorChar,
        [System.IO.Path]::AltDirectorySeparatorChar
    )
}

function Test-PathOverlap {
    param([string]$Left, [string]$Right)
    $Separator = [string][System.IO.Path]::DirectorySeparatorChar
    $LeftWithSeparator = $Left.TrimEnd('\', '/') + $Separator
    $RightWithSeparator = $Right.TrimEnd('\', '/') + $Separator
    return (
        $Left.Equals($Right, [System.StringComparison]::OrdinalIgnoreCase) -or
        $LeftWithSeparator.StartsWith(
            $RightWithSeparator,
            [System.StringComparison]::OrdinalIgnoreCase
        ) -or
        $RightWithSeparator.StartsWith(
            $LeftWithSeparator,
            [System.StringComparison]::OrdinalIgnoreCase
        )
    )
}

function Assert-NoReparsePath {
    param(
        [Parameter(Mandatory = $true)][string]$Path,
        [Parameter(Mandatory = $true)][string]$Description
    )
    $FullPath = [System.IO.Path]::GetFullPath($Path)
    if ($FullPath -match '(?:^|[\\/])[^\\/]*~[0-9]+(?:[\\/]|$)') {
        throw "$Description must not use a DOS short-path alias: $Path"
    }
    $Current = [System.IO.Path]::GetPathRoot($FullPath)
    $Relative = $FullPath.Substring($Current.Length)
    foreach ($Segment in $Relative.Split(@('\', '/'), `
        [System.StringSplitOptions]::RemoveEmptyEntries)) {
        $Current = Join-Path $Current $Segment
        $Item = Get-Item -LiteralPath $Current -Force -ErrorAction Stop
        if (($Item.Attributes -band `
            [System.IO.FileAttributes]::ReparsePoint) -ne 0) {
            throw "$Description must not traverse a reparse point: $Current"
        }
    }
}

function Assert-OrdinaryFile {
    param(
        [Parameter(Mandatory = $true)][string]$Path,
        [Parameter(Mandatory = $true)][string]$Description,
        [switch]$SingleLink
    )
    $Item = Get-Item -LiteralPath $Path -Force -ErrorAction Stop
    if ($Item.PSIsContainer) {
        throw "$Description is not a regular file: $Path"
    }
    if (($Item.Attributes -band [System.IO.FileAttributes]::ReparsePoint) -ne 0) {
        throw "$Description must not be a reparse point: $Path"
    }
    if ($SingleLink) {
        $Fsutil = Join-Path $env:SystemRoot "System32\fsutil.exe"
        $HardLinks = @(& $Fsutil hardlink list $Path 2>&1)
        $FsutilExitCode = $LASTEXITCODE
        if ($FsutilExitCode -ne 0) {
            throw "$Description hard-link inspection failed ($FsutilExitCode): $Path"
        }
        $LinkPaths = @($HardLinks | Where-Object {
            ([string]$_).Trim().StartsWith("\")
        })
        if ($LinkPaths.Count -ne 1) {
            throw "$Description must have exactly one hard link: $Path"
        }
    }
}

function Get-FileSha256 {
    param([Parameter(Mandatory = $true)][string]$Path)
    $Stream = [System.IO.File]::Open(
        $Path,
        [System.IO.FileMode]::Open,
        [System.IO.FileAccess]::Read,
        [System.IO.FileShare]::Read
    )
    $Hasher = [System.Security.Cryptography.SHA256]::Create()
    try {
        return ([System.BitConverter]::ToString(
            $Hasher.ComputeHash($Stream)
        ) -replace '-', '').ToLowerInvariant()
    } finally {
        $Hasher.Dispose()
        $Stream.Dispose()
    }
}

function Assert-NoCodeStartupInjection {
    param(
        [Parameter(Mandatory = $true)][string]$Root,
        [Parameter(Mandatory = $true)][string]$Description
    )
    if (-not (Test-Path -LiteralPath $Root -PathType Container)) { return }
    foreach ($Item in @(Get-ChildItem -LiteralPath $Root -Recurse -Force)) {
        $LowerName = $Item.Name.ToLowerInvariant()
        if (
            ($Item.PSIsContainer -and $LowerName -eq "__pycache__") -or
            (-not $Item.PSIsContainer -and (
                $Item.Extension.ToLowerInvariant() -in @(".pyc", ".pyo") -or
                $LowerName -in @("sitecustomize.py", "usercustomize.py")
            ))
        ) {
            throw "$Description contains prohibited Python startup/cache injection: $($Item.FullName)"
        }
    }
}

function Assert-NoVenvStartupCustomizer {
    param([Parameter(Mandatory = $true)][string]$SitePackages)
    foreach ($Name in @("sitecustomize.py", "usercustomize.py")) {
        $Candidate = Join-Path $SitePackages $Name
        if (Test-Path -LiteralPath $Candidate) {
            throw "venv site-packages contains a prohibited startup customizer: $Candidate"
        }
    }
}

function Assert-IsolatedPyVenvConfig {
    param([Parameter(Mandatory = $true)][string]$Path)
    $Values = @{}
    foreach ($Line in [System.IO.File]::ReadAllLines($Path)) {
        if ($Line -match '^\s*([^#=]+?)\s*=\s*(.*?)\s*$') {
            $Values[$Matches[1].Trim().ToLowerInvariant()] = $Matches[2].Trim()
        }
    }
    if (-not $Values.ContainsKey("home") -or
        [string]::IsNullOrWhiteSpace([string]$Values["home"])) {
        throw "pyvenv.cfg must bind a non-empty base Python home"
    }
    if (-not $Values.ContainsKey("include-system-site-packages") -or
        [string]$Values["include-system-site-packages"] -ine "false") {
        throw "pyvenv.cfg must set include-system-site-packages=false"
    }
}

function Get-PyVenvHome {
    param([Parameter(Mandatory = $true)][string]$Path)
    Assert-IsolatedPyVenvConfig -Path $Path
    foreach ($Line in [System.IO.File]::ReadAllLines($Path)) {
        if ($Line -match '^\s*home\s*=\s*(.*?)\s*$') {
            $BaseHomeValue = $Matches[1].Trim()
            if (-not [System.IO.Path]::IsPathRooted($BaseHomeValue)) {
                throw "pyvenv.cfg home must be absolute"
            }
            return $BaseHomeValue
        }
    }
    throw "pyvenv.cfg home is missing"
}

function Assert-NoPythonPathConfig {
    param(
        [Parameter(Mandatory = $true)][string]$Root,
        [Parameter(Mandatory = $true)][string]$Description
    )
    $PathConfigs = @(Get-ChildItem -LiteralPath $Root -Recurse -Force `
        -File -Filter "*._pth")
    if ($PathConfigs.Count -ne 0) {
        throw "$Description contains prohibited Python ._pth startup config: $($PathConfigs[0].FullName)"
    }
}

function Get-BoundTreeIdentity {
    param(
        [Parameter(Mandatory = $true)][string]$Root,
        [Parameter(Mandatory = $true)][string]$Description
    )
    Assert-NoReparsePath -Path $Root -Description $Description
    $RootPrefix = $Root.TrimEnd('\', '/') + `
        [System.IO.Path]::DirectorySeparatorChar
    $Items = @(Get-ChildItem -LiteralPath $Root -Recurse -Force)
    foreach ($Directory in @($Items | Where-Object { $_.PSIsContainer })) {
        if (($Directory.Attributes -band `
            [System.IO.FileAttributes]::ReparsePoint) -ne 0) {
            throw "$Description contains a reparse directory: $($Directory.FullName)"
        }
    }
    $FilesByRelative = @{}
    foreach ($File in @($Items | Where-Object { -not $_.PSIsContainer })) {
        if (($File.Attributes -band `
            [System.IO.FileAttributes]::ReparsePoint) -ne 0) {
            throw "$Description contains a reparse file: $($File.FullName)"
        }
        $Relative = $File.FullName.Substring($RootPrefix.Length).Replace('\', '/')
        if (-not $Relative -or $Relative.Contains("`0")) {
            throw "$Description contains a non-canonical manifest path"
        }
        $FilesByRelative[$Relative] = $File.FullName
    }
    if ($FilesByRelative.Count -gt 100000) {
        throw "$Description manifest exceeds 100000 files"
    }
    $RelativePaths = [string[]]@($FilesByRelative.Keys)
    [Array]::Sort($RelativePaths, [System.StringComparer]::Ordinal)
    $Builder = New-Object System.Text.StringBuilder
    [long]$TotalBytes = 0
    foreach ($Relative in $RelativePaths) {
        $Path = [string]$FilesByRelative[$Relative]
        $Stream = [System.IO.File]::Open(
            $Path,
            [System.IO.FileMode]::Open,
            [System.IO.FileAccess]::Read,
            [System.IO.FileShare]::Read
        )
        try {
            $Length = $Stream.Length
            $TotalBytes += $Length
            if ($TotalBytes -gt 17179869184) {
                throw "$Description manifest exceeds 16 GiB"
            }
            $Hasher = [System.Security.Cryptography.SHA256]::Create()
            try {
                $Digest = ([System.BitConverter]::ToString(
                    $Hasher.ComputeHash($Stream)
                ) -replace '-', '').ToLowerInvariant()
            } finally {
                $Hasher.Dispose()
            }
        } finally {
            $Stream.Dispose()
        }
        [void]$Builder.Append($Relative).Append("`0").Append(
            $Length.ToString([System.Globalization.CultureInfo]::InvariantCulture)
        ).Append("`0").Append($Digest).Append("`n")
    }
    $ManifestBytes = (New-Object System.Text.UTF8Encoding($false)).GetBytes(
        $Builder.ToString()
    )
    $ManifestHasher = [System.Security.Cryptography.SHA256]::Create()
    try {
        $TreeSha256 = ([System.BitConverter]::ToString(
            $ManifestHasher.ComputeHash($ManifestBytes)
        ) -replace '-', '').ToLowerInvariant()
    } finally {
        $ManifestHasher.Dispose()
    }
    return [pscustomobject][ordered]@{
        root = $Root
        file_count = $RelativePaths.Count
        total_bytes = $TotalBytes
        tree_sha256 = $TreeSha256
    }
}

function Invoke-GitReadOnly {
    param(
        [Parameter(Mandatory = $true)][string]$CodeRoot,
        [Parameter(Mandatory = $true)][string[]]$Arguments,
        [int[]]$AllowedExitCodes = @(0)
    )
    $GitEnvironment = [ordered]@{
        GIT_OPTIONAL_LOCKS = "0"
        GIT_TERMINAL_PROMPT = "0"
        GIT_CONFIG_NOSYSTEM = "1"
        GIT_CONFIG_GLOBAL = "NUL"
        GIT_CONFIG_SYSTEM = "NUL"
        GIT_DIR = $null
        GIT_WORK_TREE = $null
        GIT_INDEX_FILE = $null
        GIT_OBJECT_DIRECTORY = $null
        GIT_ALTERNATE_OBJECT_DIRECTORIES = $null
        GIT_EXEC_PATH = $null
        GIT_EXTERNAL_DIFF = $null
        GIT_DIFF_OPTS = $null
        GIT_PAGER = $null
    }
    $PreviousEnvironment = @{}
    try {
        foreach ($Name in $GitEnvironment.Keys) {
            $Exists = Test-Path "Env:$Name"
            $PreviousEnvironment[$Name] = @($Exists, [Environment]::GetEnvironmentVariable($Name))
            $Value = $GitEnvironment[$Name]
            if ($null -eq $Value) {
                Remove-Item "Env:$Name" -ErrorAction SilentlyContinue
            } else {
                [Environment]::SetEnvironmentVariable($Name, [string]$Value)
            }
        }
        $Output = @(& $script:GitExecutablePath --no-optional-locks `
            -c core.fsmonitor=false -c core.untrackedCache=false `
            -c core.hooksPath=NUL -c diff.external= `
            -C $CodeRoot @Arguments 2>&1)
        $ExitCode = $LASTEXITCODE
    } finally {
        foreach ($Name in $GitEnvironment.Keys) {
            $Prior = $PreviousEnvironment[$Name]
            if ([bool]$Prior[0]) {
                [Environment]::SetEnvironmentVariable($Name, [string]$Prior[1])
            } else {
                Remove-Item "Env:$Name" -ErrorAction SilentlyContinue
            }
        }
    }
    if ($AllowedExitCodes -notcontains $ExitCode) {
        throw "git $($Arguments -join ' ') failed ($ExitCode): $($Output -join "`n")"
    }
    return [pscustomobject]@{ ExitCode = $ExitCode; Output = $Output }
}

function Assert-TrackedFile {
    param([string]$CodeRoot, [string]$RelativePath)
    $Result = Invoke-GitReadOnly -CodeRoot $CodeRoot -Arguments @(
        "ls-files", "--error-unmatch", "--", $RelativePath
    )
    if ($Result.Output.Count -ne 1) {
        throw "code file is not uniquely tracked: $RelativePath"
    }
}

function Assert-CodeRoot {
    param([string]$CodeRoot, [string]$ExpectedHead)
    $TopResult = Invoke-GitReadOnly -CodeRoot $CodeRoot -Arguments @(
        "rev-parse", "--show-toplevel"
    )
    if ($TopResult.Output.Count -ne 1) {
        throw "code root did not resolve to one git top-level: $CodeRoot"
    }
    $TopLevel = [System.IO.Path]::GetFullPath(
        ([string]$TopResult.Output[0]).Trim()
    ).TrimEnd('\', '/')
    if (-not $TopLevel.Equals(
        $CodeRoot, [System.StringComparison]::OrdinalIgnoreCase
    )) {
        throw "CodeRoot must be the git top-level: $CodeRoot (actual $TopLevel)"
    }
    $HeadResult = Invoke-GitReadOnly -CodeRoot $CodeRoot -Arguments @(
        "rev-parse", "--verify", "HEAD"
    )
    $ActualHead = ([string]$HeadResult.Output[0]).Trim().ToLowerInvariant()
    if ($ActualHead -ne $ExpectedHead) {
        throw "CodeRoot HEAD mismatch: expected $ExpectedHead, actual $ActualHead"
    }
    $BranchResult = Invoke-GitReadOnly -CodeRoot $CodeRoot -Arguments @(
        "symbolic-ref", "-q", "HEAD"
    ) -AllowedExitCodes @(0, 1)
    if ($BranchResult.ExitCode -eq 0) {
        throw "CodeRoot must be detached, but HEAD is attached to $($BranchResult.Output -join '')"
    }
    $StatusResult = Invoke-GitReadOnly -CodeRoot $CodeRoot -Arguments @(
        "status", "--porcelain=v1", "--untracked-files=all", "--ignore-submodules=none"
    )
    if ($StatusResult.Output.Count -ne 0) {
        throw "CodeRoot has tracked changes or non-ignored untracked files: $($StatusResult.Output -join '; ')"
    }
    $IgnoredCode = Invoke-GitReadOnly -CodeRoot $CodeRoot -Arguments @(
        "ls-files", "--others", "--ignored", "--exclude-standard", "--",
        "scripts", "src"
    )
    if ($IgnoredCode.Output.Count -ne 0) {
        throw "CodeRoot code paths contain ignored injection files: $($IgnoredCode.Output -join '; ')"
    }
    foreach ($Relative in @(
        "scripts/register_holdout_preflight_task.ps1",
        "scripts/run_holdout_preflight_task.ps1",
        "scripts/preflight_holdout.py"
    )) {
        Assert-TrackedFile -CodeRoot $CodeRoot -RelativePath $Relative
    }
    return $ActualHead
}

function ConvertTo-SingleQuotedLiteral {
    param([AllowEmptyString()][string]$Value)
    return "'" + $Value.Replace("'", "''") + "'"
}

function Get-StringSha256 {
    param([string]$Value)
    $Hasher = [System.Security.Cryptography.SHA256]::Create()
    try {
        $Bytes = (New-Object System.Text.UTF8Encoding($false)).GetBytes($Value)
        return ([System.BitConverter]::ToString(
            $Hasher.ComputeHash($Bytes)
        ) -replace '-', '').ToLowerInvariant()
    } finally {
        $Hasher.Dispose()
    }
}

function Assert-DeploymentIdentity {
    param(
        [string]$CodeRoot,
        [string]$ExpectedHead,
        [string]$TaskRunner,
        [string]$ExpectedTaskRunnerSha256,
        [string]$GovernanceRunner,
        [string]$ExpectedGovernanceRunnerSha256,
        [string]$Python,
        [string]$ExpectedPythonSha256,
        [string]$PyVenvConfig,
        [string]$ExpectedPyVenvSha256
    )
    [void](Assert-CodeRoot -CodeRoot $CodeRoot -ExpectedHead $ExpectedHead)
    Assert-OrdinaryFile -Path $TaskRunner -Description "preflight task runner"
    Assert-OrdinaryFile -Path $GovernanceRunner -Description "governance runner"
    Assert-OrdinaryFile -Path $Python -Description "Python executable" -SingleLink
    Assert-OrdinaryFile -Path $PyVenvConfig -Description "pyvenv.cfg" -SingleLink
    $Checks = @(
        @("task runner", $TaskRunner, $ExpectedTaskRunnerSha256),
        @("governance runner", $GovernanceRunner, $ExpectedGovernanceRunnerSha256),
        @("Python executable", $Python, $ExpectedPythonSha256),
        @("pyvenv.cfg", $PyVenvConfig, $ExpectedPyVenvSha256)
    )
    foreach ($Check in $Checks) {
        $Actual = Get-FileSha256 -Path $Check[1]
        if ($Actual -cne $Check[2]) {
            throw "$($Check[0]) identity changed during registration"
        }
    }
}

function Assert-EnvironmentIdentity {
    param(
        [string]$CodeRoot,
        [string]$RuntimeSource,
        [string]$CodeSource,
        [string]$VenvRoot,
        [string]$SitePackages,
        [string]$PyVenvConfig,
        [string]$ExpectedVenvTreeSha256,
        [long]$ExpectedVenvFileCount,
        [long]$ExpectedVenvTotalBytes,
        [string]$ExpectedRuntimeTreeSha256,
        [long]$ExpectedRuntimeFileCount,
        [long]$ExpectedRuntimeTotalBytes,
        [string]$ExpectedCodeTreeSha256,
        [long]$ExpectedCodeFileCount,
        [long]$ExpectedCodeTotalBytes,
        [string]$BaseRuntimeRoot,
        [string]$ExpectedBaseTreeSha256,
        [long]$ExpectedBaseFileCount,
        [long]$ExpectedBaseTotalBytes
    )
    Assert-IsolatedPyVenvConfig -Path $PyVenvConfig
    Assert-NoCodeStartupInjection -Root (Join-Path $CodeRoot "scripts") `
        -Description "CodeRoot/scripts"
    Assert-NoCodeStartupInjection -Root (Join-Path $CodeRoot "src") `
        -Description "CodeRoot/src"
    Assert-NoCodeStartupInjection -Root $RuntimeSource `
        -Description "RuntimeRoot/src"
    Assert-NoVenvStartupCustomizer -SitePackages $SitePackages
    Assert-NoPythonPathConfig -Root $BaseRuntimeRoot `
        -Description "base Python runtime"
    $Venv = Get-BoundTreeIdentity -Root $VenvRoot -Description "Python venv"
    $Runtime = Get-BoundTreeIdentity -Root $RuntimeSource `
        -Description "RuntimeRoot/src"
    $Code = Get-BoundTreeIdentity -Root $CodeSource `
        -Description "CodeRoot/src"
    $Base = Get-BoundTreeIdentity -Root $BaseRuntimeRoot `
        -Description "base Python runtime"
    if (
        $Venv.tree_sha256 -cne $ExpectedVenvTreeSha256 -or
        [long]$Venv.file_count -ne $ExpectedVenvFileCount -or
        [long]$Venv.total_bytes -ne $ExpectedVenvTotalBytes
    ) {
        throw "Python venv manifest changed during registration"
    }
    if (
        $Runtime.tree_sha256 -cne $ExpectedRuntimeTreeSha256 -or
        [long]$Runtime.file_count -ne $ExpectedRuntimeFileCount -or
        [long]$Runtime.total_bytes -ne $ExpectedRuntimeTotalBytes
    ) {
        throw "RuntimeRoot/src manifest changed during registration"
    }
    if (
        $Code.tree_sha256 -cne $ExpectedCodeTreeSha256 -or
        [long]$Code.file_count -ne $ExpectedCodeFileCount -or
        [long]$Code.total_bytes -ne $ExpectedCodeTotalBytes
    ) {
        throw "CodeRoot/src manifest changed during registration"
    }
    if (
        $Base.tree_sha256 -cne $ExpectedBaseTreeSha256 -or
        [long]$Base.file_count -ne $ExpectedBaseFileCount -or
        [long]$Base.total_bytes -ne $ExpectedBaseTotalBytes
    ) {
        throw "base Python runtime manifest changed during registration"
    }
}

function Get-XmlText {
    param([xml]$Xml, [string]$XPath, [switch]$Optional)
    $Node = $Xml.SelectSingleNode($XPath)
    if ($null -eq $Node) {
        if ($Optional) { return $null }
        throw "registered task XML is missing: $XPath"
    }
    return [string]$Node.InnerText
}

function Assert-TaskDefinition {
    param(
        $Registered,
        [string]$ExportedXml,
        [System.Collections.IDictionary]$Definition
    )
    if ([string]$Registered.TaskName -ne [string]$Definition.task_name) {
        throw "registered TaskName drifted"
    }
    if ([string]$Registered.TaskPath -ne [string]$Definition.task_path) {
        throw "registered TaskPath drifted"
    }
    if ([string]$Registered.State -ne "Disabled") {
        throw "registered task state is not Disabled"
    }
    if ($Registered.Settings.Enabled -ne $false) {
        throw "registered task Settings.Enabled is not false"
    }

    $Actions = @($Registered.Actions)
    if ($Actions.Count -ne 1) {
        throw "registered task must have exactly one action"
    }
    $ActualAction = $Actions[0]
    foreach ($Property in @("Execute", "Arguments", "WorkingDirectory")) {
        $ExpectedName = switch ($Property) {
            "Execute" { "execute" }
            "Arguments" { "arguments" }
            default { "working_directory" }
        }
        if ([string]$ActualAction.$Property -cne [string]$Definition[$ExpectedName]) {
            throw "registered action $Property drifted"
        }
    }

    $Triggers = @($Registered.Triggers)
    if ($Triggers.Count -ne 1) {
        throw "registered task must have exactly one trigger"
    }
    $ActualTrigger = $Triggers[0]
    if ([int]$ActualTrigger.DaysInterval -ne 1 -or $ActualTrigger.Enabled -ne $true) {
        throw "registered daily trigger settings drifted"
    }
    $ActualStart = [datetimeoffset]::Parse(
        [string]$ActualTrigger.StartBoundary,
        [System.Globalization.CultureInfo]::InvariantCulture
    )
    if ($ActualStart.ToString("yyyy-MM-ddTHH:mm:ss") -ne `
        [string]$Definition.start_boundary_local) {
        throw "registered trigger StartBoundary drifted"
    }

    if ([string]$Registered.Principal.LogonType -ne "Interactive" -or
        [string]$Registered.Principal.RunLevel -ne "Limited") {
        throw "registered principal drifted"
    }
    $SettingsChecks = [ordered]@{
        MultipleInstances = "IgnoreNew"
        StartWhenAvailable = $true
        DisallowStartIfOnBatteries = $false
        StopIfGoingOnBatteries = $false
        WakeToRun = $true
        ExecutionTimeLimit = "PT30M"
        Hidden = $true
        Enabled = $false
        RestartCount = 0
    }
    foreach ($Name in $SettingsChecks.Keys) {
        if ([string]$Registered.Settings.$Name -cne `
            [string]$SettingsChecks[$Name]) {
            throw "registered setting $Name drifted"
        }
    }
    if ($Registered.Settings.RestartInterval -and
        [string]$Registered.Settings.RestartInterval -notin @("PT0M", "PT0S")) {
        throw "registered setting RestartInterval drifted"
    }

    [xml]$Xml = $ExportedXml
    $ExecNodes = $Xml.SelectNodes(
        "/*[local-name()='Task']/*[local-name()='Actions']/*[local-name()='Exec']"
    )
    $TriggerNodes = $Xml.SelectNodes(
        "/*[local-name()='Task']/*[local-name()='Triggers']/*[local-name()='CalendarTrigger']"
    )
    if ($ExecNodes.Count -ne 1 -or $TriggerNodes.Count -ne 1) {
        throw "registered task XML action/trigger cardinality drifted"
    }
    $XmlChecks = [ordered]@{
        "/*[local-name()='Task']/*[local-name()='Actions']/*[local-name()='Exec']/*[local-name()='Command']" = [string]$Definition.execute
        "/*[local-name()='Task']/*[local-name()='Actions']/*[local-name()='Exec']/*[local-name()='Arguments']" = [string]$Definition.arguments
        "/*[local-name()='Task']/*[local-name()='Actions']/*[local-name()='Exec']/*[local-name()='WorkingDirectory']" = [string]$Definition.working_directory
        "/*[local-name()='Task']/*[local-name()='Triggers']/*[local-name()='CalendarTrigger']/*[local-name()='StartBoundary']" = [string]$ActualTrigger.StartBoundary
        "/*[local-name()='Task']/*[local-name()='Triggers']/*[local-name()='CalendarTrigger']/*[local-name()='Enabled']" = "true"
        "/*[local-name()='Task']/*[local-name()='Triggers']/*[local-name()='CalendarTrigger']/*[local-name()='ScheduleByDay']/*[local-name()='DaysInterval']" = "1"
        "/*[local-name()='Task']/*[local-name()='Principals']/*[local-name()='Principal']/*[local-name()='UserId']" = [string]$Definition.principal_user_sid
        "/*[local-name()='Task']/*[local-name()='Principals']/*[local-name()='Principal']/*[local-name()='LogonType']" = "InteractiveToken"
        "/*[local-name()='Task']/*[local-name()='Principals']/*[local-name()='Principal']/*[local-name()='RunLevel']" = "LeastPrivilege"
        "/*[local-name()='Task']/*[local-name()='Settings']/*[local-name()='MultipleInstancesPolicy']" = "IgnoreNew"
        "/*[local-name()='Task']/*[local-name()='Settings']/*[local-name()='DisallowStartIfOnBatteries']" = "false"
        "/*[local-name()='Task']/*[local-name()='Settings']/*[local-name()='StopIfGoingOnBatteries']" = "false"
        "/*[local-name()='Task']/*[local-name()='Settings']/*[local-name()='StartWhenAvailable']" = "true"
        "/*[local-name()='Task']/*[local-name()='Settings']/*[local-name()='WakeToRun']" = "true"
        "/*[local-name()='Task']/*[local-name()='Settings']/*[local-name()='ExecutionTimeLimit']" = "PT30M"
        "/*[local-name()='Task']/*[local-name()='Settings']/*[local-name()='Hidden']" = "true"
        "/*[local-name()='Task']/*[local-name()='Settings']/*[local-name()='Enabled']" = "false"
    }
    foreach ($XPath in $XmlChecks.Keys) {
        $Actual = Get-XmlText -Xml $Xml -XPath $XPath
        if ($Actual -cne [string]$XmlChecks[$XPath]) {
            throw "registered task XML drifted at $XPath"
        }
    }
    $RestartNode = $Xml.SelectSingleNode(
        "/*[local-name()='Task']/*[local-name()='Settings']/*[local-name()='RestartOnFailure']"
    )
    if ($null -ne $RestartNode) {
        throw "registered task XML unexpectedly enables restart-on-failure"
    }
}

if ($VintageId -and $VintageId -notmatch '^holdout-vintage-[0-9a-f]{64}$') {
    throw "VintageId must be a canonical holdout vintage identifier"
}
$CodeRoot = Get-NormalizedPath -Path (Join-Path $PSScriptRoot "..") `
    -Description "CodeRoot"
$LiveRepository = Get-NormalizedPath -Path $Repository `
    -Description "live Repository"
$Runtime = Get-NormalizedPath -Path $RuntimeRoot -Description "RuntimeRoot"
$Python = Get-NormalizedPath -Path $PythonExecutable `
    -Description "Python executable" -Leaf
$Git = Get-NormalizedPath -Path $GitExecutable `
    -Description "Git executable" -Leaf
$script:GitExecutablePath = $Git

Assert-NoReparsePath -Path $CodeRoot -Description "CodeRoot"
Assert-NoReparsePath -Path $LiveRepository -Description "live Repository"
Assert-NoReparsePath -Path $Runtime -Description "RuntimeRoot"
Assert-NoReparsePath -Path $Python -Description "Python executable"
Assert-NoReparsePath -Path $Git -Description "Git executable"
Assert-OrdinaryFile -Path $Git -Description "Git executable"
$GitSha256 = Get-FileSha256 -Path $Git

foreach ($Pair in @(
    @("CodeRoot", $CodeRoot, "live Repository", $LiveRepository),
    @("CodeRoot", $CodeRoot, "RuntimeRoot", $Runtime),
    @("live Repository", $LiveRepository, "RuntimeRoot", $Runtime)
)) {
    if (Test-PathOverlap -Left $Pair[1] -Right $Pair[3]) {
        throw "$($Pair[0]) and $($Pair[2]) must be separate, non-nested roots"
    }
}
$RuntimeSource = Get-NormalizedPath -Path (Join-Path $Runtime "src") `
    -Description "RuntimeRoot/src"
Assert-NoReparsePath -Path $RuntimeSource -Description "RuntimeRoot/src"
$CodeSource = Get-NormalizedPath -Path (Join-Path $CodeRoot "src") `
    -Description "CodeRoot/src"
Assert-NoReparsePath -Path $CodeSource -Description "CodeRoot/src"
$AuthorityRegistry = Get-NormalizedPath `
    -Path (Join-Path $Runtime "data\research\governance.sqlite3") `
    -Description "authoritative governance registry" -Leaf
Assert-NoReparsePath -Path $AuthorityRegistry `
    -Description "authoritative governance registry"
Assert-OrdinaryFile -Path $AuthorityRegistry `
    -Description "authoritative governance registry"
if (-not (Test-PathOverlap -Left $CodeRoot -Right $Python)) {
    throw "Python executable must be inside CodeRoot: $Python"
}

$ActualCodeHead = Assert-CodeRoot -CodeRoot $CodeRoot `
    -ExpectedHead $ExpectedCodeHead
$TaskRunner = Get-NormalizedPath `
    -Path (Join-Path $CodeRoot "scripts\run_holdout_preflight_task.ps1") `
    -Description "preflight task runner" -Leaf
$GovernanceRunner = Get-NormalizedPath `
    -Path (Join-Path $CodeRoot "scripts\preflight_holdout.py") `
    -Description "governance runner" -Leaf
$PythonDirectory = Split-Path -Parent $Python
$VenvRoot = Get-NormalizedPath -Path (Split-Path -Parent $PythonDirectory) `
    -Description "Python venv root"
$PyVenvConfig = Get-NormalizedPath `
    -Path (Join-Path $VenvRoot "pyvenv.cfg") `
    -Description "pyvenv.cfg" -Leaf
$SitePackages = Get-NormalizedPath `
    -Path (Join-Path $VenvRoot "Lib\site-packages") `
    -Description "venv site-packages"
$BaseRuntimeRoot = Get-NormalizedPath `
    -Path (Get-PyVenvHome -Path $PyVenvConfig) `
    -Description "base Python runtime"
if ([System.IO.Path]::GetExtension($Python) -ine ".exe") {
    throw "PythonExecutable must be an .exe file"
}
Assert-IsolatedPyVenvConfig -Path $PyVenvConfig
Assert-NoCodeStartupInjection -Root (Join-Path $CodeRoot "scripts") `
    -Description "CodeRoot/scripts"
Assert-NoCodeStartupInjection -Root (Join-Path $CodeRoot "src") `
    -Description "CodeRoot/src"
Assert-NoCodeStartupInjection -Root $RuntimeSource `
    -Description "RuntimeRoot/src"
Assert-NoVenvStartupCustomizer -SitePackages $SitePackages
Assert-NoPythonPathConfig -Root $BaseRuntimeRoot `
    -Description "base Python runtime"
Assert-NoReparsePath -Path $BaseRuntimeRoot `
    -Description "base Python runtime"
foreach ($Pair in @(
    @("base Python runtime", $BaseRuntimeRoot, "CodeRoot", $CodeRoot),
    @("base Python runtime", $BaseRuntimeRoot, "live Repository", $LiveRepository),
    @("base Python runtime", $BaseRuntimeRoot, "RuntimeRoot", $Runtime),
    @("base Python runtime", $BaseRuntimeRoot, "Python venv", $VenvRoot)
)) {
    if (Test-PathOverlap -Left $Pair[1] -Right $Pair[3]) {
        throw "$($Pair[0]) and $($Pair[2]) must be separate, non-nested roots"
    }
}
Assert-OrdinaryFile -Path $TaskRunner -Description "preflight task runner"
Assert-OrdinaryFile -Path $GovernanceRunner -Description "governance runner"
Assert-OrdinaryFile -Path $Python -Description "Python executable" -SingleLink
Assert-OrdinaryFile -Path $PyVenvConfig -Description "pyvenv.cfg" -SingleLink

$WrapperSha256 = Get-FileSha256 -Path $TaskRunner
$GovernanceRunnerSha256 = Get-FileSha256 -Path $GovernanceRunner
$PythonSha256 = Get-FileSha256 -Path $Python
$PyVenvSha256 = Get-FileSha256 -Path $PyVenvConfig
$VenvIdentity = Get-BoundTreeIdentity -Root $VenvRoot `
    -Description "Python venv"
$RuntimeSourceIdentity = Get-BoundTreeIdentity -Root $RuntimeSource `
    -Description "RuntimeRoot/src"
$CodeSourceIdentity = Get-BoundTreeIdentity -Root $CodeSource `
    -Description "CodeRoot/src"
$BaseRuntimeIdentity = Get-BoundTreeIdentity -Root $BaseRuntimeRoot `
    -Description "base Python runtime"

$Parameters = [ordered]@{
    Repository = $LiveRepository
    RuntimeRoot = $Runtime
    PythonExecutable = $Python
    GitExecutable = $Git
    ExpectedCodeHead = $ExpectedCodeHead
    ExpectedWrapperSha256 = $WrapperSha256
    ExpectedPythonSha256 = $PythonSha256
    ExpectedPyVenvSha256 = $PyVenvSha256
    ExpectedGovernanceRunnerSha256 = $GovernanceRunnerSha256
    ExpectedGitSha256 = $GitSha256
    ExpectedVenvTreeSha256 = $VenvIdentity.tree_sha256
    ExpectedVenvFileCount = $VenvIdentity.file_count
    ExpectedVenvTotalBytes = $VenvIdentity.total_bytes
    ExpectedRuntimeSourceTreeSha256 = $RuntimeSourceIdentity.tree_sha256
    ExpectedRuntimeSourceFileCount = $RuntimeSourceIdentity.file_count
    ExpectedRuntimeSourceTotalBytes = $RuntimeSourceIdentity.total_bytes
    ExpectedCodeSourceTreeSha256 = $CodeSourceIdentity.tree_sha256
    ExpectedCodeSourceFileCount = $CodeSourceIdentity.file_count
    ExpectedCodeSourceTotalBytes = $CodeSourceIdentity.total_bytes
    ExpectedBaseRuntimeTreeSha256 = $BaseRuntimeIdentity.tree_sha256
    ExpectedBaseRuntimeFileCount = $BaseRuntimeIdentity.file_count
    ExpectedBaseRuntimeTotalBytes = $BaseRuntimeIdentity.total_bytes
    ExecutionTimeoutSeconds = 1500
}
if ($VintageId) { $Parameters.VintageId = $VintageId }
$ParameterLines = @($Parameters.GetEnumerator() | ForEach-Object {
    "    $($_.Key) = $(ConvertTo-SingleQuotedLiteral -Value ([string]$_.Value))"
})
$BootstrapLines = @(
    '$ErrorActionPreference = ''Stop''',
    ("`$wrapper = " + (ConvertTo-SingleQuotedLiteral -Value $TaskRunner)),
    ("`$expected = " + (ConvertTo-SingleQuotedLiteral -Value $WrapperSha256)),
    '$stream = [System.IO.File]::Open($wrapper, [System.IO.FileMode]::Open, [System.IO.FileAccess]::Read, [System.IO.FileShare]::Read)',
    'try {',
    '  $hasher = [System.Security.Cryptography.SHA256]::Create()',
    '  try { $actual = ([System.BitConverter]::ToString($hasher.ComputeHash($stream)) -replace ''-'', '''').ToLowerInvariant() } finally { $hasher.Dispose() }',
    '  if ($actual -cne $expected) { throw "preflight wrapper bootstrap hash mismatch" }',
    ('$parameters = ' + '@{')
)
$BootstrapLines += $ParameterLines
$BootstrapLines += @(
    '}', '  & $wrapper @parameters', '  $businessExit = $LASTEXITCODE',
    '} finally { $stream.Dispose() }', 'exit $businessExit'
)
$Bootstrap = $BootstrapLines -join "`r`n"
$EncodedBootstrap = [Convert]::ToBase64String(
    [System.Text.Encoding]::Unicode.GetBytes($Bootstrap)
)
$TaskArguments = (
    "-NoProfile -NonInteractive -ExecutionPolicy Bypass " +
    "-WindowStyle Hidden -EncodedCommand $EncodedBootstrap"
)

$Time = [datetime]::ParseExact(
    $DailyAt,
    "HH:mm",
    [System.Globalization.CultureInfo]::InvariantCulture
)
$FirstRunLocal = [datetime]::Today.Add($Time.TimeOfDay)
$TaskName = "guvolu-holdout-preflight"
$TaskPath = "\"
$SystemDirectory = [Environment]::GetFolderPath(
    [Environment+SpecialFolder]::System
)
$TaskPowerShell = Get-NormalizedPath `
    -Path (Join-Path $SystemDirectory "WindowsPowerShell\v1.0\powershell.exe") `
    -Description "System32 Windows PowerShell" -Leaf
Assert-OrdinaryFile -Path $TaskPowerShell -Description "System32 Windows PowerShell"
$TaskPowerShellSha256 = Get-FileSha256 -Path $TaskPowerShell
$CurrentIdentity = [System.Security.Principal.WindowsIdentity]::GetCurrent()
$PrincipalName = $CurrentIdentity.Name
$PrincipalSid = $CurrentIdentity.User.Value
$Definition = [ordered]@{
    task_name = $TaskName
    task_path = $TaskPath
    execute = $TaskPowerShell
    execute_sha256 = $TaskPowerShellSha256
    arguments = $TaskArguments
    working_directory = $CodeRoot
    code_root = $CodeRoot
    expected_code_head = $ExpectedCodeHead
    actual_code_head = $ActualCodeHead
    live_repository = $LiveRepository
    runtime_root = $Runtime
    authoritative_data_root = $Runtime
    authoritative_governance_registry = $AuthorityRegistry
    registry_access = `
        "db+wal+shm-read-share-only+memory-backup+exclusive-create-new-snapshot"
    authority_sidecar_precondition = "db+wal+shm-preexisting;rollback-journal-absent"
    registry_snapshot_journal_mode = "delete"
    registry_snapshot_auxiliaries = "absent-before-and-after-business"
    python_executable = $Python
    git_executable = $Git
    git_sha256 = $GitSha256
    python_sha256 = $PythonSha256
    pyvenv_config = $PyVenvConfig
    pyvenv_sha256 = $PyVenvSha256
    site_packages = $SitePackages
    venv_manifest = [ordered]@{
        attestation = "partial"
        file_count = $VenvIdentity.file_count
        total_bytes = $VenvIdentity.total_bytes
        tree_sha256 = $VenvIdentity.tree_sha256
    }
    runtime_source_manifest = [ordered]@{
        file_count = $RuntimeSourceIdentity.file_count
        total_bytes = $RuntimeSourceIdentity.total_bytes
        tree_sha256 = $RuntimeSourceIdentity.tree_sha256
    }
    code_source = $CodeSource
    code_source_manifest = [ordered]@{
        file_count = $CodeSourceIdentity.file_count
        total_bytes = $CodeSourceIdentity.total_bytes
        tree_sha256 = $CodeSourceIdentity.tree_sha256
    }
    base_python_runtime = $BaseRuntimeRoot
    base_runtime_manifest = [ordered]@{
        attestation = "partial"
        file_count = $BaseRuntimeIdentity.file_count
        total_bytes = $BaseRuntimeIdentity.total_bytes
        tree_sha256 = $BaseRuntimeIdentity.tree_sha256
    }
    task_runner = $TaskRunner
    task_runner_sha256 = $WrapperSha256
    governance_runner = $GovernanceRunner
    governance_runner_sha256 = $GovernanceRunnerSha256
    bootstrap_sha256 = Get-StringSha256 -Value $Bootstrap
    daily_at_local = $FirstRunLocal.ToString("HH:mm")
    start_boundary_local = $FirstRunLocal.ToString("yyyy-MM-ddTHH:mm:ss")
    local_time_zone = [System.TimeZoneInfo]::Local.Id
    trigger_enabled = $true
    trigger_days_interval = 1
    vintage_id = if ($VintageId) { $VintageId } else { $null }
    enabled = $false
    state = "Disabled"
    principal_user_sid = $PrincipalSid
    principal_logon_type = "Interactive"
    principal_run_level = "Limited"
    unattended_coverage_capable = $false
    python_startup = "-I -S -B -X utf8 -X pycache_prefix=<unique-empty>"
    environment_attestation = "partial"
    process_guard = `
        "windows-create-suspended+restricted-handle-list+assign-kill-on-close-job+resume"
    child_environment = "minimal-nonsecret-allowlist"
    execution_timeout_seconds = 1500
    multiple_instances = "IgnoreNew"
    start_when_available = $true
    allow_start_on_batteries = $true
    dont_stop_if_going_on_batteries = $true
    wake_to_run = $true
    execution_time_limit_minutes = 30
    hidden = $true
    restart_count = 0
    restart_interval_minutes = 0
}

Assert-DeploymentIdentity -CodeRoot $CodeRoot -ExpectedHead $ExpectedCodeHead `
    -TaskRunner $TaskRunner -ExpectedTaskRunnerSha256 $WrapperSha256 `
    -GovernanceRunner $GovernanceRunner `
    -ExpectedGovernanceRunnerSha256 $GovernanceRunnerSha256 `
    -Python $Python -ExpectedPythonSha256 $PythonSha256 `
    -PyVenvConfig $PyVenvConfig -ExpectedPyVenvSha256 $PyVenvSha256
Assert-EnvironmentIdentity -CodeRoot $CodeRoot -RuntimeSource $RuntimeSource `
    -CodeSource $CodeSource `
    -VenvRoot $VenvRoot -SitePackages $SitePackages `
    -PyVenvConfig $PyVenvConfig `
    -ExpectedVenvTreeSha256 $VenvIdentity.tree_sha256 `
    -ExpectedVenvFileCount $VenvIdentity.file_count `
    -ExpectedVenvTotalBytes $VenvIdentity.total_bytes `
    -ExpectedRuntimeTreeSha256 $RuntimeSourceIdentity.tree_sha256 `
    -ExpectedRuntimeFileCount $RuntimeSourceIdentity.file_count `
    -ExpectedRuntimeTotalBytes $RuntimeSourceIdentity.total_bytes `
    -ExpectedCodeTreeSha256 $CodeSourceIdentity.tree_sha256 `
    -ExpectedCodeFileCount $CodeSourceIdentity.file_count `
    -ExpectedCodeTotalBytes $CodeSourceIdentity.total_bytes `
    -BaseRuntimeRoot $BaseRuntimeRoot `
    -ExpectedBaseTreeSha256 $BaseRuntimeIdentity.tree_sha256 `
    -ExpectedBaseFileCount $BaseRuntimeIdentity.file_count `
    -ExpectedBaseTotalBytes $BaseRuntimeIdentity.total_bytes

# This must remain before the first ScheduledTasks cmdlet invocation.
if ($DescribeOnly) {
    [pscustomobject]$Definition | ConvertTo-Json -Depth 6 -Compress
    exit 0
}

$Existing = @(Get-ScheduledTask -ErrorAction Stop | Where-Object {
    [string]$_.TaskName -ieq $TaskName -and [string]$_.TaskPath -ieq $TaskPath
})
if ($Existing.Count -gt 1) {
    throw "refusing ambiguous existing preflight tasks: $TaskName"
}
if ($Existing.Count -eq 1 -and (
    $Existing[0].Settings.Enabled -ne $false -or
    [string]$Existing[0].State -ne "Disabled"
)) {
    throw "refusing to overwrite a preflight task that is not Disabled/Enabled=False: $TaskName"
}

$Action = New-ScheduledTaskAction -Execute $Definition.execute `
    -Argument $Definition.arguments -WorkingDirectory $Definition.working_directory
$Trigger = New-ScheduledTaskTrigger -Daily -At $FirstRunLocal -DaysInterval 1
$Principal = New-ScheduledTaskPrincipal -UserId $PrincipalName `
    -LogonType Interactive -RunLevel Limited
$Settings = New-ScheduledTaskSettingsSet -MultipleInstances IgnoreNew `
    -StartWhenAvailable -AllowStartIfOnBatteries `
    -DontStopIfGoingOnBatteries -WakeToRun `
    -ExecutionTimeLimit (New-TimeSpan -Minutes 30) `
    -Hidden -Disable

$RegistrationAttempted = $false
try {
    $RegistrationAttempted = $true
    Register-ScheduledTask -TaskName $TaskName -TaskPath $TaskPath `
        -Action $Action -Trigger $Trigger `
        -Principal $Principal -Settings $Settings -Force | Out-Null
    $RegisteredMatches = @(Get-ScheduledTask -ErrorAction Stop | Where-Object {
        [string]$_.TaskName -ieq $TaskName -and
        [string]$_.TaskPath -ieq $TaskPath
    })
    if ($RegisteredMatches.Count -ne 1) {
        throw "registration readback did not return exactly one task: $TaskName"
    }
    $Registered = $RegisteredMatches[0]
    $ExportedXml = Export-ScheduledTask -TaskName $TaskName `
        -TaskPath $TaskPath -ErrorAction Stop
    Assert-TaskDefinition -Registered $Registered -ExportedXml $ExportedXml `
        -Definition $Definition
    Assert-DeploymentIdentity -CodeRoot $CodeRoot `
        -ExpectedHead $ExpectedCodeHead -TaskRunner $TaskRunner `
        -ExpectedTaskRunnerSha256 $WrapperSha256 `
        -GovernanceRunner $GovernanceRunner `
        -ExpectedGovernanceRunnerSha256 $GovernanceRunnerSha256 `
        -Python $Python -ExpectedPythonSha256 $PythonSha256 `
        -PyVenvConfig $PyVenvConfig -ExpectedPyVenvSha256 $PyVenvSha256
    Assert-EnvironmentIdentity -CodeRoot $CodeRoot `
        -RuntimeSource $RuntimeSource -CodeSource $CodeSource `
        -VenvRoot $VenvRoot `
        -SitePackages $SitePackages -PyVenvConfig $PyVenvConfig `
        -ExpectedVenvTreeSha256 $VenvIdentity.tree_sha256 `
        -ExpectedVenvFileCount $VenvIdentity.file_count `
        -ExpectedVenvTotalBytes $VenvIdentity.total_bytes `
        -ExpectedRuntimeTreeSha256 $RuntimeSourceIdentity.tree_sha256 `
        -ExpectedRuntimeFileCount $RuntimeSourceIdentity.file_count `
        -ExpectedRuntimeTotalBytes $RuntimeSourceIdentity.total_bytes `
        -ExpectedCodeTreeSha256 $CodeSourceIdentity.tree_sha256 `
        -ExpectedCodeFileCount $CodeSourceIdentity.file_count `
        -ExpectedCodeTotalBytes $CodeSourceIdentity.total_bytes `
        -BaseRuntimeRoot $BaseRuntimeRoot `
        -ExpectedBaseTreeSha256 $BaseRuntimeIdentity.tree_sha256 `
        -ExpectedBaseFileCount $BaseRuntimeIdentity.file_count `
        -ExpectedBaseTotalBytes $BaseRuntimeIdentity.total_bytes
} catch {
    $PrimaryFailure = $_
    if ($RegistrationAttempted) {
        try {
            Disable-ScheduledTask -TaskName $TaskName -TaskPath $TaskPath `
                -ErrorAction Stop | Out-Null
        } catch { }
        try {
            Stop-ScheduledTask -TaskName $TaskName -TaskPath $TaskPath `
                -ErrorAction Stop | Out-Null
        } catch { }
    }
    throw $PrimaryFailure
}

$ActualAction = @($Registered.Actions)[0]
$ActualTrigger = @($Registered.Triggers)[0]
$ActualStart = [datetimeoffset]::Parse(
    [string]$ActualTrigger.StartBoundary,
    [System.Globalization.CultureInfo]::InvariantCulture
)
[pscustomobject][ordered]@{
    task_name = [string]$Registered.TaskName
    task_path = [string]$Registered.TaskPath
    state = [string]$Registered.State
    enabled = [bool]$Registered.Settings.Enabled
    execute = [string]$ActualAction.Execute
    arguments = [string]$ActualAction.Arguments
    working_directory = [string]$ActualAction.WorkingDirectory
    start_boundary = [string]$ActualTrigger.StartBoundary
    daily_at_local = $ActualStart.ToString("HH:mm")
    trigger_enabled = [bool]$ActualTrigger.Enabled
    trigger_days_interval = [int]$ActualTrigger.DaysInterval
    principal_logon_type = [string]$Registered.Principal.LogonType
    principal_run_level = [string]$Registered.Principal.RunLevel
    settings = [ordered]@{
        multiple_instances = [string]$Registered.Settings.MultipleInstances
        start_when_available = [bool]$Registered.Settings.StartWhenAvailable
        allow_start_on_batteries = -not [bool]$Registered.Settings.DisallowStartIfOnBatteries
        dont_stop_if_going_on_batteries = -not [bool]$Registered.Settings.StopIfGoingOnBatteries
        wake_to_run = [bool]$Registered.Settings.WakeToRun
        execution_time_limit = [string]$Registered.Settings.ExecutionTimeLimit
        hidden = [bool]$Registered.Settings.Hidden
        restart_count = [int]$Registered.Settings.RestartCount
        restart_interval = [string]$Registered.Settings.RestartInterval
    }
    exported_xml_sha256 = Get-StringSha256 -Value ([string]$ExportedXml)
    definition = [pscustomobject]$Definition
} | ConvertTo-Json -Depth 8 -Compress

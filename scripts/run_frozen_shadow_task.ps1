param(
    [Parameter(Mandatory = $true)]
    [ValidatePattern('^frozen-forward-plan-[0-9a-f]{64}$')]
    [string]$PlanId,
    [Parameter(Mandatory = $true)]
    [string]$Repository,
    [Parameter(Mandatory = $true)]
    [string]$RuntimeRoot,
    [Parameter(Mandatory = $true)]
    [string]$ExecutionRepository,
    [Parameter(Mandatory = $true)]
    [string]$PythonExecutable,
    [Parameter(Mandatory = $true)]
    [ValidatePattern('^[0-9a-f]{64}$')]
    [string]$ExpectedPythonSha256,
    [Parameter(Mandatory = $true)]
    [string]$GitExecutable,
    [Parameter(Mandatory = $true)]
    [ValidatePattern('^[0-9a-f]{64}$')]
    [string]$ExpectedGitSha256,
    [Parameter(Mandatory = $true)]
    [ValidatePattern('^[0-9a-f]{64}$')]
    [string]$ExpectedExecutionEnvironmentTreeSha256,
    [Parameter(Mandatory = $true)]
    [ValidatePattern('^[0-9a-f]{40,64}$')]
    [string]$ExpectedCodeHead,
    [Parameter(Mandatory = $true)]
    [ValidatePattern('^[0-9a-f]{64}$')]
    [string]$ExpectedWrapperSha256,
    [switch]$NoPaper
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

function Assert-CurrentReceiptSchemas {
    param(
        [Parameter(Mandatory = $true)]
        [string]$ExecutionRoot,
        [Parameter(Mandatory = $true)]
        [int]$ExpectedSchema
    )

    $Relatives = @(
        "data\execution\shadow\frozen-forward\receipts",
        "data\execution\paper\receipts"
    )
    foreach ($Relative in $Relatives) {
        $ReceiptRoot = Join-Path $ExecutionRoot $Relative
        if (-not (Test-Path -LiteralPath $ReceiptRoot)) {
            continue
        }
        $Physical = Resolve-PhysicalDirectoryPath `
            -LiteralPath $ReceiptRoot -Label "receipt schema barrier"
        if (-not (Test-SamePath -Left $ReceiptRoot -Right $Physical)) {
            throw "receipt schema barrier rejects directory aliases: $ReceiptRoot"
        }
        $Directories = @(Get-ChildItem -LiteralPath $Physical -Directory -Force)
        if ($Directories.Count -ne 0) {
            throw "receipt schema barrier rejects nested directories"
        }
        $Files = @(Get-ChildItem -LiteralPath $Physical -File -Force)
        foreach ($File in $Files) {
            if (
                ($File.Attributes -band [System.IO.FileAttributes]::ReparsePoint) -ne 0 -or
                $File.Name -notmatch '^frozen-forward-prediction-[0-9a-f]{64}\.json$'
            ) {
                throw "receipt schema barrier rejects a non-canonical receipt"
            }
            $Body = [System.IO.File]::ReadAllText(
                $File.FullName, [System.Text.Encoding]::UTF8
            )
            try {
                $Receipt = $Body | ConvertFrom-Json -ErrorAction Stop
            } catch {
                throw "receipt schema barrier found invalid JSON"
            }
            if ([int]$Receipt.schema_version -ne $ExpectedSchema) {
                throw (
                    "legacy receipt is not migrated or reused: " +
                    "$($File.FullName)"
                )
            }
        }
    }
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

function Open-CodeFileGuards {
    param(
        [Parameter(Mandatory = $true)]
        [string]$CodeRoot,
        [Parameter(Mandatory = $true)]
        [string]$GitExecutable
    )

    $TrackedText = Invoke-CodeGit -CodeRoot $CodeRoot `
        -GitExecutable $GitExecutable -Arguments @("ls-files") `
        -Label "list tracked code files"
    $Streams = [System.Collections.Generic.List[System.IO.FileStream]]::new()
    try {
        foreach ($Relative in @($TrackedText -split "`n")) {
            if ([string]::IsNullOrWhiteSpace($Relative)) { continue }
            $Path = Resolve-FileSystemPath `
                -LiteralPath (Join-Path $CodeRoot $Relative.Trim()) `
                -Label "tracked code file" -Leaf
            $Item = Get-Item -LiteralPath $Path -Force
            if (($Item.Attributes -band [System.IO.FileAttributes]::ReparsePoint) -ne 0) {
                throw "tracked code file is a reparse point: $Path"
            }
            $Streams.Add([System.IO.File]::Open(
                $Path, [System.IO.FileMode]::Open,
                [System.IO.FileAccess]::Read, [System.IO.FileShare]::Read
            )) | Out-Null
        }
        return $Streams
    } catch {
        foreach ($Stream in $Streams) { $Stream.Dispose() }
        throw
    }
}

function Open-TreeFileGuards {
    param(
        [Parameter(Mandatory = $true)]
        [string]$Root
    )

    $Streams = [System.Collections.Generic.List[System.IO.FileStream]]::new()
    try {
        foreach ($File in @(Get-ChildItem -LiteralPath $Root -File -Recurse -Force)) {
            if (($File.Attributes -band [System.IO.FileAttributes]::ReparsePoint) -ne 0) {
                throw "guarded tree contains a reparse file: $($File.FullName)"
            }
            $Streams.Add([System.IO.File]::Open(
                $File.FullName, [System.IO.FileMode]::Open,
                [System.IO.FileAccess]::Read, [System.IO.FileShare]::Read
            )) | Out-Null
        }
        return $Streams
    } catch {
        foreach ($Stream in $Streams) { $Stream.Dispose() }
        throw
    }
}

function Ensure-ManagedDirectory {
    param(
        [Parameter(Mandatory = $true)]
        [string]$Root,
        [Parameter(Mandatory = $true)]
        [string[]]$Segments
    )

    $Current = Resolve-PhysicalDirectoryPath -LiteralPath $Root -Label "data root"
    foreach ($Segment in $Segments) {
        if ($Segment -notmatch '^[A-Za-z0-9._-]+$') {
            throw "managed directory segment is invalid: $Segment"
        }
        $Current = Join-Path $Current $Segment
        if (-not (Test-Path -LiteralPath $Current)) {
            New-Item -ItemType Directory -Path $Current -ErrorAction Stop | Out-Null
        }
        $Item = Get-Item -LiteralPath $Current -Force -ErrorAction Stop
        if (-not $Item.PSIsContainer -or
            ($Item.Attributes -band [System.IO.FileAttributes]::ReparsePoint) -ne 0) {
            throw "managed directory is not a physical directory: $Current"
        }
        $Current = [System.IO.Path]::GetFullPath($Item.FullName)
    }
    return $Current
}

function Add-DurableUtf8Line {
    param(
        [Parameter(Mandatory = $true)]
        [string]$LiteralPath,
        [Parameter(Mandatory = $true)]
        [string]$Text
    )

    if (Test-Path -LiteralPath $LiteralPath) {
        $Item = Get-Item -LiteralPath $LiteralPath -Force -ErrorAction Stop
        if ($Item.PSIsContainer -or
            [System.StringComparer]::OrdinalIgnoreCase.Equals(
                [string]$Item.LinkType, "HardLink"
            ) -or
            ($Item.Attributes -band [System.IO.FileAttributes]::ReparsePoint) -ne 0) {
            throw "scheduler log is not a regular physical file: $LiteralPath"
        }
    }
    $Bytes = [System.Text.UTF8Encoding]::new($false).GetBytes($Text + "`n")
    $Stream = [System.IO.FileStream]::new(
        $LiteralPath,
        [System.IO.FileMode]::OpenOrCreate,
        [System.IO.FileAccess]::ReadWrite,
        [System.IO.FileShare]::Read,
        4096,
        [System.IO.FileOptions]::WriteThrough
    )
    try {
        $Start = $Stream.Seek(0, [System.IO.SeekOrigin]::End)
        $Stream.Write($Bytes, 0, $Bytes.Length)
        $Stream.Flush($true)
        if (($Stream.Position - $Start) -ne $Bytes.Length) {
            throw "scheduler log append was short"
        }
    } finally {
        $Stream.Dispose()
    }
}

$CodeRoot = Resolve-PhysicalDirectoryPath `
    -LiteralPath (Join-Path $PSScriptRoot "..") -Label "CodeRoot"
$DataRoot = Resolve-PhysicalDirectoryPath -LiteralPath $Repository -Label "DataRoot"
$Execution = Resolve-PhysicalDirectoryPath `
    -LiteralPath $ExecutionRepository -Label "execution repository"
Assert-CurrentReceiptSchemas -ExecutionRoot $Execution -ExpectedSchema 6
if (Test-OverlappingRoot -Left $CodeRoot -Right $DataRoot) {
    throw "CodeRoot and DataRoot must be distinct, non-overlapping roots"
}
$LogDirectory = Ensure-ManagedDirectory -Root $DataRoot `
    -Segments @("logs", "research", "frozen-forward")
$LogPath = Join-Path $LogDirectory "shadow-scheduler.jsonl"

$StartedAt = [datetime]::UtcNow.ToString("o")
$Runtime = $null
$ResolvedPython = $null
$ResolvedGit = $null
$ActualCodeHead = $null
$ActualWrapperSha256 = $null
$ActualPythonSha256 = $null
$ActualGitSha256 = $null
$ActualEnvironmentTreeSha256 = $null
$ExitCode = 3
$Output = @()
$CodeGuards = @()
$EnvironmentGuards = @()
$PythonGuard = $null
$GitGuard = $null

[System.Management.Automation.ActionPreference]$PreviousErrorActionPreference =
    [System.Management.Automation.ActionPreference]$ErrorActionPreference
$HadPythonPath = Test-Path -LiteralPath Env:PYTHONPATH
$PreviousPythonPath = if ($HadPythonPath) { $env:PYTHONPATH } else { $null }
$LocationPushed = $false
try {
    $ActualWrapperSha256 = Get-FileSha256 -LiteralPath $PSCommandPath
    if ($ActualWrapperSha256 -cne $ExpectedWrapperSha256) {
        throw (
            "task wrapper SHA256 does not match the registered value: " +
            "$ActualWrapperSha256 != $ExpectedWrapperSha256"
        )
    }
    $ResolvedPython = Resolve-FileSystemPath `
        -LiteralPath $PythonExecutable -Label "PythonExecutable" -Leaf
    $ResolvedGit = Resolve-FileSystemPath `
        -LiteralPath $GitExecutable -Label "GitExecutable" -Leaf
    $ActualPythonSha256 = Get-FileSha256 -LiteralPath $ResolvedPython
    if ($ActualPythonSha256 -cne $ExpectedPythonSha256) {
        throw "PythonExecutable SHA256 does not match the registered value"
    }
    $ActualGitSha256 = Get-FileSha256 -LiteralPath $ResolvedGit
    if ($ActualGitSha256 -cne $ExpectedGitSha256) {
        throw "GitExecutable SHA256 does not match the registered value"
    }
    $ActualCodeHead = Assert-CodeCheckout `
        -CodeRoot $CodeRoot -GitExecutable $ResolvedGit `
        -ExpectedHead $ExpectedCodeHead
    $Runner = Resolve-FileSystemPath `
        -LiteralPath (Join-Path $CodeRoot "scripts\run_frozen_shadow.py") `
        -Label "frozen shadow Python runner" -Leaf
    $Runtime = Resolve-FileSystemPath -LiteralPath $RuntimeRoot -Label "runtime root"
    $EnvironmentManifest = Get-VenvTreeManifest `
        -VenvRoot (Join-Path $Execution ".venv")
    $ActualEnvironmentTreeSha256 = $EnvironmentManifest.TreeSha256
    if ($ActualEnvironmentTreeSha256 -cne $ExpectedExecutionEnvironmentTreeSha256) {
        throw "execution venv tree SHA256 does not match the registered value"
    }
    if (-not $NoPaper) {
        throw "paper is disabled until fill cost provenance is bound"
    }

    Remove-Item -LiteralPath Env:PYTHONPATH -ErrorAction SilentlyContinue
    $PythonGuard = [System.IO.File]::Open(
        $ResolvedPython, [System.IO.FileMode]::Open,
        [System.IO.FileAccess]::Read, [System.IO.FileShare]::Read
    )
    $GitGuard = [System.IO.File]::Open(
        $ResolvedGit, [System.IO.FileMode]::Open,
        [System.IO.FileAccess]::Read, [System.IO.FileShare]::Read
    )
    $CodeGuards = @(Open-CodeFileGuards `
        -CodeRoot $CodeRoot -GitExecutable $ResolvedGit)
    $EnvironmentGuards = @(Open-TreeFileGuards `
        -Root (Join-Path $Execution ".venv"))
    $null = Assert-CodeCheckout -CodeRoot $CodeRoot `
        -GitExecutable $ResolvedGit -ExpectedHead $ExpectedCodeHead
    $EnvironmentManifest = Get-VenvTreeManifest `
        -VenvRoot (Join-Path $Execution ".venv")
    if ($EnvironmentManifest.TreeSha256 -cne $ExpectedExecutionEnvironmentTreeSha256) {
        throw "execution venv tree drifted before Python startup"
    }
    Push-Location -LiteralPath $CodeRoot
    $LocationPushed = $true
    $PycachePrefix = Join-Path $LogDirectory `
        ("pycache-" + [guid]::NewGuid().ToString("N"))
    if (Test-Path -LiteralPath $PycachePrefix) {
        throw "isolated pycache prefix unexpectedly exists"
    }
    $PythonBootstrap = (
        "import sys,tokenize;" +
        "script=sys.argv.pop(1);" +
        "sys.argv[0]=script;" +
        "stream=tokenize.open(script);" +
        "source=stream.read();stream.close();" +
        "namespace={'__name__':'__main__','__file__':script," +
        "'__package__':None,'__spec__':None,'__cached__':None};" +
        "exec(compile(source,script,'exec'),namespace,namespace)"
    )
    $Arguments = @(
        "-I", "-S", "-B", "-X", "utf8", "-X", "pycache_prefix=$PycachePrefix",
        "-c", $PythonBootstrap,
        $Runner,
        "--repository", $DataRoot,
        "--runtime-root", $Runtime,
        "--execution-repository", $Execution,
        "--code-root", $CodeRoot,
        "--expected-code-head", $ExpectedCodeHead,
        "--python-executable", $ResolvedPython,
        "--expected-python-sha256", $ExpectedPythonSha256,
        "--git-executable", $ResolvedGit,
        "--expected-git-sha256", $ExpectedGitSha256,
        "--expected-execution-environment-tree-sha256", `
            $ExpectedExecutionEnvironmentTreeSha256,
        "--plan-id", $PlanId
    )
    $Arguments += "--no-paper"
    # Preserve native process output in the scheduler evidence record.
    $ErrorActionPreference =
        [System.Management.Automation.ActionPreference]::Continue
    $Output = & $ResolvedPython @Arguments 2>&1
    $ExitCode = $LASTEXITCODE
    $ErrorActionPreference = [System.Management.Automation.ActionPreference]::Stop
    if (Test-Path -LiteralPath $PycachePrefix) {
        throw "isolated Python created or consumed the forbidden pycache prefix"
    }
    $null = Assert-CodeCheckout -CodeRoot $CodeRoot `
        -GitExecutable $ResolvedGit -ExpectedHead $ExpectedCodeHead
    if ((Get-FileSha256 -LiteralPath $ResolvedPython) -cne $ExpectedPythonSha256) {
        throw "PythonExecutable drifted during runner"
    }
    if ((Get-FileSha256 -LiteralPath $ResolvedGit) -cne $ExpectedGitSha256) {
        throw "GitExecutable drifted during runner"
    }
    $PostEnvironmentManifest = Get-VenvTreeManifest `
        -VenvRoot (Join-Path $Execution ".venv")
    if ($PostEnvironmentManifest.TreeSha256 -cne `
        $ExpectedExecutionEnvironmentTreeSha256) {
        throw "execution venv drifted during runner"
    }
} catch {
    $Output = @($_.Exception.Message)
    $ExitCode = 3
} finally {
    foreach ($Guard in $EnvironmentGuards) { $Guard.Dispose() }
    foreach ($Guard in $CodeGuards) { $Guard.Dispose() }
    if ($null -ne $GitGuard) { $GitGuard.Dispose() }
    if ($null -ne $PythonGuard) { $PythonGuard.Dispose() }
    if ($LocationPushed) {
        Pop-Location
    }
    if ($HadPythonPath) {
        $env:PYTHONPATH = $PreviousPythonPath
    } else {
        Remove-Item -LiteralPath Env:PYTHONPATH -ErrorAction SilentlyContinue
    }
    $ErrorActionPreference = $PreviousErrorActionPreference
}

$Record = [ordered]@{
    started_at = $StartedAt
    completed_at = [datetime]::UtcNow.ToString("o")
    plan_id = $PlanId
    code_root = $CodeRoot
    expected_code_head = $ExpectedCodeHead
    actual_code_head = $ActualCodeHead
    expected_wrapper_sha256 = $ExpectedWrapperSha256
    actual_wrapper_sha256 = $ActualWrapperSha256
    repository = $Repository
    resolved_data_root = $DataRoot
    runtime_root = $RuntimeRoot
    resolved_runtime_root = $Runtime
    execution_repository = $ExecutionRepository
    python_executable = $PythonExecutable
    resolved_python_executable = $ResolvedPython
    expected_python_sha256 = $ExpectedPythonSha256
    actual_python_sha256 = $ActualPythonSha256
    git_executable = $GitExecutable
    resolved_git_executable = $ResolvedGit
    expected_git_sha256 = $ExpectedGitSha256
    actual_git_sha256 = $ActualGitSha256
    expected_execution_environment_tree_sha256 = `
        $ExpectedExecutionEnvironmentTreeSha256
    actual_execution_environment_tree_sha256 = `
        $ActualEnvironmentTreeSha256
    execution_environment_attestation = "partial"
    python_base_runtime_attestation = "unbound-partial"
    paper_fill_cost_provenance = "unbound"
    pythonpath = $null
    python_isolated = $true
    python_site_disabled = $true
    python_default_pyc_disabled = $true
    python_utf8 = $true
    no_paper = [bool]$NoPaper
    exit_code = $ExitCode
    output = ($Output -join "`n")
}
$LogWriteStatus = "written"
try {
    Add-DurableUtf8Line -LiteralPath $LogPath `
        -Text ($Record | ConvertTo-Json -Compress)
} catch {
    # A committed runner result must not be retried solely because this log failed.
    $LogWriteStatus = "failed"
    Write-Warning "scheduler log append failed: $($_.Exception.Message)"
}
exit $ExitCode

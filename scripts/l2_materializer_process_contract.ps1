function ConvertTo-L2ProcessCommandTokens {
    param(
        [Parameter(Mandatory = $true)]
        [string]$CommandLine
    )

    $ParseErrors = $null
    $Parsed = @(
        [System.Management.Automation.PSParser]::Tokenize(
            ('& ' + $CommandLine),
            [ref]$ParseErrors
        )
    )
    if ($null -ne $ParseErrors -and @($ParseErrors).Count -gt 0) {
        throw 'L2 materializer process command line cannot be tokenized.'
    }
    $AllowedTypes = @(
        'Command',
        'CommandArgument',
        'CommandParameter',
        'String',
        'Number'
    )
    $Values = @()
    for ($Index = 0; $Index -lt $Parsed.Count; $Index += 1) {
        $Token = $Parsed[$Index]
        $Type = $Token.Type.ToString()
        if (
            $Index -eq 0 -and
            $Type -eq 'Operator' -and
            $Token.Content -eq '&'
        ) {
            continue
        }
        if ($Type -notin $AllowedTypes) {
            throw (
                'L2 materializer process command line contains an opaque ' +
                "token type: $Type"
            )
        }
        $Values += [string]$Token.Content
    }
    if ($Values.Count -eq 0) {
        throw 'L2 materializer process command line is empty.'
    }
    return $Values
}

function Assert-L2PowerShellFileEntry {
    param(
        [Parameter(Mandatory = $true)]
        [string[]]$Tokens,
        [Parameter(Mandatory = $true)]
        [string]$ProcessName,
        [Parameter(Mandatory = $true)]
        [int]$FilePathIndex
    )

    if (
        $FilePathIndex -lt 2 -or
        [System.IO.Path]::GetFileName([string]$Tokens[0]) -ine
        [System.IO.Path]::GetFileName($ProcessName)
    ) {
        throw 'PowerShell interpreter identity is opaque.'
    }
    $FileSwitchIndices = @()
    for ($Index = 1; $Index -lt $Tokens.Count; $Index += 1) {
        if ($Tokens[$Index] -iin @('-File', '-f')) {
            $FileSwitchIndices += $Index
        }
    }
    if (
        $FileSwitchIndices.Count -ne 1 -or
        $FileSwitchIndices[0] -ne ($FilePathIndex - 1)
    ) {
        throw 'PowerShell runner invocation is opaque.'
    }
    $AllowedSwitches = @(
        '-NoExit', '-NoLogo', '-NoProfile', '-NonInteractive',
        '-Sta', '-Mta'
    )
    $ValueOptions = @(
        '-ExecutionPolicy', '-InputFormat', '-OutputFormat',
        '-Version', '-WindowStyle'
    )
    $SeenPrefixOptions = @{}
    $Index = 1
    while ($Index -lt ($FilePathIndex - 1)) {
        $Token = [string]$Tokens[$Index]
        $Canonical = @(
            $AllowedSwitches + $ValueOptions |
                Where-Object { $_ -ieq $Token }
        )
        if ($Canonical.Count -ne 1) {
            throw "PowerShell interpreter entry is opaque: $Token"
        }
        $Name = [string]$Canonical[0]
        if ($SeenPrefixOptions.ContainsKey($Name.ToLowerInvariant())) {
            throw "PowerShell interpreter option is repeated: $Name"
        }
        $SeenPrefixOptions[$Name.ToLowerInvariant()] = $true
        if ($Name -iin $ValueOptions) {
            if (
                $Index + 1 -ge ($FilePathIndex - 1) -or
                ([string]$Tokens[$Index + 1]).StartsWith('-')
            ) {
                throw "PowerShell interpreter option has no value: $Name"
            }
            $Index += 1
        }
        $Index += 1
    }
}

function Get-L2PhysicalPathDescriptor {
    param(
        [Parameter(Mandatory = $true)]
        [string]$Path
    )

    try {
        if (-not [System.IO.Path]::IsPathRooted($Path)) {
            throw 'path is not absolute'
        }
        $FullPath = [System.IO.Path]::GetFullPath($Path).TrimEnd('\', '/')
        $ExistingPath = $FullPath
        $Tail = @()
        while (-not (Test-Path -LiteralPath $ExistingPath)) {
            $Leaf = [System.IO.Path]::GetFileName($ExistingPath)
            if (-not $Leaf) {
                throw 'no existing ancestor can establish physical identity'
            }
            $Tail = @($Leaf) + $Tail
            $Parent = [System.IO.Path]::GetDirectoryName($ExistingPath)
            if (-not $Parent -or $Parent -eq $ExistingPath) {
                throw 'no existing ancestor can establish physical identity'
            }
            $ExistingPath = $Parent
        }
        if (-not ('L2PhysicalPath' -as [type])) {
            $null = Add-Type -TypeDefinition @'
using System;
using System.ComponentModel;
using System.IO;
using System.Runtime.InteropServices;
using System.Text;
using Microsoft.Win32.SafeHandles;

public static class L2PhysicalPath {
    [StructLayout(LayoutKind.Sequential)]
    private struct BY_HANDLE_FILE_INFORMATION {
        public uint FileAttributes;
        public System.Runtime.InteropServices.ComTypes.FILETIME CreationTime;
        public System.Runtime.InteropServices.ComTypes.FILETIME LastAccessTime;
        public System.Runtime.InteropServices.ComTypes.FILETIME LastWriteTime;
        public uint VolumeSerialNumber;
        public uint FileSizeHigh;
        public uint FileSizeLow;
        public uint NumberOfLinks;
        public uint FileIndexHigh;
        public uint FileIndexLow;
    }

    [DllImport("kernel32.dll", CharSet = CharSet.Unicode, SetLastError = true)]
    private static extern SafeFileHandle CreateFile(
        string name, uint access, uint share, IntPtr security,
        uint creation, uint flags, IntPtr template);

    [DllImport("kernel32.dll", CharSet = CharSet.Unicode, SetLastError = true)]
    private static extern uint GetFinalPathNameByHandle(
        SafeFileHandle file, StringBuilder path, uint length, uint flags);

    [DllImport("kernel32.dll", SetLastError = true)]
    private static extern bool GetFileInformationByHandle(
        SafeFileHandle file, out BY_HANDLE_FILE_INFORMATION information);

    public static string[] Describe(string path) {
        const uint ShareAll = 0x00000001 | 0x00000002 | 0x00000004;
        const uint OpenExisting = 3;
        const uint BackupSemantics = 0x02000000;
        using (SafeFileHandle handle = CreateFile(
            path, 0, ShareAll, IntPtr.Zero, OpenExisting,
            BackupSemantics, IntPtr.Zero)) {
            if (handle.IsInvalid) {
                throw new Win32Exception(Marshal.GetLastWin32Error());
            }
            var buffer = new StringBuilder(32768);
            uint length = GetFinalPathNameByHandle(
                handle, buffer, (uint)buffer.Capacity, 0);
            if (length == 0 || length >= buffer.Capacity) {
                throw new Win32Exception(Marshal.GetLastWin32Error());
            }
            BY_HANDLE_FILE_INFORMATION information;
            if (!GetFileInformationByHandle(handle, out information)) {
                throw new Win32Exception(Marshal.GetLastWin32Error());
            }
            string identity = String.Format(
                "{0:X8}:{1:X8}:{2:X8}",
                information.VolumeSerialNumber,
                information.FileIndexHigh,
                information.FileIndexLow);
            return new string[] { buffer.ToString(), identity };
        }
    }
}
'@
        }
        $Description = [L2PhysicalPath]::Describe($ExistingPath)
        $ResolvedPath = [string]$Description[0]
        foreach ($Part in $Tail) {
            $ResolvedPath = Join-Path $ResolvedPath $Part
        }
        $Identity = if ($Tail.Count -eq 0) {
            [string]$Description[1]
        } else {
            ''
        }
        return [pscustomobject]@{
            ResolvedPath = (
                [System.IO.Path]::GetFullPath($ResolvedPath).TrimEnd('\', '/')
            )
            Identity = $Identity
        }
    } catch {
        throw "cannot establish physical path identity for '$Path': $($_.Exception.Message)"
    }
}

function Test-L2LexicalPathEqual {
    param(
        [Parameter(Mandatory = $true)]
        [string]$Left,
        [Parameter(Mandatory = $true)]
        [string]$Right
    )
    try {
        if (
            -not [System.IO.Path]::IsPathRooted($Left) -or
            -not [System.IO.Path]::IsPathRooted($Right)
        ) {
            return $false
        }
        return [string]::Equals(
            [System.IO.Path]::GetFullPath($Left).TrimEnd('\', '/'),
            [System.IO.Path]::GetFullPath($Right).TrimEnd('\', '/'),
            [System.StringComparison]::OrdinalIgnoreCase
        )
    } catch {
        return $false
    }
}

function Test-L2CanonicalPathEqual {
    param(
        [Parameter(Mandatory = $true)]
        [string]$Left,
        [Parameter(Mandatory = $true)]
        [string]$Right
    )

    $LeftPath = Get-L2PhysicalPathDescriptor -Path $Left
    $RightPath = Get-L2PhysicalPathDescriptor -Path $Right
    if (-not [string]::Equals(
        $LeftPath.ResolvedPath,
        $RightPath.ResolvedPath,
        [System.StringComparison]::OrdinalIgnoreCase
    )) {
        return $false
    }
    if ($LeftPath.Identity -and $RightPath.Identity) {
        return [string]::Equals(
            $LeftPath.Identity,
            $RightPath.Identity,
            [System.StringComparison]::OrdinalIgnoreCase
        )
    }
    return $true
}

function Assert-L2KnownOptionSpellings {
    param(
        [Parameter(Mandatory = $true)]
        [string[]]$Tokens,
        [Parameter(Mandatory = $true)]
        [string[]]$Names
    )

    foreach ($TokenValue in $Tokens) {
        $Token = [string]$TokenValue
        if (-not $Token.StartsWith('-')) {
            continue
        }
        $EqualsIndex = $Token.IndexOf('=')
        $Base = if ($EqualsIndex -gt 0) {
            $Token.Substring(0, $EqualsIndex)
        } else {
            $Token
        }
        foreach ($Name in $Names) {
            if ($Base -ieq $Name) {
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
                throw "L2 materializer option has an opaque form: $Token"
            }
        }
    }
}

function Get-L2OptionValues {
    param(
        [Parameter(Mandatory = $true)]
        [string[]]$Tokens,
        [Parameter(Mandatory = $true)]
        [string]$Name,
        [switch]$AllowEquals
    )

    $Values = @()
    for ($Index = 0; $Index -lt $Tokens.Count; $Index += 1) {
        $Token = [string]$Tokens[$Index]
        if ($Token -ieq $Name) {
            if ($Index + 1 -ge $Tokens.Count) {
                throw "L2 materializer option has no value: $Name"
            }
            $Values += [string]$Tokens[$Index + 1]
            continue
        }
        $EqualsPrefix = "$Name="
        if (
            $AllowEquals -and
            $Token.StartsWith(
                $EqualsPrefix,
                [System.StringComparison]::OrdinalIgnoreCase
            )
        ) {
            $Values += $Token.Substring($EqualsPrefix.Length)
            continue
        }
        if (
            $Token.StartsWith('-') -and (
                $Token.StartsWith(
                    $Name,
                    [System.StringComparison]::OrdinalIgnoreCase
                ) -or
                $Name.StartsWith(
                    $Token,
                    [System.StringComparison]::OrdinalIgnoreCase
                )
            )
        ) {
            throw "L2 materializer option has an opaque form: $Token"
        }
    }
    return $Values
}

function Get-L2SelectionFromTokens {
    param(
        [Parameter(Mandatory = $true)]
        [string[]]$Tokens,
        [ValidateSet('python', 'runner')]
        [string]$Kind
    )

    $LatestName = if ($Kind -eq 'python') {
        '--latest-run-only'
    } else {
        '-LatestRunOnly'
    }
    $BoundedName = if ($Kind -eq 'python') {
        '--latest-sealed-segments-per-stream'
    } else {
        '-LatestSealedSegmentsPerStream'
    }
    $LatestCount = 0
    foreach ($Token in $Tokens) {
        if ($Token -ieq $LatestName) {
            $LatestCount += 1
        } elseif (
            $Token.StartsWith('-') -and (
                $Token.StartsWith(
                    $LatestName,
                    [System.StringComparison]::OrdinalIgnoreCase
                ) -or
                $LatestName.StartsWith(
                    $Token,
                    [System.StringComparison]::OrdinalIgnoreCase
                )
            )
        ) {
            throw "L2 materializer switch has an opaque form: $Token"
        }
    }
    if ($LatestCount -gt 1) {
        throw "L2 materializer switch is repeated: $LatestName"
    }
    $BoundedValues = @(
        Get-L2OptionValues `
            -Tokens $Tokens `
            -Name $BoundedName `
            -AllowEquals:($Kind -eq 'python')
    )
    if ($BoundedValues.Count -gt 1) {
        throw "L2 materializer option is repeated: $BoundedName"
    }
    if ($LatestCount -eq 1 -and $BoundedValues.Count -eq 1) {
        throw 'L2 materializer input selections are mutually exclusive.'
    }
    if ($LatestCount -eq 1) {
        return 'latest_run'
    }
    if ($BoundedValues.Count -eq 1) {
        $Limit = 0
        if (
            -not [int]::TryParse($BoundedValues[0], [ref]$Limit) -or
            $Limit -le 0
        ) {
            throw 'L2 materializer bounded selection is not a positive integer.'
        }
        return "latest_sealed_per_stream:$Limit"
    }
    return 'all'
}

function Get-L2MaterializerProcessContract {
    param(
        [Parameter(Mandatory = $true)]
        [string]$ProcessName,
        [Parameter(Mandatory = $true)]
        [string]$CommandLine,
        [Parameter(Mandatory = $true)]
        [int]$ProcessId,
        [Parameter(Mandatory = $true)]
        [string]$RepositoryRoot,
        [Parameter(Mandatory = $true)]
        [string]$DataRoot,
        [Parameter(Mandatory = $true)]
        [string]$RunnerPath,
        [Parameter(Mandatory = $true)]
        [string]$ExpectedPythonPath,
        [string]$ExpectedPythonBasePath = '',
        [string]$ExecutablePath = ''
    )

    $IsPython = $ProcessName -match '^(?i:pythonw?)(?:\.exe)?$'
    $IsPowerShell = $ProcessName -match '^(?i:powershell|pwsh)(?:\.exe)?$'
    if (-not $IsPython -and -not $IsPowerShell) {
        return $null
    }
    $Module = 'guvolu.data.l2_materialize'
    $RunnerName = [System.IO.Path]::GetFileName($RunnerPath)
    $LooksLikePythonWatch = (
        $IsPython -and
        $CommandLine -match (
            '(?i)(?:^|[\s"''])' + [regex]::Escape($Module) +
            '(?:$|[\s"''])'
        ) -and
        $CommandLine -match '(?i)(?:^|[\s"''])watch(?:$|[\s"''])'
    )
    $LooksLikeRunner = (
        $IsPowerShell -and
        $CommandLine.IndexOf(
            $RunnerName,
            [System.StringComparison]::OrdinalIgnoreCase
        ) -ge 0
    )
    if (-not $LooksLikePythonWatch -and -not $LooksLikeRunner) {
        return $null
    }

    try {
        $Tokens = @(ConvertTo-L2ProcessCommandTokens -CommandLine $CommandLine)
        if ($LooksLikePythonWatch) {
            $ModuleIndices = @()
            $ModuleSwitchIndices = @()
            $WatchIndices = @()
            for ($Index = 0; $Index -lt $Tokens.Count; $Index += 1) {
                if ($Tokens[$Index] -ieq $Module) {
                    $ModuleIndices += $Index
                }
                if ($Tokens[$Index] -ieq '-m') {
                    $ModuleSwitchIndices += $Index
                }
                if ($Tokens[$Index] -ieq 'watch') {
                    $WatchIndices += $Index
                }
            }
            if (
                $ModuleIndices.Count -ne 1 -or
                $ModuleSwitchIndices.Count -ne 1 -or
                $ModuleIndices[0] -ne ($ModuleSwitchIndices[0] + 1) -or
                $WatchIndices.Count -ne 1 -or
                $WatchIndices[0] -le $ModuleIndices[0]
            ) {
                throw 'Python L2 watch module/command identity is ambiguous.'
            }
            $ModuleArguments = @(
                $Tokens[($ModuleIndices[0] + 1)..($Tokens.Count - 1)]
            )
            Assert-L2KnownOptionSpellings `
                -Tokens $ModuleArguments `
                -Names @(
                    '--data-root',
                    '--latest-run-only',
                    '--latest-sealed-segments-per-stream',
                    '--interval-seconds'
                )
            $Roots = @(
                Get-L2OptionValues `
                    -Tokens $ModuleArguments `
                    -Name '--data-root' -AllowEquals
            )
            if ($Roots.Count -ne 1) {
                throw 'Python L2 watch data-root identity is ambiguous.'
            }
            if (-not [System.IO.Path]::IsPathRooted($Roots[0])) {
                throw 'Python L2 watch data-root is not an absolute path.'
            }
            if (-not (Test-L2CanonicalPathEqual $Roots[0] $DataRoot)) {
                return $null
            }
            $CommandExecutable = [string]$Tokens[0]
            if (
                [System.IO.Path]::GetFileName($CommandExecutable) -ine
                [System.IO.Path]::GetFileName($ExpectedPythonPath) -or
                [System.IO.Path]::GetFileName($CommandExecutable) -ine
                [System.IO.Path]::GetFileName($ProcessName)
            ) {
                throw 'Python L2 watch interpreter identity differs.'
            }
            if (-not $ExecutablePath) {
                throw 'Python L2 watch executable identity is opaque.'
            }
            $ExecutableMatchesProject = (
                [System.IO.Path]::IsPathRooted($ExecutablePath) -and (
                    (Test-L2LexicalPathEqual `
                        $ExecutablePath $ExpectedPythonPath
                    ) -or
                    (Test-L2CanonicalPathEqual `
                        $ExecutablePath $ExpectedPythonPath
                    )
                )
            )
            if (
                -not $ExecutableMatchesProject -and
                $ExpectedPythonBasePath
            ) {
                $ExecutableMatchesProject = (
                    [System.IO.Path]::IsPathRooted(
                        $ExpectedPythonBasePath
                    ) -and (
                        (Test-L2LexicalPathEqual `
                            $ExecutablePath $ExpectedPythonBasePath
                        ) -or
                        (Test-L2CanonicalPathEqual `
                            $ExecutablePath $ExpectedPythonBasePath
                        )
                    )
                )
            }
            if (-not $ExecutableMatchesProject) {
                throw (
                    'Python L2 watch executable identity differs; actual=' +
                    $ExecutablePath
                )
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
                        'Python L2 watch interpreter entry is opaque: ' +
                        [string]$Tokens[$Index]
                    )
                }
            }
            $Selection = Get-L2SelectionFromTokens `
                -Tokens $ModuleArguments -Kind 'python'
            return [pscustomobject]@{
                ProcessId = $ProcessId
                Kind = 'python'
                Selection = $Selection
                ExecutablePath = $ExecutablePath
                ArgumentSignature = [string]::Join(
                    [char]0,
                    [string[]]$Tokens[1..($Tokens.Count - 1)]
                )
                CommandLine = $CommandLine
            }
        }

        $RunnerIndices = @()
        for ($Index = 0; $Index -lt $Tokens.Count; $Index += 1) {
            $Leaf = [System.IO.Path]::GetFileName([string]$Tokens[$Index])
            if ($Leaf -ieq $RunnerName) {
                $RunnerIndices += $Index
            }
        }
        if ($RunnerIndices.Count -ne 1) {
            throw 'PowerShell L2 runner path identity is ambiguous.'
        }
        $RunnerIndex = $RunnerIndices[0]
        Assert-L2PowerShellFileEntry `
            -Tokens $Tokens `
            -ProcessName $ProcessName `
            -FilePathIndex $RunnerIndex
        if (-not [System.IO.Path]::IsPathRooted($Tokens[$RunnerIndex])) {
            throw 'PowerShell L2 runner path is not absolute.'
        }
        if (-not (Test-L2CanonicalPathEqual $Tokens[$RunnerIndex] $RunnerPath)) {
            return $null
        }
        Assert-L2KnownOptionSpellings `
            -Tokens $Tokens `
            -Names @(
                '-Repository',
                '-LatestRunOnly',
                '-LatestSealedSegmentsPerStream',
                '-IntervalSeconds'
            )
        $Repositories = @(
            Get-L2OptionValues -Tokens $Tokens -Name '-Repository'
        )
        if ($Repositories.Count -ne 1) {
            throw 'PowerShell L2 runner repository identity is ambiguous.'
        }
        if (-not [System.IO.Path]::IsPathRooted($Repositories[0])) {
            throw 'PowerShell L2 runner repository is not an absolute path.'
        }
        if (
            -not (Test-L2CanonicalPathEqual $Repositories[0] $RepositoryRoot)
        ) {
            return $null
        }
        $Selection = Get-L2SelectionFromTokens `
            -Tokens $Tokens -Kind 'runner'
        return [pscustomobject]@{
            ProcessId = $ProcessId
            Kind = 'runner'
            Selection = $Selection
            ExecutablePath = $ExecutablePath
            CommandLine = $CommandLine
        }
    } catch {
        throw (
            '[l2-materializer] existing process is opaque; refusing any ' +
            "pipeline side effect. PID=$ProcessId; $($_.Exception.Message)"
        )
    }
}

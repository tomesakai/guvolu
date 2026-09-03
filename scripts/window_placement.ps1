<#
.SYNOPSIS
窗口安置辅助：控制台窗口移入副屏且不夺焦。
.DESCRIPTION
点源本文件后调用 Move-ProcessWindowToSecondary。
仅单屏时不移动；任何失败只写警告，不中断调用方。
本机默认终端为 Windows Terminal 时，正常启动的窗口
由其托管并夺焦；故启动方应先隐藏启动，再以
-RevealHidden 无激活显示。隐藏启动的控制台不经
Windows Terminal 托管，显示后仍是 conhost 窗口。
#>
if (-not ('Guvolu.WindowPlacement.Native' -as [type])) {
    Add-Type -TypeDefinition @'
using System;
using System.Collections.Generic;
using System.Runtime.InteropServices;
using System.Text;
namespace Guvolu.WindowPlacement {
    [StructLayout(LayoutKind.Sequential)]
    public struct Rect {
        public int Left;
        public int Top;
        public int Right;
        public int Bottom;
    }
    public static class Native {
        public const uint SWP_NOSIZE = 0x0001;
        public const uint SWP_NOZORDER = 0x0004;
        public const uint SWP_NOACTIVATE = 0x0010;
        public const int SW_SHOWNOACTIVATE = 4;
        public delegate bool EnumWindowsProc(IntPtr hWnd, IntPtr lParam);
        [DllImport("user32.dll", SetLastError = true)]
        public static extern bool SetWindowPos(
            IntPtr hWnd, IntPtr hWndInsertAfter,
            int x, int y, int cx, int cy, uint uFlags);
        [DllImport("user32.dll")]
        public static extern bool ShowWindow(IntPtr hWnd, int nCmdShow);
        [DllImport("user32.dll")]
        public static extern bool IsWindow(IntPtr hWnd);
        [DllImport("user32.dll")]
        public static extern bool IsWindowVisible(IntPtr hWnd);
        [DllImport("user32.dll")]
        public static extern bool IsIconic(IntPtr hWnd);
        [DllImport("user32.dll")]
        public static extern IntPtr GetForegroundWindow();
        [DllImport("user32.dll", SetLastError = true)]
        public static extern bool GetWindowRect(IntPtr hWnd, out Rect rect);
        [DllImport("user32.dll")]
        public static extern bool EnumWindows(EnumWindowsProc callback, IntPtr lParam);
        [DllImport("user32.dll")]
        public static extern uint GetWindowThreadProcessId(IntPtr hWnd, out uint processId);
        [DllImport("user32.dll")]
        public static extern IntPtr GetWindow(IntPtr hWnd, uint uCmd);
        [DllImport("user32.dll", CharSet = CharSet.Unicode)]
        public static extern int GetClassName(IntPtr hWnd, StringBuilder buffer, int size);
        [DllImport("kernel32.dll")]
        public static extern IntPtr GetConsoleWindow();
        // 枚举进程的无属主顶层窗口，含隐藏
        public static List<IntPtr> FindWindowsForProcess(uint processId) {
            var found = new List<IntPtr>();
            EnumWindows((hWnd, lParam) => {
                uint owner;
                GetWindowThreadProcessId(hWnd, out owner);
                if (owner == processId && GetWindow(hWnd, 4) == IntPtr.Zero) {
                    found.Add(hWnd);
                }
                return true;
            }, IntPtr.Zero);
            return found;
        }
        public static string ClassNameOf(IntPtr hWnd) {
            var buffer = new StringBuilder(256);
            GetClassName(hWnd, buffer, buffer.Capacity);
            return buffer.ToString();
        }
    }
}
'@
}
Add-Type -AssemblyName System.Windows.Forms

# 级联计数跨进程共享，缺失时退回进程内
$script:GuvoluCascadeCounterPath = Join-Path `
    ([Environment]::GetFolderPath('LocalApplicationData')) `
    'guvolu\window-cascade.txt'
$script:GuvoluCascadeIndex = -1

function Get-SecondaryScreen {
    $Screens = @([System.Windows.Forms.Screen]::AllScreens)
    if ($Screens.Count -lt 2) {
        return $null
    }
    return @($Screens | Where-Object { -not $_.Primary })[0]
}

function Get-NextCascadeIndex {
    $Index = $null
    try {
        $Directory = Split-Path -Parent $script:GuvoluCascadeCounterPath
        New-Item -ItemType Directory -Force -Path $Directory | Out-Null
        $Stream = [System.IO.File]::Open(
            $script:GuvoluCascadeCounterPath,
            [System.IO.FileMode]::OpenOrCreate,
            [System.IO.FileAccess]::ReadWrite,
            [System.IO.FileShare]::None)
        try {
            $Reader = New-Object System.IO.StreamReader($Stream)
            $Text = $Reader.ReadToEnd().Trim()
            $Previous = 0
            if ($Text -and [int]::TryParse($Text, [ref]$Previous)) {
                $Index = $Previous + 1
            } else {
                $Index = 0
            }
            $Stream.SetLength(0)
            $Writer = New-Object System.IO.StreamWriter($Stream)
            $Writer.Write([string]$Index)
            $Writer.Flush()
        } finally {
            $Stream.Dispose()
        }
    } catch {
        $Index = $null
    }
    if ($null -eq $Index) {
        $script:GuvoluCascadeIndex += 1
        $Index = $script:GuvoluCascadeIndex
    }
    return [int]$Index
}

function Select-ConsoleWindow {
    param([IntPtr[]]$Handles)
    $Native = [Guvolu.WindowPlacement.Native]
    foreach ($Handle in $Handles) {
        if ($Native::ClassNameOf($Handle) -eq 'ConsoleWindowClass') {
            return $Handle
        }
    }
    if ($Handles.Count -gt 0) {
        return $Handles[0]
    }
    return [IntPtr]::Zero
}

function Find-ProcessWindowHandle {
    param(
        [Parameter(Mandatory = $true)]
        [int]$ProcessId,
        [int]$TimeoutSeconds = 10
    )
    $Native = [Guvolu.WindowPlacement.Native]
    if ($ProcessId -eq $PID) {
        # 自身进程直接取控制台窗口句柄
        $Own = $Native::GetConsoleWindow()
        if ($Own -ne [IntPtr]::Zero) {
            return $Own
        }
    }
    $Deadline = [datetime]::UtcNow.AddSeconds($TimeoutSeconds)
    do {
        $Process = Get-Process -Id $ProcessId -ErrorAction SilentlyContinue
        if ($null -eq $Process) {
            return [IntPtr]::Zero
        }
        $Process.Refresh()
        if ($Process.MainWindowHandle -ne [IntPtr]::Zero) {
            return $Process.MainWindowHandle
        }
        # 隐藏窗口不计入主窗口，按进程枚举顶层窗口
        $Handle = Select-ConsoleWindow `
            -Handles @($Native::FindWindowsForProcess([uint32]$ProcessId))
        if ($Handle -ne [IntPtr]::Zero) {
            return $Handle
        }
        # 退回查找子 conhost 的窗口
        $Children = @(
            Get-CimInstance Win32_Process `
                -Filter "ParentProcessId=$ProcessId AND Name='conhost.exe'" `
                -ErrorAction SilentlyContinue
        )
        foreach ($Child in $Children) {
            $Handle = Select-ConsoleWindow -Handles @(
                $Native::FindWindowsForProcess([uint32]$Child.ProcessId))
            if ($Handle -ne [IntPtr]::Zero) {
                return $Handle
            }
        }
        Start-Sleep -Milliseconds 100
    } while ([datetime]::UtcNow -lt $Deadline)
    return [IntPtr]::Zero
}

function Move-ProcessWindowToSecondary {
    <#
    .SYNOPSIS
    把指定进程的主窗口移入副屏工作区，不激活、不改层序。
    .DESCRIPTION
    等待进程出现窗口句柄，选取非主显示器，按级联偏移放置，
    保持窗口原有尺寸。仅一块显示器时不移动。
    隐藏窗口默认保持隐藏；指定 -RevealHidden 时以
    SW_SHOWNOACTIVATE 显示。最小化窗口同样无激活还原。
    绝不调用 SetForegroundWindow。返回 [bool]，是否已移动。
    #>
    [CmdletBinding()]
    param(
        [Parameter(Mandatory = $true)]
        [int]$ProcessId,
        [ValidateRange(0, 600)]
        [int]$TimeoutSeconds = 10,
        [ValidateRange(0, 4000)]
        [int]$CascadeStep = 40,
        [switch]$RevealHidden
    )
    $Native = [Guvolu.WindowPlacement.Native]
    $Handle = Find-ProcessWindowHandle `
        -ProcessId $ProcessId -TimeoutSeconds $TimeoutSeconds
    if ($Handle -eq [IntPtr]::Zero) {
        Write-Warning "[window-placement] PID=$ProcessId 未找到窗口句柄"
        return $false
    }
    if ($Native::IsIconic($Handle)) {
        # 最小化窗口还原但不激活
        $Native::ShowWindow($Handle, $Native::SW_SHOWNOACTIVATE) | Out-Null
    } elseif (-not $Native::IsWindowVisible($Handle)) {
        if (-not $RevealHidden) {
            return $false
        }
        $Native::ShowWindow($Handle, $Native::SW_SHOWNOACTIVATE) | Out-Null
    }
    $Screen = Get-SecondaryScreen
    if ($null -eq $Screen) {
        return $false
    }
    $Rect = New-Object Guvolu.WindowPlacement.Rect
    if (-not $Native::GetWindowRect($Handle, [ref]$Rect)) {
        Write-Warning "[window-placement] PID=$ProcessId 无法读取窗口矩形"
        return $false
    }
    $Width = $Rect.Right - $Rect.Left
    $Height = $Rect.Bottom - $Rect.Top
    $Area = $Screen.WorkingArea
    $Index = Get-NextCascadeIndex
    $SlackX = [Math]::Max(1, $Area.Width - $Width + 1)
    $SlackY = [Math]::Max(1, $Area.Height - $Height + 1)
    $X = $Area.X + (($Index * $CascadeStep) % $SlackX)
    $Y = $Area.Y + (($Index * $CascadeStep) % $SlackY)
    $Flags = $Native::SWP_NOACTIVATE -bor $Native::SWP_NOZORDER -bor `
        $Native::SWP_NOSIZE
    $Moved = $Native::SetWindowPos(
        $Handle, [IntPtr]::Zero, $X, $Y, 0, 0, $Flags)
    if (-not $Moved) {
        $Code = [Runtime.InteropServices.Marshal]::GetLastWin32Error()
        Write-Warning "[window-placement] SetWindowPos 失败 Win32=$Code"
        return $false
    }
    return $true
}

function Move-StartedWindowToSecondary {
    <#
    .SYNOPSIS
    启动方专用：显示隐藏启动的窗口并移入副屏，失败只写一行警告。
    #>
    param(
        [Parameter(Mandatory = $true)]
        [int]$ProcessId,
        [int]$TimeoutSeconds = 10
    )
    try {
        Move-ProcessWindowToSecondary -ProcessId $ProcessId `
            -TimeoutSeconds $TimeoutSeconds -RevealHidden | Out-Null
    } catch {
        Write-Warning (
            "[window-placement] PID=$ProcessId 窗口安置失败: " +
            $_.Exception.Message
        )
    }
}

function Move-OwnWindowToSecondary {
    <#
    .SYNOPSIS
    把当前进程自身的控制台窗口移入副屏，隐藏窗口保持隐藏。
    #>
    try {
        Move-ProcessWindowToSecondary -ProcessId $PID -TimeoutSeconds 1 | Out-Null
    } catch {
        Write-Warning "[window-placement] 自身窗口安置失败: $($_.Exception.Message)"
    }
}

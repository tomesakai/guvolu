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
    [Parameter(Mandatory = $true)]
    [ValidatePattern('^[0-9a-f]{64}$')]
    [string]$ExpectedWrapperSha256,
    [Parameter(Mandatory = $true)]
    [ValidatePattern('^[0-9a-f]{64}$')]
    [string]$ExpectedPythonSha256,
    [Parameter(Mandatory = $true)]
    [ValidatePattern('^[0-9a-f]{64}$')]
    [string]$ExpectedPyVenvSha256,
    [Parameter(Mandatory = $true)]
    [ValidatePattern('^[0-9a-f]{64}$')]
    [string]$ExpectedGovernanceRunnerSha256,
    [Parameter(Mandatory = $true)]
    [ValidatePattern('^[0-9a-f]{64}$')]
    [string]$ExpectedGitSha256,
    [Parameter(Mandatory = $true)]
    [ValidatePattern('^[0-9a-f]{64}$')]
    [string]$ExpectedVenvTreeSha256,
    [Parameter(Mandatory = $true)]
    [ValidateRange(1, 100000)]
    [int]$ExpectedVenvFileCount,
    [Parameter(Mandatory = $true)]
    [ValidateRange(1, 17179869184)]
    [long]$ExpectedVenvTotalBytes,
    [Parameter(Mandatory = $true)]
    [ValidatePattern('^[0-9a-f]{64}$')]
    [string]$ExpectedRuntimeSourceTreeSha256,
    [Parameter(Mandatory = $true)]
    [ValidateRange(0, 100000)]
    [int]$ExpectedRuntimeSourceFileCount,
    [Parameter(Mandatory = $true)]
    [ValidateRange(0, 17179869184)]
    [long]$ExpectedRuntimeSourceTotalBytes,
    [Parameter(Mandatory = $true)]
    [ValidatePattern('^[0-9a-f]{64}$')]
    [string]$ExpectedCodeSourceTreeSha256,
    [Parameter(Mandatory = $true)]
    [ValidateRange(1, 100000)]
    [int]$ExpectedCodeSourceFileCount,
    [Parameter(Mandatory = $true)]
    [ValidateRange(1, 17179869184)]
    [long]$ExpectedCodeSourceTotalBytes,
    [Parameter(Mandatory = $true)]
    [ValidatePattern('^[0-9a-f]{64}$')]
    [string]$ExpectedBaseRuntimeTreeSha256,
    [Parameter(Mandatory = $true)]
    [ValidateRange(1, 100000)]
    [int]$ExpectedBaseRuntimeFileCount,
    [Parameter(Mandatory = $true)]
    [ValidateRange(1, 17179869184)]
    [long]$ExpectedBaseRuntimeTotalBytes,
    [ValidateRange(1, 1500)]
    [int]$ExecutionTimeoutSeconds = 1500,
    [string]$VintageId = ""
)

Set-StrictMode -Version Latest
$ErrorActionPreference = "Stop"

if (-not ("Guvolu.PreflightNative" -as [type])) {
    Add-Type -TypeDefinition @'
using System;
using System.Collections.Generic;
using System.ComponentModel;
using System.Diagnostics;
using System.IO;
using System.Runtime.InteropServices;
using System.Text;
using System.Threading;

namespace Guvolu {
    public static class PreflightNative {
        private const UInt32 FILE_READ_ATTRIBUTES = 0x80;
        private const UInt32 FILE_SHARE_READ = 0x1;
        private const UInt32 FILE_SHARE_WRITE = 0x2;
        private const UInt32 OPEN_EXISTING = 3;
        private const UInt32 FILE_FLAG_BACKUP_SEMANTICS = 0x02000000;
        private const UInt32 FILE_FLAG_OPEN_REPARSE_POINT = 0x00200000;
        private const UInt32 FILE_FLAG_OVERLAPPED = 0x40000000;
        private const UInt32 JOB_OBJECT_LIMIT_KILL_ON_JOB_CLOSE = 0x2000;
        private const UInt32 FSCTL_REQUEST_OPLOCK = 0x00090240;
        private const UInt32 ERROR_IO_PENDING = 997;
        private const UInt32 ERROR_IO_INCOMPLETE = 996;
        private const UInt32 ERROR_OPERATION_ABORTED = 995;
        private const UInt32 ERROR_NOT_FOUND = 1168;
        private const UInt32 CREATE_SUSPENDED = 0x00000004;
        private const UInt32 CREATE_UNICODE_ENVIRONMENT = 0x00000400;
        private const UInt32 CREATE_NO_WINDOW = 0x08000000;
        private const UInt32 EXTENDED_STARTUPINFO_PRESENT = 0x00080000;
        private const UInt32 STARTF_USESTDHANDLES = 0x00000100;
        private const UInt32 HANDLE_FLAG_INHERIT = 0x00000001;
        private const UInt32 WAIT_OBJECT_0 = 0;
        private const UInt32 WAIT_TIMEOUT = 258;
        private const UInt32 STILL_ACTIVE = 259;
        private static readonly IntPtr PROC_THREAD_ATTRIBUTE_HANDLE_LIST =
            new IntPtr(0x00020002);
        private static readonly IntPtr INVALID_HANDLE_VALUE = new IntPtr(-1);
        private static readonly object OplockSync = new object();
        private static readonly Dictionary<IntPtr, OplockState> Oplocks =
            new Dictionary<IntPtr, OplockState>();

        private sealed class OplockState {
            public IntPtr Event;
            public IntPtr Input;
            public IntPtr Output;
            public IntPtr Overlapped;
        }

        public sealed class BoundedProcessResult {
            public Int32 ExitCode;
            public bool TimedOut;
            public bool OutputLimitExceeded;
            public string StandardOutput;
            public string StandardError;
        }

        [StructLayout(LayoutKind.Sequential)]
        private struct SECURITY_ATTRIBUTES {
            public UInt32 Length;
            public IntPtr SecurityDescriptor;
            [MarshalAs(UnmanagedType.Bool)] public bool InheritHandle;
        }

        [StructLayout(LayoutKind.Sequential, CharSet = CharSet.Unicode)]
        private struct STARTUPINFO {
            public UInt32 cb;
            public string lpReserved;
            public string lpDesktop;
            public string lpTitle;
            public UInt32 dwX;
            public UInt32 dwY;
            public UInt32 dwXSize;
            public UInt32 dwYSize;
            public UInt32 dwXCountChars;
            public UInt32 dwYCountChars;
            public UInt32 dwFillAttribute;
            public UInt32 dwFlags;
            public UInt16 wShowWindow;
            public UInt16 cbReserved2;
            public IntPtr lpReserved2;
            public IntPtr hStdInput;
            public IntPtr hStdOutput;
            public IntPtr hStdError;
        }

        [StructLayout(LayoutKind.Sequential)]
        private struct PROCESS_INFORMATION {
            public IntPtr hProcess;
            public IntPtr hThread;
            public UInt32 dwProcessId;
            public UInt32 dwThreadId;
        }

        [StructLayout(LayoutKind.Sequential)]
        private struct STARTUPINFOEX {
            public STARTUPINFO StartupInfo;
            public IntPtr AttributeList;
        }

        [StructLayout(LayoutKind.Sequential)]
        private struct REQUEST_OPLOCK_INPUT_BUFFER {
            public UInt16 StructureVersion;
            public UInt16 StructureLength;
            public UInt32 RequestedOplockLevel;
            public UInt32 Flags;
        }

        [StructLayout(LayoutKind.Sequential)]
        private struct REQUEST_OPLOCK_OUTPUT_BUFFER {
            public UInt16 StructureVersion;
            public UInt16 StructureLength;
            public UInt32 OriginalOplockLevel;
            public UInt32 NewOplockLevel;
            public UInt32 Flags;
            public UInt32 AccessMode;
            public UInt16 ShareMode;
        }

        [StructLayout(LayoutKind.Sequential)]
        private struct OVERLAPPED_BUFFER {
            public IntPtr Internal;
            public IntPtr InternalHigh;
            public UInt32 Offset;
            public UInt32 OffsetHigh;
            public IntPtr Event;
        }

        [StructLayout(LayoutKind.Sequential)]
        private struct JOBOBJECT_BASIC_LIMIT_INFORMATION {
            public Int64 PerProcessUserTimeLimit;
            public Int64 PerJobUserTimeLimit;
            public UInt32 LimitFlags;
            public UIntPtr MinimumWorkingSetSize;
            public UIntPtr MaximumWorkingSetSize;
            public UInt32 ActiveProcessLimit;
            public Int64 Affinity;
            public UInt32 PriorityClass;
            public UInt32 SchedulingClass;
        }

        [StructLayout(LayoutKind.Sequential)]
        private struct IO_COUNTERS {
            public UInt64 ReadOperationCount;
            public UInt64 WriteOperationCount;
            public UInt64 OtherOperationCount;
            public UInt64 ReadTransferCount;
            public UInt64 WriteTransferCount;
            public UInt64 OtherTransferCount;
        }

        [StructLayout(LayoutKind.Sequential)]
        private struct JOBOBJECT_EXTENDED_LIMIT_INFORMATION {
            public JOBOBJECT_BASIC_LIMIT_INFORMATION BasicLimitInformation;
            public IO_COUNTERS IoInfo;
            public UIntPtr ProcessMemoryLimit;
            public UIntPtr JobMemoryLimit;
            public UIntPtr PeakProcessMemoryUsed;
            public UIntPtr PeakJobMemoryUsed;
        }

        [DllImport("kernel32.dll", CharSet = CharSet.Unicode,
            SetLastError = true)]
        private static extern IntPtr CreateFileW(
            string name, UInt32 access, UInt32 share, IntPtr security,
            UInt32 disposition, UInt32 flags, IntPtr templateFile);

        [DllImport("kernel32.dll", SetLastError = true)]
        private static extern bool DeviceIoControl(
            IntPtr handle, UInt32 controlCode, IntPtr input,
            UInt32 inputLength, IntPtr output, UInt32 outputLength,
            IntPtr bytesReturned, IntPtr overlapped);

        [DllImport("kernel32.dll", SetLastError = true)]
        private static extern IntPtr CreateEventW(
            IntPtr attributes, bool manualReset, bool initialState, string name);

        [DllImport("kernel32.dll", SetLastError = true)]
        private static extern bool CancelIoEx(IntPtr handle, IntPtr overlapped);

        [DllImport("kernel32.dll", SetLastError = true)]
        private static extern bool GetOverlappedResult(
            IntPtr handle, IntPtr overlapped, out UInt32 transferred, bool wait);

        [DllImport("kernel32.dll", CharSet = CharSet.Unicode,
            SetLastError = true)]
        private static extern IntPtr CreateJobObjectW(
            IntPtr security, string name);

        [DllImport("kernel32.dll", SetLastError = true)]
        private static extern bool SetInformationJobObject(
            IntPtr job, Int32 infoClass,
            ref JOBOBJECT_EXTENDED_LIMIT_INFORMATION info,
            UInt32 length);

        [DllImport("kernel32.dll", SetLastError = true)]
        private static extern bool AssignProcessToJobObject(
            IntPtr job, IntPtr process);

        [DllImport("kernel32.dll", SetLastError = true)]
        private static extern bool TerminateJobObject(
            IntPtr job, UInt32 exitCode);

        [DllImport("kernel32.dll", SetLastError = true)]
        private static extern bool CloseHandle(IntPtr handle);

        [DllImport("kernel32.dll", SetLastError = true)]
        private static extern bool CreatePipe(
            out IntPtr readPipe, out IntPtr writePipe,
            ref SECURITY_ATTRIBUTES attributes, UInt32 size);

        [DllImport("kernel32.dll", SetLastError = true)]
        private static extern bool SetHandleInformation(
            IntPtr handle, UInt32 mask, UInt32 flags);

        [DllImport("kernel32.dll", CharSet = CharSet.Unicode,
            SetLastError = true)]
        private static extern bool CreateProcessW(
            string applicationName, StringBuilder commandLine,
            IntPtr processAttributes, IntPtr threadAttributes,
            bool inheritHandles, UInt32 creationFlags,
            IntPtr environment, string currentDirectory,
            ref STARTUPINFOEX startupInfo,
            out PROCESS_INFORMATION processInformation);

        [DllImport("kernel32.dll", SetLastError = true)]
        private static extern bool InitializeProcThreadAttributeList(
            IntPtr attributeList, Int32 attributeCount, UInt32 flags,
            ref IntPtr size);

        [DllImport("kernel32.dll", SetLastError = true)]
        private static extern bool UpdateProcThreadAttribute(
            IntPtr attributeList, UInt32 flags, IntPtr attribute,
            IntPtr value, IntPtr size, IntPtr previousValue,
            IntPtr returnSize);

        [DllImport("kernel32.dll")]
        private static extern void DeleteProcThreadAttributeList(
            IntPtr attributeList);

        [DllImport("kernel32.dll", SetLastError = true)]
        private static extern UInt32 ResumeThread(IntPtr thread);

        [DllImport("kernel32.dll", SetLastError = true)]
        private static extern UInt32 WaitForSingleObject(
            IntPtr handle, UInt32 milliseconds);

        [DllImport("kernel32.dll", SetLastError = true)]
        private static extern bool GetExitCodeProcess(
            IntPtr process, out UInt32 exitCode);

        [DllImport("kernel32.dll", SetLastError = true)]
        private static extern bool TerminateProcess(
            IntPtr process, UInt32 exitCode);

        [DllImport("kernel32.dll", SetLastError = true)]
        private static extern bool ReadFile(
            IntPtr file, byte[] buffer, UInt32 bytesToRead,
            out UInt32 bytesRead, IntPtr overlapped);

        [DllImport("kernel32.dll", SetLastError = true)]
        private static extern bool PeekNamedPipe(
            IntPtr pipe, IntPtr buffer, UInt32 bufferSize,
            IntPtr bytesRead, out UInt32 bytesAvailable,
            IntPtr bytesLeftThisMessage);

        private static void DrainPipeNonBlocking(
                IntPtr pipe, MemoryStream output, Int32 limit,
                out bool broken, out bool exceeded) {
            broken = false;
            exceeded = false;
            UInt32 available;
            if (!PeekNamedPipe(pipe, IntPtr.Zero, 0, IntPtr.Zero,
                    out available, IntPtr.Zero)) {
                int error = Marshal.GetLastWin32Error();
                if (error == 109) { broken = true; return; }
                throw new Win32Exception(error,
                    "cannot inspect isolated Python output pipe");
            }
            byte[] buffer = new byte[8192];
            while (available > 0) {
                UInt32 requested = Math.Min((UInt32)buffer.Length, available);
                UInt32 read;
                if (!ReadFile(pipe, buffer, requested, out read, IntPtr.Zero)) {
                    int error = Marshal.GetLastWin32Error();
                    if (error == 109) { broken = true; return; }
                    throw new Win32Exception(error,
                        "cannot read isolated Python output pipe");
                }
                Int32 remaining = limit - (Int32)output.Length;
                Int32 accepted = Math.Min(remaining, (Int32)read);
                if (accepted > 0) output.Write(buffer, 0, accepted);
                if (accepted != (Int32)read) { exceeded = true; return; }
                available -= read;
            }
        }

        private static IntPtr BuildEnvironmentBlock(string[] entries) {
            string[] sorted = (string[])entries.Clone();
            Array.Sort(sorted, StringComparer.OrdinalIgnoreCase);
            foreach (string entry in sorted) {
                if (String.IsNullOrEmpty(entry) || entry.IndexOf('\0') >= 0 ||
                        entry.IndexOf('=') <= 0) {
                    throw new ArgumentException("invalid isolated child environment");
                }
            }
            return Marshal.StringToHGlobalUni(
                String.Join("\0", sorted) + "\0\0");
        }

        private static void CloseHandleOnce(ref IntPtr handle, string name) {
            IntPtr closing = handle;
            handle = IntPtr.Zero;
            if (closing != IntPtr.Zero && closing != INVALID_HANDLE_VALUE &&
                    !CloseHandle(closing)) {
                throw new Win32Exception(Marshal.GetLastWin32Error(),
                    "cannot close " + name);
            }
        }

        public static BoundedProcessResult RunSuspendedCapped(
                string application, string commandLine,
                string workingDirectory, string[] environment,
                Int32 timeoutSeconds, Int32 outputLimitBytes,
                bool forceAssignFailureForTest) {
            if (timeoutSeconds <= 0 || outputLimitBytes <= 0) {
                throw new ArgumentOutOfRangeException();
            }
            IntPtr job = IntPtr.Zero;
            IntPtr stdoutRead = IntPtr.Zero;
            IntPtr stdoutWrite = IntPtr.Zero;
            IntPtr stderrRead = IntPtr.Zero;
            IntPtr stderrWrite = IntPtr.Zero;
            IntPtr stdinRead = IntPtr.Zero;
            IntPtr stdinWrite = IntPtr.Zero;
            IntPtr environmentBlock = IntPtr.Zero;
            IntPtr attributeList = IntPtr.Zero;
            IntPtr inheritedHandleList = IntPtr.Zero;
            bool attributeListInitialized = false;
            PROCESS_INFORMATION process = new PROCESS_INFORMATION();
            bool created = false;
            bool assigned = false;
            Exception primary = null;
            Exception cleanup = null;
            BoundedProcessResult result = null;
            MemoryStream stdoutBuffer = new MemoryStream();
            MemoryStream stderrBuffer = new MemoryStream();
            try {
                job = CreateKillOnCloseJob();
                SECURITY_ATTRIBUTES attributes = new SECURITY_ATTRIBUTES();
                attributes.Length = (UInt32)Marshal.SizeOf(attributes);
                attributes.InheritHandle = true;
                if (!CreatePipe(out stdoutRead, out stdoutWrite,
                        ref attributes, 0) ||
                    !CreatePipe(out stderrRead, out stderrWrite,
                        ref attributes, 0) ||
                    !CreatePipe(out stdinRead, out stdinWrite,
                        ref attributes, 0)) {
                    throw new Win32Exception(Marshal.GetLastWin32Error(),
                        "cannot create isolated Python pipes");
                }
                if (!SetHandleInformation(stdoutRead, HANDLE_FLAG_INHERIT, 0) ||
                    !SetHandleInformation(stderrRead, HANDLE_FLAG_INHERIT, 0) ||
                    !SetHandleInformation(stdinWrite, HANDLE_FLAG_INHERIT, 0)) {
                    throw new Win32Exception(Marshal.GetLastWin32Error(),
                        "cannot make isolated Python parent pipes non-inheritable");
                }
                STARTUPINFOEX startup = new STARTUPINFOEX();
                startup.StartupInfo.cb = (UInt32)Marshal.SizeOf(startup);
                startup.StartupInfo.dwFlags = STARTF_USESTDHANDLES;
                startup.StartupInfo.hStdInput = stdinRead;
                startup.StartupInfo.hStdOutput = stdoutWrite;
                startup.StartupInfo.hStdError = stderrWrite;
                IntPtr attributeBytes = IntPtr.Zero;
                InitializeProcThreadAttributeList(
                    IntPtr.Zero, 1, 0, ref attributeBytes);
                if (attributeBytes == IntPtr.Zero) {
                    throw new Win32Exception(Marshal.GetLastWin32Error(),
                        "cannot size isolated child handle allowlist");
                }
                attributeList = Marshal.AllocHGlobal(attributeBytes);
                if (!InitializeProcThreadAttributeList(
                        attributeList, 1, 0, ref attributeBytes)) {
                    throw new Win32Exception(Marshal.GetLastWin32Error(),
                        "cannot initialize isolated child handle allowlist");
                }
                attributeListInitialized = true;
                inheritedHandleList = Marshal.AllocHGlobal(IntPtr.Size * 3);
                Marshal.WriteIntPtr(inheritedHandleList, 0, stdinRead);
                Marshal.WriteIntPtr(inheritedHandleList, IntPtr.Size, stdoutWrite);
                Marshal.WriteIntPtr(
                    inheritedHandleList, IntPtr.Size * 2, stderrWrite);
                if (!UpdateProcThreadAttribute(
                        attributeList, 0, PROC_THREAD_ATTRIBUTE_HANDLE_LIST,
                        inheritedHandleList, new IntPtr(IntPtr.Size * 3),
                        IntPtr.Zero, IntPtr.Zero)) {
                    throw new Win32Exception(Marshal.GetLastWin32Error(),
                        "cannot bind isolated child handle allowlist");
                }
                startup.AttributeList = attributeList;
                environmentBlock = BuildEnvironmentBlock(environment);
                if (!CreateProcessW(
                        application, new StringBuilder(commandLine),
                        IntPtr.Zero, IntPtr.Zero, true,
                        CREATE_SUSPENDED | CREATE_UNICODE_ENVIRONMENT |
                            CREATE_NO_WINDOW | EXTENDED_STARTUPINFO_PRESENT,
                        environmentBlock, workingDirectory,
                        ref startup, out process)) {
                    throw new Win32Exception(Marshal.GetLastWin32Error(),
                        "cannot create suspended isolated Python process");
                }
                created = true;
                DeleteProcThreadAttributeList(attributeList);
                attributeListInitialized = false;
                Marshal.FreeHGlobal(attributeList);
                attributeList = IntPtr.Zero;
                Marshal.FreeHGlobal(inheritedHandleList);
                inheritedHandleList = IntPtr.Zero;
                CloseHandleOnce(ref stdoutWrite, "parent stdout write pipe");
                CloseHandleOnce(ref stderrWrite, "parent stderr write pipe");
                CloseHandleOnce(ref stdinRead, "parent stdin read pipe");
                CloseHandleOnce(ref stdinWrite, "parent stdin write pipe");
                bool assignmentSucceeded = !forceAssignFailureForTest &&
                    AssignProcessToJobObject(job, process.hProcess);
                if (!assignmentSucceeded) {
                    int error = forceAssignFailureForTest ? 5 :
                        Marshal.GetLastWin32Error();
                    // The child is still suspended and cannot have executed or
                    // spawned descendants.  Direct termination is mandatory if
                    // assignment fails.
                    TerminateProcess(process.hProcess, 125);
                    WaitForSingleObject(process.hProcess, 10000);
                    throw new Win32Exception(error,
                        "cannot assign suspended isolated Python to job");
                }
                assigned = true;
                if (ResumeThread(process.hThread) == UInt32.MaxValue) {
                    throw new Win32Exception(Marshal.GetLastWin32Error(),
                        "cannot resume isolated Python after job assignment");
                }
                Stopwatch elapsed = Stopwatch.StartNew();
                bool timedOut = false;
                bool outputExceeded = false;
                bool stdoutBroken = false;
                bool stderrBroken = false;
                while (true) {
                    bool broken;
                    bool exceeded;
                    DrainPipeNonBlocking(
                        stdoutRead, stdoutBuffer, outputLimitBytes,
                        out broken, out exceeded);
                    stdoutBroken = stdoutBroken || broken;
                    outputExceeded = outputExceeded || exceeded;
                    DrainPipeNonBlocking(
                        stderrRead, stderrBuffer, outputLimitBytes,
                        out broken, out exceeded);
                    stderrBroken = stderrBroken || broken;
                    outputExceeded = outputExceeded || exceeded;
                    if (outputExceeded) {
                        TerminateJob(job, 126);
                        break;
                    }
                    UInt32 wait = WaitForSingleObject(process.hProcess, 20);
                    if (wait == WAIT_OBJECT_0) break;
                    if (wait != WAIT_TIMEOUT) {
                        throw new Win32Exception(Marshal.GetLastWin32Error(),
                            "cannot wait for isolated Python process");
                    }
                    if (elapsed.ElapsedMilliseconds >= timeoutSeconds * 1000L) {
                        timedOut = true;
                        TerminateJob(job, 124);
                        break;
                    }
                }
                if (WaitForSingleObject(process.hProcess, 10000) != WAIT_OBJECT_0) {
                    throw new TimeoutException(
                        "isolated Python process did not terminate");
                }
                UInt32 exitCode;
                if (!GetExitCodeProcess(process.hProcess, out exitCode) ||
                        exitCode == STILL_ACTIVE) {
                    throw new Win32Exception(Marshal.GetLastWin32Error(),
                        "cannot obtain isolated Python exit code");
                }
                // Kill any descendant that retained a pipe after the trusted
                // launcher exited, then close the job exactly once.
                TerminateJob(job, 127);
                CloseHandleOnce(ref job, "preflight job object");
                Stopwatch pipeDrain = Stopwatch.StartNew();
                while (!stdoutBroken || !stderrBroken) {
                    bool broken;
                    bool exceeded;
                    if (!stdoutBroken) {
                        DrainPipeNonBlocking(
                            stdoutRead, stdoutBuffer, outputLimitBytes,
                            out broken, out exceeded);
                        stdoutBroken = stdoutBroken || broken;
                        outputExceeded = outputExceeded || exceeded;
                    }
                    if (!stderrBroken) {
                        DrainPipeNonBlocking(
                            stderrRead, stderrBuffer, outputLimitBytes,
                            out broken, out exceeded);
                        stderrBroken = stderrBroken || broken;
                        outputExceeded = outputExceeded || exceeded;
                    }
                    if (stdoutBroken && stderrBroken) break;
                    if (pipeDrain.ElapsedMilliseconds >= 10000) {
                        throw new TimeoutException(
                            "isolated Python output pipes did not close");
                    }
                    Thread.Sleep(10);
                }
                UTF8Encoding utf8 = new UTF8Encoding(false, true);
                result = new BoundedProcessResult {
                    ExitCode = unchecked((Int32)exitCode),
                    TimedOut = timedOut,
                    OutputLimitExceeded = outputExceeded,
                    StandardOutput = utf8.GetString(stdoutBuffer.ToArray()),
                    StandardError = utf8.GetString(stderrBuffer.ToArray())
                };
            } catch (Exception error) {
                primary = error;
            } finally {
                if (job != IntPtr.Zero) {
                    if (created && !assigned && process.hProcess != IntPtr.Zero) {
                        try {
                            TerminateProcess(process.hProcess, 125);
                            WaitForSingleObject(process.hProcess, 10000);
                        } catch { }
                    } else if (assigned) {
                        try { TerminateJobObject(job, 125); } catch { }
                    }
                    IntPtr closingJob = job;
                    job = IntPtr.Zero;
                    if (!CloseHandle(closingJob) && cleanup == null) {
                        cleanup = new Win32Exception(Marshal.GetLastWin32Error(),
                            "cannot close preflight job object during cleanup");
                    }
                }
                IntPtr[] handles = new IntPtr[] {
                    process.hThread, process.hProcess,
                    stdoutRead, stdoutWrite, stderrRead, stderrWrite,
                    stdinRead, stdinWrite
                };
                process.hThread = IntPtr.Zero;
                process.hProcess = IntPtr.Zero;
                stdoutRead = IntPtr.Zero;
                stdoutWrite = IntPtr.Zero;
                stderrRead = IntPtr.Zero;
                stderrWrite = IntPtr.Zero;
                stdinRead = IntPtr.Zero;
                stdinWrite = IntPtr.Zero;
                foreach (IntPtr handle in handles) {
                    if (handle != IntPtr.Zero && handle != INVALID_HANDLE_VALUE &&
                            !CloseHandle(handle) && cleanup == null) {
                        cleanup = new Win32Exception(Marshal.GetLastWin32Error(),
                            "cannot close isolated Python native handle");
                    }
                }
                if (environmentBlock != IntPtr.Zero) {
                    Marshal.FreeHGlobal(environmentBlock);
                }
                if (attributeList != IntPtr.Zero) {
                    if (attributeListInitialized) {
                        DeleteProcThreadAttributeList(attributeList);
                    }
                    Marshal.FreeHGlobal(attributeList);
                }
                if (inheritedHandleList != IntPtr.Zero) {
                    Marshal.FreeHGlobal(inheritedHandleList);
                }
                stdoutBuffer.Dispose();
                stderrBuffer.Dispose();
            }
            if (primary != null) throw primary;
            if (cleanup != null) throw cleanup;
            return result;
        }

        public static IntPtr OpenDirectoryGuard(string path) {
            IntPtr handle = CreateFileW(
                path, 0x80000000, 0x1, IntPtr.Zero,
                OPEN_EXISTING,
                FILE_FLAG_BACKUP_SEMANTICS | FILE_FLAG_OVERLAPPED,
                IntPtr.Zero);
            if (handle == INVALID_HANDLE_VALUE) {
                throw new Win32Exception(Marshal.GetLastWin32Error(),
                    "cannot open no-share-write directory guard: " + path);
            }
            OplockState state = new OplockState();
            try {
                state.Event = CreateEventW(IntPtr.Zero, true, false, null);
                if (state.Event == IntPtr.Zero) {
                    throw new Win32Exception(Marshal.GetLastWin32Error(),
                        "cannot create directory oplock event: " + path);
                }
                REQUEST_OPLOCK_INPUT_BUFFER input =
                    new REQUEST_OPLOCK_INPUT_BUFFER();
                input.StructureVersion = 1;
                input.StructureLength = (UInt16)Marshal.SizeOf(input);
                // RH protects this directory object against rename/delete until
                // the break is acknowledged.  Child namespace changes are not a
                // physical lock: they can only signal a break, which the caller
                // checks fail-closed at transaction boundaries.  Complete-on-
                // close releases a pending break after the protected operation.
                input.RequestedOplockLevel = 0x1 | 0x2;
                input.Flags = 0x1 | 0x4;
                REQUEST_OPLOCK_OUTPUT_BUFFER output =
                    new REQUEST_OPLOCK_OUTPUT_BUFFER();
                OVERLAPPED_BUFFER overlapped = new OVERLAPPED_BUFFER();
                overlapped.Event = state.Event;
                state.Input = Marshal.AllocHGlobal(Marshal.SizeOf(input));
                state.Output = Marshal.AllocHGlobal(Marshal.SizeOf(output));
                state.Overlapped = Marshal.AllocHGlobal(Marshal.SizeOf(overlapped));
                Marshal.StructureToPtr(input, state.Input, false);
                Marshal.StructureToPtr(output, state.Output, false);
                Marshal.StructureToPtr(overlapped, state.Overlapped, false);
                bool immediate = DeviceIoControl(
                    handle, FSCTL_REQUEST_OPLOCK, state.Input,
                    (UInt32)Marshal.SizeOf(input), state.Output,
                    (UInt32)Marshal.SizeOf(output), IntPtr.Zero,
                    state.Overlapped);
                int error = Marshal.GetLastWin32Error();
                if (immediate || error != ERROR_IO_PENDING) {
                    throw new Win32Exception(error,
                        "directory oplock request was not pending (" + error +
                        ", input=" + Marshal.SizeOf(input) +
                        ", output=" + Marshal.SizeOf(output) +
                        ", overlapped=" + Marshal.SizeOf(overlapped) + "): " + path);
                }
                lock (OplockSync) { Oplocks.Add(handle, state); }
                return handle;
            } catch {
                if (state.Overlapped != IntPtr.Zero) Marshal.FreeHGlobal(state.Overlapped);
                if (state.Output != IntPtr.Zero) Marshal.FreeHGlobal(state.Output);
                if (state.Input != IntPtr.Zero) Marshal.FreeHGlobal(state.Input);
                if (state.Event != IntPtr.Zero) CloseHandle(state.Event);
                CloseHandle(handle);
                throw;
            }
        }

        public static IntPtr OpenDirectoryIdentityGuard(string path) {
            // Child entry creation remains allowed, but omitting FILE_SHARE_DELETE
            // pins this exact directory object against rename/delete while paths
            // beneath it are used by the isolated launcher.
            IntPtr handle = CreateFileW(
                path, FILE_READ_ATTRIBUTES, FILE_SHARE_READ | FILE_SHARE_WRITE,
                IntPtr.Zero, OPEN_EXISTING,
                FILE_FLAG_BACKUP_SEMANTICS | FILE_FLAG_OPEN_REPARSE_POINT,
                IntPtr.Zero);
            if (handle == INVALID_HANDLE_VALUE) {
                throw new Win32Exception(Marshal.GetLastWin32Error(),
                    "cannot open directory identity guard: " + path);
            }
            return handle;
        }

        public static bool DirectoryGuardBreakPending(IntPtr handle) {
            OplockState state;
            lock (OplockSync) {
                if (!Oplocks.TryGetValue(handle, out state)) {
                    throw new InvalidOperationException("unknown directory oplock handle");
                }
            }
            UInt32 transferred;
            if (GetOverlappedResult(handle, state.Overlapped,
                    out transferred, false)) return true;
            int error = Marshal.GetLastWin32Error();
            if (error == ERROR_IO_INCOMPLETE) return false;
            throw new Win32Exception(error,
                "cannot inspect directory oplock state");
        }

        public static IntPtr CreateKillOnCloseJob() {
            IntPtr job = CreateJobObjectW(IntPtr.Zero, null);
            if (job == IntPtr.Zero) {
                throw new Win32Exception(Marshal.GetLastWin32Error(),
                    "cannot create preflight job object");
            }
            JOBOBJECT_EXTENDED_LIMIT_INFORMATION info =
                new JOBOBJECT_EXTENDED_LIMIT_INFORMATION();
            info.BasicLimitInformation.LimitFlags =
                JOB_OBJECT_LIMIT_KILL_ON_JOB_CLOSE;
            if (!SetInformationJobObject(job, 9, ref info,
                    (UInt32)Marshal.SizeOf(info))) {
                int error = Marshal.GetLastWin32Error();
                CloseHandle(job);
                throw new Win32Exception(error,
                    "cannot configure kill-on-close preflight job object");
            }
            return job;
        }

        public static void AssignToJob(IntPtr job, IntPtr process) {
            if (!AssignProcessToJobObject(job, process)) {
                throw new Win32Exception(Marshal.GetLastWin32Error(),
                    "cannot assign preflight launcher to job object");
            }
        }

        public static void TerminateJob(IntPtr job, UInt32 exitCode) {
            if (!TerminateJobObject(job, exitCode)) {
                throw new Win32Exception(Marshal.GetLastWin32Error(),
                    "cannot terminate preflight job object");
            }
        }

        public static void CloseChecked(IntPtr handle, string name) {
            if (handle == IntPtr.Zero || handle == INVALID_HANDLE_VALUE) return;
            OplockState state = null;
            lock (OplockSync) {
                if (Oplocks.TryGetValue(handle, out state)) Oplocks.Remove(handle);
            }
            int closeError = 0;
            if (state != null) {
                bool cancelled = CancelIoEx(handle, state.Overlapped);
                int cancelError = cancelled ? 0 : Marshal.GetLastWin32Error();
                UInt32 transferred;
                bool drained = GetOverlappedResult(
                    handle, state.Overlapped, out transferred, true);
                int drainError = drained ? 0 : Marshal.GetLastWin32Error();
                if (!cancelled && cancelError != ERROR_NOT_FOUND) {
                    closeError = cancelError;
                }
                if (!drained && drainError != ERROR_OPERATION_ABORTED &&
                        closeError == 0) {
                    closeError = drainError;
                }
            }
            if (!CloseHandle(handle) && closeError == 0) {
                closeError = Marshal.GetLastWin32Error();
            }
            if (state != null) {
                if (state.Event != IntPtr.Zero && !CloseHandle(state.Event) &&
                        closeError == 0) closeError = Marshal.GetLastWin32Error();
                if (state.Overlapped != IntPtr.Zero) Marshal.FreeHGlobal(state.Overlapped);
                if (state.Output != IntPtr.Zero) Marshal.FreeHGlobal(state.Output);
                if (state.Input != IntPtr.Zero) Marshal.FreeHGlobal(state.Input);
            }
            if (closeError != 0) {
                throw new Win32Exception(closeError, "cannot close " + name);
            }
        }
    }
}
'@
}

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
        [Parameter(Mandatory = $true)][string]$Description,
        [switch]$HoldOpen
    )
    Assert-NoReparsePath -Path $Root -Description $Description
    $RootPrefix = $Root.TrimEnd('\', '/') + `
        [System.IO.Path]::DirectorySeparatorChar
    $DirectoryHandles = New-Object 'System.Collections.Generic.List[System.IntPtr]'
    $Streams = New-Object 'System.Collections.Generic.List[System.IO.FileStream]'
    try {
    if ($HoldOpen) {
        $DirectoryHandles.Add(
            [Guvolu.PreflightNative]::OpenDirectoryGuard($Root)
        )
    }
    $Items = @(Get-ChildItem -LiteralPath $Root -Recurse -Force)
    foreach ($Directory in @($Items | Where-Object { $_.PSIsContainer })) {
        if (($Directory.Attributes -band `
            [System.IO.FileAttributes]::ReparsePoint) -ne 0) {
            throw "$Description contains a reparse directory: $($Directory.FullName)"
        }
        if ($HoldOpen) {
            $DirectoryHandles.Add(
                [Guvolu.PreflightNative]::OpenDirectoryGuard($Directory.FullName)
            )
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
    if ($HoldOpen) {
        $VerifiedItems = @(Get-ChildItem -LiteralPath $Root -Recurse -Force)
        $VerifiedPaths = [string[]]@($VerifiedItems | ForEach-Object {
            $Marker = if ($_.PSIsContainer) { "D:" } else { "F:" }
            $Marker + $_.FullName.Substring($RootPrefix.Length).Replace('\', '/')
        })
        $InitialPaths = [string[]]@($Items | ForEach-Object {
            $Marker = if ($_.PSIsContainer) { "D:" } else { "F:" }
            $Marker + $_.FullName.Substring($RootPrefix.Length).Replace('\', '/')
        })
        [Array]::Sort($VerifiedPaths, [System.StringComparer]::Ordinal)
        [Array]::Sort($InitialPaths, [System.StringComparer]::Ordinal)
        if (($VerifiedPaths -join "`0") -cne ($InitialPaths -join "`0")) {
            throw "$Description changed while directory guards were acquired"
        }
    }
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
            $Streams.Add($Stream)
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
        $Result = [pscustomobject][ordered]@{
            root = $Root
            file_count = $RelativePaths.Count
            total_bytes = $TotalBytes
            tree_sha256 = $TreeSha256
            streams = $Streams
            directory_handles = $DirectoryHandles
        }
        if (-not $HoldOpen) {
            foreach ($Stream in $Streams) { $Stream.Dispose() }
            $Result.streams = @()
            foreach ($Handle in $DirectoryHandles) {
                [Guvolu.PreflightNative]::CloseChecked(
                    $Handle, "$Description directory guard"
                )
            }
            $Result.directory_handles = @()
        }
        return $Result
    } catch {
        $Primary = $_
        foreach ($Stream in $Streams) {
            try { $Stream.Dispose() } catch { }
        }
        foreach ($Handle in $DirectoryHandles) {
            try {
                [Guvolu.PreflightNative]::CloseChecked(
                    $Handle, "$Description directory guard"
                )
            } catch { }
        }
        throw $Primary
    }
}

function Close-BoundTreeIdentity {
    param(
        [AllowNull()][object]$Identity,
        [Parameter(Mandatory = $true)][string]$Description
    )
    if ($null -eq $Identity) { return }
    $Failures = New-Object 'System.Collections.Generic.List[string]'
    foreach ($Stream in @($Identity.streams)) {
        try { $Stream.Dispose() } catch { $Failures.Add($_.Exception.Message) }
    }
    foreach ($Handle in @($Identity.directory_handles)) {
        try {
            [Guvolu.PreflightNative]::CloseChecked(
                $Handle, "$Description directory guard"
            )
        } catch { $Failures.Add($_.Exception.Message) }
    }
    if ($Failures.Count -ne 0) {
        throw "$Description guard cleanup failed: $($Failures -join '; ')"
    }
}

function Assert-BoundTreeGuardsUnbroken {
    param(
        [Parameter(Mandatory = $true)][object]$Identity,
        [Parameter(Mandatory = $true)][string]$Description
    )
    foreach ($Handle in @($Identity.directory_handles)) {
        if ([Guvolu.PreflightNative]::DirectoryGuardBreakPending($Handle)) {
            throw "$Description directory oplock was broken by a write/delete attempt"
        }
    }
}

function Assert-TreeIdentity {
    param(
        [Parameter(Mandatory = $true)][object]$Identity,
        [Parameter(Mandatory = $true)][string]$ExpectedSha256,
        [Parameter(Mandatory = $true)][long]$ExpectedFileCount,
        [Parameter(Mandatory = $true)][long]$ExpectedTotalBytes,
        [Parameter(Mandatory = $true)][string]$Description
    )
    if (
        [string]$Identity.tree_sha256 -cne $ExpectedSha256 -or
        [long]$Identity.file_count -ne $ExpectedFileCount -or
        [long]$Identity.total_bytes -ne $ExpectedTotalBytes
    ) {
        throw "$Description manifest mismatch"
    }
}

function Get-GovernanceStoreIdentity {
    param([Parameter(Mandatory = $true)][string]$Registry)
    $Entries = @()
    foreach ($Path in @(
        $Registry,
        "$Registry-wal",
        "$Registry-shm",
        "$Registry-journal"
    )) {
        if (Test-Path -LiteralPath $Path) {
            Assert-NoReparsePath -Path $Path `
                -Description "governance SQLite file"
            Assert-OrdinaryFile -Path $Path `
                -Description "governance SQLite file"
            $Item = Get-Item -LiteralPath $Path -Force -ErrorAction Stop
            $Entries += [ordered]@{
                path = $Path
                exists = $true
                length = [long]$Item.Length
                sha256 = Get-FileSha256 -Path $Path
            }
        } else {
            $Entries += [ordered]@{
                path = $Path
                exists = $false
                length = $null
                sha256 = $null
            }
        }
    }
    return [ordered]@{ files = $Entries }
}

function Assert-NoGovernanceRollbackJournal {
    param([Parameter(Mandatory = $true)][string]$Registry)
    $Journal = "$Registry-journal"
    if (Test-Path -LiteralPath $Journal) {
        throw "authoritative governance rollback journal is present; fail closed: $Journal"
    }
}

function Assert-GovernanceSidecarsGuardable {
    param([Parameter(Mandatory = $true)][string]$Registry)
    $WalExists = Test-Path -LiteralPath "$Registry-wal" -PathType Leaf
    $ShmExists = Test-Path -LiteralPath "$Registry-shm" -PathType Leaf
    if (-not $WalExists -and -not $ShmExists) {
        throw "sidecarless authoritative governance store cannot prevent transient WAL/SHM creation; fail closed before business invocation"
    }
    if ($WalExists -ne $ShmExists) {
        throw "authoritative governance WAL/SHM sidecars are incomplete; fail closed before business invocation"
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

function Get-CodeIdentity {
    param(
        [string]$CodeRoot,
        [string]$ExpectedHead,
        [string]$Wrapper,
        [string]$GovernanceRunner,
        [string]$Python,
        [string]$PyVenvConfig
    )
    $TopLevelResult = Invoke-GitReadOnly -CodeRoot $CodeRoot -Arguments @(
        "rev-parse", "--show-toplevel"
    )
    if ($TopLevelResult.Output.Count -ne 1) {
        throw "code root did not resolve to one git top-level: $CodeRoot"
    }
    $TopLevel = [System.IO.Path]::GetFullPath(
        ([string]$TopLevelResult.Output[0]).Trim()
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

    Assert-TrackedFile -CodeRoot $CodeRoot `
        -RelativePath "scripts/run_holdout_preflight_task.ps1"
    Assert-TrackedFile -CodeRoot $CodeRoot `
        -RelativePath "scripts/preflight_holdout.py"
    Assert-OrdinaryFile -Path $Wrapper -Description "preflight wrapper"
    Assert-OrdinaryFile -Path $GovernanceRunner -Description "governance runner"
    Assert-OrdinaryFile -Path $Python -Description "Python executable" -SingleLink
    Assert-OrdinaryFile -Path $PyVenvConfig -Description "pyvenv.cfg" -SingleLink

    return [ordered]@{
        code_head = $ActualHead
        wrapper_sha256 = Get-FileSha256 -Path $Wrapper
        governance_runner_sha256 = Get-FileSha256 -Path $GovernanceRunner
        python_sha256 = Get-FileSha256 -Path $Python
        pyvenv_sha256 = Get-FileSha256 -Path $PyVenvConfig
    }
}

function Assert-ExpectedIdentity {
    param([System.Collections.IDictionary]$Identity)
    $Expected = [ordered]@{
        wrapper_sha256 = $ExpectedWrapperSha256
        governance_runner_sha256 = $ExpectedGovernanceRunnerSha256
        python_sha256 = $ExpectedPythonSha256
        pyvenv_sha256 = $ExpectedPyVenvSha256
    }
    foreach ($Name in $Expected.Keys) {
        if ([string]$Identity[$Name] -ne [string]$Expected[$Name]) {
            throw "$Name mismatch: expected $($Expected[$Name]), actual $($Identity[$Name])"
        }
    }
}

function Write-AtomicUtf8File {
    param([string]$Path, [string]$Text)
    $Temporary = "$Path.pending-$([guid]::NewGuid().ToString('N'))"
    $Bytes = (New-Object System.Text.UTF8Encoding($false)).GetBytes($Text)
    try {
        $Stream = New-Object System.IO.FileStream(
            $Temporary,
            [System.IO.FileMode]::CreateNew,
            [System.IO.FileAccess]::Write,
            [System.IO.FileShare]::None
        )
        try {
            $Stream.Write($Bytes, 0, $Bytes.Length)
            if ($Stream.Position -ne $Bytes.Length) {
                throw "short write while creating scheduler record"
            }
            $Stream.Flush($true)
        } finally {
            $Stream.Dispose()
        }
        [System.IO.File]::Move($Temporary, $Path)
    } finally {
        if (Test-Path -LiteralPath $Temporary) {
            Remove-Item -LiteralPath $Temporary -Force -ErrorAction SilentlyContinue
        }
    }
}

function Add-DurableJsonLine {
    param([string]$Path, [string]$Line)
    $Bytes = (New-Object System.Text.UTF8Encoding($false)).GetBytes($Line + "`n")
    $Stream = New-Object System.IO.FileStream(
        $Path,
        [System.IO.FileMode]::OpenOrCreate,
        [System.IO.FileAccess]::ReadWrite,
        [System.IO.FileShare]::ReadWrite
    )
    $Locked = $false
    $OriginalLength = [long]0
    try {
        $Stream.Lock(0, 1)
        $Locked = $true
        $OriginalLength = $Stream.Length
        [void]$Stream.Seek(0, [System.IO.SeekOrigin]::End)
        $Stream.Write($Bytes, 0, $Bytes.Length)
        if ($Stream.Position -ne ($OriginalLength + $Bytes.Length)) {
            throw "short write while appending scheduler index"
        }
        $Stream.Flush($true)
    } catch {
        $Primary = $_
        if ($Locked) {
            try {
                $Stream.SetLength($OriginalLength)
                $Stream.Flush($true)
            } catch {
                # Preserve the append failure; the per-run atomic record remains authoritative.
            }
        }
        throw $Primary
    } finally {
        if ($Locked) {
            try { $Stream.Unlock(0, 1) } catch { }
        }
        $Stream.Dispose()
    }
}

function ConvertTo-WindowsCommandLineArgument {
    param([AllowEmptyString()][string]$Value)
    $Builder = New-Object System.Text.StringBuilder
    [void]$Builder.Append('"')
    [int]$Backslashes = 0
    foreach ($Character in $Value.ToCharArray()) {
        if ($Character -eq '\') {
            $Backslashes += 1
            continue
        }
        if ($Character -eq '"') {
            [void]$Builder.Append(('\' * (($Backslashes * 2) + 1)))
            [void]$Builder.Append('"')
        } else {
            if ($Backslashes -ne 0) {
                [void]$Builder.Append(('\' * $Backslashes))
            }
            [void]$Builder.Append($Character)
        }
        $Backslashes = 0
    }
    if ($Backslashes -ne 0) {
        [void]$Builder.Append(('\' * ($Backslashes * 2)))
    }
    [void]$Builder.Append('"')
    return $Builder.ToString()
}

function Invoke-BoundedIsolatedPython {
    param(
        [Parameter(Mandatory = $true)][string]$Python,
        [Parameter(Mandatory = $true)][string]$WorkingDirectory,
        [Parameter(Mandatory = $true)][string[]]$Arguments,
        [Parameter(Mandatory = $true)][int]$TimeoutSeconds,
        [ValidateRange(1024, 16777216)][int]$OutputLimitBytes = 1048576
    )
    $ChildSystemRoot = Split-Path -Parent ([Environment]::SystemDirectory)
    $ChildTemp = [System.IO.Path]::GetFullPath(
        [System.IO.Path]::GetTempPath()
    ).TrimEnd('\', '/')
    [string[]]$ChildEnvironment = @(
        "SYSTEMROOT=$ChildSystemRoot",
        "WINDIR=$ChildSystemRoot",
        "COMSPEC=$(Join-Path $ChildSystemRoot 'System32\cmd.exe')",
        "TEMP=$ChildTemp",
        "TMP=$ChildTemp"
    )
    $CommandLine = (
        ConvertTo-WindowsCommandLineArgument -Value $Python
    ) + " " + (($Arguments | ForEach-Object {
        ConvertTo-WindowsCommandLineArgument -Value ([string]$_)
    }) -join ' ')
    $NativeResult = [Guvolu.PreflightNative]::RunSuspendedCapped(
        $Python,
        $CommandLine,
        $WorkingDirectory,
        $ChildEnvironment,
        $TimeoutSeconds,
        $OutputLimitBytes,
        $false
    )
    return [pscustomobject][ordered]@{
        exit_code = [int]$NativeResult.ExitCode
        timed_out = [bool]$NativeResult.TimedOut
        output_limit_exceeded = [bool]$NativeResult.OutputLimitExceeded
        stdout = [string]$NativeResult.StandardOutput
        stderr = [string]$NativeResult.StandardError
    }
}

$IsolatedLauncher = @'
import ctypes
import hashlib
import importlib.machinery
import json
import os
import runpy
import sqlite3
import stat
import sys
from pathlib import Path

INVALID_HANDLE = ctypes.c_void_p(-1).value
REPARSE = getattr(stat, "FILE_ATTRIBUTE_REPARSE_POINT", 0x400)
DIRECTORY = getattr(stat, "FILE_ATTRIBUTE_DIRECTORY", 0x10)
kernel = ctypes.WinDLL("kernel32", use_last_error=True)
kernel.CreateFileW.argtypes = (
    ctypes.c_wchar_p, ctypes.c_uint32, ctypes.c_uint32, ctypes.c_void_p,
    ctypes.c_uint32, ctypes.c_uint32, ctypes.c_void_p,
)
kernel.CreateFileW.restype = ctypes.c_void_p
kernel.CloseHandle.argtypes = (ctypes.c_void_p,)
kernel.CloseHandle.restype = ctypes.c_int
kernel.WriteFile.argtypes = (
    ctypes.c_void_p, ctypes.c_void_p, ctypes.c_uint32,
    ctypes.POINTER(ctypes.c_uint32), ctypes.c_void_p,
)
kernel.WriteFile.restype = ctypes.c_int
kernel.FlushFileBuffers.argtypes = (ctypes.c_void_p,)
kernel.FlushFileBuffers.restype = ctypes.c_int
kernel.CreateEventW.argtypes = (
    ctypes.c_void_p, ctypes.c_int, ctypes.c_int, ctypes.c_wchar_p,
)
kernel.CreateEventW.restype = ctypes.c_void_p
kernel.ReadDirectoryChangesW.argtypes = (
    ctypes.c_void_p, ctypes.c_void_p, ctypes.c_uint32, ctypes.c_int,
    ctypes.c_uint32, ctypes.POINTER(ctypes.c_uint32), ctypes.c_void_p,
    ctypes.c_void_p,
)
kernel.ReadDirectoryChangesW.restype = ctypes.c_int
kernel.CancelIoEx.argtypes = (ctypes.c_void_p, ctypes.c_void_p)
kernel.CancelIoEx.restype = ctypes.c_int
kernel.GetOverlappedResult.argtypes = (
    ctypes.c_void_p, ctypes.c_void_p, ctypes.POINTER(ctypes.c_uint32), ctypes.c_int,
)
kernel.GetOverlappedResult.restype = ctypes.c_int
kernel.WaitForSingleObject.argtypes = (ctypes.c_void_p, ctypes.c_uint32)
kernel.WaitForSingleObject.restype = ctypes.c_uint32


class OVERLAPPED(ctypes.Structure):
    _fields_ = (
        ("Internal", ctypes.c_void_p),
        ("InternalHigh", ctypes.c_void_p),
        ("Offset", ctypes.c_uint32),
        ("OffsetHigh", ctypes.c_uint32),
        ("hEvent", ctypes.c_void_p),
    )


def canonical(value):
    return os.path.normcase(os.path.realpath(os.path.abspath(value)))


def within(path, root):
    try:
        return os.path.commonpath((canonical(path), canonical(root))) == canonical(root)
    except ValueError:
        return False


def reject_reparse(path, name):
    info = os.lstat(path)
    if getattr(info, "st_file_attributes", 0) & REPARSE:
        raise RuntimeError(f"{name} contains reparse path: {path}")
    return info


def sha256_file(path):
    digest = hashlib.sha256()
    with open(path, "rb", buffering=0) as stream:
        while True:
            block = stream.read(1024 * 1024)
            if not block:
                break
            digest.update(block)
    return digest.hexdigest()


def ordinary_file_identity(path, name):
    before = reject_reparse(path, name)
    if getattr(before, "st_file_attributes", 0) & DIRECTORY:
        raise RuntimeError(f"{name} is not an ordinary file: {path}")
    if not stat.S_ISREG(before.st_mode):
        raise RuntimeError(f"{name} is not a regular file: {path}")
    if before.st_nlink != 1:
        raise RuntimeError(f"{name} must have exactly one hard link: {path}")
    if not before.st_ino:
        raise RuntimeError(f"{name} has no stable file identity: {path}")
    digest = sha256_file(path)
    after = reject_reparse(path, name)
    identity = (
        before.st_dev,
        before.st_ino,
        before.st_nlink,
        before.st_size,
        before.st_mtime_ns,
        before.st_ctime_ns,
    )
    verified = (
        after.st_dev,
        after.st_ino,
        after.st_nlink,
        after.st_size,
        after.st_mtime_ns,
        after.st_ctime_ns,
    )
    if identity != verified:
        raise RuntimeError(f"{name} changed while it was hashed: {path}")
    return identity, digest


def assert_snapshot_aux_absent(snapshot):
    for suffix in ("-wal", "-shm", "-journal"):
        if os.path.lexists(snapshot + suffix):
            raise RuntimeError(
                f"disposable governance snapshot auxiliary appeared: {suffix}",
            )


def create_exclusive_guarded_file(path, payload, label="governance snapshot"):
    # CREATE_NEW closes the exists()/open race against files, hard links and
    # reparse points.  FILE_SHARE_READ permits only the later mode=ro reader;
    # write/delete/replace remain denied while this exact handle is retained.
    handle = kernel.CreateFileW(
        path,
        0x80000000 | 0x40000000,
        0x1,
        None,
        1,
        0x80 | 0x00200000,
        None,
    )
    if handle is None or int(handle) == INVALID_HANDLE:
        error = ctypes.get_last_error()
        raise OSError(error, f"cannot CREATE_NEW {label}: {path}")
    handle = int(handle)
    try:
        offset = 0
        while offset < len(payload):
            block = payload[offset : offset + 1024 * 1024]
            buffer = ctypes.create_string_buffer(block)
            written = ctypes.c_uint32()
            if not kernel.WriteFile(
                ctypes.c_void_p(handle),
                buffer,
                len(block),
                ctypes.byref(written),
                None,
            ):
                error = ctypes.get_last_error()
                raise OSError(error, f"cannot write {label}: {path}")
            if written.value != len(block):
                raise OSError(f"short write while creating {label}")
            offset += written.value
        if not kernel.FlushFileBuffers(ctypes.c_void_p(handle)):
            error = ctypes.get_last_error()
            raise OSError(error, f"cannot flush {label}: {path}")
        return handle
    except BaseException:
        kernel.CloseHandle(ctypes.c_void_p(handle))
        raise


def scan(root, name):
    root = canonical(root)
    reject_reparse(root, name)
    directories = [root]
    files = []
    for current, names, filenames in os.walk(root, topdown=True, followlinks=False):
        names.sort()
        filenames.sort()
        for entry in names:
            path = os.path.join(current, entry)
            info = reject_reparse(path, name)
            if not info.st_file_attributes & DIRECTORY:
                raise RuntimeError(f"{name} has non-directory traversal entry: {path}")
            directories.append(path)
        for entry in filenames:
            path = os.path.join(current, entry)
            info = reject_reparse(path, name)
            if info.st_file_attributes & DIRECTORY:
                raise RuntimeError(f"{name} has directory in file set: {path}")
            relative = os.path.relpath(path, root).replace("\\", "/")
            if not relative or "\x00" in relative:
                raise RuntimeError(f"{name} has non-canonical manifest path")
            files.append((relative, path))
    files.sort(key=lambda item: item[0])
    directories.sort()
    if len(files) > 100000:
        raise RuntimeError(f"{name} manifest exceeds 100000 files")
    return directories, files


def open_guard(path, directory):
    access = 0x80 if directory else 0x80000000
    flags = (0x02000000 if directory else 0) | 0x00200000
    handle = kernel.CreateFileW(path, access, 0x1, None, 3, flags, None)
    if handle is None or int(handle) == INVALID_HANDLE:
        error = ctypes.get_last_error()
        raise OSError(error, f"cannot open no-share-write guard: {path}")
    return int(handle)


def close_guards(handles):
    failures = []
    for handle in reversed(handles):
        if not kernel.CloseHandle(ctypes.c_void_p(handle)):
            failures.append(ctypes.get_last_error())
    return failures


def bound_manifest(root, name, expected):
    directories, files = scan(root, name)
    handles = []
    entries = []
    try:
        # The PowerShell parent holds fail-closed RH oplocks on every directory
        # in these trees.  Opening a second no-delete directory handle here
        # would itself request an RH break.  File handles are duplicated in the
        # child so executable bytes remain pinned across runpy.
        verified_directories, verified_files = scan(root, name)
        if directories != verified_directories or [x[0] for x in files] != [x[0] for x in verified_files]:
            raise RuntimeError(f"{name} changed while directory guards were acquired")
        for _, path in files:
            handles.append(open_guard(path, False))
        body = bytearray()
        total = 0
        for relative, path in files:
            digest = hashlib.sha256()
            length = 0
            with open(path, "rb", buffering=0) as stream:
                while True:
                    block = stream.read(1024 * 1024)
                    if not block:
                        break
                    digest.update(block)
                    length += len(block)
            total += length
            if total > 17179869184:
                raise RuntimeError(f"{name} manifest exceeds 16 GiB")
            file_digest = digest.hexdigest()
            entries.append((relative, path, length, file_digest))
            body.extend(relative.encode("utf-8"))
            body.extend(b"\x00" + str(length).encode("ascii") + b"\x00")
            body.extend(file_digest.encode("ascii") + b"\n")
        actual = (hashlib.sha256(body).hexdigest(), len(files), total)
        wanted = (expected["sha256"], int(expected["count"]), int(expected["bytes"]))
        if actual != wanted:
            raise RuntimeError(f"{name} manifest mismatch: {actual!r} != {wanted!r}")
        return (
            handles,
            {canonical(path) for _, path in files},
            {canonical(path) for path in directories},
            entries,
        )
    except BaseException:
        close_guards(handles)
        raise


def validate_readonly_database(connection, label):
    if connection.execute("PRAGMA query_only").fetchone() != (1,):
        raise RuntimeError(f"{label} connection is not query_only")
    check = connection.execute("PRAGMA quick_check").fetchall()
    if check != [("ok",)]:
        raise RuntimeError(f"{label} quick_check failed: {check!r}")
    foreign_key_violation = connection.execute(
        "PRAGMA foreign_key_check",
    ).fetchone()
    if foreign_key_violation is not None:
        raise RuntimeError(
            f"{label} foreign_key_check failed: {foreign_key_violation!r}",
        )
    return connection.execute(
        "SELECT type, name, tbl_name, rootpage, sql "
        "FROM sqlite_schema ORDER BY type, name, tbl_name, rootpage, sql",
    ).fetchall()


def create_readonly_snapshot(authority, snapshot):
    authority = canonical(authority)
    snapshot = canonical(snapshot)
    if os.path.exists(authority + "-journal"):
        raise RuntimeError(
            "authoritative governance rollback journal is present; fail closed",
        )
    wal_exists = os.path.exists(authority + "-wal")
    shm_exists = os.path.exists(authority + "-shm")
    if not wal_exists and not shm_exists:
        raise RuntimeError(
            "sidecarless authoritative governance store is not guardable",
        )
    if wal_exists != shm_exists:
        raise RuntimeError("authoritative governance WAL/SHM sidecars are incomplete")
    source_uri = Path(authority).as_uri() + "?mode=ro&cache=private"
    source = sqlite3.connect(source_uri, uri=True, isolation_level=None)
    destination = None
    verification = None
    try:
        source.execute("PRAGMA query_only=ON")
        destination = sqlite3.connect(":memory:", isolation_level=None)
        source.backup(destination)
        destination.execute("PRAGMA query_only=ON")
        source_schema = validate_readonly_database(
            source, "authoritative governance source",
        )
        snapshot_schema = validate_readonly_database(
            destination, "governance snapshot",
        )
        if source_schema != snapshot_schema:
            raise RuntimeError(
                "governance snapshot sqlite_schema does not match its source",
            )
        payload = bytearray(destination.serialize())
        if len(payload) < 100 or payload[:16] != b"SQLite format 3\x00":
            raise RuntimeError("governance snapshot serialization is not SQLite3")
        # SQLite file-format header bytes 18 and 19 are the one-byte file write
        # and read versions (1=legacy rollback, 2=WAL).  backup() preserves the
        # source's WAL values even for an in-memory destination.  This complete
        # backup is a standalone single-file database, so bind both versions to
        # rollback mode before disk export, then independently verify the actual
        # disk image through SQLite below.
        payload[18] = 1
        payload[19] = 1
        verification = sqlite3.connect(":memory:", isolation_level=None)
        verification.deserialize(bytes(payload))
        verification.execute("PRAGMA query_only=ON")
        verified_schema = validate_readonly_database(
            verification, "serialized governance snapshot",
        )
        if verified_schema != source_schema:
            raise RuntimeError(
                "serialized governance snapshot sqlite_schema does not match its source",
            )
    finally:
        if verification is not None:
            verification.close()
        if destination is not None:
            destination.close()
        source.close()
    snapshot_handle = create_exclusive_guarded_file(snapshot, bytes(payload))
    disk = None
    try:
        assert_snapshot_aux_absent(snapshot)
        identity, digest = ordinary_file_identity(
            snapshot, "governance snapshot",
        )
        disk = sqlite3.connect(
            Path(snapshot).as_uri() + "?mode=ro&cache=private",
            uri=True,
            isolation_level=None,
        )
        disk.execute("PRAGMA query_only=ON")
        if disk.execute("PRAGMA journal_mode").fetchone() != ("delete",):
            raise RuntimeError(
                "disk governance snapshot journal_mode is not delete",
            )
        disk_schema = validate_readonly_database(
            disk, "disk governance snapshot",
        )
        if disk_schema != source_schema:
            raise RuntimeError(
                "disk governance snapshot sqlite_schema does not match its source",
            )
        disk.close()
        disk = None
        assert_snapshot_aux_absent(snapshot)
        return snapshot_handle, identity, digest
    except BaseException:
        disk_primary = sys.exc_info()
        if disk is not None:
            try:
                disk.close()
            except BaseException:
                pass
        kernel.CloseHandle(ctypes.c_void_p(snapshot_handle))
        raise disk_primary[1].with_traceback(disk_primary[2])


def ensure_closure_directory(root, relative):
    current = root
    for part in relative:
        if part in ("", ".", "..") or "\x00" in part:
            raise RuntimeError("import closure contains an unsafe directory component")
        current = os.path.join(current, part)
        try:
            os.mkdir(current)
        except FileExistsError:
            pass
        info = reject_reparse(current, "import closure directory")
        if not info.st_file_attributes & DIRECTORY:
            raise RuntimeError(f"import closure path is not a directory: {current}")
    return current


def build_code_import_closure(config, source_entries, runner):
    if source_entries is None:
        raise RuntimeError("CodeRoot/src manifest was not captured")
    root = canonical(config["closure_root"])
    reject_reparse(root, "import closure root")
    if os.listdir(root):
        raise RuntimeError("import closure root was not empty at guarded launch")
    captured = []
    for relative, source, expected_length, expected_digest in source_entries:
        with open(source, "rb", buffering=0) as stream:
            payload = stream.read()
        if len(payload) != expected_length or hashlib.sha256(payload).hexdigest() != expected_digest:
            raise RuntimeError(f"CodeRoot/src file changed before closure capture: {relative}")
        captured.append(("repo/src/" + relative, payload, expected_digest))
    with open(runner, "rb", buffering=0) as stream:
        runner_payload = stream.read()
    runner_digest = hashlib.sha256(runner_payload).hexdigest()
    if runner_digest != config["runner_sha256"]:
        raise RuntimeError("governance runner changed before closure capture")
    captured.append((
        "repo/scripts/preflight_holdout.py", runner_payload, runner_digest,
    ))
    captured.sort(key=lambda item: item[0])
    tree_body = bytearray()
    for logical, payload, digest in captured:
        tree_body.extend(logical.encode("utf-8"))
        tree_body.extend(b"\x00" + str(len(payload)).encode("ascii") + b"\x00")
        tree_body.extend(digest.encode("ascii") + b"\n")
    tree_digest = hashlib.sha256(tree_body).hexdigest()
    manifest_payload = (
        json.dumps(
            {
                "schema_version": 1,
                "role": "holdout-preflight-business-code",
                "tree_sha256": tree_digest,
                "file_count": len(captured),
                "total_bytes": sum(len(payload) for _, payload, _ in captured),
                "files": [
                    {
                        "path": logical,
                        "size": len(payload),
                        "sha256": digest,
                    }
                    for logical, payload, digest in captured
                ],
            },
            ensure_ascii=False,
            separators=(",", ":"),
            sort_keys=True,
        ) + "\n"
    ).encode("utf-8")
    handles = []
    expected_paths = set()
    try:
        for logical, payload, digest in captured:
            parts = logical.split("/")
            parent = ensure_closure_directory(root, parts[:-1])
            target = os.path.join(parent, parts[-1])
            handles.append(create_exclusive_guarded_file(
                target, payload, "import closure file",
            ))
            identity, observed = ordinary_file_identity(
                target, "import closure file",
            )
            del identity
            if observed != digest:
                raise RuntimeError(f"import closure file digest mismatch: {logical}")
            expected_paths.add(logical)
        manifest_parent = ensure_closure_directory(root, ("manifests",))
        manifest = os.path.join(
            manifest_parent, f"holdout-preflight-{tree_digest}.json",
        )
        handles.append(create_exclusive_guarded_file(
            manifest, manifest_payload, "import closure manifest",
        ))
        manifest_digest = hashlib.sha256(manifest_payload).hexdigest()
        _, observed_manifest = ordinary_file_identity(
            manifest, "import closure manifest",
        )
        if observed_manifest != manifest_digest:
            raise RuntimeError("import closure manifest digest mismatch")
        expected_paths.add(
            os.path.relpath(manifest, root).replace("\\", "/"),
        )
        directories, files = scan(root, "import closure")
        actual_paths = {relative for relative, _ in files}
        if actual_paths != expected_paths:
            raise RuntimeError("import closure file membership mismatch")
        closure_files = {canonical(path) for _, path in files}
        closure_directories = {canonical(path) for path in directories}
        return {
            "handles": handles,
            "files": closure_files,
            "directories": closure_directories,
            "source": canonical(os.path.join(root, "repo", "src")),
            "runner": canonical(os.path.join(
                root, "repo", "scripts", "preflight_holdout.py",
            )),
            "manifest": canonical(manifest),
            "tree_sha256": tree_digest,
            "manifest_sha256": manifest_digest,
            "file_count": len(captured),
            "total_bytes": sum(len(payload) for _, payload, _ in captured),
        }
    except BaseException:
        close_guards(handles)
        raise


raw_path_finder = importlib.machinery.PathFinder.find_spec


class BoundPathFinder:
    allowed_files = set()
    allowed_directories = set()
    allowed_archives = set()

    @classmethod
    def find_spec(cls, fullname, path=None, target=None):
        spec = raw_path_finder(fullname, path, target)
        if spec is None:
            return None
        origin = spec.origin
        if origin not in (None, "built-in", "frozen"):
            resolved = canonical(origin)
            archive_ok = any(
                resolved.startswith(archive + os.sep)
                for archive in cls.allowed_archives
            )
            if resolved not in cls.allowed_files and not archive_ok:
                raise ImportError(
                    f"module origin is outside the bound manifests: {fullname}={origin}",
                )
        locations = spec.submodule_search_locations
        if locations is not None:
            for location in locations:
                if canonical(location) not in cls.allowed_directories:
                    raise ImportError(
                        f"package location is outside the bound manifests: {fullname}={location}",
                    )
        return spec


def guarded_direct_path_find_spec(cls, fullname, path=None, target=None):
    del cls
    return BoundPathFinder.find_spec(fullname, path, target)


config = json.loads(sys.argv[1])
if not (
    sys.flags.isolated == 1
    and sys.flags.no_site == 1
    and sys.flags.dont_write_bytecode == 1
    and sys.flags.utf8_mode == 1
):
    raise RuntimeError("isolated Python flags are incomplete")
if canonical(sys.pycache_prefix or "") != canonical(config["pycache"]):
    raise RuntimeError("isolated pycache prefix mismatch")
if os.listdir(config["pycache"]):
    raise RuntimeError("isolated pycache prefix was not empty at launcher start")
base = canonical(config["base_root"])
if canonical(sys.prefix) != base or canonical(sys.base_prefix) != base:
    raise RuntimeError("base Python prefix does not match the bound runtime")
for entry in sys.path:
    if not entry or not within(entry, base):
        raise RuntimeError(f"unbound initial sys.path entry: {entry!r}")
    if not os.path.exists(entry):
        expected_absent_zip = os.path.join(
            base, f"python{sys.version_info.major}{sys.version_info.minor}.zip",
        )
        if canonical(entry) != canonical(expected_absent_zip):
            raise RuntimeError(f"unbound absent initial sys.path entry: {entry!r}")
for forbidden in (config["code_root"], config["runtime_source"], config["venv_root"]):
    if any(within(entry, forbidden) for entry in sys.path):
        raise RuntimeError(f"business path was active before guarded launch: {forbidden}")
if canonical(sys.executable) != canonical(config["python"]):
    raise RuntimeError("Python executable path mismatch")
if not within(config["site_packages"], config["venv_root"]):
    raise RuntimeError("site-packages is outside the bound venv")

handles = []
allowed_files = set()
allowed_directories = set()
primary = None
post_failure = None
snapshot_identity = None
snapshot_digest = None
code_entries = None
original_code_files = set()
original_code_directories = set()
closure_identity = None
closure_digest = None
try:
    for root, name, expected in (
        (config["base_root"], "base Python runtime", config["base"]),
        (config["venv_root"], "Python venv", config["venv"]),
        (config["code_source"], "CodeRoot/src", config["code"]),
        (config["runtime_source"], "RuntimeRoot/src", config["runtime"]),
    ):
        tree_handles, tree_files, tree_directories, tree_entries = bound_manifest(
            root, name, expected,
        )
        handles += tree_handles
        allowed_files.update(tree_files)
        allowed_directories.update(tree_directories)
        if name == "CodeRoot/src":
            code_entries = tree_entries
            original_code_files = tree_files
            original_code_directories = tree_directories
    runner = canonical(config["runner"])
    handles.append(open_guard(runner, False))
    with open(runner, "rb", buffering=0) as stream:
        if hashlib.sha256(stream.read()).hexdigest() != config["runner_sha256"]:
            raise RuntimeError("governance runner SHA256 mismatch in isolated launcher")
    closure_identity = build_code_import_closure(config, code_entries, runner)
    handles += closure_identity["handles"]
    allowed_files.difference_update(original_code_files)
    allowed_directories.difference_update(original_code_directories)
    allowed_files.update(closure_identity["files"])
    allowed_directories.update(closure_identity["directories"])
    closure_digest = closure_identity["tree_sha256"]
    print(config["closure_evidence_prefix"] + closure_digest, flush=True)
    print(
        config["closure_manifest_evidence_prefix"]
        + closure_identity["manifest_sha256"],
        flush=True,
    )
    snapshot_handle, snapshot_identity, snapshot_digest = create_readonly_snapshot(
        config["authority_registry"], config["registry_snapshot"],
    )
    handles.append(snapshot_handle)
    guarded_identity, guarded_digest = ordinary_file_identity(
        config["registry_snapshot"], "guarded governance snapshot",
    )
    if (guarded_identity, guarded_digest) != (snapshot_identity, snapshot_digest):
        raise RuntimeError(
            "governance snapshot changed while its no-share guard was acquired",
        )
    assert_snapshot_aux_absent(config["registry_snapshot"])
    print(config["snapshot_evidence_prefix"] + snapshot_digest, flush=True)
    BoundPathFinder.allowed_files = allowed_files
    BoundPathFinder.allowed_directories = allowed_directories
    BoundPathFinder.allowed_archives = {
        path for path in allowed_files if path.lower().endswith((".zip", ".egg"))
    }
    importlib.machinery.PathFinder.find_spec = classmethod(
        guarded_direct_path_find_spec,
    )
    sys.meta_path[:] = [
        BoundPathFinder if finder is importlib.machinery.PathFinder else finder
        for finder in sys.meta_path
    ]
    sys.path[:] = [closure_identity["source"], canonical(config["site_packages"]), *sys.path]
    sys.path_importer_cache.clear()
    closure_runner = closure_identity["runner"]
    sys.argv[:] = [closure_runner, *config["business_arguments"]]
    print(config["sentinel"], flush=True)
    try:
        runpy.run_path(closure_runner, run_name="__main__")
    except BaseException:
        primary = sys.exc_info()
    try:
        assert_snapshot_aux_absent(config["registry_snapshot"])
        final_identity, final_digest = ordinary_file_identity(
            config["registry_snapshot"], "guarded governance snapshot",
        )
        if (final_identity, final_digest) != (snapshot_identity, snapshot_digest):
            raise RuntimeError("governance snapshot changed during business execution")
        closure_directories, closure_files = scan(
            config["closure_root"], "import closure",
        )
        if {canonical(path) for _, path in closure_files} != closure_identity["files"]:
            raise RuntimeError("import closure file membership changed during business")
        if {canonical(path) for path in closure_directories} != closure_identity["directories"]:
            raise RuntimeError("import closure directory membership changed during business")
    except BaseException:
        post_failure = sys.exc_info()
finally:
    cleanup_failures = close_guards(handles)
if primary is not None:
    raise primary[1].with_traceback(primary[2])
if post_failure is not None:
    raise post_failure[1].with_traceback(post_failure[2])
if cleanup_failures:
    raise OSError(cleanup_failures[0], "isolated launcher guard cleanup failed")
'@

$StartedAt = [datetime]::UtcNow
$ExitCode = 3
$BusinessExitCode = $null
$BusinessExecuted = $false
$BusinessInvocationAttempted = $false
$Output = @()
$Failure = $null
$IntegrityFailure = $null
$ResolvedRepository = $null
$ResolvedRuntime = $null
$CodeRoot = $null
$ResolvedPython = $null
$ResolvedGit = $null
$PyVenvConfig = $null
$VenvRoot = $null
$SitePackages = $null
$BaseRuntimeRoot = $null
$RuntimeSource = $null
$CodeSource = $null
$AuthorityRegistry = $null
$GovernanceStoreBefore = $null
$GovernanceStoreAfter = $null
$AuthorityStoreDirectory = $null
$AuthorityDirectoryGuard = [IntPtr]::Zero
$AuthorityFileGuards = New-Object 'System.Collections.Generic.List[System.IO.FileStream]'
$Wrapper = $null
$GovernanceRunner = $null
$JsonOutput = $null
$JsonStagingOutput = $null
$PendingJsonOutputText = $null
$RecordPath = $null
$LogPath = $null
$RegistrySnapshot = $null
$RegistrySnapshotDirectory = $null
$RegistrySnapshotDirectoryGuard = [IntPtr]::Zero
$RegistrySnapshotSha256 = $null
$LauncherSnapshotSha256 = $null
$SnapshotEvidencePrefix = $null
$ImportClosureRoot = $null
$ImportClosureEvidencePrefix = $null
$LauncherImportClosureSha256 = $null
$ImportClosureManifestEvidencePrefix = $null
$LauncherImportClosureManifestSha256 = $null
$LogDirectory = $null
$IdentityBefore = $null
$IdentityAfter = $null
$JsonOutputSha256 = $null
$VenvGuard = $null
$RuntimeGuard = $null
$CodeSourceGuard = $null
$BaseRuntimeGuard = $null
$GovernanceRunnerGuard = $null
$GitGuard = $null
$PyCacheRoot = $null
$LauncherExitCode = $null
$OutputLimitExceeded = $false
$JobGuardStrength = `
    "windows-create-suspended+assign-kill-on-close-job+resume-before-launcher"
$EnvironmentAttestation = "partial"
$PreviousPythonEnvironment = @{}
foreach ($Entry in @(Get-ChildItem Env: | Where-Object {
    $_.Name.StartsWith("PYTHON", [System.StringComparison]::OrdinalIgnoreCase)
})) {
    $PreviousPythonEnvironment[$Entry.Name] = [string]$Entry.Value
}
$PythonPathRestored = $false

try {
    if ($VintageId -and $VintageId -notmatch '^holdout-vintage-[0-9a-f]{64}$') {
        throw "VintageId must be a canonical holdout vintage identifier"
    }
    $CodeRoot = Get-NormalizedPath -Path (Join-Path $PSScriptRoot "..") `
        -Description "CodeRoot"
    $ResolvedRepository = Get-NormalizedPath -Path $Repository `
        -Description "live Repository"
    $ResolvedRuntime = Get-NormalizedPath -Path $RuntimeRoot `
        -Description "RuntimeRoot"
    $ResolvedPython = Get-NormalizedPath -Path $PythonExecutable `
        -Description "Python executable" -Leaf
    $ResolvedGit = Get-NormalizedPath -Path $GitExecutable `
        -Description "Git executable" -Leaf
    $script:GitExecutablePath = $ResolvedGit

    Assert-NoReparsePath -Path $CodeRoot -Description "CodeRoot"
    Assert-NoReparsePath -Path $ResolvedRepository -Description "live Repository"
    Assert-NoReparsePath -Path $ResolvedRuntime -Description "RuntimeRoot"
    Assert-NoReparsePath -Path $ResolvedPython -Description "Python executable"
    Assert-NoReparsePath -Path $ResolvedGit -Description "Git executable"
    Assert-OrdinaryFile -Path $ResolvedGit -Description "Git executable"
    $ActualGitSha256 = Get-FileSha256 -Path $ResolvedGit
    if ($ActualGitSha256 -cne $ExpectedGitSha256) {
        throw "Git executable SHA256 mismatch"
    }
    $GitGuard = [System.IO.File]::Open(
        $ResolvedGit,
        [System.IO.FileMode]::Open,
        [System.IO.FileAccess]::Read,
        [System.IO.FileShare]::Read
    )

    foreach ($Pair in @(
        @("CodeRoot", $CodeRoot, "live Repository", $ResolvedRepository),
        @("CodeRoot", $CodeRoot, "RuntimeRoot", $ResolvedRuntime),
        @("live Repository", $ResolvedRepository, "RuntimeRoot", $ResolvedRuntime)
    )) {
        if (Test-PathOverlap -Left $Pair[1] -Right $Pair[3]) {
            throw "$($Pair[0]) and $($Pair[2]) must be separate, non-nested roots"
        }
    }
    $RuntimeSource = Get-NormalizedPath `
        -Path (Join-Path $ResolvedRuntime "src") `
        -Description "RuntimeRoot/src"
    Assert-NoReparsePath -Path $RuntimeSource -Description "RuntimeRoot/src"
    $CodeSource = Get-NormalizedPath -Path (Join-Path $CodeRoot "src") `
        -Description "CodeRoot/src"
    Assert-NoReparsePath -Path $CodeSource -Description "CodeRoot/src"
    $AuthorityRegistry = Get-NormalizedPath `
        -Path (Join-Path $ResolvedRuntime "data\research\governance.sqlite3") `
        -Description "authoritative governance registry" -Leaf
    Assert-NoReparsePath -Path $AuthorityRegistry `
        -Description "authoritative governance registry"
    Assert-NoGovernanceRollbackJournal -Registry $AuthorityRegistry
    Assert-GovernanceSidecarsGuardable -Registry $AuthorityRegistry
    $AuthorityStoreDirectory = Get-NormalizedPath `
        -Path (Split-Path -Parent $AuthorityRegistry) `
        -Description "authoritative governance directory"
    $GovernanceStoreBefore = Get-GovernanceStoreIdentity `
        -Registry $AuthorityRegistry
    $AuthorityDirectoryGuard = [Guvolu.PreflightNative]::OpenDirectoryGuard(
        $AuthorityStoreDirectory
    )
    Assert-NoGovernanceRollbackJournal -Registry $AuthorityRegistry
    Assert-GovernanceSidecarsGuardable -Registry $AuthorityRegistry
    $GovernanceStoreGuarded = Get-GovernanceStoreIdentity `
        -Registry $AuthorityRegistry
    Assert-GovernanceSidecarsGuardable -Registry $AuthorityRegistry
    Assert-NoGovernanceRollbackJournal -Registry $AuthorityRegistry
    if (
        ($GovernanceStoreGuarded | ConvertTo-Json -Depth 5 -Compress) -cne
        ($GovernanceStoreBefore | ConvertTo-Json -Depth 5 -Compress)
    ) {
        throw "authoritative governance directory changed while its oplock was acquired"
    }
    foreach ($Entry in @($GovernanceStoreBefore.files | Where-Object { $_.exists })) {
        $AuthorityFileGuards.Add([System.IO.File]::Open(
            [string]$Entry.path,
            [System.IO.FileMode]::Open,
            [System.IO.FileAccess]::Read,
            [System.IO.FileShare]::Read
        ))
    }
    $GovernanceStoreGuarded = Get-GovernanceStoreIdentity `
        -Registry $AuthorityRegistry
    if (
        ($GovernanceStoreGuarded | ConvertTo-Json -Depth 5 -Compress) -cne
        ($GovernanceStoreBefore | ConvertTo-Json -Depth 5 -Compress)
    ) {
        throw "authoritative governance store changed while file guards were acquired"
    }
    if (-not (Test-PathOverlap -Left $CodeRoot -Right $ResolvedPython)) {
        throw "Python executable must be inside CodeRoot: $ResolvedPython"
    }
    if ([System.IO.Path]::GetExtension($ResolvedPython) -ine ".exe") {
        throw "PythonExecutable must be an .exe file"
    }

    $Wrapper = Get-NormalizedPath `
        -Path (Join-Path $CodeRoot "scripts\run_holdout_preflight_task.ps1") `
        -Description "preflight wrapper" -Leaf
    $GovernanceRunner = Get-NormalizedPath `
        -Path (Join-Path $CodeRoot "scripts\preflight_holdout.py") `
        -Description "governance runner" -Leaf
    $PythonDirectory = Split-Path -Parent $ResolvedPython
    $VenvRoot = Get-NormalizedPath `
        -Path (Split-Path -Parent $PythonDirectory) `
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
    Assert-NoReparsePath -Path $VenvRoot -Description "Python venv root"
    Assert-NoReparsePath -Path $SitePackages -Description "venv site-packages"
    Assert-NoReparsePath -Path $BaseRuntimeRoot `
        -Description "base Python runtime"
    foreach ($Pair in @(
        @("base Python runtime", $BaseRuntimeRoot, "CodeRoot", $CodeRoot),
        @("base Python runtime", $BaseRuntimeRoot, "live Repository", $ResolvedRepository),
        @("base Python runtime", $BaseRuntimeRoot, "RuntimeRoot", $ResolvedRuntime),
        @("base Python runtime", $BaseRuntimeRoot, "Python venv", $VenvRoot)
    )) {
        if (Test-PathOverlap -Left $Pair[1] -Right $Pair[3]) {
            throw "$($Pair[0]) and $($Pair[2]) must be separate, non-nested roots"
        }
    }

    Assert-NoCodeStartupInjection -Root (Join-Path $CodeRoot "scripts") `
        -Description "CodeRoot/scripts"
    Assert-NoCodeStartupInjection -Root (Join-Path $CodeRoot "src") `
        -Description "CodeRoot/src"
    Assert-NoCodeStartupInjection -Root $RuntimeSource `
        -Description "RuntimeRoot/src"
    Assert-NoVenvStartupCustomizer -SitePackages $SitePackages
    Assert-NoPythonPathConfig -Root $BaseRuntimeRoot `
        -Description "base Python runtime"

    $IdentityBefore = Get-CodeIdentity -CodeRoot $CodeRoot `
        -ExpectedHead $ExpectedCodeHead -Wrapper $Wrapper `
        -GovernanceRunner $GovernanceRunner -Python $ResolvedPython `
        -PyVenvConfig $PyVenvConfig
    Assert-ExpectedIdentity -Identity $IdentityBefore

    $GovernanceRunnerGuard = [System.IO.File]::Open(
        $GovernanceRunner,
        [System.IO.FileMode]::Open,
        [System.IO.FileAccess]::Read,
        [System.IO.FileShare]::Read
    )
    $BaseRuntimeGuard = Get-BoundTreeIdentity -Root $BaseRuntimeRoot `
        -Description "base Python runtime" -HoldOpen
    Assert-TreeIdentity -Identity $BaseRuntimeGuard `
        -ExpectedSha256 $ExpectedBaseRuntimeTreeSha256 `
        -ExpectedFileCount $ExpectedBaseRuntimeFileCount `
        -ExpectedTotalBytes $ExpectedBaseRuntimeTotalBytes `
        -Description "base Python runtime"
    $VenvGuard = Get-BoundTreeIdentity -Root $VenvRoot `
        -Description "Python venv" -HoldOpen
    Assert-TreeIdentity -Identity $VenvGuard `
        -ExpectedSha256 $ExpectedVenvTreeSha256 `
        -ExpectedFileCount $ExpectedVenvFileCount `
        -ExpectedTotalBytes $ExpectedVenvTotalBytes `
        -Description "Python venv"
    $RuntimeGuard = Get-BoundTreeIdentity -Root $RuntimeSource `
        -Description "RuntimeRoot/src" -HoldOpen
    Assert-TreeIdentity -Identity $RuntimeGuard `
        -ExpectedSha256 $ExpectedRuntimeSourceTreeSha256 `
        -ExpectedFileCount $ExpectedRuntimeSourceFileCount `
        -ExpectedTotalBytes $ExpectedRuntimeSourceTotalBytes `
        -Description "RuntimeRoot/src"
    $CodeSourceGuard = Get-BoundTreeIdentity -Root $CodeSource `
        -Description "CodeRoot/src" -HoldOpen
    Assert-TreeIdentity -Identity $CodeSourceGuard `
        -ExpectedSha256 $ExpectedCodeSourceTreeSha256 `
        -ExpectedFileCount $ExpectedCodeSourceFileCount `
        -ExpectedTotalBytes $ExpectedCodeSourceTotalBytes `
        -Description "CodeRoot/src"

    $LogDirectory = Join-Path $ResolvedRepository `
        "logs\research\frozen-forward\preflight"
    $LogPath = Join-Path $ResolvedRepository `
        "logs\research\frozen-forward\preflight-scheduler.jsonl"
    New-Item -ItemType Directory -Path $LogDirectory -Force | Out-Null
    Assert-NoReparsePath -Path $LogDirectory -Description "scheduler log directory"
    if (Test-Path -LiteralPath $LogPath) {
        Assert-OrdinaryFile -Path $LogPath -Description "scheduler JSONL index"
    }
    $RunId = $StartedAt.ToString("yyyyMMddTHHmmss.fffffffZ") + "-" + `
        [guid]::NewGuid().ToString("N")
    $JsonOutput = Join-Path $LogDirectory ("preflight-$RunId.json")
    $JsonStagingOutput = Join-Path $LogDirectory (
        "preflight-staging-$RunId.json"
    )
    $RecordPath = Join-Path $LogDirectory ("scheduler-$RunId.json")
    $RegistrySnapshotDirectory = Join-Path $LogDirectory `
        ("governance-snapshot-$RunId")
    New-Item -ItemType Directory -Path $RegistrySnapshotDirectory `
        -ErrorAction Stop | Out-Null
    Assert-NoReparsePath -Path $RegistrySnapshotDirectory `
        -Description "disposable governance snapshot directory"
    $RegistrySnapshotDirectoryGuard = `
        [Guvolu.PreflightNative]::OpenDirectoryIdentityGuard(
            $RegistrySnapshotDirectory
        )
    Assert-NoReparsePath -Path $RegistrySnapshotDirectory `
        -Description "guarded governance snapshot directory"
    if (@(Get-ChildItem -LiteralPath $RegistrySnapshotDirectory -Force).Count -ne 0) {
        throw "governance snapshot directory was not created empty"
    }
    $RegistrySnapshot = Join-Path $RegistrySnapshotDirectory `
        "governance.sqlite3"

    $PyCacheRoot = Join-Path $LogDirectory ("pycache-$RunId")
    New-Item -ItemType Directory -Path $PyCacheRoot -ErrorAction Stop | Out-Null
    Assert-NoReparsePath -Path $PyCacheRoot `
        -Description "isolated empty pycache prefix"
    if (@(Get-ChildItem -LiteralPath $PyCacheRoot -Force).Count -ne 0) {
        throw "isolated pycache prefix was not created empty"
    }

    foreach ($Entry in @(Get-ChildItem Env: | Where-Object {
        $_.Name.StartsWith("PYTHON", [System.StringComparison]::OrdinalIgnoreCase)
    })) {
        Remove-Item ("Env:" + $Entry.Name) -ErrorAction Stop
    }
    $Arguments = @(
        "--root", $ResolvedRuntime,
        "--registry", $RegistrySnapshot,
        "--json-output", $JsonStagingOutput
    )
    if ($VintageId) {
        $Arguments += @("--vintage-id", $VintageId)
    }
    $Sentinel = "GUVALU_PREFLIGHT_BUSINESS_ENTERED_" + `
        [guid]::NewGuid().ToString("N")
    $SnapshotEvidencePrefix = "GUVALU_PREFLIGHT_SNAPSHOT_SHA256_" + `
        [guid]::NewGuid().ToString("N") + "="
    $ImportClosureEvidencePrefix = "GUVALU_PREFLIGHT_IMPORT_CLOSURE_SHA256_" + `
        [guid]::NewGuid().ToString("N") + "="
    $ImportClosureManifestEvidencePrefix = `
        "GUVALU_PREFLIGHT_IMPORT_CLOSURE_MANIFEST_SHA256_" + `
        [guid]::NewGuid().ToString("N") + "="
    $ImportClosureRoot = Join-Path $LogDirectory (
        "import-closure-$($ExpectedCodeSourceTreeSha256.Substring(0, 16))-$RunId"
    )
    New-Item -ItemType Directory -Path $ImportClosureRoot `
        -ErrorAction Stop | Out-Null
    Assert-NoReparsePath -Path $ImportClosureRoot `
        -Description "empty import closure root"
    if (@(Get-ChildItem -LiteralPath $ImportClosureRoot -Force).Count -ne 0) {
        throw "import closure root was not created empty"
    }
    $LauncherConfig = [ordered]@{
        sentinel = $Sentinel
        snapshot_evidence_prefix = $SnapshotEvidencePrefix
        closure_evidence_prefix = $ImportClosureEvidencePrefix
        closure_manifest_evidence_prefix = `
            $ImportClosureManifestEvidencePrefix
        python = $ResolvedPython
        code_root = $CodeRoot
        code_source = $CodeSource
        runner = $GovernanceRunner
        runner_sha256 = $ExpectedGovernanceRunnerSha256
        runtime_source = $RuntimeSource
        site_packages = $SitePackages
        venv_root = $VenvRoot
        base_root = $BaseRuntimeRoot
        pycache = $PyCacheRoot
        authority_registry = $AuthorityRegistry
        registry_snapshot = $RegistrySnapshot
        closure_root = $ImportClosureRoot
        business_arguments = $Arguments
        venv = [ordered]@{
            sha256 = $ExpectedVenvTreeSha256
            count = $ExpectedVenvFileCount
            bytes = $ExpectedVenvTotalBytes
        }
        runtime = [ordered]@{
            sha256 = $ExpectedRuntimeSourceTreeSha256
            count = $ExpectedRuntimeSourceFileCount
            bytes = $ExpectedRuntimeSourceTotalBytes
        }
        code = [ordered]@{
            sha256 = $ExpectedCodeSourceTreeSha256
            count = $ExpectedCodeSourceFileCount
            bytes = $ExpectedCodeSourceTotalBytes
        }
        base = [ordered]@{
            sha256 = $ExpectedBaseRuntimeTreeSha256
            count = $ExpectedBaseRuntimeFileCount
            bytes = $ExpectedBaseRuntimeTotalBytes
        }
    }
    $LauncherConfigJson = $LauncherConfig | ConvertTo-Json -Depth 6 -Compress
    $LauncherArguments = @(
        "-I", "-S", "-B", "-X", "utf8", "-X",
        "pycache_prefix=$PyCacheRoot", "-c", $IsolatedLauncher,
        $LauncherConfigJson
    )
    if ([Guvolu.PreflightNative]::DirectoryGuardBreakPending(
        $AuthorityDirectoryGuard
    )) {
        throw "authoritative governance directory oplock broke before business invocation"
    }
    Assert-NoGovernanceRollbackJournal -Registry $AuthorityRegistry
    Assert-GovernanceSidecarsGuardable -Registry $AuthorityRegistry
    $GovernanceStorePreInvocation = Get-GovernanceStoreIdentity `
        -Registry $AuthorityRegistry
    if (
        ($GovernanceStorePreInvocation | ConvertTo-Json -Depth 5 -Compress) -cne
        ($GovernanceStoreBefore | ConvertTo-Json -Depth 5 -Compress)
    ) {
        throw "authoritative governance store changed before business invocation"
    }
    $LaunchResult = Invoke-BoundedIsolatedPython -Python $ResolvedPython `
        -WorkingDirectory $CodeRoot `
        -Arguments $LauncherArguments `
        -TimeoutSeconds $ExecutionTimeoutSeconds
    $LauncherExitCode = [int]$LaunchResult.exit_code
    $StdoutLines = @(([string]$LaunchResult.stdout) -split "\r?\n")
    $BusinessExecuted = $StdoutLines -ccontains $Sentinel
    $SnapshotEvidenceLines = @($StdoutLines | Where-Object {
        $_.StartsWith(
            $SnapshotEvidencePrefix,
            [System.StringComparison]::Ordinal
        )
    })
    $ImportClosureEvidenceLines = @($StdoutLines | Where-Object {
        $_.StartsWith(
            $ImportClosureEvidencePrefix,
            [System.StringComparison]::Ordinal
        )
    })
    $ImportClosureManifestEvidenceLines = @($StdoutLines | Where-Object {
        $_.StartsWith(
            $ImportClosureManifestEvidencePrefix,
            [System.StringComparison]::Ordinal
        )
    })
    $CleanStdout = @($StdoutLines | Where-Object {
        $_ -and $_ -cne $Sentinel -and -not $_.StartsWith(
            $SnapshotEvidencePrefix,
            [System.StringComparison]::Ordinal
        ) -and -not $_.StartsWith(
            $ImportClosureEvidencePrefix,
            [System.StringComparison]::Ordinal
        ) -and -not $_.StartsWith(
            $ImportClosureManifestEvidencePrefix,
            [System.StringComparison]::Ordinal
        )
    })
    $Output = @($CleanStdout)
    if ([string]$LaunchResult.stderr) {
        $Output += @(([string]$LaunchResult.stderr) -split "\r?\n" | `
            Where-Object { $_ })
    }
    $OutputLimitExceeded = [bool]$LaunchResult.output_limit_exceeded
    if ($OutputLimitExceeded) {
        if ($BusinessExecuted) { $BusinessExitCode = 126 }
        throw "isolated Python stdout/stderr exceeded the 1048576-byte per-stream limit and its job was terminated"
    }
    if ($SnapshotEvidenceLines.Count -ne 1) {
        throw "isolated launcher did not emit exactly one governance snapshot identity"
    }
    if ($ImportClosureEvidenceLines.Count -ne 1) {
        throw "isolated launcher did not emit exactly one import closure identity"
    }
    if ($ImportClosureManifestEvidenceLines.Count -ne 1) {
        throw "isolated launcher did not emit exactly one import closure manifest identity"
    }
    $LauncherImportClosureSha256 = $ImportClosureEvidenceLines[0].Substring(
        $ImportClosureEvidencePrefix.Length
    )
    if ($LauncherImportClosureSha256 -notmatch '^[0-9a-f]{64}$') {
        throw "isolated launcher emitted a malformed import closure identity"
    }
    $LauncherImportClosureManifestSha256 = `
        $ImportClosureManifestEvidenceLines[0].Substring(
            $ImportClosureManifestEvidencePrefix.Length
        )
    if ($LauncherImportClosureManifestSha256 -notmatch '^[0-9a-f]{64}$') {
        throw "isolated launcher emitted a malformed import closure manifest identity"
    }
    $LauncherSnapshotSha256 = $SnapshotEvidenceLines[0].Substring(
        $SnapshotEvidencePrefix.Length
    )
    if ($LauncherSnapshotSha256 -notmatch '^[0-9a-f]{64}$') {
        throw "isolated launcher emitted a malformed governance snapshot identity"
    }
    $RegistrySnapshotSha256 = $LauncherSnapshotSha256
    if (-not $BusinessExecuted) {
        throw "isolated launcher did not enter governance runner (exit $LauncherExitCode)"
    }
    $BusinessInvocationAttempted = $true
    if ([bool]$LaunchResult.timed_out) {
        $BusinessExitCode = 124
        throw "governance preflight exceeded $ExecutionTimeoutSeconds seconds and its job was terminated"
    }
    $BusinessExitCode = $LauncherExitCode
    $ExitCode = $LauncherExitCode

    try {
        Assert-BoundTreeGuardsUnbroken -Identity $BaseRuntimeGuard `
            -Description "base Python runtime"
        Assert-BoundTreeGuardsUnbroken -Identity $VenvGuard `
            -Description "Python venv"
        Assert-BoundTreeGuardsUnbroken -Identity $CodeSourceGuard `
            -Description "CodeRoot/src"
        Assert-BoundTreeGuardsUnbroken -Identity $RuntimeGuard `
            -Description "RuntimeRoot/src"
        Assert-NoCodeStartupInjection -Root (Join-Path $CodeRoot "scripts") `
            -Description "CodeRoot/scripts"
        Assert-NoCodeStartupInjection -Root (Join-Path $CodeRoot "src") `
            -Description "CodeRoot/src"
        Assert-NoCodeStartupInjection -Root $RuntimeSource `
            -Description "RuntimeRoot/src"
        Assert-NoVenvStartupCustomizer -SitePackages $SitePackages
        Assert-NoPythonPathConfig -Root $BaseRuntimeRoot `
            -Description "base Python runtime"
        if ([Guvolu.PreflightNative]::DirectoryGuardBreakPending(
            $AuthorityDirectoryGuard
        )) {
            throw "authoritative governance directory oplock was broken by a write/delete attempt"
        }
        $GovernanceStoreAfter = Get-GovernanceStoreIdentity `
            -Registry $AuthorityRegistry
        Assert-NoGovernanceRollbackJournal -Registry $AuthorityRegistry
        if (
            ($GovernanceStoreAfter | ConvertTo-Json -Depth 5 -Compress) -cne
            ($GovernanceStoreBefore | ConvertTo-Json -Depth 5 -Compress)
        ) {
            throw "authoritative governance SQLite DB/WAL/SHM/journal changed during read-only preflight"
        }
        if (-not (Test-Path -LiteralPath $RegistrySnapshot -PathType Leaf)) {
            throw "isolated launcher did not create its governance snapshot"
        }
        foreach ($SnapshotAuxiliary in @(
            "$RegistrySnapshot-wal",
            "$RegistrySnapshot-shm",
            "$RegistrySnapshot-journal"
        )) {
            if (Test-Path -LiteralPath $SnapshotAuxiliary) {
                throw "disposable governance snapshot auxiliary appeared: $SnapshotAuxiliary"
            }
        }
        Assert-OrdinaryFile -Path $RegistrySnapshot `
            -Description "disposable governance snapshot" -SingleLink
        $ObservedSnapshotSha256 = Get-FileSha256 -Path $RegistrySnapshot
        if ($ObservedSnapshotSha256 -cne $LauncherSnapshotSha256) {
            throw "disposable governance snapshot identity changed after guarded business execution"
        }
        Assert-TreeIdentity `
            -Identity (Get-BoundTreeIdentity -Root $BaseRuntimeRoot `
                -Description "base Python runtime") `
            -ExpectedSha256 $ExpectedBaseRuntimeTreeSha256 `
            -ExpectedFileCount $ExpectedBaseRuntimeFileCount `
            -ExpectedTotalBytes $ExpectedBaseRuntimeTotalBytes `
            -Description "base Python runtime"
        Assert-TreeIdentity `
            -Identity (Get-BoundTreeIdentity -Root $VenvRoot `
                -Description "Python venv") `
            -ExpectedSha256 $ExpectedVenvTreeSha256 `
            -ExpectedFileCount $ExpectedVenvFileCount `
            -ExpectedTotalBytes $ExpectedVenvTotalBytes `
            -Description "Python venv"
        Assert-TreeIdentity `
            -Identity (Get-BoundTreeIdentity -Root $RuntimeSource `
                -Description "RuntimeRoot/src") `
            -ExpectedSha256 $ExpectedRuntimeSourceTreeSha256 `
            -ExpectedFileCount $ExpectedRuntimeSourceFileCount `
            -ExpectedTotalBytes $ExpectedRuntimeSourceTotalBytes `
            -Description "RuntimeRoot/src"
        Assert-TreeIdentity `
            -Identity (Get-BoundTreeIdentity -Root $CodeSource `
                -Description "CodeRoot/src") `
            -ExpectedSha256 $ExpectedCodeSourceTreeSha256 `
            -ExpectedFileCount $ExpectedCodeSourceFileCount `
            -ExpectedTotalBytes $ExpectedCodeSourceTotalBytes `
            -Description "CodeRoot/src"
        $IdentityAfter = Get-CodeIdentity -CodeRoot $CodeRoot `
            -ExpectedHead $ExpectedCodeHead -Wrapper $Wrapper `
            -GovernanceRunner $GovernanceRunner -Python $ResolvedPython `
            -PyVenvConfig $PyVenvConfig
        Assert-ExpectedIdentity -Identity $IdentityAfter
        if (Test-Path -LiteralPath $JsonStagingOutput -PathType Leaf) {
            Assert-OrdinaryFile -Path $JsonStagingOutput `
                -Description "staged preflight JSON output" -SingleLink
            $PendingJsonOutputText = [System.IO.File]::ReadAllText(
                $JsonStagingOutput,
                (New-Object System.Text.UTF8Encoding($false, $true))
            )
            [void]($PendingJsonOutputText | ConvertFrom-Json -ErrorAction Stop)
        } elseif ($BusinessExitCode -eq 0) {
            throw "successful governance preflight did not create its staged JSON output"
        }
    } catch {
        $IntegrityFailure = $_.Exception.Message
        $ExitCode = 3
    }
} catch {
    $Failure = $_.Exception.Message
    if ($Output.Count -eq 0) { $Output = @($Failure) }
    $ExitCode = 3
} finally {
    $CleanupFailures = New-Object 'System.Collections.Generic.List[string]'
    foreach ($Entry in @(Get-ChildItem Env: | Where-Object {
        $_.Name.StartsWith("PYTHON", [System.StringComparison]::OrdinalIgnoreCase)
    })) {
        try { Remove-Item ("Env:" + $Entry.Name) -ErrorAction Stop } catch {
            $CleanupFailures.Add($_.Exception.Message)
        }
    }
    foreach ($Name in $PreviousPythonEnvironment.Keys) {
        try {
            [Environment]::SetEnvironmentVariable(
                [string]$Name, [string]$PreviousPythonEnvironment[$Name]
            )
        } catch { $CleanupFailures.Add($_.Exception.Message) }
    }
    $CurrentPythonEnvironment = @{}
    foreach ($Entry in @(Get-ChildItem Env: | Where-Object {
        $_.Name.StartsWith("PYTHON", [System.StringComparison]::OrdinalIgnoreCase)
    })) {
        $CurrentPythonEnvironment[$Entry.Name] = [string]$Entry.Value
    }
    $PythonPathRestored = $CurrentPythonEnvironment.Count -eq `
        $PreviousPythonEnvironment.Count
    if ($PythonPathRestored) {
        foreach ($Name in $PreviousPythonEnvironment.Keys) {
            if (-not $CurrentPythonEnvironment.ContainsKey($Name) -or
                [string]$CurrentPythonEnvironment[$Name] -cne `
                    [string]$PreviousPythonEnvironment[$Name]) {
                $PythonPathRestored = $false
                break
            }
        }
    }
    if (-not $PythonPathRestored) {
        $CleanupFailures.Add("PYTHON* environment was not restored exactly")
    }
    foreach ($Stream in $AuthorityFileGuards) {
        try { $Stream.Dispose() } catch {
            $CleanupFailures.Add(
                "authority SQLite guard cleanup failed: $($_.Exception.Message)"
            )
        }
    }
    if ($AuthorityDirectoryGuard -ne [IntPtr]::Zero) {
        try {
            [Guvolu.PreflightNative]::CloseChecked(
                $AuthorityDirectoryGuard, "authority directory oplock"
            )
        } catch { $CleanupFailures.Add($_.Exception.Message) }
    }
    try {
        Close-BoundTreeIdentity -Identity $RuntimeGuard `
            -Description "RuntimeRoot/src"
    } catch { $CleanupFailures.Add($_.Exception.Message) }
    try {
        Close-BoundTreeIdentity -Identity $CodeSourceGuard `
            -Description "CodeRoot/src"
    } catch { $CleanupFailures.Add($_.Exception.Message) }
    try {
        Close-BoundTreeIdentity -Identity $VenvGuard `
            -Description "Python venv"
    } catch { $CleanupFailures.Add($_.Exception.Message) }
    try {
        Close-BoundTreeIdentity -Identity $BaseRuntimeGuard `
            -Description "base Python runtime"
    } catch { $CleanupFailures.Add($_.Exception.Message) }
    if ($null -ne $GovernanceRunnerGuard) {
        try { $GovernanceRunnerGuard.Dispose() } catch {
            $CleanupFailures.Add(
                "governance runner guard cleanup failed: $($_.Exception.Message)"
            )
        }
    }
    if ($null -ne $GitGuard) {
        try { $GitGuard.Dispose() } catch {
            $CleanupFailures.Add(
                "Git executable guard cleanup failed: $($_.Exception.Message)"
            )
        }
    }
    if ($PyCacheRoot -and (Test-Path -LiteralPath $PyCacheRoot)) {
        try {
            Assert-NoReparsePath -Path $PyCacheRoot `
                -Description "isolated pycache prefix"
            if (@(Get-ChildItem -LiteralPath $PyCacheRoot -Force).Count -ne 0) {
                throw "isolated pycache prefix is not empty after -B execution"
            }
            [System.IO.Directory]::Delete($PyCacheRoot, $false)
        } catch { $CleanupFailures.Add($_.Exception.Message) }
    }
    if ($RegistrySnapshot) {
        foreach ($SnapshotFile in @(
            $RegistrySnapshot,
            "$RegistrySnapshot-wal",
            "$RegistrySnapshot-shm",
            "$RegistrySnapshot-journal"
        )) {
            if (Test-Path -LiteralPath $SnapshotFile) {
                try {
                    Assert-NoReparsePath -Path $SnapshotFile `
                        -Description "disposable governance snapshot"
                    Assert-OrdinaryFile -Path $SnapshotFile `
                        -Description "disposable governance snapshot"
                    [System.IO.File]::Delete($SnapshotFile)
                } catch { $CleanupFailures.Add($_.Exception.Message) }
            }
        }
    }
    if ($RegistrySnapshotDirectoryGuard -ne [IntPtr]::Zero) {
        try {
            [Guvolu.PreflightNative]::CloseChecked(
                $RegistrySnapshotDirectoryGuard,
                "governance snapshot directory identity guard"
            )
        } catch { $CleanupFailures.Add($_.Exception.Message) }
    }
    if ($RegistrySnapshotDirectory -and (
        Test-Path -LiteralPath $RegistrySnapshotDirectory
    )) {
        try {
            Assert-NoReparsePath -Path $RegistrySnapshotDirectory `
                -Description "disposable governance snapshot directory cleanup"
            if (@(
                Get-ChildItem -LiteralPath $RegistrySnapshotDirectory -Force
            ).Count -ne 0) {
                throw "governance snapshot directory is not empty at cleanup"
            }
            [System.IO.Directory]::Delete($RegistrySnapshotDirectory, $false)
        } catch { $CleanupFailures.Add($_.Exception.Message) }
    }
    if ($ImportClosureRoot -and (Test-Path -LiteralPath $ImportClosureRoot)) {
        try {
            if (-not $LogDirectory -or -not (
                Test-PathOverlap -Left $ImportClosureRoot -Right $LogDirectory
            ) -or -not ([System.IO.Path]::GetFileName(
                $ImportClosureRoot
            )).StartsWith(
                "import-closure-", [System.StringComparison]::Ordinal
            )) {
                throw "refusing to clean an import closure outside its exact scheduler run root"
            }
            Assert-NoReparsePath -Path $ImportClosureRoot `
                -Description "disposable import closure root"
            foreach ($ClosureItem in @(
                Get-ChildItem -LiteralPath $ImportClosureRoot -Recurse -Force
            )) {
                if (($ClosureItem.Attributes -band `
                    [System.IO.FileAttributes]::ReparsePoint) -ne 0) {
                    throw "disposable import closure contains a reparse entry: $($ClosureItem.FullName)"
                }
            }
            [System.IO.Directory]::Delete($ImportClosureRoot, $true)
        } catch { $CleanupFailures.Add($_.Exception.Message) }
    }
    if ($CleanupFailures.Count -ne 0) {
        $CleanupMessage = $CleanupFailures -join "; "
        $IntegrityFailure = if ($IntegrityFailure) {
            "$IntegrityFailure; $CleanupMessage"
        } else { $CleanupMessage }
        $ExitCode = 3
    }
}

$PublishedJsonOutput = $false
if (
    $ExitCode -eq 0 -and $BusinessExitCode -eq 0 -and
    $null -ne $PendingJsonOutputText
) {
    try {
        Write-AtomicUtf8File -Path $JsonOutput -Text $PendingJsonOutputText
        Assert-OrdinaryFile -Path $JsonOutput `
            -Description "published preflight JSON output" -SingleLink
        $JsonOutputSha256 = Get-FileSha256 -Path $JsonOutput
        $PublishedJsonOutput = $true
    } catch {
        $IntegrityFailure = if ($IntegrityFailure) {
            "$IntegrityFailure; staged preflight JSON publish failed: $($_.Exception.Message)"
        } else {
            "staged preflight JSON publish failed: $($_.Exception.Message)"
        }
        $ExitCode = 3
    }
}
if ($JsonStagingOutput -and (Test-Path -LiteralPath $JsonStagingOutput)) {
    try {
        Assert-OrdinaryFile -Path $JsonStagingOutput `
            -Description "staged preflight JSON cleanup" -SingleLink
        [System.IO.File]::Delete($JsonStagingOutput)
    } catch {
        $IntegrityFailure = if ($IntegrityFailure) {
            "$IntegrityFailure; staged preflight JSON cleanup failed: $($_.Exception.Message)"
        } else {
            "staged preflight JSON cleanup failed: $($_.Exception.Message)"
        }
        $ExitCode = 3
        if ($PublishedJsonOutput -and (Test-Path -LiteralPath $JsonOutput)) {
            try { [System.IO.File]::Delete($JsonOutput) } catch { }
            $JsonOutputSha256 = $null
        }
    }
}

$Record = [ordered]@{
    started_at = $StartedAt.ToString("o")
    completed_at = [datetime]::UtcNow.ToString("o")
    business_executed = $BusinessExecuted
    business_invocation_attempted = $BusinessInvocationAttempted
    business_exit_code = $BusinessExitCode
    exit_code = $ExitCode
    failure = $Failure
    integrity_failure = $IntegrityFailure
    code_root = $CodeRoot
    expected_code_head = $ExpectedCodeHead
    identity_before = $IdentityBefore
    identity_after = $IdentityAfter
    live_repository = $ResolvedRepository
    runtime_root = $ResolvedRuntime
    authoritative_data_root = $ResolvedRuntime
    authoritative_governance_registry = $AuthorityRegistry
    governance_store_before = $GovernanceStoreBefore
    governance_store_after = $GovernanceStoreAfter
    governance_store_guard = `
        "read-share-only-files+directory-rh-swap-block+namespace-break-monitor"
    authority_sidecar_precondition = `
        "db+wal+shm-preexisting;rollback-journal-absent"
    registry_snapshot = $RegistrySnapshot
    registry_snapshot_sha256 = $RegistrySnapshotSha256
    launcher_snapshot_sha256 = $LauncherSnapshotSha256
    registry_snapshot_disposable = $true
    registry_snapshot_validation = `
        "source+snapshot-query_only+sqlite_schema+quick_check+foreign_key_check"
    registry_snapshot_journal_mode = "delete"
    registry_snapshot_auxiliaries = "absent-before-and-after-business"
    import_closure = $ImportClosureRoot
    import_closure_schema_version = 1
    import_closure_tree_sha256 = $LauncherImportClosureSha256
    import_closure_manifest_sha256 = $LauncherImportClosureManifestSha256
    import_closure_source_tree_sha256 = $ExpectedCodeSourceTreeSha256
    import_closure_entry_sha256 = $ExpectedGovernanceRunnerSha256
    import_closure_file_count = if ($ExpectedCodeSourceFileCount) {
        $ExpectedCodeSourceFileCount + 1
    } else { $null }
    child_pythonpath_present = $false
    expected_pythonpath = $null
    pythonpath_restored = $PythonPathRestored
    python_executable = $ResolvedPython
    git_executable = $ResolvedGit
    expected_git_sha256 = $ExpectedGitSha256
    expected_wrapper_sha256 = $ExpectedWrapperSha256
    expected_governance_runner_sha256 = $ExpectedGovernanceRunnerSha256
    expected_python_sha256 = $ExpectedPythonSha256
    expected_pyvenv_sha256 = $ExpectedPyVenvSha256
    expected_venv_tree_sha256 = $ExpectedVenvTreeSha256
    expected_runtime_source_tree_sha256 = $ExpectedRuntimeSourceTreeSha256
    expected_code_source_tree_sha256 = $ExpectedCodeSourceTreeSha256
    base_python_runtime = $BaseRuntimeRoot
    expected_base_runtime_tree_sha256 = $ExpectedBaseRuntimeTreeSha256
    environment_attestation = $EnvironmentAttestation
    isolated_python_startup = "-I -S -B -X utf8 -X pycache_prefix=<unique-empty>"
    job_guard_strength = $JobGuardStrength
    execution_timeout_seconds = $ExecutionTimeoutSeconds
    execution_timeout_scope = "post-CreateProcess suspended child through job completion"
    output_limit_bytes_per_stream = 1048576
    output_limit_exceeded = $OutputLimitExceeded
    launcher_exit_code = $LauncherExitCode
    vintage_id = if ($VintageId) { $VintageId } else { $null }
    governance_runner = $GovernanceRunner
    json_output = $JsonOutput
    json_output_staging = $JsonStagingOutput
    json_output_published = $PublishedJsonOutput -and $ExitCode -eq 0
    json_output_sha256 = $JsonOutputSha256
    scheduler_record = $RecordPath
    output = ($Output -join "`n")
}

if ($RecordPath) {
    $RecordJson = $Record | ConvertTo-Json -Depth 8 -Compress
    try {
        Write-AtomicUtf8File -Path $RecordPath -Text ($RecordJson + "`n")
        Add-DurableJsonLine -Path $LogPath -Line $RecordJson
    } catch {
        [Console]::Error.WriteLine(
            "scheduler evidence write failed after business_executed={0}: {1}",
            $BusinessExecuted,
            $_.Exception.Message
        )
        # Do not turn an already executed business result into an apparent retry.
    }
} elseif ($Failure) {
    [Console]::Error.WriteLine(
        "business_invocation_attempted={0}: {1}",
        $BusinessInvocationAttempted,
        $Failure
    )
}

exit $ExitCode

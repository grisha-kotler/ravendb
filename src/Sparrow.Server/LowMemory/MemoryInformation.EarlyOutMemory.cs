using System;
using System.Runtime.InteropServices;
using Sparrow.Platform;
using Sparrow.Server.Platform.Win32;
using Sparrow.Utils;

namespace Sparrow.LowMemory;

public partial class MemoryInformation
{
    public static bool DisableEarlyOutOfMemoryCheck =
        string.Equals(Environment.GetEnvironmentVariable("RAVEN_DISABLE_EARLY_OOM"), "true", StringComparison.OrdinalIgnoreCase);

    public static bool EnableEarlyOutOfMemoryCheck =
        string.Equals(Environment.GetEnvironmentVariable("RAVEN_ENABLE_EARLY_OOM"), "true", StringComparison.OrdinalIgnoreCase);

    public static bool EnableEarlyOutOfMemoryChecks = false; // we don't want this to run on the clients

    private static readonly EarlyOutOfMemoryInfoResult FailedEarlyOutOfMemoryResult = new()
    {
        AvailableMemory = new Size(256, SizeUnit.Megabytes),
        TotalCommittableMemory = new Size(384, SizeUnit.Megabytes), // also include "page file"
        CurrentCommitCharge = new Size(256, SizeUnit.Megabytes),
    };

    public static void AssertNotAboutToRunOutOfMemory()
    {
        if (EnableEarlyOutOfMemoryChecks == false)
            return;

        if (DisableEarlyOutOfMemoryCheck)
            return;

        if (PlatformDetails.RunningOnPosix &&       // we only _need_ this check on Windows
            EnableEarlyOutOfMemoryCheck == false)   // but we want to enable this manually if needed
            return;

        var memInfo = GetEarlyOutOfMemoryInfo();
        if (IsEarlyOutOfMemoryInternal(memInfo, earlyOutOfMemoryWarning: false, out _))
            ThrowInsufficientMemory(GetMemoryInfo());
    }

    private static EarlyOutOfMemoryInfoResult GetEarlyOutOfMemoryInfo()
    {
        if (_failedToGetAvailablePhysicalMemory)
        {
            if (Logger.IsInfoEnabled)
                Logger.Info("Because of a previous error in getting available memory, we are now lying and saying we have 256MB free");
            return FailedEarlyOutOfMemoryResult;
        }

        try
        {
            EarlyOutOfMemoryInfoResult result;

            if (PlatformDetails.RunningOnPosix == false)
            {
                result = GetEarlyOutOfMemoryInfoWindows();
            }
            else if (PlatformDetails.RunningOnMacOsx)
            {
                var info = GetMemoryInfoMacOs(process: null, extended: false);
                result = new EarlyOutOfMemoryInfoResult
                {
                    AvailableMemory = info.AvailableMemory,
                    CurrentCommitCharge = info.CurrentCommitCharge,
                    TotalCommittableMemory = info.TotalCommittableMemory
                };
            }
            else
            {
                var info = GetMemoryInfoLinux(smapsReader: null, extended: false);
                result = new EarlyOutOfMemoryInfoResult
                {
                    AvailableMemory = info.AvailableMemory,
                    CurrentCommitCharge = info.CurrentCommitCharge,
                    TotalCommittableMemory = info.TotalCommittableMemory
                };
            }

            return result;

        }
        catch (Exception e)
        {
            if (Logger.IsOperationsEnabled)
                Logger.Operations("Error while trying to get available memory, will stop trying and report that there is 256MB free only from now on", e);
            _failedToGetAvailablePhysicalMemory = true;
            return FailedEarlyOutOfMemoryResult;
        }
    }

    private static unsafe EarlyOutOfMemoryInfoResult GetEarlyOutOfMemoryInfoWindows()
    {
        // windows
        var memoryStatus = new Win32MemoryMethods.MemoryStatusEx
        {
            dwLength = (uint)sizeof(Win32MemoryMethods.MemoryStatusEx)
        };

        if (Win32MemoryMethods.GlobalMemoryStatusEx(&memoryStatus) == false)
        {
            if (Logger.IsInfoEnabled)
                Logger.Info("Failure when trying to read memory info from Windows, error code is: " + Marshal.GetLastWin32Error());
            return FailedEarlyOutOfMemoryResult;
        }

        long memoryStatusUllAvailPhys = (long)memoryStatus.ullAvailPhys;
        long totalPageFile = (long)memoryStatus.ullTotalPageFile;
        long availPageFile = (long)(memoryStatus.ullTotalPageFile - memoryStatus.ullAvailPageFile);

        if (Win32MemoryMethods.IsProcessInJob(ProcessHandle, IntPtr.Zero, out var isInJob) && isInJob)
        {
            Win32MemoryMethods.JOBOBJECT_EXTENDED_LIMIT_INFORMATION limits = default;
            if (Win32MemoryMethods.QueryInformationJobObject(IntPtr.Zero,
                    Win32MemoryMethods.JOBOBJECTINFOCLASS.ExtendedLimitInformation, (void*)&limits,
                    sizeof(Win32MemoryMethods.JOBOBJECT_EXTENDED_LIMIT_INFORMATION),
                    out int limitsOutputSize) == false ||
                limitsOutputSize != sizeof(Win32MemoryMethods.JOBOBJECT_EXTENDED_LIMIT_INFORMATION))
            {
                if (_reportedQueryJobObjectFailure == false && Logger.IsInfoEnabled)
                {
                    _reportedQueryJobObjectFailure = true;
                    Logger.Info(
                        $"Failure when trying to query job object information info from Windows, error code is: {Marshal.GetLastWin32Error()}. Output size: {limitsOutputSize} instead of {sizeof(Win32MemoryMethods.JOBOBJECT_EXTENDED_LIMIT_INFORMATION)}!");
                }
            }
            else
            {
                long maxSize = long.MaxValue;
                if (limits.BasicLimitInformation.MaximumWorkingSetSize != UIntPtr.Zero)
                {
                    maxSize = (long)limits.BasicLimitInformation.MaximumWorkingSetSize;
                }

                if (limits.ProcessMemoryLimit != UIntPtr.Zero)
                {
                    maxSize = Math.Min(maxSize, (long)limits.ProcessMemoryLimit);
                }

                if (limits.JobMemoryLimit != UIntPtr.Zero)
                {
                    maxSize = Math.Min(maxSize, (long)limits.ProcessMemoryLimit);
                }

                if (maxSize != long.MaxValue)
                {
                    (long workingSetInBytes, long _, long ? _) = GetProcessMemoryInfoForWindows();
                    var availableMemoryForProcessingInBytes = Math.Max(maxSize - workingSetInBytes, 0);
                    availPageFile = Math.Max(maxSize - workingSetInBytes, 0);
                    totalPageFile = maxSize;
                    memoryStatusUllAvailPhys = Math.Min(availableMemoryForProcessingInBytes, memoryStatusUllAvailPhys);
                }
            }
        }

        return new EarlyOutOfMemoryInfoResult
        {
            TotalCommittableMemory = new Size(totalPageFile, SizeUnit.Bytes),
            CurrentCommitCharge = new Size(availPageFile, SizeUnit.Bytes),
            AvailableMemory = new Size(memoryStatusUllAvailPhys, SizeUnit.Bytes),
        };
    }

    public static bool IsEarlyOutOfMemory(MemoryInfoResult memInfo, out Size commitChargeThreshold)
    {
        if (PlatformDetails.RunningOnPosix &&       // we only _need_ this check on Windows
            EnableEarlyOutOfMemoryCheck == false)   // but we want to enable this manually if needed
        {
            commitChargeThreshold = Size.Zero;
            return false;
        }

        return IsEarlyOutOfMemoryInternal(new EarlyOutOfMemoryInfoResult
        {
            AvailableMemory = memInfo.AvailableMemory,
            CurrentCommitCharge = memInfo.CurrentCommitCharge,
            TotalCommittableMemory = memInfo.TotalCommittableMemory
        }, earlyOutOfMemoryWarning: true, out commitChargeThreshold);
    }

    private static bool IsEarlyOutOfMemoryInternal(EarlyOutOfMemoryInfoResult memInfo, bool earlyOutOfMemoryWarning, out Size commitChargeThreshold)
    {
        // if we are about to create a new thread, might not always be a good idea:
        // https://ayende.com/blog/181537-B/production-test-run-overburdened-and-under-provisioned
        // https://ayende.com/blog/181569-A/threadpool-vs-pool-thread

        Size overage;
        if (memInfo.CurrentCommitCharge > memInfo.TotalCommittableMemory)
        {
            // this can happen on containers, since we get this information from the host, and
            // sometimes this kind of stat is shared, see:
            // https://fabiokung.com/2014/03/13/memory-inside-linux-containers/

            commitChargeThreshold = GetMinCommittedToKeep(TotalPhysicalMemory);
            overage =
                commitChargeThreshold +                                    //extra to keep free
                (TotalPhysicalMemory - memInfo.AvailableMemory);   //actually in use now

            return overage >= TotalPhysicalMemory;
        }

        commitChargeThreshold = GetMinCommittedToKeep(memInfo.TotalCommittableMemory);
        overage = commitChargeThreshold + memInfo.CurrentCommitCharge;
        return overage >= memInfo.TotalCommittableMemory;

        Size GetMinCommittedToKeep(Size currentValue)
        {
            var minFreeToKeep = Size.Min(_maxFreeCommittedMemoryToKeep, currentValue * _minimumFreeCommittedMemoryPercentage);

            if (earlyOutOfMemoryWarning)
            {
                return Size.Min(
                    _lowMemoryCommitLimitInMb,
                    // needs to be bigger than the MaxFreeCommittedMemoryToKeep
                    Size.Max(currentValue / 20, minFreeToKeep * 1.5));
            }

            return minFreeToKeep;
        }
    }

    private static void ThrowInsufficientMemory(MemoryInfoResult memInfo)
    {
        LowMemoryNotification.Instance.SimulateLowMemoryNotification();

        throw new EarlyOutOfMemoryException($"The amount of available memory to commit on the system is low. " +
                                            MemoryUtils.GetExtendedMemoryInfo(memInfo, GetDirtyMemoryState()), memInfo);

    }

    private struct EarlyOutOfMemoryInfoResult
    {
        public Size CurrentCommitCharge { get; set; }
        public Size TotalCommittableMemory { get; set; }
        public Size AvailableMemory { get; set; }
    }
}

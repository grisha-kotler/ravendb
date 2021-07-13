using System;
using System.Diagnostics;
using System.Runtime.InteropServices;
using Sparrow;

namespace Raven.Server.Utils
{
    public class MemoryHelper : IDisposable
    {
        private static IntPtr _handle = IntPtr.Zero;

        private static bool _disposed;

        [DllImport("kernel32.dll", CharSet = CharSet.Unicode)]
        private static extern IntPtr CreateJobObject(IntPtr a, string lpName);

        [DllImport("kernel32.dll")]
        private static extern bool SetInformationJobObject(IntPtr hJob, JobObjectInfoType infoType, IntPtr lpJobObjectInfo, UInt32 cbJobObjectInfoLength);

        [DllImport("kernel32.dll", SetLastError = true)]
        private static extern bool AssignProcessToJobObject(IntPtr job, IntPtr process);

        [DllImport("kernel32.dll", SetLastError = true)]
        [return: MarshalAs(UnmanagedType.Bool)]
        private static extern bool CloseHandle(IntPtr hObject);

        public static void SetMemoryLimit(Process process, double ramInGb)
        {
            if (_handle == IntPtr.Zero)
            {
                _handle = CreateJobObject(IntPtr.Zero, null);

                var info = new JOBOBJECT_BASIC_LIMIT_INFORMATION
                {
                    LimitFlags = 0x0200
                };

                var maxWorkingSetInBytes = (long)Size.ConvertToBytes(ramInGb, SizeUnit.Gigabytes);
                var extendedInfo = new JOBOBJECT_EXTENDED_LIMIT_INFORMATION
                {
                    BasicLimitInformation = info,
                    JobMemoryLimit = new UIntPtr((ulong)maxWorkingSetInBytes),
                    //PeakJobMemoryUsed = new UIntPtr((ulong)maxWorkingSetInBytes),
                    //PeakProcessMemoryUsed = new UIntPtr((ulong)ramInGb),
                    //ProcessMemoryLimit = new UIntPtr((ulong)maxWorkingSetInBytes),
                };

                int length = Marshal.SizeOf(typeof(JOBOBJECT_EXTENDED_LIMIT_INFORMATION));
                IntPtr extendedInfoPtr = Marshal.AllocHGlobal(length);
                Marshal.StructureToPtr(extendedInfo, extendedInfoPtr, false);

                if (SetInformationJobObject(_handle, JobObjectInfoType.ExtendedLimitInformation, extendedInfoPtr, (uint)length) == false)
                    throw new Exception(string.Format("Unable to set information.  Error: {0}", Marshal.GetLastWin32Error()));

                var result = AssignProcessToJobObject(_handle, process.Handle);
            }

        }

        public void Close()
        {
            CloseHandle(_handle);
            _handle = IntPtr.Zero;
        }

        public void Dispose()
        {
            if (_disposed)
                return;

            Close();
            _disposed = true;
            GC.SuppressFinalize(this);
        }

        [StructLayout(LayoutKind.Sequential)]
        struct IO_COUNTERS
        {
            public UInt64 ReadOperationCount;
            public UInt64 WriteOperationCount;
            public UInt64 OtherOperationCount;
            public UInt64 ReadTransferCount;
            public UInt64 WriteTransferCount;
            public UInt64 OtherTransferCount;
        }


        [StructLayout(LayoutKind.Sequential)]
        struct JOBOBJECT_BASIC_LIMIT_INFORMATION
        {
            public Int64 PerProcessUserTimeLimit;
            public Int64 PerJobUserTimeLimit;
            public UInt32 LimitFlags;
            public UIntPtr MinimumWorkingSetSize;
            public UIntPtr MaximumWorkingSetSize;
            public UInt32 ActiveProcessLimit;
            public UIntPtr Affinity;
            public UInt32 PriorityClass;
            public UInt32 SchedulingClass;
        }

        [StructLayout(LayoutKind.Sequential)]
        public struct SECURITY_ATTRIBUTES
        {
            public UInt32 nLength;
            public IntPtr lpSecurityDescriptor;
            public Int32 bInheritHandle;
        }

        [StructLayout(LayoutKind.Sequential)]
        struct JOBOBJECT_EXTENDED_LIMIT_INFORMATION
        {
            public JOBOBJECT_BASIC_LIMIT_INFORMATION BasicLimitInformation;
            public IO_COUNTERS IoInfo;
            public UIntPtr ProcessMemoryLimit;
            public UIntPtr JobMemoryLimit;
            public UIntPtr PeakProcessMemoryUsed;
            public UIntPtr PeakJobMemoryUsed;
        }

        public enum JobObjectInfoType
        {
            AssociateCompletionPortInformation = 7,
            BasicLimitInformation = 2,
            BasicUIRestrictions = 4,
            EndOfJobTimeInformation = 6,
            ExtendedLimitInformation = 9,
            SecurityLimitInformation = 5,
            GroupInformation = 11
        }

        [Flags]
        public enum LimitFlags
        {
            JOB_OBJECT_LIMIT_JOB_MEMORY = 0x00000200,
            JOB_OBJECT_LIMIT_PROCESS_MEMORY = 0x00000100
        }
    }
}

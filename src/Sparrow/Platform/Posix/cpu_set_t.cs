using System.Runtime.InteropServices;

namespace Sparrow.Platform.Posix
{
    [StructLayout(LayoutKind.Sequential)]
    public unsafe struct cpu_set_t
    {
        public fixed ulong __bits[CpuSet.Size];
    }

    public class CpuSet
    {
        public const int Size = sizeof(long) * 8;
    }
}

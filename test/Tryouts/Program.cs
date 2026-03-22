using System;
using System.Buffers;
using System.Collections.Generic;
using System.Diagnostics;
using System.Diagnostics.CodeAnalysis;
using System.Numerics;
using System.Runtime.CompilerServices;
using System.Runtime.InteropServices;
using System.Threading;
using System.Threading.Tasks;
using Lucene.Net.Search;
using Lucene.Net.Util;
using Sparrow;
using Sparrow.Json;

namespace Tryouts;

public static class Program
{
    static Program()
    {
        XunitLogging.RedirectStreams = false;
    }

    private static class CollectorsPool
    {
        public static ObjectPool<List<ScoreDoc>> Instance = new ObjectPool<List<ScoreDoc>>(() => new List<ScoreDoc>());
    }

    public static async Task Main(string[] args)
    {
        var runs = 1;
        var count = 16;
        var numberOfTasks = 8;

        var contextPool = new JsonContextPool();

        // warmup
        for (var i = 0; i < 131_072; i += 4096)
        {
            await RunOriginal(runs, i, runs, print: false);
            await RunNewArrayPool(runs, i, runs, print: false);
            await RunNewMemoryPool(runs, i, runs, print: false);
            await RunNewCustomMemoryPool(runs, i, runs, contextPool, print: false);
            await RunUnmanagedScoreDocArray(runs, i, runs, contextPool, print: false);
        }

        await RunOriginal(4, numberOfTasks, count, print: false);
        await RunNewArrayPool(4, numberOfTasks, count, print: false);
        await RunNewMemoryPool(4, numberOfTasks, count, print: false);
        await RunNewCustomMemoryPool(4, numberOfTasks, count, contextPool, print: false);
        await RunUnmanagedScoreDocArray(4, numberOfTasks, count, contextPool, print: false);

        Console.WriteLine($"Warmup done");

        var tuples = new List<(int runs, int numberOfTasks, int count)>
        {
            (128, 1, 128),
            (128, 1, 128),
            (128, 1, 128),
            (128, 4, 128),
            (128, 8, 128),
            (128, 16, 128),
            (128, 128, 128),
            (128, 8092, 128),
            (1024, 256, 128),
            (1024, 256, 128),
            (1024, 1024, 128),
            (1024, 8092, 128),
            (128, 1, 1_024),
            (128, 128, 1_024),
            (1024, 128, 1_024),
            (128, 4, 16_384),
            (128, 8, 16_384),
            (128, 128, 16_384),
            (128, 16, 16_384),
            (128, 32, 16_384),
            (128, 1, 131_072),
            (128, 4, 131_072),
            (128, 8, 131_072),
            (128, 1, 131_072),
            (128, 16, 131_072),
            (128, 32, 131_072),
        };

        Console.WriteLine($"Warmup 2 started");

        foreach ((int runs, int numberOfTasks, int count) tuple in tuples)
        {
            await RunOriginal(tuple.runs, tuple.numberOfTasks, tuple.count, print: false);
            await RunNewArrayPool(tuple.runs, tuple.numberOfTasks, tuple.count, print: false);
            await RunNewMemoryPool(tuple.runs, tuple.numberOfTasks, tuple.count, print: false);
            await RunNewCustomMemoryPool(tuple.runs, tuple.numberOfTasks, tuple.count, contextPool, print: false);
            await RunUnmanagedScoreDocArray(tuple.runs, tuple.numberOfTasks, tuple.count, contextPool, print: false);
        }

        Console.WriteLine($"Warmup 2 done");

        foreach ((int runs, int numberOfTasks, int count) tuple in tuples)
        {
            await RunOriginal(tuple.runs, tuple.numberOfTasks, tuple.count);
            await RunNewArrayPool(tuple.runs, tuple.numberOfTasks, tuple.count);
            await RunNewMemoryPool(tuple.runs, tuple.numberOfTasks, tuple.count);
            await RunNewCustomMemoryPool(tuple.runs, tuple.numberOfTasks, tuple.count, contextPool);
            await RunUnmanagedScoreDocArray(tuple.runs, tuple.numberOfTasks, tuple.count, contextPool);
            Console.WriteLine();
        }

        Console.WriteLine($"All Done");
        Console.ReadLine();
    }

    private static async Task RunNewArrayPool(int runs, int numberOfConcurrentTasks, int count, bool print = true)
    {
        var tasks = new List<Task>();
        long totalAllocatedBytes = 0;
        var sp = Stopwatch.StartNew();

        for (var i = 0; i < runs; i++)
        {
            tasks.Clear();

            for (var j = 0; j < numberOfConcurrentTasks; j++)
            {
                tasks.Add(Task.Run(() =>
                {
                    long localAllocated = 0;

                    var before = GC.GetAllocatedBytesForCurrentThread();

                    using var managedArray = new ManagedScoreDocArray();

                    for (var k = 0; k < count; k++)
                    {
                        managedArray.Add(k, k);
                    }

                    var reader = managedArray.GetReader(0);

                    while (reader.Read(out var doc, out var score))
                    {
                        var test = doc + score;
                    }

                    localAllocated += GC.GetAllocatedBytesForCurrentThread() - before;

                    Interlocked.Add(ref totalAllocatedBytes, localAllocated);
                }));
            }

            await Task.WhenAll(tasks.ToArray());
        }



        if (print)
            Console.WriteLine(
                $"NEW ArrayPool          | Docs: {count,9:N0} | Tasks: {numberOfConcurrentTasks} | Total time: {sp.Elapsed.TotalMilliseconds,10:N5} | AVG time: {sp.Elapsed.TotalMilliseconds / numberOfConcurrentTasks / count,10:N5} ms | Alloc: {new Size(totalAllocatedBytes, SizeUnit.Bytes)}");
    }

    private static async Task RunNewMemoryPool(int runs, int numberOfConcurrentTasks, int count, bool print = true)
    {
        var tasks = new List<Task>();
        long totalAllocatedBytes = 0;
        var sp = Stopwatch.StartNew();

        for (var i = 0; i < runs; i++)
        {
            tasks.Clear();

            for (var j = 0; j < numberOfConcurrentTasks; j++)
            {
                tasks.Add(Task.Run(() =>
                {
                    long localAllocated = 0;

                    var before = GC.GetAllocatedBytesForCurrentThread();

                    using var managedArray = new ManagedScoreDocArray_V2();

                    for (var k = 0; k < count; k++)
                    {
                        managedArray.Add(k, k);
                    }

                    var reader = managedArray.GetReader(0);

                    while (reader.Read(out var doc, out var score))
                    {
                        var test = doc + score;
                    }

                    localAllocated += GC.GetAllocatedBytesForCurrentThread() - before;

                    Interlocked.Add(ref totalAllocatedBytes, localAllocated);
                }));
            }

            await Task.WhenAll(tasks.ToArray());
        }

        if (print)
            Console.WriteLine(
                $"NEW MemoryPool         | Docs: {count,9:N0} | Tasks: {numberOfConcurrentTasks} | Total time: {sp.Elapsed.TotalMilliseconds,10:N5} | AVG time: {sp.Elapsed.TotalMilliseconds / numberOfConcurrentTasks / count,10:N5} ms | Alloc: {new Size(totalAllocatedBytes, SizeUnit.Bytes)}");
    }

    private static async Task RunUnmanagedScoreDocArray(int runs, int numberOfConcurrentTasks, int count, JsonContextPool contextPool, bool print = true)
    {
        var tasks = new List<Task>();
        long totalAllocatedBytes = 0;
        var sp = Stopwatch.StartNew();

        for (var i = 0; i < runs; i++)
        {
            tasks.Clear();

            for (var j = 0; j < numberOfConcurrentTasks; j++)
            {
                var toDispose = contextPool.AllocateOperationContext(out JsonOperationContext context);
                tasks.Add(Task.Run(() =>
                {
                    using (toDispose)
                    {
                        long localAllocated = 0;

                        var before = GC.GetAllocatedBytesForCurrentThread();
                        var beforeUnmanaged = context.AllocatedMemory;

                        using var managedArray = new UnmanagedScoreDocArray(context);

                        for (var k = 0; k < count; k++)
                        {
                            managedArray.Add(k, k);
                        }

                        var reader = managedArray.GetReader(0);

                        while (reader.Read(out var doc, out var score))
                        {
                            var test = doc + score;
                        }

                        localAllocated += GC.GetAllocatedBytesForCurrentThread() - before + (context.AllocatedMemory - beforeUnmanaged);

                        Interlocked.Add(ref totalAllocatedBytes, localAllocated);
                    }

                }));
            }

            await Task.WhenAll(tasks.ToArray());
        }

        if (print)
            Console.WriteLine(
                $"NEW unmanaged          | Docs: {count,9:N0} | Tasks: {numberOfConcurrentTasks} | Total time: {sp.Elapsed.TotalMilliseconds,10:N5} | AVG time: {sp.Elapsed.TotalMilliseconds / numberOfConcurrentTasks / count,10:N5} ms | Alloc: {new Size(totalAllocatedBytes, SizeUnit.Bytes)}");
    }

    public class CustomMemoryPool : MemoryPool<long>
    {
        private readonly JsonOperationContext _context;
        private int _totalAllocated;

        public CustomMemoryPool(JsonOperationContext context)
        {
            _context = context;
        }

        public override int MaxBufferSize => int.MaxValue;

        public override IMemoryOwner<long> Rent(int minBufferSize = -1)
        {
            if (minBufferSize <= 0)
                minBufferSize = 1;

            if (_totalAllocated > 16 * 1024)
            {
                return Shared.Rent(minBufferSize);
            }

            int sizeInBytes = minBufferSize * sizeof(long);
            AllocatedMemoryData allocated = _context.GetMemory(sizeInBytes);
            _totalAllocated += sizeInBytes;
            return new ContextMemoryOwner(allocated, _context);
        }

        protected override void Dispose(bool disposing)
        {
        }

        private sealed unsafe class ContextMemoryOwner : MemoryManager<long>
        {
            private AllocatedMemoryData _allocated;
            private readonly JsonOperationContext _context;

            public ContextMemoryOwner(AllocatedMemoryData allocated, JsonOperationContext context)
            {
                _allocated = allocated;
                _context = context;
            }

            public override Span<long> GetSpan()
            {
                if (_allocated == null)
                    ThrowDisposed();
                return new Span<long>(_allocated.Address, _allocated.SizeInBytes / sizeof(long));
            }

            public override MemoryHandle Pin(int elementIndex = 0)
            {
                if (_allocated == null)
                    ThrowDisposed();
                return new MemoryHandle(_allocated.Address + (elementIndex * sizeof(long)));
            }

            public override void Unpin()
            {
            }

            protected override void Dispose(bool disposing)
            {
                if (_allocated == null)
                    return;
                _context.ReturnMemory(_allocated);
                _allocated = null;
            }

            [DoesNotReturn]
            private static void ThrowDisposed() =>
                throw new ObjectDisposedException(nameof(ContextMemoryOwner));
        }
    }

    private static async Task RunNewCustomMemoryPool(int runs, int numberOfConcurrentTasks, int count, JsonContextPool contextPool, bool print = true)
    {
        var tasks = new List<Task>();
        long totalAllocatedBytes = 0;
        var sp = Stopwatch.StartNew();

        for (var i = 0; i < runs; i++)
        {
            tasks.Clear();

            for (var j = 0; j < numberOfConcurrentTasks; j++)
            {
                var toDispose = contextPool.AllocateOperationContext(out JsonOperationContext context);

                tasks.Add(Task.Run(() =>
                {
                    try
                    {
                        using (toDispose)
                        {
                            long localAllocated = 0;

                            var before = GC.GetAllocatedBytesForCurrentThread();
                            var beforeUnmanaged = context.AllocatedMemory;
                            using var managedArray = new ManagedScoreDocArray_V2(new CustomMemoryPool(context));

                            for (var k = 0; k < count; k++)
                            {
                                managedArray.Add(k, k);
                            }

                            var reader = managedArray.GetReader(0);

                            while (reader.Read(out var doc, out var score))
                            {
                                var test = doc + score;
                            }

                            localAllocated += GC.GetAllocatedBytesForCurrentThread() - before + (context.AllocatedMemory - beforeUnmanaged);

                            Interlocked.Add(ref totalAllocatedBytes, localAllocated);
                        }
                    }
                    catch (Exception e)
                    {
                        Console.WriteLine(e);
                        throw;
                    }
                }));
            }

            await Task.WhenAll(tasks.ToArray());
        }

        if (print)
            Console.WriteLine(
                $"NEW MemoryPool Context | Docs: {count,9:N0} | Tasks: {numberOfConcurrentTasks} | Total time: {sp.Elapsed.TotalMilliseconds,10:N5} | AVG time: {sp.Elapsed.TotalMilliseconds / numberOfConcurrentTasks / count,10:N5} ms | Alloc: {new Size(totalAllocatedBytes, SizeUnit.Bytes)}");
    }

    private static async Task RunOriginal(int runs, int numberOfConcurrentTasks, int count, bool print = true)
    {
        var tasks = new List<Task>();
        var sp = Stopwatch.StartNew();
        long totalAllocatedBytes = 0;

        for (var i = 0; i < runs; i++)
        {
            tasks.Clear();

            for (var j = 0; j < numberOfConcurrentTasks; j++)
            {
                tasks.Add(Task.Run(() =>
                {
                    long localAllocated = 0;
                    var before = GC.GetAllocatedBytesForCurrentThread();

                    var list = CollectorsPool.Instance.Allocate();

                    for (var k = 0; k < count; k++)
                    {
                        list.Add(new ScoreDoc(k, k));
                    }

                    var array = list.ToArray();

                    for (var k = 0; k < count; k++)
                    {
                        var doc = array[k];
                        var test = doc.Doc + doc.Score;
                    }

                    if (list.Count < 1_000_000)
                    {
                        list.Clear();
                        CollectorsPool.Instance.Free(list);
                    }

                    localAllocated += GC.GetAllocatedBytesForCurrentThread() - before;

                    Interlocked.Add(ref totalAllocatedBytes, localAllocated);
                }));
            }

            await Task.WhenAll(tasks.ToArray());
        }

        if (print)
            Console.WriteLine(
                $"OLD                    | Docs: {count,9:N0} | Tasks: {numberOfConcurrentTasks} | Total time: {sp.Elapsed.TotalMilliseconds,10:N5} | AVG time: {sp.Elapsed.TotalMilliseconds / numberOfConcurrentTasks / count,10:N5} ms |  Alloc: {new Size(totalAllocatedBytes, SizeUnit.Bytes)}");
    }


    public class ManagedScoreDocArray_V2 : IDisposable
    {
        public static readonly ManagedScoreDocArray_V2 Empty = new();

        public static MemoryPool<long> LongMemoryPool = MemoryPool<long>.Shared;
        public static MemoryPool<IComparable[]> FieldsMemoryPool = MemoryPool<IComparable[]>.Shared;

        private readonly MemoryPool<long> _longMemoryPool;
        private readonly MemoryPool<IComparable[]> _fieldsMemoryPool;

        // size of a single packed item (int doc + float score) = 8 bytes
        private const int SingleItemSize = sizeof(long);

        // 8,192 items * 8 bytes = 64KB. 
        // we keep a single array under the 85KB LOH threshold.
        private const int MaxItemsPerSegment = 64 * 1024 / SingleItemSize;

        // 128 items * 8 bytes = 1KB
        private const int InitialItems = 1 * 1024 / SingleItemSize;

        // Log2(8192) = 13
        private const int MaxItemsLog2 = 13;

        // Sequence: 128 -> 256 -> 512 -> 1024 -> 2048 -> 4096
        // The next one (8192) is the start of stable phase.
        // Sum = 128 + 256 + ... + 4096 = 8064 items.
        private const int GrowthPhaseTotalItems = 8064;

        // There are 6 segments in the growth phase (0 to 5)
        // Segment 6 is the first "Stable" (max Size) segment.
        private const int StablePhaseSegmentStartIndex = 6;

        // Log2(128) = 7. Used for shifting in growth phase.
        private const int GrowthPhaseShift = 7;

        public readonly List<Segment> _segments = new();

        // Hot path caching - points to the current segment's memory
        private Memory<long> _currentPacked;
        public int _currentSegmentUsed;
        private int _currentSegmentCapacity;

        private int _length;
        public int Length => _length;

        public ManagedScoreDocArray_V2(MemoryPool<long> longMemoryPool = null, MemoryPool<IComparable[]> fieldsMemoryPool = null)
        {
            _longMemoryPool = longMemoryPool ?? LongMemoryPool;
            _fieldsMemoryPool = fieldsMemoryPool ?? FieldsMemoryPool;
        }

        public ManagedScoreDocArray_V2(int totalItems, bool hasFields) : this()
        {
            _length = totalItems;

            int remainingToAllocate = totalItems;
            int currentSize = InitialItems;

            while (_segments.Count < StablePhaseSegmentStartIndex && remainingToAllocate > 0)
            {
                AllocateSegment(currentSize, hasFields);
                remainingToAllocate -= currentSize;
                currentSize *= 2;
            }

            if (remainingToAllocate > 0)
            {
                int stableSegmentsNeeded = (remainingToAllocate + (MaxItemsPerSegment - 1)) >> MaxItemsLog2;

                for (int i = 0; i < stableSegmentsNeeded; i++)
                {
                    AllocateSegment(MaxItemsPerSegment, hasFields);
                }
            }

            if (_segments.Count > 0)
            {
                var lastIndex = _segments.Count - 1;
                var lastSeg = _segments[lastIndex];

                _currentPacked = lastSeg.PackedOwner.Memory;
                _currentSegmentCapacity = lastSeg.Capacity;

                int itemsInPreviousSegments = 0;
                for (int i = 0; i < lastIndex; i++)
                {
                    itemsInPreviousSegments += _segments[i].Capacity;
                }

                _currentSegmentUsed = totalItems - itemsInPreviousSegments;
                lastSeg.Used = _currentSegmentUsed;
            }
            else
            {
                _currentPacked = Memory<long>.Empty;
                _currentSegmentCapacity = 0;
                _currentSegmentUsed = 0;
            }
        }

        private void AllocateSegment(int size, bool hasFields)
        {
            var packedOwner = _longMemoryPool.Rent(size);

            var segment = new Segment
            {
                PackedOwner = packedOwner,
                Capacity = size,
                Used = size
            };

            if (hasFields)
            {
                segment.FieldsOwner = _fieldsMemoryPool.Rent(size);
            }

            _segments.Add(segment);
        }

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public void Add(int doc, float score)
        {
            if (_currentSegmentUsed == _currentSegmentCapacity)
            {
                EnsureCapacity();
            }

            // OPTIMIZATION: Ref aliasing via Span
            Span<long> span = _currentPacked.Span;
            ref long longStart = ref MemoryMarshal.GetReference(span);
            ref int intStart = ref Unsafe.As<long, int>(ref longStart);

            int offset = _currentSegmentUsed * 2;

            Unsafe.Add(ref intStart, offset) = doc;
            Unsafe.Add(ref Unsafe.As<int, float>(ref intStart), offset + 1) = score;

            _currentSegmentUsed++;
            _length++;
        }

        [MethodImpl(MethodImplOptions.NoInlining)]
        private void EnsureCapacity()
        {
            if (_segments.Count > 0)
            {
                var lastSeg = _segments[^1];
                lastSeg.Used = _currentSegmentUsed;
            }

            int newSize;
            if (_segments.Count == 0)
            {
                newSize = InitialItems;
            }
            else
            {
                newSize = _currentSegmentCapacity == MaxItemsPerSegment
                    ? MaxItemsPerSegment
                    : Math.Min(_currentSegmentCapacity * 2, MaxItemsPerSegment);
            }

            var packedOwner = _longMemoryPool.Rent(newSize);

            var newSegment = new Segment
            {
                PackedOwner = packedOwner,
                Used = 0,
                Capacity = newSize
            };

            _segments.Add(newSegment);

            _currentPacked = packedOwner.Memory;
            _currentSegmentCapacity = newSize;
            _currentSegmentUsed = 0;
        }

        public (int Doc, float Score) this[int index]
        {
            [MethodImpl(MethodImplOptions.AggressiveInlining)]
            get
            {
                if ((uint)index >= (uint)_length)
                    ThrowIndexOutOfRangeException();

                if (index >= GrowthPhaseTotalItems)
                {
                    int relativeIndex = index - GrowthPhaseTotalItems;
                    int segmentOffset = relativeIndex >> MaxItemsLog2;
                    int indexInSegment = relativeIndex & (MaxItemsPerSegment - 1);

                    var seg = _segments[StablePhaseSegmentStartIndex + segmentOffset];
                    return GetPackedValue(seg.PackedOwner.Memory.Span, indexInSegment);
                }

                return GetFromGrowthSegment(index);
            }
        }

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        private (int Doc, float Score) GetFromGrowthSegment(int index)
        {
            int segIndex = BitOperations.Log2((uint)(index >> GrowthPhaseShift) + 1);
            int segmentStart = InitialItems * ((1 << segIndex) - 1);
            int indexInSegment = index - segmentStart;
            var seg = _segments[segIndex];
            return GetPackedValue(seg.PackedOwner.Memory.Span, indexInSegment);
        }

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        private static (int, float) GetPackedValue(Span<long> packed, int index)
        {
            ref long longStart = ref MemoryMarshal.GetReference(packed);
            ref int intStart = ref Unsafe.As<long, int>(ref longStart);
            int offset = index * 2;

            int doc = Unsafe.Add(ref intStart, offset);
            float score = Unsafe.Add(ref Unsafe.As<int, float>(ref intStart), offset + 1);

            return (doc, score);
        }

        public void Dispose()
        {
            // never dispose the static Empty instance
            if (ReferenceEquals(this, Empty))
                return;

            GC.SuppressFinalize(this);

            foreach (var seg in _segments)
            {
                seg.PackedOwner?.Dispose();
                seg.FieldsOwner?.Dispose();
            }

            _segments.Clear();
            _currentPacked = Memory<long>.Empty;
            _length = _currentSegmentUsed = _currentSegmentCapacity = 0;
        }

        ~ManagedScoreDocArray_V2()
        {
            Dispose();
        }

        private static void ThrowIndexOutOfRangeException() => throw new IndexOutOfRangeException();

        public class Segment
        {
            public IMemoryOwner<long> PackedOwner;
            public IMemoryOwner<IComparable[]> FieldsOwner;
            public int Used;
            public int Capacity;
        }

        public BackwardsWriter GetBackwardsWriter() => new(this);

        public ref struct BackwardsWriter
        {
            private readonly ManagedScoreDocArray_V2 _parent;
            private ref long _currentPackedRef;
            private ref IComparable[] _currentFieldsRef;

            private int _segIndex;
            private int _indexInSegment;

            public BackwardsWriter(ManagedScoreDocArray_V2 parent)
            {
                _parent = parent;

                if (_parent.Length == 0)
                    return;

                int startIndex = parent._length - 1;

                if (startIndex >= GrowthPhaseTotalItems)
                {
                    int relativeIndex = startIndex - GrowthPhaseTotalItems;
                    _segIndex = StablePhaseSegmentStartIndex + (relativeIndex >> MaxItemsLog2);
                    _indexInSegment = relativeIndex & (MaxItemsPerSegment - 1);
                }
                else
                {
                    _segIndex = BitOperations.Log2((uint)(startIndex >> GrowthPhaseShift) + 1);
                    int segmentStart = InitialItems * ((1 << _segIndex) - 1);
                    _indexInSegment = startIndex - segmentStart;
                }

                var seg = _parent._segments[_segIndex];
                _currentPackedRef = ref MemoryMarshal.GetReference(seg.PackedOwner.Memory.Span);
                _currentFieldsRef = ref (seg.FieldsOwner != null
                    ? ref MemoryMarshal.GetReference(seg.FieldsOwner.Memory.Span)
                    : ref Unsafe.NullRef<IComparable[]>());
            }

            [MethodImpl(MethodImplOptions.AggressiveInlining)]
            public void Write(int doc, float score, IComparable[] fields = null)
            {
                if (_parent.Length == 0)
                    ThrowOnEmptyArray();

                if (_indexInSegment >= 0)
                {
                    ref int intStart = ref Unsafe.As<long, int>(ref _currentPackedRef);

                    int offset = _indexInSegment * 2;
                    Unsafe.Add(ref intStart, offset) = doc;
                    Unsafe.Add(ref Unsafe.As<int, float>(ref intStart), offset + 1) = score;

                    if (fields != null)
                    {
                        Unsafe.Add(ref _currentFieldsRef, _indexInSegment) = fields;
                    }

                    _indexInSegment--;
                    return;
                }

                SwitchToPreviousSegment(doc, score, fields);
            }

            [MethodImpl(MethodImplOptions.NoInlining)]
            private void SwitchToPreviousSegment(int doc, float score, IComparable[] fields = null)
            {
                _segIndex--;
                if (_segIndex < 0)
                    ThrowWriterOutOfRange();

                var seg = _parent._segments[_segIndex];
                _currentPackedRef = ref MemoryMarshal.GetReference(seg.PackedOwner.Memory.Span);
                _currentFieldsRef = ref (seg.FieldsOwner != null
                    ? ref MemoryMarshal.GetReference(seg.FieldsOwner.Memory.Span)
                    : ref Unsafe.NullRef<IComparable[]>());
                _indexInSegment = seg.Capacity - 1;

                Write(doc, score, fields); // Recursively call Write to hit the fast path
            }

            private static void ThrowOnEmptyArray() => throw new InvalidOperationException("Cannot write to an empty array");
            private static void ThrowWriterOutOfRange() => throw new IndexOutOfRangeException("Writer went below index 0");
        }

        public ScoreDocReader GetReader(int start) => new(this, start);

        public struct ScoreDocReader
        {
            private readonly ManagedScoreDocArray_V2 _managedParent;
            private Memory<long> _currentPacked;
            private Memory<IComparable[]> _currentFields;
            private int _currentSegUsedCount;

            private int _segIndex;
            private int _indexInSegment;

            public ScoreDocReader(ManagedScoreDocArray_V2 parent, int startIndex)
            {
                _managedParent = parent;

                if (startIndex >= GrowthPhaseTotalItems)
                {
                    int relativeIndex = startIndex - GrowthPhaseTotalItems;
                    _segIndex = StablePhaseSegmentStartIndex + (relativeIndex >> MaxItemsLog2);
                    _indexInSegment = relativeIndex & (MaxItemsPerSegment - 1);
                }
                else
                {
                    _segIndex = BitOperations.Log2((uint)(startIndex >> GrowthPhaseShift) + 1);
                    int segmentStart = InitialItems * ((1 << _segIndex) - 1);
                    _indexInSegment = startIndex - segmentStart;
                }

                if (_segIndex < parent._segments.Count)
                {
                    var seg = parent._segments[_segIndex];
                    _currentPacked = seg.PackedOwner.Memory;
                    _currentFields = seg.FieldsOwner != null ? seg.FieldsOwner.Memory : Memory<IComparable[]>.Empty;
                    _currentSegUsedCount = (_segIndex == parent._segments.Count - 1) ? parent._currentSegmentUsed : seg.Used;
                }
                else
                {
                    _currentPacked = Memory<long>.Empty;
                    _currentFields = Memory<IComparable[]>.Empty;
                    _currentSegUsedCount = 0;
                }
            }

            [MethodImpl(MethodImplOptions.AggressiveInlining)]
            public bool Read(out int doc, out float score)
            {
                if (_indexInSegment < _currentSegUsedCount)
                {
                    Span<long> span = _currentPacked.Span;
                    ref long longStart = ref MemoryMarshal.GetReference(span);
                    ref int intStart = ref Unsafe.As<long, int>(ref longStart);
                    int offset = _indexInSegment * 2;

                    doc = Unsafe.Add(ref intStart, offset);
                    score = Unsafe.Add(ref Unsafe.As<int, float>(ref intStart), offset + 1);

                    _indexInSegment++;
                    return true;
                }

                return ReadNextManagedSegment(out doc, out score);
            }

            [MethodImpl(MethodImplOptions.NoInlining)]
            private bool ReadNextManagedSegment(out int doc, out float score)
            {
                _segIndex++;
                _indexInSegment = 0;

                if (_managedParent == null || _segIndex >= _managedParent._segments.Count)
                {
                    doc = 0;
                    score = 0;
                    return false;
                }

                var seg = _managedParent._segments[_segIndex];
                _currentPacked = seg.PackedOwner.Memory;
                _currentSegUsedCount = (_segIndex == _managedParent._segments.Count - 1) ? _managedParent._currentSegmentUsed : seg.Used;

                if (_indexInSegment < _currentSegUsedCount)
                {
                    Span<long> span = _currentPacked.Span;
                    ref long longStart = ref MemoryMarshal.GetReference(span);
                    ref int intStart = ref Unsafe.As<long, int>(ref longStart);
                    int offset = _indexInSegment * 2;

                    doc = Unsafe.Add(ref intStart, offset);
                    score = Unsafe.Add(ref Unsafe.As<int, float>(ref intStart), offset + 1);

                    _indexInSegment++;
                    return true;
                }

                doc = 0;
                score = 0;
                return false;
            }

            [MethodImpl(MethodImplOptions.AggressiveInlining)]
            public bool Read(out int doc, out float score, out IComparable[] fields)
            {
                // Fast path logic duplicated to allow inlining of the "with fields" variant
                if (_indexInSegment < _currentSegUsedCount)
                {
                    Span<long> span = _currentPacked.Span;
                    ref long longStart = ref MemoryMarshal.GetReference(span);
                    ref int intStart = ref Unsafe.As<long, int>(ref longStart);

                    int offset = _indexInSegment * 2;

                    doc = Unsafe.Add(ref intStart, offset);
                    score = Unsafe.Add(ref Unsafe.As<int, float>(ref intStart), offset + 1);
                    fields = _currentFields.Span[_indexInSegment];

                    _indexInSegment++;
                    return true;
                }
                return ReadNextManagedSegment(out doc, out score, out fields);
            }

            [MethodImpl(MethodImplOptions.NoInlining)]
            private bool ReadNextManagedSegment(out int doc, out float score, out IComparable[] fields)
            {
                _segIndex++;
                _indexInSegment = 0;

                if (_managedParent == null || _segIndex >= _managedParent._segments.Count)
                {
                    doc = 0;
                    score = 0;
                    fields = null;
                    return false;
                }

                var seg = _managedParent._segments[_segIndex];
                _currentPacked = seg.PackedOwner.Memory;
                _currentFields = seg.FieldsOwner != null ? seg.FieldsOwner.Memory : Memory<IComparable[]>.Empty;
                _currentSegUsedCount = (_segIndex == _managedParent._segments.Count - 1) ? _managedParent._currentSegmentUsed : seg.Used;

                if (_indexInSegment < _currentSegUsedCount)
                {
                    Span<long> span = _currentPacked.Span;
                    ref long longStart = ref MemoryMarshal.GetReference(span);
                    ref int intStart = ref Unsafe.As<long, int>(ref longStart);

                    int offset = _indexInSegment * 2;
                    doc = Unsafe.Add(ref intStart, offset);
                    score = Unsafe.Add(ref Unsafe.As<int, float>(ref intStart), offset + 1);
                    fields = _currentFields.Span[_indexInSegment];

                    _indexInSegment++;
                    return true;
                }

                doc = 0;
                score = 0;
                fields = null;
                return false;
            }
        }
    }

    public unsafe class UnmanagedScoreDocArray : IDisposable
    {
        // size of a single packed item (int doc + float score) = 8 bytes
        private const int SingleItemSize = sizeof(long);

        // 8,192 items * 8 bytes = 64KB — stays under the 85KB LOH threshold
        private const int MaxItemsPerSegment = 64 * 1024 / SingleItemSize;

        // 128 items * 8 bytes = 1KB
        private const int InitialItems = 1 * 1024 / SingleItemSize;

        // Log2(8192) = 13
        private const int MaxItemsLog2 = 13;

        // Sequence: 128 -> 256 -> 512 -> 1024 -> 2048 -> 4096 (6 segments, 8064 items total)
        private const int GrowthPhaseTotalItems = 8064;
        private const int StablePhaseSegmentStartIndex = 6;

        // Log2(128) = 7
        private const int GrowthPhaseShift = 7;

        // Upper bound on segment count:
        //   6 growth segments + ceil(int.MaxValue / 8192) stable segments would be absurd,
        //   so we cap at a generous 256 stable segments (256 * 8192 = 2M items).
        private const int MaxSegments = StablePhaseSegmentStartIndex + 256;

        public struct Segment
        {
            public AllocatedMemoryData Allocation;
            public long* Packed;      // pointer into Allocation
            public int Capacity;
            public int Used;
        }

        private readonly JsonOperationContext _context;

        // Inline fixed-size segment table — no List<T> heap allocation
        private Segment* _segments;
        private AllocatedMemoryData _segmentsAllocation;
        private int _segmentCount;

        // Hot-path cache for the current (last) segment
        private long* _currentPacked;
        private int _currentSegmentUsed;
        private int _currentSegmentCapacity;

        private int _length;
        public int Length => _length;

        public UnmanagedScoreDocArray(JsonOperationContext context)
        {
            _context = context;

            // Allocate the segment-metadata table from the arena
            _segmentsAllocation = context.GetMemory(MaxSegments * sizeof(Segment));
            _segments = (Segment*)_segmentsAllocation.Address;
        }

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public void Add(int doc, float score)
        {
            if (_currentSegmentUsed == _currentSegmentCapacity)
                EnsureCapacity();

            ref int intStart = ref Unsafe.As<long, int>(ref *(_currentPacked + _currentSegmentUsed));
            intStart = doc;
            Unsafe.As<int, float>(ref Unsafe.Add(ref intStart, 1)) = score;

            _currentSegmentUsed++;
            _length++;
        }

        [MethodImpl(MethodImplOptions.NoInlining)]
        private void EnsureCapacity()
        {
            if (_segmentCount > 0)
                _segments[_segmentCount - 1].Used = _currentSegmentUsed;

            int newSize;
            if (_segmentCount == 0)
            {
                newSize = InitialItems;
            }
            else
            {
                newSize = _currentSegmentCapacity == MaxItemsPerSegment
                    ? MaxItemsPerSegment
                    : Math.Min(_currentSegmentCapacity * 2, MaxItemsPerSegment);
            }

            AllocateSegment(newSize);
        }

        private void AllocateSegment(int size)
        {
            var alloc = _context.GetMemory(size * SingleItemSize);
            ref Segment seg = ref _segments[_segmentCount++];
            seg.Allocation = alloc;
            seg.Packed = (long*)alloc.Address;
            seg.Capacity = size;
            seg.Used = 0;

            _currentPacked = seg.Packed;
            _currentSegmentCapacity = size;
            _currentSegmentUsed = 0;
        }

        public (int Doc, float Score) this[int index]
        {
            [MethodImpl(MethodImplOptions.AggressiveInlining)]
            get
            {
                if ((uint)index >= (uint)_length)
                    ThrowIndexOutOfRangeException();

                if (index >= GrowthPhaseTotalItems)
                {
                    int relativeIndex = index - GrowthPhaseTotalItems;
                    int segmentOffset = relativeIndex >> MaxItemsLog2;
                    int indexInSegment = relativeIndex & (MaxItemsPerSegment - 1);
                    return GetPackedValue(_segments[StablePhaseSegmentStartIndex + segmentOffset].Packed, indexInSegment);
                }

                return GetFromGrowthSegment(index);
            }
        }

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        private (int Doc, float Score) GetFromGrowthSegment(int index)
        {
            int segIndex = BitOperations.Log2((uint)(index >> GrowthPhaseShift) + 1);
            int segmentStart = InitialItems * ((1 << segIndex) - 1);
            int indexInSegment = index - segmentStart;
            return GetPackedValue(_segments[segIndex].Packed, indexInSegment);
        }

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        private static (int Doc, float Score) GetPackedValue(long* packed, int index)
        {
            ref int intStart = ref Unsafe.As<long, int>(ref *(packed + index));
            int doc = intStart;
            float score = Unsafe.As<int, float>(ref Unsafe.Add(ref intStart, 1));
            return (doc, score);
        }

        public void Dispose()
        {
            for (int i = 0; i < _segmentCount; i++)
                _context.ReturnMemory(_segments[i].Allocation);

            _segmentCount = 0;
            _currentPacked = null;
            _length = _currentSegmentUsed = _currentSegmentCapacity = 0;

            if (_segmentsAllocation != null)
            {
                _context.ReturnMemory(_segmentsAllocation);
                _segmentsAllocation = null;
                _segments = null;
            }
        }

        private static void ThrowIndexOutOfRangeException() => throw new IndexOutOfRangeException();

        public BackwardsWriter GetBackwardsWriter() => new(this);

        public ref struct BackwardsWriter
        {
            private readonly UnmanagedScoreDocArray _parent;
            private long* _currentPacked;
            private int _segIndex;
            private int _indexInSegment;

            public BackwardsWriter(UnmanagedScoreDocArray parent)
            {
                _parent = parent;

                if (parent._length == 0)
                    return;

                int startIndex = parent._length - 1;

                if (startIndex >= GrowthPhaseTotalItems)
                {
                    int relativeIndex = startIndex - GrowthPhaseTotalItems;
                    _segIndex = StablePhaseSegmentStartIndex + (relativeIndex >> MaxItemsLog2);
                    _indexInSegment = relativeIndex & (MaxItemsPerSegment - 1);
                }
                else
                {
                    _segIndex = BitOperations.Log2((uint)(startIndex >> GrowthPhaseShift) + 1);
                    int segmentStart = InitialItems * ((1 << _segIndex) - 1);
                    _indexInSegment = startIndex - segmentStart;
                }

                _currentPacked = parent._segments[_segIndex].Packed;
            }

            [MethodImpl(MethodImplOptions.AggressiveInlining)]
            public void Write(int doc, float score)
            {
                if (_parent._length == 0)
                    ThrowOnEmptyArray();

                if (_indexInSegment >= 0)
                {
                    ref int intStart = ref Unsafe.As<long, int>(ref *(_currentPacked + _indexInSegment));
                    intStart = doc;
                    Unsafe.As<int, float>(ref Unsafe.Add(ref intStart, 1)) = score;
                    _indexInSegment--;
                    return;
                }

                SwitchToPreviousSegment(doc, score);
            }

            [MethodImpl(MethodImplOptions.NoInlining)]
            private void SwitchToPreviousSegment(int doc, float score)
            {
                _segIndex--;
                if (_segIndex < 0)
                    ThrowWriterOutOfRange();

                ref Segment seg = ref _parent._segments[_segIndex];
                _currentPacked = seg.Packed;
                _indexInSegment = seg.Capacity - 1;

                Write(doc, score);
            }

            private static void ThrowOnEmptyArray() => throw new InvalidOperationException("Cannot write to an empty array");
            private static void ThrowWriterOutOfRange() => throw new IndexOutOfRangeException("Writer went below index 0");
        }

        public ScoreDocReader GetReader(int start) => new(this, start);

        public ref struct ScoreDocReader
        {
            private readonly UnmanagedScoreDocArray _parent;
            // Points directly at the current element — no pointer addition on the hot path
            private ref long _currentRef;
            private int _remaining;
            private int _segIndex;

            public ScoreDocReader(UnmanagedScoreDocArray parent, int startIndex)
            {
                _parent = parent;

                int indexInSegment;
                if (startIndex >= GrowthPhaseTotalItems)
                {
                    int relativeIndex = startIndex - GrowthPhaseTotalItems;
                    _segIndex = StablePhaseSegmentStartIndex + (relativeIndex >> MaxItemsLog2);
                    indexInSegment = relativeIndex & (MaxItemsPerSegment - 1);
                }
                else
                {
                    _segIndex = BitOperations.Log2((uint)(startIndex >> GrowthPhaseShift) + 1);
                    int segmentStart = InitialItems * ((1 << _segIndex) - 1);
                    indexInSegment = startIndex - segmentStart;
                }

                if (_segIndex < parent._segmentCount)
                {
                    ref Segment seg = ref parent._segments[_segIndex];
                    int used = (_segIndex == parent._segmentCount - 1)
                        ? parent._currentSegmentUsed
                        : seg.Used;
                    _currentRef = ref *(seg.Packed + indexInSegment);
                    _remaining = used - indexInSegment;
                }
                else
                {
                    _currentRef = ref Unsafe.NullRef<long>();
                    _remaining = 0;
                }
            }

            [MethodImpl(MethodImplOptions.AggressiveInlining)]
            public bool Read(out int doc, out float score)
            {
                if (_remaining > 0)
                {
                    ref int intStart = ref Unsafe.As<long, int>(ref _currentRef);
                    doc = intStart;
                    score = Unsafe.As<int, float>(ref Unsafe.Add(ref intStart, 1));
                    _currentRef = ref Unsafe.Add(ref _currentRef, 1);
                    _remaining--;
                    return true;
                }

                return ReadNextSegment(out doc, out score);
            }

            [MethodImpl(MethodImplOptions.NoInlining)]
            private bool ReadNextSegment(out int doc, out float score)
            {
                _segIndex++;

                if (_parent == null || _segIndex >= _parent._segmentCount)
                {
                    doc = 0;
                    score = 0;
                    return false;
                }

                ref Segment seg = ref _parent._segments[_segIndex];
                int used = (_segIndex == _parent._segmentCount - 1)
                    ? _parent._currentSegmentUsed
                    : seg.Used;

                _currentRef = ref *seg.Packed;
                _remaining = used;

                if (_remaining > 0)
                {
                    ref int intStart = ref Unsafe.As<long, int>(ref _currentRef);
                    doc = intStart;
                    score = Unsafe.As<int, float>(ref Unsafe.Add(ref intStart, 1));
                    _currentRef = ref Unsafe.Add(ref _currentRef, 1);
                    _remaining--;
                    return true;
                }

                doc = 0;
                score = 0;
                return false;
            }
        }
    }
}

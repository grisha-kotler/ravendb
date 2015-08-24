﻿using System;
using System.Collections.Concurrent;
using System.Collections.Generic;
using System.Diagnostics;
using System.Linq;
using System.Threading;
using Raven.Abstractions.Extensions;
using Raven.Abstractions.Logging;
using Raven.Database.Config;

namespace Raven.Database.Impl.BackgroundTaskExecuter
{
    public class RavenThreadPool : IDisposable, ICpuUsageHandler
    {
        [ThreadStatic]
        private static ThreadData _selfInformation;

        private readonly ConcurrentQueue<CountdownEvent> _concurrentEvents = new ConcurrentQueue<CountdownEvent>();
        private readonly CancellationTokenSource _createLinkedTokenSource;
        private readonly CancellationToken _ct;
        private readonly object _locker = new object();
        private readonly ConcurrentDictionary<ThreadTask, object> _runningTasks =
            new ConcurrentDictionary<ThreadTask, object>();
        private readonly BlockingCollection<ThreadTask> _tasks = new BlockingCollection<ThreadTask>();
        private readonly AutoResetEvent _threadHasNoWorkToDo = new AutoResetEvent(false);
        private readonly ILog logger = LogManager.GetCurrentClassLogger();
        public readonly string Name;
        private int _currentWorkingThreadsAmount;
        public ThreadLocal<bool> _freedThreadsValue = new ThreadLocal<bool>(true);
        public int _partialMaxWait = 2500;
        private int _partialMaxWaitChangeFlag = 1;
        private ThreadData[] _threads;
        public int UnstoppableTasksCount;

        public RavenThreadPool(int maxLevelOfParallelism, CancellationToken ct, string name = "RavenThreadPool",
            Action[] longRunningActions = null)
        {
            _createLinkedTokenSource = CancellationTokenSource.CreateLinkedTokenSource(ct);
            _ct = _createLinkedTokenSource.Token;
            _threads = new ThreadData[maxLevelOfParallelism];
            Name = name;
            if (longRunningActions != null)
            {
                if (longRunningActions.Length >= maxLevelOfParallelism)
                    throw new ArgumentException("maximum level of parallelism should bigger the amount of unstoppable tasks");

                UnstoppableTasksCount = longRunningActions.Length;
                for (var i = 0; i < UnstoppableTasksCount; i++)
                {
                    var copy = i;
                    _threads[i] = new ThreadData
                    {
                        StopWork = new ManualResetEventSlim(true),
                        Unstoppable = true,
                        Thread = new Thread(() =>
                        {
                            ExecuteLongRunningTask(_threads[copy], longRunningActions[copy]);
                        })
                        {
                            Priority = ThreadPriority.Normal,
                            Name = string.Format("{0} U {1}", name, copy),
                            IsBackground = true
                        }
                    };
                }
            }


            for (var i = UnstoppableTasksCount; i < _threads.Length; i++)
            {
                var copy = i;
                _threads[i] = new ThreadData
                {
                    StopWork = new ManualResetEventSlim(true),
                    Thread = new Thread(() => ExecutePoolWork(_threads[copy]))
                    {
                        Name = name + " S " + copy,
                        IsBackground = true,
                        Priority = ThreadPriority.Normal
                    }
                };
            }
            _currentWorkingThreadsAmount = maxLevelOfParallelism;
            CpuStatistics.RegisterCpuUsageHandler(this);
        }

        private void ExecuteLongRunningTask(ThreadData threadData, Action longRunningAction)
        {
            _selfInformation = threadData;
            try
            {
                longRunningAction();
            }
            catch (OperationCanceledException)
            {
            }
            catch (Exception e)
            {
                logger.FatalException("Error from long running task in thread pool, task terminated, this is VERY bad", e);
            }
        }

        public void HandleHighCpuUsage()
        {
            lock (_locker)
            {
                try
                {
                    if (_threads == null)
                    {
                    }
                    foreach (var thread in _threads)
                    {
                        if (thread.Thread.IsAlive == false)
                        {
                        }
                        if (thread.Thread.Priority != ThreadPriority.Normal)
                            continue;

                        thread.Thread.Priority = ThreadPriority.BelowNormal;
                    }

                    if (_currentWorkingThreadsAmount > (UnstoppableTasksCount + 1))
                    {
                        _threads[_currentWorkingThreadsAmount - 1].StopWork.Reset();
                        _currentWorkingThreadsAmount--;
                    }
                }
                catch (Exception e)
                {
                    logger.ErrorException(string.Format("Error occured while throttling down RTP named {0}", Name), e);
                    throw;
                }
            }
        }

        public void HandleLowCpuUsage()
        {
            lock (_locker)
            {
                try
                {
                    if (_threads == null)
                    {
                    }
                    if (_currentWorkingThreadsAmount < _threads.Length)
                    {
                        _threads[_currentWorkingThreadsAmount].StopWork.Set();
                        _currentWorkingThreadsAmount++;
                    }

                    foreach (var thread in _threads)
                    {
                        if (thread.Thread.IsAlive == false)
                        {
                        }
                        if (thread.Thread.Priority != ThreadPriority.BelowNormal)
                            continue;

                        thread.Thread.Priority = ThreadPriority.Normal;
                    }
                }
                catch (Exception e)
                {
                    logger.ErrorException(string.Format("Error occured while throttling up RTP named {0}", Name), e);
                    throw;
                }
            }
        }

        public void Dispose()
        {
            lock (_locker)
            {
                if (_threads == null)
                    return;
                try
                {
                    _createLinkedTokenSource.Cancel();
                    var concurrentEvents = new List<CountdownEvent>();
                    CountdownEvent ce;
                    while (_concurrentEvents.TryDequeue(out ce))
                        concurrentEvents.Add(ce);

                    foreach (var concurrentEvent in concurrentEvents)
                    {
                        if (concurrentEvent.IsSet == false)
                            concurrentEvent.Signal();
                    }
                    foreach (var t in _threads)
                    {
                        t.StopWork.Set();
                        t.Thread.Join();
                        t.StopWork.Dispose();
                    }
                    foreach (var concurrentEvent in concurrentEvents)
                    {
                        concurrentEvent.Dispose();
                    }
                    _threads = null;
                }
                catch (Exception e)
                {
                    logger.ErrorException(string.Format("Error occured while disposing RTP named {0}", Name), e);
                    throw;
                }
            }
        }

        public ThreadsSummary GetThreadPoolStats()
        {
            var ts = new ThreadsSummary
            {
                ThreadsPrioritiesCounts = new Dictionary<ThreadPriority, int>(),
                UnstoppableThreadsCount = _threads.Count(x => x.Unstoppable),
                PartialMaxWait = Thread.VolatileRead(ref _partialMaxWait),
                FreeThreadsAmount = _freedThreadsValue.Values.Count(isFree => isFree),
                ConcurrentEventsCount = _concurrentEvents.Count,
                ConcurrentWorkingThreadsAmount = _currentWorkingThreadsAmount
            };
            _threads.ForEach(x =>
            {
                if (!ts.ThreadsPrioritiesCounts.Keys.Contains(x.Thread.Priority))
                {
                    ts.ThreadsPrioritiesCounts.Add(x.Thread.Priority, 0);
                }
                ts.ThreadsPrioritiesCounts[x.Thread.Priority] = ts.ThreadsPrioritiesCounts[x.Thread.Priority] + 1;
            });
            return ts;
        }

        public RavenThreadPool Start()
        {
            foreach (var t in _threads)
            {
                t.Thread.Start();
            }
            return this;
        }

        public void DrainThePendingTasks()
        {
            do
            {
                ExecuteWorkOnce(true, shouldWaitForWork: false);
            } while (_tasks.Count > 0);
        }

        public ThreadTask[] GetRunningTasks()
        {
            return _runningTasks.Keys.ToArray();
        }

        public ThreadTask[] GetAllWaitingTasks()
        {
            return _tasks.ToArray();
        }

        public int WaitingTasksAmount
        {
            get { return _tasks.Count; }
        }

        public int RunningTasksAmount
        {
            get { return _runningTasks.Count; }
        }

        private void ExecutePoolWork(ThreadData selfData)
        {
            try
            {
                var raiseNothingToDoNotification = false;
                while (_ct.IsCancellationRequested == false) // cancellation token
                {
                    selfData.StopWork.Wait(_ct);
                    _selfInformation = selfData;

                    raiseNothingToDoNotification = ExecuteWorkOnce(raiseNothingToDoNotification);
                    _freedThreadsValue.Value = true;
                }
            }
            catch (OperationCanceledException)
            {
                //expected
            }
            catch (Exception e)
            {
                logger.FatalException("Error while running background tasks, this is very bad", e);
            }
        }

        private bool ExecuteWorkOnce(bool raiseNothingToDoNotification, bool shouldWaitForWork = true)
        {
            ThreadTask threadTask = null;
            if (!shouldWaitForWork && _tasks.TryTake(out threadTask) == false)
            {
                return true;
            }

            if (threadTask == null)
                threadTask = GetNextTask(raiseNothingToDoNotification);
            if (threadTask == null)
                return false;

            try
            {
                threadTask.Duration = Stopwatch.StartNew();
                try
                {
                    _runningTasks.TryAdd(threadTask, null);
                    threadTask.Action();
                }
                finally
                {
                    if (threadTask.BatchStats != null)
                        Interlocked.Increment(ref threadTask.BatchStats.Completed);
                    threadTask.Duration.Stop();
                    object _;
                    _runningTasks.TryRemove(threadTask, out _);
                }
                return threadTask.EarlyBreak;
            }
            catch (Exception e)
            {
                logger.ErrorException(
                    string.Format(
                        "Error occured while executing RavenThreadPool task; ThreadPool name :{0} ; Task queued at: {1} ; Task Description: {2} ",
                        Name, threadTask.QueuedAt, threadTask.Description), e);

                return false;
            }
        }

        private ThreadTask GetNextTask(bool raiseNothingToDoNotification)
        {
            ThreadTask threadTask = null;


            while (!_ct.IsCancellationRequested)
            {
                if (raiseNothingToDoNotification)
                {
                    if (_tasks.TryTake(out threadTask, 1000, _ct))
                        break;

                    _threadHasNoWorkToDo.Set();
                }
                else
                {
                    threadTask = _tasks.Take(_ct);
                    break;
                }
            }
            _freedThreadsValue.Value = false;
            return threadTask;
        }

        public void ExecuteBatch<T>(IList<T> src, Action<IEnumerator<T>> action, int pageSize = 1024,
            string description = null)
        {
            if (src.Count == 0)
                return;
            if (src.Count == 1)
            {
                using (var enumerator = src.GetEnumerator())
                {
                    var threadTask = new ThreadTask
                    {
                        Action = () => { action(enumerator); },
                        BatchStats = new BatchStatistics
                        {
                            Total = 1,
                            Completed = 0
                        },
                        EarlyBreak = false,
                        Description = new OperationDescription
                        {
                            From = 1,
                            To = 1,
                            Total = 1,
                            PlainText = description
                        }
                    };

                    object _;
                    try
                    {
                        threadTask.Action();
                        threadTask.BatchStats.Completed++;
                        _runningTasks.TryAdd(threadTask, null);
                    }
                    catch (Exception e)
                    {
                        logger.ErrorException(
                            string.Format(
                                "Error occured while executing RavenThreadPool task; ThreadPool name :{0} ; Task queued at: {1} ; Task Description: {2} ",
                                Name, threadTask.QueuedAt, threadTask.Description), e);
                    }
                    finally
                    {
                        _runningTasks.TryRemove(threadTask, out _);
                    }
                }
                return;
            }
            var ranges = new ConcurrentQueue<Tuple<int, int>>();
            var now = DateTime.UtcNow;


            var numOfBatches = (src.Count/pageSize) + (src.Count%pageSize == 0 ? 0 : 1);

            CountdownEvent totalTasks;
            if (_concurrentEvents.TryDequeue(out totalTasks) == false)
                totalTasks = new CountdownEvent(numOfBatches);
            else
                totalTasks.Reset(numOfBatches);

            for (var i = 0; i < src.Count; i += pageSize)
            {
                var rangeStart = i;
                var rangeEnd = i + pageSize - 1 < src.Count ? i + pageSize - 1 : src.Count - 1;
                ranges.Enqueue(Tuple.Create(rangeStart, rangeEnd));

                if (i > 0)
                    totalTasks.AddCount();

                var threadTask = new ThreadTask
                {
                    Action = () =>
                    {
                        var numOfBatchesUsed = new Reference<int>();
                        try
                        {
                            Tuple<int, int> range;
                            if (!ranges.TryDequeue(out range)) return;
                            action(YieldFromRange(ranges, range, src, numOfBatchesUsed));
                        }
                        finally
                        {
                            totalTasks.Signal(numOfBatchesUsed.Value);
                        }
                    },
                    Description = new OperationDescription
                    {
                        Type = OperationDescription.OpeartionType.Range,
                        PlainText = description,
                        From = i*pageSize,
                        To = i*(pageSize + 1),
                        Total = src.Count
                    },
                    BatchStats = new BatchStatistics
                    {
                        Total = rangeEnd - rangeStart,
                        Completed = 0
                    },
                    QueuedAt = now,
                    DoneEvent = totalTasks
                };
                _tasks.Add(threadTask, _ct);
            }
            WaitForBatchToCompletion(totalTasks);
        }

        private IEnumerator<T> YieldFromRange<T>(ConcurrentQueue<Tuple<int, int>> ranges, Tuple<int, int> boundaries, IList<T> input, Reference<int> numOfBatchesUsed)
        {
            do
            {
                numOfBatchesUsed.Value++;
                for (var i = boundaries.Item1; i <= boundaries.Item2; i++)
                {
                    yield return input[i];
                }
            } while (ranges.TryDequeue(out boundaries));
        }

        public void ExecuteBatch<T>(IList<T> src, Action<T> action, string description = null,
            bool allowPartialBatchResumption = false, int completedMultiplier = 2, int freeThreadsMultiplier = 2, int maxWaitMultiplier = 1)
        {
            if (src.Count == 0)
                return;

            if (allowPartialBatchResumption && src.Count == 1)
            {
                var threadTask = new ThreadTask
                {
                    Action = () => { action(src[0]); },
                    BatchStats = new BatchStatistics
                    {
                        Total = 1,
                        Completed = 0
                    },
                    EarlyBreak = false,
                    Description = new OperationDescription
                    {
                        From = 1,
                        To = 1,
                        Total = 1,
                        PlainText = description
                    }
                };

                object _;
                try
                {
                    threadTask.Action();
                    threadTask.BatchStats.Completed++;
                    _runningTasks.TryAdd(threadTask, null);
                }
                catch (Exception e)
                {
                    logger.ErrorException(
                        string.Format(
                            "Error occured while executing RavenThreadPool task; ThreadPool name :{0} ; Task queued at: {1} ; Task Description: {2} ",
                            Name, threadTask.QueuedAt, threadTask.Description), e);
                }
                finally
                {
                    _runningTasks.TryRemove(threadTask, out _);
                }


                return;
            }

            var now = DateTime.UtcNow;
            CountdownEvent lastEvent;
            var itemsCount = 0;

            var batch = new BatchStatistics
            {
                Total = src.Count,
                Completed = 0
            };

            if (_concurrentEvents.TryDequeue(out lastEvent) == false)
                lastEvent = new CountdownEvent(src.Count);
            else
                lastEvent.Reset(src.Count);


            for (; itemsCount < src.Count && _ct.IsCancellationRequested == false; itemsCount++)
            {
                var copy = itemsCount;
                var threadTask = new ThreadTask
                {
                    Action = () =>
                    {
                        try
                        {
                            action(src[copy]);
                        }
                        finally
                        {
                            lastEvent.Signal();
                        }
                    },
                    Description = new OperationDescription
                    {
                        Type = OperationDescription.OpeartionType.Atomic,
                        PlainText = description,
                        From = itemsCount + 1,
                        To = itemsCount + 1,
                        Total = src.Count
                    },
                    BatchStats = batch,
                    EarlyBreak = allowPartialBatchResumption,
                    QueuedAt = now,
                    DoneEvent = lastEvent
                };
                _tasks.Add(threadTask, _ct);
            }

            if (!allowPartialBatchResumption)
            {
                WaitForBatchToCompletion(lastEvent);
                return;
            }

            WaitForBatchAllowingPartialBatchResumption(lastEvent, batch, completedMultiplier, freeThreadsMultiplier, maxWaitMultiplier);
        }

        private void WaitForBatchToCompletion(CountdownEvent completionEvent)
        {
            try
            {
                if (_selfInformation != null)
                {
                    var setEarlyBreak = false;
                    do
                    {
                        setEarlyBreak = ExecuteWorkOnce(setEarlyBreak, shouldWaitForWork: false);
                    } while (completionEvent.IsSet == false && _ct.IsCancellationRequested == false);
                    return;
                }

                completionEvent.Wait(_ct);
            }
            finally
            {
                _concurrentEvents.Enqueue(completionEvent);
            }
        }

        private void WaitForBatchAllowingPartialBatchResumption(CountdownEvent completionEvent, BatchStatistics batch, int completedMultiplier = 2, int freeThreadsMultiplier = 2, int maxWaitMultiplier = 1)
        {

            var waitHandles = new[] { completionEvent.WaitHandle, _threadHasNoWorkToDo };
            var sp = Stopwatch.StartNew();
            var batchLeftEarly = false;
            while (_ct.IsCancellationRequested == false)
            {
                var i = WaitHandle.WaitAny(waitHandles);
                if (i == 0)
                    break;

                if (Thread.VolatileRead(ref batch.Completed) < batch.Total / completedMultiplier)
                    continue;

                var currentFreeThreads = _freedThreadsValue.Values.Count(isFree => isFree);
                if (currentFreeThreads > _currentWorkingThreadsAmount / freeThreadsMultiplier)
                {
                    break;
                }
            }

            var elapsedMilliseconds = (int)sp.ElapsedMilliseconds;
            if (completionEvent.Wait(Math.Max(elapsedMilliseconds / 2, Thread.VolatileRead(ref _partialMaxWait)) / maxWaitMultiplier, _ct))
            {
                _concurrentEvents.Enqueue(completionEvent);
            }
            if (batch.Completed != batch.Total)
            {
                batchLeftEarly = true;
                if (_partialMaxWaitChangeFlag > 0)
                {
                    Interlocked.Exchange(ref _partialMaxWait, Math.Min(2500, (int)(Thread.VolatileRead(ref _partialMaxWait) * 1.25)));
                }
            }
            else if (_partialMaxWaitChangeFlag < 0)
            {
                Interlocked.Exchange(ref _partialMaxWait, Math.Max(Thread.VolatileRead(ref _partialMaxWait) / 2, 10));
            }


            Interlocked.Exchange(ref _partialMaxWaitChangeFlag, Thread.VolatileRead(ref _partialMaxWaitChangeFlag) * -1);

            // completionEvent is explicitly left to the finalizer
            // we expect this to be rare, and it isn't worth the trouble of trying
            // to manage this

            logger.Info("Raven Thread Pool named {0} ended batch. Done {1} items out of {2}. Batch {3} left early",
                Name, batch.Completed, batch.Total, batchLeftEarly ? "was" : "was not");
        }

        public class ThreadTask
        {
            public Action Action;
            public BatchStatistics BatchStats;
            public object Description;
            public CountdownEvent DoneEvent;
            public Stopwatch Duration;
            public bool EarlyBreak;
            public DateTime QueuedAt;
        }

        public class ThreadData
        {
            public ManualResetEventSlim StopWork;
            public Thread Thread;
            public bool Unstoppable;
        }


        public class ThreadsSummary
        {
            public Dictionary<ThreadPriority, int> ThreadsPrioritiesCounts { get; set; }
            public int UnstoppableThreadsCount { get; set; }
            public int PartialMaxWait { get; set; }
            public int FreeThreadsAmount { get; set; }
            public int ConcurrentEventsCount { get; set; }
            public int ConcurrentWorkingThreadsAmount { get; set; }
        }

        public class BatchStatistics
        {
            public enum BatchType
            {
                Atomic,
                Range
            }

            public int Completed;
            public int Total;
            public BatchType Type;
        }

        public class OperationDescription
        {
            public enum OpeartionType
            {
                Atomic,
                Range,
                Maintaenance
            }

            public int From;
            public string PlainText;
            public int To;
            public int Total;
            public OpeartionType Type;

            public override string ToString()
            {
                return string.Format("{0} range {1} to {2} of {3}", PlainText, From, To, Total);
            }
        }
    }
}
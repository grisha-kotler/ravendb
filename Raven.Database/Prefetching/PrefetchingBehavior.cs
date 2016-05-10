﻿// -----------------------------------------------------------------------
//  <copyright file="PrefetchingBehavior.cs" company="Hibernating Rhinos LTD">
//      Copyright (c) Hibernating Rhinos LTD. All rights reserved.
//  </copyright>
// -----------------------------------------------------------------------
using System;
using System.Collections.Concurrent;
using System.Collections.Generic;
using System.Diagnostics;
using System.Linq;
using System.Text;
using System.Threading;
using System.Threading.Tasks;
using metrics;
using metrics.Core;
using Raven.Abstractions;
using Raven.Abstractions.Data;
using Raven.Abstractions.Extensions;
using Raven.Abstractions.Indexing;
using Raven.Abstractions.Logging;
using Raven.Database.Actions;
using Raven.Database.Config;
using Raven.Database.Impl;
using Raven.Database.Indexing;
using Raven.Database.Linq;
using Raven.Database.Queries;
using Raven.Database.Storage;
using Raven.Json.Linq;

namespace Raven.Database.Prefetching
{
    public class PrefetchingBehavior : IDisposable, ILowMemoryHandler
    {
        private class DocAddedAfterCommit
        {
            public Etag Etag;
            public DateTime AddedAt;
        }

        private static readonly ILog log = LogManager.GetCurrentClassLogger();
        private readonly BaseBatchSizeAutoTuner autoTuner;
        private readonly WorkContext context;
        private readonly ConcurrentDictionary<string, HashSet<Etag>> documentsToRemove = new ConcurrentDictionary<string, HashSet<Etag>>(StringComparer.InvariantCultureIgnoreCase);
        private readonly ConcurrentDictionary<Etag, FutureIndexBatch> futureIndexBatches = new ConcurrentDictionary<Etag, FutureIndexBatch>();

        private readonly ConcurrentJsonDocumentSortedList prefetchingQueue = new ConcurrentJsonDocumentSortedList();

        private readonly ConcurrentQueue<DiskFetchPerformanceStats> loadTimes = new ConcurrentQueue<DiskFetchPerformanceStats>();

        private class DiskFetchPerformanceStats
        {
            public int NumberOfDocuments;
            public long TotalSize;
            public long LoadingTimeInMillseconds;
            public long LargestDocSize;
            public string LargestDocKey;
        }

        private int numberOfTimesWaitedHadToWaitForIO = 0;
        private int splitPrefetchingCount = 0;

        private DocAddedAfterCommit lowestInMemoryDocumentAddedAfterCommit;
        private int currentIndexingAge;

        public Action<int> FutureBatchCompleted = delegate { };
        private readonly MeterMetric ingestMeter;
        private readonly MeterMetric returnedDocsMeter;
        private int numberOfTimesIngestRateWasTooHigh;
        private readonly string userDescription;
        private Etag recentEtag = Etag.Empty;

        public PrefetchingBehavior(PrefetchingUser prefetchingUser,
            WorkContext context,
            BaseBatchSizeAutoTuner autoTuner,
            string prefetchingUserDescription,
            bool isDefault = false,
            Func<int> getPrefetchintBehavioursCount = null,
            Func<PrefetchingSummary> getPrefetcherSummary = null)
        {
            this.context = context;
            this.autoTuner = autoTuner;
            PrefetchingUser = prefetchingUser;
            this.userDescription = prefetchingUserDescription;
            this.IsDefault = isDefault;
            this.getPrefetchintBehavioursCount = getPrefetchintBehavioursCount ?? (() => 1);
            this.getPrefetcherSummary = getPrefetcherSummary ?? GetSummary;
            MemoryStatistics.RegisterLowMemoryHandler(this);
            LastTimeUsed = DateTime.MinValue;

            ingestMeter = context.MetricsCounters.DbMetrics.Meter("metrics",
                "ingest/sec", "In memory documents held by this prefetcher", TimeUnit.Seconds);
            returnedDocsMeter = context.MetricsCounters.DbMetrics.Meter("metrics",
                  "returned docs/sec", "Documents being served by this prefetcher", TimeUnit.Seconds);

            if (isDefault)
            {
                context.Database.TransactionalStorage.Batch(accessor =>
                {
                    recentEtag = accessor.Staleness.GetMostRecentDocumentEtag();
                });
            }
        }

        public PrefetchingUser PrefetchingUser { get; private set; }
        public bool IsDefault { get; private set; }
        private readonly Func<int> getPrefetchintBehavioursCount;
        private readonly Func<PrefetchingSummary> getPrefetcherSummary;
        public List<IndexToWorkOn> Indexes { get; set; }
        public string LastIndexedEtag { get; set; }
        public DateTime LastTimeUsed { get; private set; }
        public string AdditionalInfo { get; set; }

        public bool DisableCollectingDocumentsAfterCommit { get; set; }
        public bool ShouldHandleUnusedDocumentsAddedAfterCommit { get; set; }

        public int InMemoryIndexingQueueSize
        {
            get { return prefetchingQueue.Count; }
        }

        public int InMemoryFutureIndexBatchesSize
        {
            get
            {
                return futureIndexBatches
                    .Where(futureIndexBatch => futureIndexBatch.Value.Task.IsCompleted)
                    .Sum(futureIndexBatch => futureIndexBatch.Value.Task.Result.Count);
            }
        }

        #region IDisposable Members

        public void Dispose()
        {
            foreach (var futureIndexBatch in futureIndexBatches)
            {
                var cts = futureIndexBatch.Value.CancellationTokenSource;
                if (cts != null)
                {
                    try
                    {
                        cts.Cancel();
                    }
                    catch (ObjectDisposedException)
                    {
                        // this is expected because we are racing with the future batch completion
                    }
                }
            }

            Task.WaitAll(futureIndexBatches.Values.Select(ObserveDiscardedTask).ToArray());
            futureIndexBatches.Clear();
        }

        #endregion

        public IDisposable DocumentBatchFrom(Etag etag, out List<JsonDocument> documents, List<int> indexIds = null)
        {
            //var docs = GetDocumentsByIndex(indexIds, etag, null, new CancellationTokenSource(), 80000000000);
            LastTimeUsed = DateTime.UtcNow;
            documents = GetDocumentsBatchFrom(etag, null, indexIds);
            return UpdateCurrentlyUsedBatches(documents);
        }

        public List<JsonDocument> GetDocumentsBatchFrom(Etag etag, int? take = null, List<int> indexIds = null)
        {
            if (take != null && take.Value <= 0)
                throw new ArgumentException("Take must be greater than 0.");

            HandleCollectingDocumentsAfterCommit(etag);
            RemoveOutdatedFutureIndexBatches(etag);

            List<JsonDocument> results;
            if (indexIds != null && IsDefault == false)
            {
                var nextEtagToIndex = GetNextDocEtag(etag);
                results = GetDocsFromBatchWithPossibleDuplicatesByIndex(indexIds, nextEtagToIndex, null, ref etag);
                if (results.Count == 0)
                {
                    results = GetDocsFromBatchWithPossibleDuplicates(etag, take);
                    if (results.Count == 0)
                    {
                        context.TransactionalStorage.Batch(actions =>
                        {
                            var document = actions.Documents.GetDocumentsAfter(etag.DecrementBy(1), 1, context.CancellationToken).FirstOrDefault();
                            if (document != null)
                                results.Add(document);
                        });
                    }
                }
            }
            else
            {
                results = GetDocsFromBatchWithPossibleDuplicates(etag, take);
            }

            // a single doc may appear multiple times, if it was updated while we were fetching things, 
            // so we have several versions of the same doc loaded, this will make sure that we will only  
            // take one of them.
            var ids = new HashSet<string>(StringComparer.OrdinalIgnoreCase);
            for (int i = results.Count - 1; i >= 0; i--)
            {
                if (CanBeConsideredAsDuplicate(results[i]) && ids.Add(results[i].Key) == false)
                {
                    results.RemoveAt(i);
                }
            }
            returnedDocsMeter.Mark(results.Count);
            return results;
        }

        private List<JsonDocument> GetDocsFromBatchWithPossibleDuplicatesByIndex(List<int> indexIds, 
            Etag fromEtag, Etag untilEtag, ref Etag lastIndexed, Reference<bool> earlyExit = null)
        {
            try
            {
                Etag lastIndexedInternal = null;
                Func<IStorageActionsAccessor, int, long, TimeSpan, IEnumerable<JsonDocument>> getDocuments =
                    (actions, numberOfItemsToProcessInSingleBatch, maxSize, timeOut) =>
                        GetDocumentsByIndex(indexIds,
                            fromEtag,
                            untilEtag,
                            actions,
                            numberOfItemsToProcessInSingleBatch,
                            maxSize,
                            timeOut,
                            out lastIndexedInternal,
                            earlyExit);

                var documents = GetDocumentsList(getDocuments);
                if (lastIndexedInternal != null)
                    lastIndexed = lastIndexedInternal;

                return documents;
            }
            catch (Exception e)
            {
                return new List<JsonDocument>();
            }
        }

        private List<JsonDocument> GetDocumentsList(Func<IStorageActionsAccessor, int, long, TimeSpan, IEnumerable<JsonDocument>> getDocuments)
        {
            List<JsonDocument> jsonDocs = null;

            // We take an snapshot because the implementation of accessing Values from a ConcurrentDictionary involves a lock.
            // Taking the snapshot should be safe enough. 
            long currentlyUsedBatchSizesInBytes = autoTuner.CurrentlyUsedBatchSizesInBytes.Values.Sum();

            using (DocumentCacher.SkipSetDocumentsInDocumentCache())
                context.TransactionalStorage.Batch(actions =>
                {
                    //limit how much data we load from disk --> better adhere to memory limits
                    var totalSizeAllowedToLoadInBytes =
                        (context.Configuration.DynamicMemoryLimitForProcessing) -
                        (prefetchingQueue.LoadedSize + currentlyUsedBatchSizesInBytes);

                    // at any rate, we will load a min of 512Kb docs
                    long maxSize = Math.Max(
                        Math.Min(totalSizeAllowedToLoadInBytes, autoTuner.MaximumSizeAllowedToFetchFromStorageInBytes),
                        1024*512);

                    var sp = Stopwatch.StartNew();
                    var totalSize = 0L;
                    var largestDocSize = 0L;
                    string largestDocKey = null;
                    jsonDocs = getDocuments(actions,
                            GetNumberOfItemsToProcessInSingleBatch(),
                            maxSize,
                            autoTuner.FetchingDocumentsFromDiskTimeout)
                        .Where(x => x != null)
                        .Select(doc =>
                        {
                            if (largestDocSize < doc.SerializedSizeOnDisk)
                            {
                                largestDocSize = doc.SerializedSizeOnDisk;
                                largestDocKey = doc.Key;
                            }

                            totalSize += doc.SerializedSizeOnDisk;
                            JsonDocument.EnsureIdInMetadata(doc);
                            return doc;
                        })
                        .ToList();

                    loadTimes.Enqueue(new DiskFetchPerformanceStats
                    {
                        LoadingTimeInMillseconds = sp.ElapsedMilliseconds,
                        NumberOfDocuments = jsonDocs.Count,
                        TotalSize = totalSize,
                        LargestDocSize = largestDocSize,
                        LargestDocKey = largestDocKey
                    });
                    while (loadTimes.Count > 10)
                    {
                        DiskFetchPerformanceStats _;
                        loadTimes.TryDequeue(out _);
                    }
                });

            return jsonDocs;
        }

        private void HandleCollectingDocumentsAfterCommit(Etag requestedEtag)
        {
            if (ShouldHandleUnusedDocumentsAddedAfterCommit == false)
                return;

            var current = lowestInMemoryDocumentAddedAfterCommit;
            if (DisableCollectingDocumentsAfterCommit)
            {
                if (current != null && requestedEtag.CompareTo(current.Etag) > 0)
                {
                    lowestInMemoryDocumentAddedAfterCommit = null;
                    DisableCollectingDocumentsAfterCommit = false;
                    numberOfTimesIngestRateWasTooHigh = 0;
                }
            }
            else if (current != null)
            {
                var oldestTimeInQueue = SystemTime.UtcNow - current.AddedAt;
                if (
                    // this can happen if we have a very long indexing time, which takes
                    // us a long while to process, so we might as might as well stop keeping
                    // stuff in memory, because we lag
                    oldestTimeInQueue > TimeSpan.FromMinutes(10)
                    )
                {
                    DisableCollectingDocumentsAfterCommit = true;
                    // If we disable in memory collection of data, we need to also remove all the
                    // items in the prefetching queue that are after the last in memory docs
                    // immediately, not wait for them to be indexed. They have already been in 
                    // memory for ten minutes
                    // But not if our time has finally been reached
                    if (requestedEtag.CompareTo(current.Etag) >= 0)
                        prefetchingQueue.RemoveAfter(current.Etag);
                }
            }
        }

        private void RemoveOutdatedFutureIndexBatches(Etag etag)
        {
            foreach (FutureIndexBatch source in futureIndexBatches.Values.Where(x => etag.CompareTo(x.StartingEtag) > 0))
            {
                ObserveDiscardedTask(source);
                DisposeCancellationToken(source.CancellationTokenSource);
                FutureIndexBatch batch;
                futureIndexBatches.TryRemove(source.StartingEtag, out batch);
            }
        }

        private static void DisposeCancellationToken(CancellationTokenSource cts)
        {
            if (cts != null)
            {
                try
                {
                    cts.Cancel();
                }
                catch (ObjectDisposedException)
                {
                    // this is an expected race with the actual task, this is fine
                }
            }
        }

        private void HandleCleanupOfUnusedDocumentsInQueue()
        {
            if (ShouldHandleUnusedDocumentsAddedAfterCommit == false)
                return;

            if (DisableCollectingDocumentsAfterCommit == false)
                return;

            var current = lowestInMemoryDocumentAddedAfterCommit;
            if (current == null)
                return;

            prefetchingQueue.RemoveAfter(current.Etag);
        }

        private bool CanBeConsideredAsDuplicate(JsonDocument document)
        {
            if (document.Metadata[Constants.RavenReplicationConflict] != null)
                return false;

            return true;
        }

        public bool CanUsePrefetcherToLoadFromUsingExistingData(Etag fromEtag)
        {
            var nextEtagToIndex = GetNextDocEtag(fromEtag);

            var firstEtagInQueue = prefetchingQueue.NextDocumentETag();
            // queue isn't empty and docs for requested etag are already in queue
            if (firstEtagInQueue != null && nextEtagToIndex == firstEtagInQueue)
                return true;

            // found a future batch that includes the requested etag in it
            if (CanLoadDocumentsFromFutureBatches(nextEtagToIndex) != null)
                return true;

            return false;
        }

        public bool CanUseDefaultPrefetcher(Etag fromEtag)
        {
            if (IsDefault == false)
                return false;

            // we assume that the default prefetcher should be ahead of all other prefetchers.
            // if we find the givven etag bigger than the recent etag that was used - 
            // than we use the default prefetcher.
            // the documents with the smaller etags (if any) will be removed
            // when trying to remove the documents from the queue.
            if (recentEtag.CompareTo(fromEtag) > 0)
                return false;

            return true;
        }

        public bool IsEmpty()
        {
            return prefetchingQueue.Count == 0 && futureIndexBatches.Count == 0;
        }

        private List<JsonDocument> GetDocsFromBatchWithPossibleDuplicates(Etag etag, int? take)
        {
            var result = new List<JsonDocument>();
            bool docsLoaded;
            int prefetchingQueueSizeInBytes;
            var prefetchingDurationTimer = Stopwatch.StartNew();

            // We take an snapshot because the implementation of accessing Values from a ConcurrentDictionary involves a lock.
            // Taking the snapshot should be safe enough.
            long currentlyUsedBatchSizesInBytes = autoTuner.CurrentlyUsedBatchSizesInBytes.Values.Sum();
            do
            {
                var nextEtagToIndex = GetNextDocEtag(etag);
                var firstEtagInQueue = prefetchingQueue.NextDocumentETag();

                if (firstEtagInQueue == null || 
                   (firstEtagInQueue != nextEtagToIndex && prefetchingQueue.DocumentExists(nextEtagToIndex) == false))
                {
                    // if we have no results, and there is a future batch for it, we would wait for the results
                    // if there are no other results that have been read.
                    if (TryLoadDocumentsFromFutureBatches(nextEtagToIndex, allowWaiting: result.Count == 0) == false)
                    {
                        // we don't have a something ready in the future batch, now we need to know if we
                        // have to wait for I/O, or if we can just let the caller get whatever it is that we 
                        // have right now, and schedule another background task to run it.
                        //
                        // The idea is that we'll give you whatever we have right now, and you'll be happy with it, 
                        // and next time you'll call, we'll have something ready
                        if (result.Count > 0)
                        {
                            if (log.IsDebugEnabled)
                                log.Debug("Not enough results to fill requested count ({0:#,#;;0}), but we have some ({1:#,#;;0}) in memory. " +
                                          "Going to index the results while loading more from disk in background.",
                                    Math.Min(take ?? int.MaxValue, GetNumberOfItemsToProcessInSingleBatch()),
                                    result.Count);

                            MaybeAddFutureBatch(result);
                            return result;
                        }

                        if (log.IsDebugEnabled)
                            log.Debug("Didn't load any documents from previous batches. Loading documents directly from disk.");

                        if (firstEtagInQueue == null || firstEtagInQueue.CompareTo(etag) <= 0)
                        {
                            //if the first etag in queue is smaller than the already requested one,
                            //we can look for the next etag in the future batches
                            firstEtagInQueue = GetMinimumEtagFromFutureBatches(Abstractions.Util.EtagUtil.Increment(etag, 1));
                        }
                        // if there has been no results, AND no future batch that we can wait for, then we just load directly from disk
                        LoadDocumentsFromDisk(etag, firstEtagInQueue); // here we _intentionally_ use the current etag, not the next one
                    }
                }

                docsLoaded = TryGetDocumentsFromQueue(nextEtagToIndex, result, take);

                // we don't need to add a future batch to a prefetcher that
                // gets the documents after commit -> the default one,
                // except when we disabled collecting documents after commit
                if (ShouldHandleUnusedDocumentsAddedAfterCommit == false ||
                    DisableCollectingDocumentsAfterCommit)
                {
                    // we removed some documents from the queue
                    // we'll try to create a new future batch, if possible
                    MaybeAddFutureBatch(result.LastOrDefault());
                }

                if (docsLoaded)
                {
                    etag = result[result.Count - 1].Etag;
                    if (IsDefault)
                        recentEtag = recentEtag.CompareTo(etag) < 0 ? etag : recentEtag;
                }
                prefetchingQueueSizeInBytes = prefetchingQueue.LoadedSize;
            }
            while (
                result.Count < GetNumberOfItemsToProcessInSingleBatch() &&
                (take.HasValue == false || result.Count < take.Value) &&
                docsLoaded &&
                prefetchingDurationTimer.ElapsedMilliseconds <= context.Configuration.PrefetchingDurationLimit &&
                ((prefetchingQueueSizeInBytes + currentlyUsedBatchSizesInBytes) < (context.Configuration.DynamicMemoryLimitForProcessing)));

            return result;
        }

        private int GetNumberOfItemsToProcessInSingleBatch()
        {
            var prefetchintBehavioursCount = getPrefetchintBehavioursCount();
            var numberOfItemsToProcessInSingleBatch = autoTuner.NumberOfItemsToProcessInSingleBatch;
            numberOfItemsToProcessInSingleBatch =
                Math.Min(numberOfItemsToProcessInSingleBatch,
                         context.Configuration.MaxNumberOfItemsToProcessInSingleBatch / prefetchintBehavioursCount);

            return Math.Max(numberOfItemsToProcessInSingleBatch, context.Configuration.InitialNumberOfItemsToProcessInSingleBatch);
        }

        private void LoadDocumentsFromDisk(Etag etag, Etag untilEtag)
        {
            var sp = Stopwatch.StartNew();
            var jsonDocs = GetJsonDocsFromDisk(etag, untilEtag, context.CancellationToken);
            if (log.IsDebugEnabled)
            {
                log.Debug("Loaded {0} documents ({3:#,#;;0} kb) from disk, starting from etag {1}, took {2}ms", jsonDocs.Count, etag, sp.ElapsedMilliseconds,
                    jsonDocs.Sum(x => x.SerializedSizeOnDisk) / 1024);
            }

            // if we are forced to load from disk in a sync fashion, let us start the process
            // of making sure that we don't need to do this next time by starting an async load
            MaybeAddFutureBatch(jsonDocs);

            using (prefetchingQueue.EnterWriteLock())
            {
                foreach (var jsonDocument in jsonDocs)
                    prefetchingQueue.Add(jsonDocument);
            }
        }

        private bool TryGetDocumentsFromQueue(Etag nextDocEtag, List<JsonDocument> items, int? take, bool isFromIndex = false)
        {
            JsonDocument result;

            bool hasDocs = false;

            while (items.Count < GetNumberOfItemsToProcessInSingleBatch() &&
                prefetchingQueue.TryPeek(out result) &&
                // we compare to current or _smaller_ so we will remove from the queue old versions
                // of documents that we have already loaded
                nextDocEtag.CompareTo(result.Etag) >= 0)
            {
                // safe to do peek then dequeue because we are the only one doing the dequeues
                // and here we are single threaded, but still, better to check
                if (prefetchingQueue.TryDequeue(out result) == false)
                    continue;

                // this shouldn't happen, but... 
                if (result == null)
                    continue;

                if (result.Etag != nextDocEtag)
                    continue;

                items.Add(result);

                hasDocs = true;

                if (take.HasValue && items.Count >= take.Value)
                    break;

                nextDocEtag = Abstractions.Util.EtagUtil.Increment(nextDocEtag, 1);
            }

            return hasDocs;
        }

        public IEnumerable<JsonDocument> DebugGetDocumentsInPrefetchingQueue()
        {
            return prefetchingQueue.Clone().Values;
        }

        public FutureBatchesSummary DebugGetDocumentsInFutureBatches()
        {
            var result = new List<object>();
            var totalResults = 0;
            var totalCanceled = 0;
            var totalFaulted = 0;

            foreach (var futureBatch in futureIndexBatches)
            {
                var task = futureBatch.Value.Task;
                if (task.Status == TaskStatus.Canceled)
                {
                    totalCanceled += 1;
                    continue;
                }
                if (task.Status == TaskStatus.Faulted)
                {
                    totalFaulted += 1;
                    continue;
                }

                if (task.IsCompleted == false)
                {
                    result.Add(new
                    {
                        FromEtag = futureBatch.Key,
                        Docs = "Loading documents from disk in progress"
                    });

                    continue;
                }

                var docs = task.Result;

                var take = Math.Min(5, docs.Count);

                var etagsWithKeysTail = Enumerable.Range(0, take).Select(
                    i => docs[docs.Count - take + i]).ToDictionary(x => x.Etag, x => x.Key);

                result.Add(new
                {
                    FromEtag = futureBatch.Key,
                    EtagsWithKeysHead = docs.Take(5).ToDictionary(x => x.Etag, x => x.Key),
                    EtagsWithKeysTail = etagsWithKeysTail,
                    TotalDocsCount = docs.Count
                });

                totalResults += docs.Count;
            }

            return new FutureBatchesSummary
            {
                Summary = result,
                Total = totalResults,
                Canceled = totalCanceled,
                Faulted = totalFaulted
            };
        }

        private TaskStatus? CanLoadDocumentsFromFutureBatches(Etag nextDocEtag)
        {
            if (context.Configuration.DisableDocumentPreFetching)
                return null;

            FutureIndexBatch batch;
            if (futureIndexBatches.TryGetValue(nextDocEtag, out batch) == false)
                return null;

            if (Task.CurrentId == batch.Task.Id)
                return null;

            return batch.Task.Status;
        }

        private bool TryLoadDocumentsFromFutureBatches(Etag nextDocEtag, bool allowWaiting)
        {
            try
            {
                switch (CanLoadDocumentsFromFutureBatches(nextDocEtag))
                {
                    case TaskStatus.Created:
                    case TaskStatus.WaitingForActivation:
                    case TaskStatus.WaitingToRun:
                    case TaskStatus.Running:
                    case TaskStatus.WaitingForChildrenToComplete:
                        if (log.IsDebugEnabled)
                            log.Info("Future batch is not completed, will wait: {0}", allowWaiting);

                        if (allowWaiting == false)
                            return false;

                        //if we are here, after that we are actually going to wait for the batch to finish
                        if (splitPrefetchingCount <= 0)
                        {
                            var numOfIOWaits = Interlocked.Increment(ref numberOfTimesWaitedHadToWaitForIO);
                            Interlocked.Exchange(ref splitPrefetchingCount, numOfIOWaits * 8);
                        }
                        break;
                    case TaskStatus.RanToCompletion:
                        break;
                    case TaskStatus.Canceled:
                    case TaskStatus.Faulted:
                        //remove canceled or faulted tasks from the future index batches
                        FutureIndexBatch failed;
                        if (futureIndexBatches.TryRemove(nextDocEtag, out failed))
                        {
                            log.WarnException("When trying to get future batch for etag " + nextDocEtag + " we got an error, will try again",
                                failed.Task.Exception);
                        }
                        return false;
                    case null:
                        return false;
                    default:
                        throw new ArgumentOutOfRangeException();
                }

                FutureIndexBatch nextBatch;
                if (futureIndexBatches.TryRemove(nextDocEtag, out nextBatch) == false) // here we need to remove the batch
                    return false;

                List<JsonDocument> jsonDocuments = nextBatch.Task.Result;
                MaybeAddFutureBatch(jsonDocuments);
                using (prefetchingQueue.EnterWriteLock())
                {
                    foreach (var jsonDocument in jsonDocuments)
                        prefetchingQueue.Add(jsonDocument);
                }

                return true;
            }
            catch (Exception e)
            {
                log.WarnException("Error when getting next batch value asynchronously, will try in sync manner", e);
                return false;
            }
        }

        private List<JsonDocument> GetJsonDocsFromDisk(Etag fromEtag, Etag untilEtag, 
            CancellationToken cancellationToken, Reference<bool> earlyExit = null)
        {
            Func<IStorageActionsAccessor, int, long, TimeSpan, IEnumerable<JsonDocument>> getDocuments =
                (actions, numberOfItemsToProcessInSingleBatch, maxSize, timeOut) =>
                    actions.Documents
                        .GetDocumentsAfter(
                            fromEtag,
                            numberOfItemsToProcessInSingleBatch,
                            cancellationToken,
                            maxSize,
                            untilEtag,
                            autoTuner.FetchingDocumentsFromDiskTimeout,
                            earlyExit: earlyExit
                        );

            return GetDocumentsList(getDocuments);
        }

        private static void GetQueryForAllMatchingDocumentsForIndex(
            DocumentDatabase database, AbstractViewGenerator generator, StringBuilder sb)
        {
            var terms = new TermsQueryRunner(database)
                .GetTerms(Constants.DocumentsByEntityNameIndex, "Tag", null, int.MaxValue);

            foreach (var entityName in generator.ForEntityNames)
            {
                bool added = false;
                foreach (var term in terms)
                {
                    if (string.Equals(entityName, term, StringComparison.OrdinalIgnoreCase))
                    {
                        AppendTermToQuery(term, sb);
                        added = true;
                    }
                }
                if (added == false)
                    AppendTermToQuery(entityName, sb);
            }
        }

        public static string GetQueryForAllMatchingDocumentsForIndex(DocumentDatabase database, AbstractViewGenerator generator)
        {
            var sb = new StringBuilder();
            GetQueryForAllMatchingDocumentsForIndex(database, generator, sb);
            return sb.ToString();
        }

        private static void AppendTermToQuery(string term, StringBuilder sb)
        {
            if (sb.Length != 0)
                sb.Append(" OR ");

            sb.Append("Tag:[[").Append(term).Append("]]");
        }

        private IEnumerable<JsonDocument> GetDocumentsByIndex(List<int> indexIds, Etag fromEtag, Etag untilEtag, 
            IStorageActionsAccessor actions, int numberOfItemsToProcessInSingleBatch,
            long maxSize, TimeSpan timeout, out Etag lastIndexed, Reference<bool> earlyExit = null)
        {
            using (var cts = new CancellationTokenSource())
            using (var linked = CancellationTokenSource.CreateLinkedTokenSource(cts.Token, context.CancellationToken))
            {
                var docsToIndex = new List<JsonDocument>();
                lastIndexed = null;
                if (indexIds == null)
                {
                    //
                    return docsToIndex;
                }

                var sb = new StringBuilder();
                foreach (var indexId in indexIds)
                {
                    var generator = context.Database.IndexDefinitionStorage.GetViewGenerator(indexId);
                    if (generator == null)
                        continue;

                    if (generator.ForEntityNames.Count == 0)
                    {
                        //
                        return docsToIndex;
                    }

                    GetQueryForAllMatchingDocumentsForIndex(context.Database, generator, sb);
                }

                if (sb.Length == 0)
                {
                    //
                    return docsToIndex;
                }

                var isUntilEtag = untilEtag != null;

                sb = new StringBuilder("(" + sb);
                /*sb.Append(") AND (Etag: [")
                    .Append(fromEtag)
                    .Append(" TO null])");*/
                sb.Append(") AND ((Restarts:")
                    .Append(fromEtag.Restarts)
                    .Append(" AND Changes_Range:[Lx")
                    .Append(fromEtag.Changes)
                    .Append(" TO ")
                    .Append(isUntilEtag ? $"{untilEtag.Changes}Lx" : "*")
                    .Append("]) OR (Restarts_Range:{Lx")
                    .Append(fromEtag.Restarts)
                    .Append(" TO ")
                    .Append(isUntilEtag ? $"{untilEtag.Restarts}Lx" : "*")
                    .Append("]))");

                var duration = Stopwatch.StartNew();

                using (var op = new QueryActions.DatabaseQueryOperation(context.Database, Constants.DocumentsByEntityNameIndex, new IndexQuery
                {
                    Query = sb.ToString(),
                    PageSize = numberOfItemsToProcessInSingleBatch,
                    SortedFields = new[] { /*new SortedField("Etag")*/new SortedField("Restarts"), new SortedField("Changes") }
                }, actions, linked)
                {
                    ShouldSkipDuplicateChecking = true
                })
                {
                    op.Init();

                    /*if (log.IsDebugEnabled)
                        log.Debug("For new index {0}, using precomputed indexing batch optimization for {1} docs", index,
                              op.Header.TotalResults);*/

                    int totalLoadedDocumentSize = 0;
                    op.Execute(document =>
                    {
                        var metadata = document.Value<RavenJObject>(Constants.Metadata);
                        var key = metadata.Value<string>("@id");
                        var etag = Etag.Parse(metadata.Value<string>("@etag"));
                        var lastModified = DateTime.Parse(metadata.Value<string>(Constants.LastModified));
                        document.Remove(Constants.Metadata);
                        var serializedSizeOnDisk = metadata.Value<int>(Constants.SerializedSizeOnDisk);
                        metadata.Remove(Constants.SerializedSizeOnDisk);

                        var doc = new JsonDocument
                        {
                            DataAsJson = document,
                            Etag = etag,
                            Key = key,
                            SerializedSizeOnDisk = serializedSizeOnDisk,
                            LastModified = lastModified,
                            SkipDeleteFromIndex = true,
                            Metadata = metadata
                        };

                        totalLoadedDocumentSize += serializedSizeOnDisk;

                        if (duration.Elapsed > timeout)
                        {
                            if (earlyExit != null)
                                earlyExit.Value = true;
                            return;
                        }

                        if (totalLoadedDocumentSize > maxSize)
                        {
                            if (untilEtag != null && earlyExit != null)
                                earlyExit.Value = true;
                            return;
                        }

                        docsToIndex.Add(doc);
                    });

                    if (numberOfItemsToProcessInSingleBatch > docsToIndex.Count &&
                        (earlyExit == null || earlyExit.Value == false))
                    {
                        lastIndexed = op.Header.IndexEtag;
                    }
                }

                return docsToIndex;
            }
        }

        public PrefetchingSummary GetSummary()
        {
            var loadTimesCount = loadTimes.Count;
            var totalDocumentsCount = loadTimes.Sum(x => x.NumberOfDocuments);

            long size = 50000;
            var approximateDocumentCount = context.Configuration.InitialNumberOfItemsToProcessInSingleBatch;
            if (loadTimesCount > 0 && totalDocumentsCount > 0)
            {
                size = loadTimes.Sum(x => x.TotalSize) / loadTimesCount;
                approximateDocumentCount = totalDocumentsCount / loadTimesCount;
            }

            var futureIndexBatchesLoadedSize = futureIndexBatches.Values.Sum(x =>
            {
                if (x.Task.Status == TaskStatus.RanToCompletion)
                    return x.Task.Result.Sum(doc => doc.SerializedSizeOnDisk);

                return size;
            });

            var futureIndexBatchesDocsCount = futureIndexBatches.Values.Sum(x =>
            {
                if (x.Task.Status == TaskStatus.RanToCompletion)
                    return x.Task.Result.Count;

                if (x.DocsCount != null && x.DocsCount.Value != null &&
                    (x.Type == FutureBatchType.Splitted || x.Type == FutureBatchType.EarlyExit))
                {
                    return x.DocsCount.Value.Value;
                }

                return approximateDocumentCount;
            });

            return new PrefetchingSummary
            {
                PrefetchingQueueLoadedSize = prefetchingQueue.LoadedSize,
                PrefetchingQueueDocsCount = prefetchingQueue.Count,
                FutureIndexBatchesLoadedSize = futureIndexBatchesLoadedSize,
                FutureIndexBatchesDocsCount = futureIndexBatchesDocsCount,
            };
        }

        private void MaybeAddFutureBatch(JsonDocument lastDocumentFromResult)
        {
            var maxFutureBatch = GetCompletedFutureBatchWithMaxStartingEtag();
            if (maxFutureBatch != null)
            {
                // we found a future batch with the latest starting etag (which completed fetching documents)
                // we'll try to create a new future batch using the latest results
                MaybeAddFutureBatch(maxFutureBatch.Task.Result);
            }
            else if (futureIndexBatches.Count == 0 && prefetchingQueue.Count > 0)
            {
                // we don't have any future batches, but have some documents in the queue
                // we'll try to create a new future batch using the latest document in the queue
                JsonDocument lastDocument;
                if (prefetchingQueue.TryPeekLastDocument(out lastDocument))
                {
                    MaybeAddFutureBatch(new List<JsonDocument> { lastDocument });
                }
            }
            else if (futureIndexBatches.Count == 0 && prefetchingQueue.Count == 0 &&
                     lastDocumentFromResult != null)
            {
                // prefetching queue and future batches are empty
                // we'll try to create a new future batch using the last document
                // from the results that we are going to return
                MaybeAddFutureBatch(new List<JsonDocument> { lastDocumentFromResult });
            }
        }

        private void MaybeAddFutureBatch(List<JsonDocument> past)
        {
            if (context.Configuration.DisableDocumentPreFetching || context.RunIndexing == false)
                return;
            if (context.Configuration.MaxNumberOfParallelProcessingTasks == 1)
                return;
            if (past.Count == 0)
                return;

            var numberOfSplitTasks = Math.Max(2, Environment.ProcessorCount / 2);
            var actualFutureIndexBatchesCount = GetActualFutureIndexBatchesCount(numberOfSplitTasks);
            // no need to load more than 5 future batches
            if (actualFutureIndexBatchesCount > 5)
                return;

            // ensure that we don't use too much memory
            if (CanPrefetchMoreDocs(isFutureBatch: true) == false)
                return;

            if (MemoryStatistics.IsLowMemory)
                return;

            // ensure we don't do TOO much future caching
            if (MemoryStatistics.AvailableMemoryInMb < context.Configuration.AvailableMemoryForRaisingBatchSizeLimit)
            {
                log.Info("Skipping background prefetching because we have {0}mb of availiable memory and the availiable memory for raising the batch size limit is: {1}mb",
                        MemoryStatistics.AvailableMemoryInMb, context.Configuration.AvailableMemoryForRaisingBatchSizeLimit / 1024 / 1024);
                return;
            }

            TryScheduleFutureIndexBatch(past, numberOfSplitTasks);
        }

        private bool CanPrefetchMoreDocs(bool isFutureBatch = false)
        {
            var globalSummary = getPrefetcherSummary();
            var maxAllowedToLoadInBytes = Math.Min(autoTuner.MaximumSizeAllowedToFetchFromStorageInBytes,
                context.Configuration.AvailableMemoryForRaisingBatchSizeLimit * 1024 * 1024);
            var loadedSizeInBytes = globalSummary.PrefetchingQueueLoadedSize + globalSummary.FutureIndexBatchesLoadedSize;
            if (loadedSizeInBytes >= maxAllowedToLoadInBytes)
            {
                log.Info("Skipping {2} prefetching because we already have {0:#,#;;0} kb (in all prefetchers)" +
                         "in the prefetching queue and in the future tasks and we have a limit of {1:#,#;;0} kb",
                    loadedSizeInBytes / 1024,
                    maxAllowedToLoadInBytes / 1024,
                    isFutureBatch ? "background" : "after commit");
                return false;  // already have too much in memory in all prefetching behaviors
            }

            var loadedDocsCount = globalSummary.PrefetchingQueueDocsCount + globalSummary.FutureIndexBatchesDocsCount;
            if (loadedDocsCount >= context.Configuration.MaxNumberOfItemsToProcessInSingleBatch)
            {
                log.Info("Skipping {2} prefetching because we already have {0:#,#;;0} documents (in all prefetchers) " +
                         "in the prefetching queue and in the future tasks and our limit is {1:#,#;;0}",
                    loadedDocsCount,
                    context.Configuration.MaxNumberOfItemsToProcessInSingleBatch,
                    isFutureBatch ? "background" : "after commit");
                return false; // already have too much items in all prefetching behaviors
            }

            var localSummary = GetSummary();
            var numberOfPrefetchingBehaviors = getPrefetchintBehavioursCount();

            loadedSizeInBytes = localSummary.PrefetchingQueueLoadedSize + localSummary.FutureIndexBatchesLoadedSize;
            var maxLoadedSizeInBytesInASingleBatch = maxAllowedToLoadInBytes / numberOfPrefetchingBehaviors;
            if (loadedSizeInBytes >= maxLoadedSizeInBytesInASingleBatch)
            {
                log.Info("Skipping {2} prefetching because we already have {0:#,#;;0} kb (in a single prefetcher) " +
                         "in the prefetching queue and in the future tasks and we have a limit of {1:#,#;;0} kb",
                    loadedSizeInBytes / 1024,
                    maxLoadedSizeInBytesInASingleBatch / 1024,
                    isFutureBatch ? "background" : "after commit");
                return false; // already have too much in memory in this prefetching behavior
            }

            loadedDocsCount = localSummary.PrefetchingQueueDocsCount + localSummary.FutureIndexBatchesDocsCount;
            var maxDocsInASingleBatch = context.Configuration.MaxNumberOfItemsToProcessInSingleBatch / numberOfPrefetchingBehaviors;
            if (loadedDocsCount >= maxDocsInASingleBatch)
            {
                log.Info("Skipping {2} prefetching because we already have {0:#,#;;0} documents (in a single prefetcher)" +
                         "in the prefetching queue and in the future tasks and our limit is {1:#,#;;0}",
                    loadedDocsCount,
                    maxDocsInASingleBatch,
                    isFutureBatch ? "background" : "after commit");
                return false; // already have too much items in this prefetching behavior
            }

            return true;
        }

        private int GetActualFutureIndexBatchesCount(int numberOfSplitTasks)
        {
            var actualFutureIndexBatchesCount = 0;
            var splittedFutureIndexBatchesCount = 0;
            foreach (var futureIndexBatch in futureIndexBatches.Values)
            {
                if (futureIndexBatch.Type == FutureBatchType.EarlyExit)
                {
                    // we don't count the early exit future batches,
                    // since they were originally created by the splitted batches
                    continue;
                }

                if (futureIndexBatch.Type == FutureBatchType.Splitted)
                {
                    splittedFutureIndexBatchesCount += 1;
                    if (splittedFutureIndexBatchesCount / numberOfSplitTasks != 1)
                        continue;

                    splittedFutureIndexBatchesCount = 0;
                }
                actualFutureIndexBatchesCount++;
            }

            if (splittedFutureIndexBatchesCount > 0)
                actualFutureIndexBatchesCount++;

            return actualFutureIndexBatchesCount;
        }

        private void TryScheduleFutureIndexBatch(List<JsonDocument> past, int numberOfSplitTasks)
        {
            // we loaded the maximum amount, there are probably more items to read now.
            Etag highestLoadedEtag = GetHighestEtag(past);
            Etag nextEtag = GetNextDocumentEtagFromDisk(highestLoadedEtag);

            if (IsDefault && prefetchingQueue.DocumentExists(nextEtag))
            {
                //checking only for the default prefetcher:
                //1) this case can rarely happen for the other prefetchers
                //2) DocumentExists is o(log n)
                log.Info("Skipping background prefetching because we already have a document with {0} etag in the queue.", nextEtag);
                return;
            }

            if (nextEtag == highestLoadedEtag)
            {
                log.Info("Skipping background prefetching because we got no documents to fetch. last etag: {0}", nextEtag);
                return;
            }

            if (futureIndexBatches.ContainsKey(nextEtag))
            {
                var maxFutureIndexBatch = GetCompletedFutureBatchWithMaxStartingEtag();

                if (maxFutureIndexBatch == null || nextEtag.CompareTo(maxFutureIndexBatch.StartingEtag) >= 0 ||
                    maxFutureIndexBatch.Task.IsCompleted == false || maxFutureIndexBatch.Task.Status != TaskStatus.RanToCompletion)
                {
                    log.Info("Skipping background prefetching because we already have a future batch that starts with etag: {0}", nextEtag);
                    return;
                }

                // we couldn't schedule a new future batch using the next etag
                // let's try and schedule a future batch with the last etag in the last future batch
                TryScheduleFutureIndexBatch(maxFutureIndexBatch.Task.Result, numberOfSplitTasks);
                return;
            }

            var currentSplitCount = Interlocked.Decrement(ref splitPrefetchingCount);
            if (currentSplitCount < 0)
            {
                // it is fine if we lose this, because another thread will fix up this 
                // value before we get integer underflow
                Interlocked.CompareExchange(ref splitPrefetchingCount, 0, currentSplitCount);
            }
            if (currentSplitCount <= 0)
            {
                if (numberOfTimesWaitedHadToWaitForIO > 0)
                {
                    Interlocked.Decrement(ref numberOfTimesWaitedHadToWaitForIO);
                }
                AddFutureBatch(nextEtag, null, FutureBatchType.Normal);
                return;
            }

            SplitFutureBatches(numberOfSplitTasks, nextEtag);
        }

        private void SplitFutureBatches(int numberOfSplitTasks, Etag nextEtag)
        {
            context.TransactionalStorage.Batch(accessor =>
            {
                double loadTimePerDocMs;
                long largestDocSize;
                string largestDocKey;
                CalculateAverageLoadTimes(out loadTimePerDocMs, out largestDocSize, out largestDocKey);

                var numOfDocsToTakeInEachSplit =
                    (int)Math.Min((autoTuner.FetchingDocumentsFromDiskTimeout.TotalMilliseconds * 0.9) / loadTimePerDocMs,
                        (autoTuner.MaximumSizeAllowedToFetchFromStorageInBytes * 0.9) / largestDocSize);

                var maxDocsInASingleBatch = context.Configuration.MaxNumberOfItemsToProcessInSingleBatch /
                                            getPrefetchintBehavioursCount();

                // we need to limit the number of documents to take in each split
                // in total count, it should be less than we can process in single batch
                numOfDocsToTakeInEachSplit = Math.Max(
                    context.Configuration.InitialNumberOfItemsToProcessInSingleBatch,
                    Math.Min(numOfDocsToTakeInEachSplit / numberOfSplitTasks,
                        maxDocsInASingleBatch / numberOfSplitTasks));

                if (log.IsDebugEnabled)
                {
                    log.Debug("Splitting future batch to {0} batches with {1:#,#;;0} items per each, assuming that largest recent doc size is {2:#,#.#;;0} kb, " +
                              "largest doc key is: {3} and avg load time per doc is {4:0.#,#;;0} ms",
                        numberOfSplitTasks,
                        numOfDocsToTakeInEachSplit,
                        largestDocSize / 1024d,
                        largestDocKey,
                        loadTimePerDocMs);
                }

                for (int i = 0; i < numberOfSplitTasks; i++)
                {
                    int count;
                    var lastEtagInBatch = accessor.Documents.GetEtagAfterSkip(nextEtag,
                        numOfDocsToTakeInEachSplit, context.CancellationToken, out count);

                    if (lastEtagInBatch == null || lastEtagInBatch == nextEtag)
                        break;

                    if (AddFutureBatch(nextEtag, lastEtagInBatch, FutureBatchType.Splitted, count) == false)
                    {
                        if (log.IsDebugEnabled)
                        {
                            log.Debug("Failed to add future batch because" +
                                      "another future batch with starting etag {0} already exists", nextEtag);
                        }
                        // no need to continue to add the next splitted batches,
                        // the existing future batch will try to generate a future batch
                        return;
                    }

                    nextEtag = accessor.Documents.GetBestNextDocumentEtag(lastEtagInBatch);
                }
            });
        }

        /// <summary>
        /// Gets the minimum etag from the future batches that is bigger than the startEtag
        /// </summary>
        /// <param name="startEtag">The minimum etag we start to look from</param>
        /// <returns></returns>
        private Etag GetMinimumEtagFromFutureBatches(Etag startEtag)
        {
            Etag minEtag = null;

            foreach (var futureIndexBatch in futureIndexBatches.Values)
            {
                if (startEtag.CompareTo(futureIndexBatch.StartingEtag) < 0 &&
                    (minEtag == null || futureIndexBatch.StartingEtag.CompareTo(minEtag) < 0))
                {
                    minEtag = futureIndexBatch.StartingEtag;
                }
            }

            if (minEtag != null && minEtag != Etag.Empty)
                minEtag = Abstractions.Util.EtagUtil.Increment(minEtag, -1);

            return minEtag;
        }

        private FutureIndexBatch GetCompletedFutureBatchWithMaxStartingEtag()
        {
            FutureIndexBatch maxFutureIndexBatch = null;

            foreach (var futureIndexBatch in futureIndexBatches.Values)
            {
                if (futureIndexBatch.Task.IsCompleted == false ||
                    futureIndexBatch.Task.Status != TaskStatus.RanToCompletion)
                    continue;

                if (maxFutureIndexBatch == null ||
                    futureIndexBatch.StartingEtag.CompareTo(maxFutureIndexBatch.StartingEtag) > 0)
                    maxFutureIndexBatch = futureIndexBatch;
            }

            return maxFutureIndexBatch;
        }

        private void CalculateAverageLoadTimes(out double loadTimePerDocMs, out long largestDocSize, out string largestDocKey)
        {
            var docCount = 0;
            var totalTime = 0L;
            largestDocSize = 0;
            largestDocKey = null;
            foreach (var diskFetchPerformanceStats in loadTimes)
            {
                docCount += diskFetchPerformanceStats.NumberOfDocuments;
                totalTime += diskFetchPerformanceStats.LoadingTimeInMillseconds;
                if (diskFetchPerformanceStats.LargestDocSize <= largestDocSize)
                    continue;

                largestDocSize = diskFetchPerformanceStats.LargestDocSize;
                largestDocKey = diskFetchPerformanceStats.LargestDocKey;
            }

            if (largestDocSize == 0)
            {
                // default, since we have no info yet
                largestDocSize = 4096;
            }

            if (docCount == 0)
            {
                // default, since we have no info yet
                loadTimePerDocMs = 30;
            }
            else
            {
                loadTimePerDocMs = (double)totalTime / docCount;
            }
        }

        private bool AddFutureBatch(Etag nextEtag, Etag untilEtag,
            FutureBatchType batchType, int? docsCount = null)
        {
            var futureBatchStat = new FutureBatchStats
            {
                Timestamp = SystemTime.UtcNow,
                PrefetchingUser = PrefetchingUser
            };
            Stopwatch sp = Stopwatch.StartNew();
            context.AddFutureBatch(futureBatchStat);

            var docsCountRef = new Reference<int?>() { Value = docsCount };
            var cts = new CancellationTokenSource();
            var linkedToken = CancellationTokenSource.CreateLinkedTokenSource(cts.Token, context.CancellationToken);
            var futureIndexBatch = new FutureIndexBatch
            {
                StartingEtag = nextEtag,
                Age = Interlocked.Increment(ref currentIndexingAge),
                CancellationTokenSource = linkedToken,
                Type = batchType,
                DocsCount = docsCountRef,
                Task = Task.Factory.StartNew(() =>
                {
                    List<JsonDocument> jsonDocuments = null;
                    int localWork = 0;
                    var earlyExit = new Reference<bool>();
                    while (context.RunIndexing)
                    {
                        linkedToken.Token.ThrowIfCancellationRequested();
                        jsonDocuments = GetJsonDocsFromDisk(
                            Abstractions.Util.EtagUtil.Increment(nextEtag, -1), untilEtag, 
                            linkedToken.Token, earlyExit);

                        if (jsonDocuments.Count > 0)
                            break;

                        futureBatchStat.Retries++;

                        context.WaitForWork(TimeSpan.FromMinutes(10), ref localWork, "PreFetching");
                    }

                    futureBatchStat.Duration = sp.Elapsed;
                    futureBatchStat.Size = jsonDocuments == null ? 0 : jsonDocuments.Count;

                    if (jsonDocuments == null)
                        return null;

                    LogEarlyExit(nextEtag, untilEtag, batchType == FutureBatchType.EarlyExit,
                        jsonDocuments, sp.ElapsedMilliseconds);

                    if (untilEtag != null && earlyExit.Value)
                    {
                        var lastEtag = GetHighestEtag(jsonDocuments);
                        context.TransactionalStorage.Batch(accessor =>
                        {
                            lastEtag = accessor.Documents.GetBestNextDocumentEtag(lastEtag);
                        });

                        if (log.IsDebugEnabled)
                        {
                            log.Debug("Early exit from last future splitted batch, need to fetch documents from etag: {0} to etag: {1}",
                                lastEtag, untilEtag);
                        }

                        linkedToken.Token.ThrowIfCancellationRequested();
                        docsCountRef.Value = jsonDocuments.Count;
                        var docsLeft = docsCount - jsonDocuments.Count;
                        if (docsLeft > 0 && lastEtag.CompareTo(untilEtag) <= 0)
                            AddFutureBatch(lastEtag, untilEtag, FutureBatchType.EarlyExit, docsLeft);
                    }
                    else
                    {
                        linkedToken.Token.ThrowIfCancellationRequested();
                        MaybeAddFutureBatch(jsonDocuments);
                    }
                    return jsonDocuments;
                }, linkedToken.Token)
                .ContinueWith(t =>
                {
                    using (cts)
                    using (linkedToken)
                    {
                        t.AssertNotFailed();
                    }
                    return t.Result;
                })
            };

            futureIndexBatch.Task.ContinueWith(t =>
            {
                try
                {
                    if (linkedToken.IsCancellationRequested == false)
                        FutureBatchCompleted(t.Result.Count);
                }
                catch (ObjectDisposedException)
                {
                    // this is an expected race with the actual task, this is fine
                }
            });

            return futureIndexBatches.TryAdd(nextEtag, futureIndexBatch);
        }

        private enum FutureBatchType
        {
            Normal,
            Splitted,
            EarlyExit
        }

        private static void LogEarlyExit(Etag nextEtag, Etag untilEtag,
            bool isEarlyExitBatch, List<JsonDocument> jsonDocuments, long timeElapsed)
        {
            var size = jsonDocuments.Sum(x => x.SerializedSizeOnDisk) / 1024;
            if (isEarlyExitBatch)
            {
                log.Warn("After early exit: Got {0} documents ({1:#,#;;0} kb) in a future batch, starting from etag {2} to etag {4}, took {3:#,#;;0}ms",
                    jsonDocuments.Count, size, nextEtag, timeElapsed, untilEtag);
            }

            if (log.IsDebugEnabled)
            {
                if (log.IsDebugEnabled && isEarlyExitBatch == false)
                {
                    log.Debug("Got {0} documents ({1:#,#;;0} kb) in a future batch, starting from etag {2}, took {3:#,#;;0}ms",
                        jsonDocuments.Count, size, nextEtag, timeElapsed);
                }

                if (log.IsDebugEnabled && (size > jsonDocuments.Count * 8 || timeElapsed > 3000))
                {
                    var topSizes = jsonDocuments.OrderByDescending(x => x.SerializedSizeOnDisk).Take(10).Select(x => string.Format("{0} - {1:#,#;;0}kb", x.Key, x.SerializedSizeOnDisk / 1024));
                    log.Debug("Slow load of documents in batch, maybe large docs? Top 10 largest docs are: ({0})", string.Join(", ", topSizes));
                }
            }
        }

        private Etag GetNextDocEtag(Etag etag)
        {
            var oneUpEtag = Abstractions.Util.EtagUtil.Increment(etag, 1);

            // no need to go to disk to find the next etag if we already have it in memory
            if (prefetchingQueue.NextDocumentETag() == oneUpEtag)
                return oneUpEtag;

            return GetNextDocumentEtagFromDisk(etag);
        }

        private Etag GetNextDocumentEtagFromDisk(Etag etag)
        {
            Etag nextDocEtag = null;
            context.TransactionalStorage.Batch(
                accessor => { nextDocEtag = accessor.Documents.GetBestNextDocumentEtag(etag); });

            return nextDocEtag;
        }

        private static Etag GetHighestEtag(List<JsonDocument> past)
        {
            JsonDocument jsonDocument = GetHighestJsonDocumentByEtag(past);
            if (jsonDocument == null)
                return Etag.Empty;
            return jsonDocument.Etag ?? Etag.Empty;
        }

        public static JsonDocument GetHighestJsonDocumentByEtag(List<JsonDocument> past)
        {
            var highest = Etag.Empty;
            JsonDocument highestDoc = null;
            for (int i = past.Count - 1; i >= 0; i--)
            {
                Etag etag = past[i].Etag;
                if (highest.CompareTo(etag) > 0)
                {
                    continue;
                }
                highest = etag;
                highestDoc = past[i];
            }
            return highestDoc;
        }

        private static Task ObserveDiscardedTask(FutureIndexBatch source)
        {
            return source.Task.ContinueWith(task =>
            {
                if (task.Exception != null)
                {
                    log.WarnException("Error happened on discarded future work batch", task.Exception);
                }
                else
                {
                    log.Warn("WASTE: Discarding future work item without using it, to reduce memory usage");
                }
            });
        }

        public void BatchProcessingComplete()
        {
            int indexingAge = Interlocked.Increment(ref currentIndexingAge);

            // make sure that we don't have too much "future cache" items
            const int numberOfIndexingGenerationsAllowed = 64;
            foreach (FutureIndexBatch source in futureIndexBatches.Values.Where(x => (indexingAge - x.Age) > numberOfIndexingGenerationsAllowed).ToList())
            {
                ObserveDiscardedTask(source);
                DisposeCancellationToken(source.CancellationTokenSource);
                FutureIndexBatch batch;
                futureIndexBatches.TryRemove(source.StartingEtag, out batch);
            }
        }

        public void AfterStorageCommitBeforeWorkNotifications(JsonDocument[] docs)
        {
            if (ShouldHandleUnusedDocumentsAddedAfterCommit == false ||
                context.Configuration.DisableDocumentPreFetching ||
                docs.Length == 0 ||
                DisableCollectingDocumentsAfterCommit ||
                context.RunIndexing == false)
                return;

            // don't use too much, this is an optimization and we need to be careful about using too much memory
            if (CanPrefetchMoreDocs(isFutureBatch: false) == false)
                return;

            if (prefetchingQueue.Count >= context.Configuration.MaxNumberOfItemsToPreFetch)
                return;

            ingestMeter.Mark(docs.Length);

            var current = lowestInMemoryDocumentAddedAfterCommit;
            if (current != null &&
                // ingest rate is too high, maybe we need to protect ourselves from 
                // ingest overflow that can cause high memory usage
                ingestMeter.OneMinuteRate > returnedDocsMeter.OneMinuteRate * 1.5
                )
            {
                // we don't want to trigger it too soon, so we need a few commits
                // with the high ingest rate before we'll decide that we need
                // to take such measures
                if (numberOfTimesIngestRateWasTooHigh++ > 3)
                {
                    DisableCollectingDocumentsAfterCommit = true;
                    // remove everything after this
                    prefetchingQueue.RemoveAfter(current.Etag);
                }

                return;
            }
            else
            {
                numberOfTimesIngestRateWasTooHigh = 0;
            }

            Etag lowestEtag = null;

            using (prefetchingQueue.EnterWriteLock())
            {
                foreach (var jsonDocument in docs)
                {
                    JsonDocument.EnsureIdInMetadata(jsonDocument);
                    prefetchingQueue.Add(jsonDocument);

                    if (lowestEtag == null || jsonDocument.Etag.CompareTo(lowestEtag) < 0)
                    {
                        lowestEtag = jsonDocument.Etag;
                    }
                }
            }

            if (lowestEtag != null)
            {
                if (lowestInMemoryDocumentAddedAfterCommit == null || lowestEtag.CompareTo(lowestInMemoryDocumentAddedAfterCommit.Etag) < 0)
                {
                    lowestInMemoryDocumentAddedAfterCommit = new DocAddedAfterCommit
                    {
                        Etag = lowestEtag,
                        AddedAt = SystemTime.UtcNow
                    };
                }
            }
        }

        public void CleanupDocuments(Etag lastIndexedEtag)
        {
            if (lastIndexedEtag == null)
                return;

            foreach (var docToRemove in documentsToRemove)
            {
                if (docToRemove.Value == null)
                    continue;
                if (docToRemove.Value.All(etag => lastIndexedEtag.CompareTo(etag) > 0) == false)
                    continue;

                HashSet<Etag> _;
                documentsToRemove.TryRemove(docToRemove.Key, out _);
            }

            HandleCleanupOfUnusedDocumentsInQueue();
        }

        public bool FilterDocuments(JsonDocument document)
        {
            HashSet<Etag> etags;
            return (documentsToRemove.TryGetValue(document.Key, out etags) && etags.Any(x => x.CompareTo(document.Etag) >= 0)) == false;
        }

        public void AfterDelete(string key, Etag deletedEtag)
        {
            if (context.RunIndexing == false && PrefetchingUser == PrefetchingUser.Indexer)
            {
                // no need to collect info about deleted documents for the indexer's prefetcher when the indexing is disabled
                // we use documentsToRemove collection to filter out already deleted documents retrieved by the prefetcher (FilterDocuments)
                // and in order to skip deletions of docs inserted for the first time from an index (ShouldSkipDeleteFromIndex)
                // because those two cases are important for the live indexing process, we can safely omit that when the indexing is off

                return;
            }

            documentsToRemove.AddOrUpdate(key, s => new HashSet<Etag> { deletedEtag },
                                          (s, set) => new HashSet<Etag>(set) { deletedEtag });
        }

        public bool ShouldSkipDeleteFromIndex(JsonDocument item)
        {
            if (item.SkipDeleteFromIndex == false)
                return false;
            return documentsToRemove.ContainsKey(item.Key) == false;
        }

        #region Nested type: FutureIndexBatch

        private class FutureIndexBatch
        {
            public int Age;
            public Etag StartingEtag;
            public CancellationTokenSource CancellationTokenSource;
            public FutureBatchType Type { get; set; }
            public Reference<int?> DocsCount { get; set; }
            public Task<List<JsonDocument>> Task;
        }

        #endregion

        public IDisposable UpdateCurrentlyUsedBatches(List<JsonDocument> docBatch)
        {
            var batchId = Guid.NewGuid();

            autoTuner.CurrentlyUsedBatchSizesInBytes.TryAdd(batchId, docBatch.Sum(x => x.SerializedSizeOnDisk));
            return new DisposableAction(() =>
            {
                long _;
                autoTuner.CurrentlyUsedBatchSizesInBytes.TryRemove(batchId, out _);
            });
        }

        public void UpdateAutoThrottler(List<JsonDocument> jsonDocs, TimeSpan indexingDuration)
        {
            int currentBatchLength = autoTuner.NumberOfItemsToProcessInSingleBatch;
            int futureLen = futureIndexBatches.Values.Sum(x =>
            {
                if (x.Task.IsCompleted)
                {
                    return x.Task.Result.Count;
                }
                return currentBatchLength / 15;
            });

            long futureSize = futureIndexBatches.Values.Sum(x =>
            {
                if (x.Task.IsCompleted)
                {
                    var jsonResults = x.Task.Result;
                    return jsonResults.Sum(s => (long)s.SerializedSizeOnDisk);
                }
                return currentBatchLength * 256;
            });

            autoTuner.AutoThrottleBatchSize(
                jsonDocs.Count + futureLen,
                futureSize + jsonDocs.Sum(x => (long)x.SerializedSizeOnDisk),
                indexingDuration);
        }

        public void OutOfMemoryExceptionHappened()
        {
            autoTuner.HandleOutOfMemory();
        }

        public void HandleLowMemory()
        {
            ClearQueueAndFutureBatches();
        }

        public void SoftMemoryRelease()
        {

        }

        public LowMemoryHandlerStatistics GetStats()
        {
            var futureIndexBatchesSize = futureIndexBatches.Sum(x => x.Value.Task.IsCompleted ? x.Value.Task.Result.Sum(y => y.SerializedSizeOnDisk) : 0);
            var futureIndexBatchesDocCount = futureIndexBatches.Sum(x => x.Value.Task.IsCompleted ? x.Value.Task.Result.Count : 0);
            return new LowMemoryHandlerStatistics
            {
                Name = "PrefetchingBehavior",
                DatabaseName = context.DatabaseName,
                EstimatedUsedMemory = prefetchingQueue.LoadedSize + futureIndexBatchesSize,
                Metadata = new
                {
                    PrefetchingUserType = this.PrefetchingUser,
                    PrefetchingUserDescription = userDescription,
                    PrefetchingQueueDocCount = prefetchingQueue.Count,
                    FutureIndexBatchSizeDocCount = futureIndexBatchesDocCount

                }
            };
        }

        public void ClearQueueAndFutureBatches()
        {
            //cancel any running future batches and prevent the creation of new ones
            foreach (var futureIndexBatch in futureIndexBatches)
            {
                DisposeCancellationToken(futureIndexBatch.Value.CancellationTokenSource);
            }

            futureIndexBatches.Clear();
            prefetchingQueue.Clear();
        }
    }

    public class PrefetchingSummary
    {
        public int PrefetchingQueueLoadedSize { get; set; }
        public int PrefetchingQueueDocsCount { get; set; }
        public long FutureIndexBatchesLoadedSize { get; set; }
        public int FutureIndexBatchesDocsCount { get; set; }
    }

    public class FutureBatchesSummary
    {
        public List<object> Summary { get; set; }
        public int Total { get; set; }
        public int Canceled { get; set; }
        public int Faulted { get; set; }
    }
}

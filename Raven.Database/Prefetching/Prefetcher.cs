// -----------------------------------------------------------------------
//  <copyright file="Prefetcher.cs" company="Hibernating Rhinos LTD">
//      Copyright (c) Hibernating Rhinos LTD. All rights reserved.
//  </copyright>
// -----------------------------------------------------------------------
using System.Collections.Generic;
using Raven.Abstractions.Data;
using Raven.Database.Config;
using Raven.Database.Indexing;

namespace Raven.Database.Prefetching
{
    using System.Linq;

    public class Prefetcher : ILowMemoryHandler
    {
        private readonly WorkContext workContext;
        private List<PrefetchingBehavior> prefetchingBehaviors = new List<PrefetchingBehavior>();

        public Prefetcher(WorkContext workContext)
        {
            this.workContext = workContext;
            MemoryStatistics.RegisterLowMemoryHandler(this);
        }

        public PrefetchingBehavior CreatePrefetchingBehavior(PrefetchingUser user, BaseBatchSizeAutoTuner autoTuner, string prefetchingUserDescription, bool isDefault = false)
        {
            lock (this)
            {
                var newPrefetcher = 
                    new PrefetchingBehavior(user, 
                                            workContext, 
                                            autoTuner ?? new IndependentBatchSizeAutoTuner(workContext, user), 
                                            prefetchingUserDescription, 
                                            isDefault,
                                            GetPrefetchintBehavioursCount,
                                            GetPrefetchingBehaviourSummary,
                                            IsDefatultPrefetchingBehaviourBusy);

                prefetchingBehaviors = new List<PrefetchingBehavior>(prefetchingBehaviors)
                {
                    newPrefetcher
                };

                return newPrefetcher;
            }
        }

        public void RemovePrefetchingBehavior(PrefetchingBehavior prefetchingBehavior)
        {
            lock (this)
            {
                prefetchingBehaviors = new List<PrefetchingBehavior>(prefetchingBehaviors.Except(new[]
                {
                    prefetchingBehavior
                }));

                prefetchingBehavior.Dispose();
            }
        }

        public void AfterDelete(string key, Etag deletedEtag)
        {
            foreach (var behavior in prefetchingBehaviors)
            {
                behavior.AfterDelete(key, deletedEtag);
            }
        }

        public int[] GetInMemoryIndexingQueueSizes(PrefetchingUser user)
        {
            return prefetchingBehaviors.Where(x => x.PrefetchingUser == user).Select(value => value.InMemoryIndexingQueueSize).ToArray();
        }

        public void AfterStorageCommitBeforeWorkNotifications(PrefetchingUser user, JsonDocument[] documents)
        {
            foreach (var prefetcher in prefetchingBehaviors.Where(x => x.PrefetchingUser == user))
            {
                prefetcher.AfterStorageCommitBeforeWorkNotifications(documents);
            }
        }

        private int GetPrefetchintBehavioursCount()
        {
            return prefetchingBehaviors.Count;
        }

        private PrefetchingSummary GetPrefetchingBehaviourSummary()
        {
            var summary = new PrefetchingSummary();

            foreach (var prefetcher in prefetchingBehaviors)
            {
                var prefetchingBehaviorSummary = prefetcher.GetSummary();
                summary.PrefetchingQueueLoadedSize += prefetchingBehaviorSummary.PrefetchingQueueLoadedSize;
                summary.PrefetchingQueueDocsCount += prefetchingBehaviorSummary.PrefetchingQueueDocsCount;
                summary.FutureIndexBatchesLoadedSize += prefetchingBehaviorSummary.FutureIndexBatchesLoadedSize;
                summary.FutureIndexBatchesDocsCount += prefetchingBehaviorSummary.FutureIndexBatchesDocsCount;
            }

            return summary;
        }

        private bool IsDefatultPrefetchingBehaviourBusy()
        {
            var defaultPrefetcher = prefetchingBehaviors.FirstOrDefault(x => x.IsDefault);
            return defaultPrefetcher != null && defaultPrefetcher.IsEmpty() == false;
        }

        public void Dispose()
        {
            foreach (var prefetchingBehavior in prefetchingBehaviors)
            {
                prefetchingBehavior.Dispose();
            }
        }

        public void HandleLowMemory()
        {
            foreach (var prefetchingBehavior in prefetchingBehaviors)
            {
                prefetchingBehavior.ClearQueueAndFutureBatches();
            }
        }

        public void SoftMemoryRelease()
        {
            
        }

        // todo: consider removing ILowMemoryHandler implementation, because the prefetching behaviors already implement it
        public LowMemoryHandlerStatistics GetStats()
        {
            return new LowMemoryHandlerStatistics()
            {
                Name = "Prefetcher",
                DatabaseName = workContext.DatabaseName,
                EstimatedUsedMemory = 0
            };
        }
    }
}

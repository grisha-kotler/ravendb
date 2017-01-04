﻿using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading;
using System.Threading.Tasks;
using Raven.Abstractions;
using Raven.Abstractions.Extensions;
using Raven.Server.Alerts;
using Raven.Server.Commercial;
using Raven.Server.Config;
using Raven.Server.Config.Settings;
using Raven.Server.Documents;
using Raven.Server.ServerWide.Context;
using Raven.Server.ServerWide.LowMemoryNotification;
using Raven.Server.Utils;
using Sparrow.Json;
using Voron;
using Sparrow;
using Sparrow.Collections;
using Sparrow.Logging;
using Voron.Data.Tables;
using Voron.Exceptions;
using Bits = Sparrow.Binary.Bits;

namespace Raven.Server.ServerWide
{
    /// <summary>
    /// Persistent store for server wide configuration, such as cluster settings, database configuration, etc
    /// </summary>
    public unsafe class ServerStore : IDisposable
    {
        private CancellationTokenSource _shutdownNotification;

        public CancellationToken ServerShutdown => _shutdownNotification.Token;

        private static Logger _logger;

        private StorageEnvironment _env;

        private static readonly TableSchema _itemsSchema;

        public readonly DatabasesLandlord DatabasesLandlord;

        private readonly IList<IDisposable> toDispose = new List<IDisposable>();
        private static readonly Slice EtagIndexName;

        public readonly RavenConfiguration Configuration;
        public readonly IoMetrics IoMetrics;
        public readonly AlertsStorage Alerts;
        public static LicenseStorage LicenseStorage { get; } = new LicenseStorage();

        // this is only modified by write transactions under lock
        // no need to use thread safe ops
        private long _lastEtag;
        private readonly ConcurrentSet<AsyncQueue<GlobalAlertNotification>> _changes = new ConcurrentSet<AsyncQueue<GlobalAlertNotification>>();

        private readonly TimeSpan _frequencyToCheckForIdleDatabases;

        static ServerStore()
        {
            

            Slice.From(StorageEnvironment.LabelsContext, "EtagIndexName", out EtagIndexName);

            _itemsSchema = new TableSchema();

            // We use the follow format for the items data
            // { lowered key, key, data, etag }
            _itemsSchema.DefineKey(new TableSchema.SchemaIndexDef
            {
                StartIndex = 0,
                Count = 0
            });

            _itemsSchema.DefineFixedSizeIndex(new TableSchema.FixedSizeSchemaIndexDef
            {
                Name = EtagIndexName,
                IsGlobal = true,
                StartIndex = 3
            });
        }

        public ServerStore(RavenConfiguration configuration)
        {
            if (configuration == null) throw new ArgumentNullException(nameof(configuration));
            IoMetrics = new IoMetrics(8,8); // TODO:: increase this to 256,256 ?
            Configuration = configuration;
            _logger = LoggingSource.Instance.GetLogger<ServerStore>("ServerStore");
            DatabasesLandlord = new DatabasesLandlord(this);

            Alerts = new AlertsStorage("Raven/Server", this);

            DatabaseInfoCache = new DatabaseInfoCache();

            _frequencyToCheckForIdleDatabases = Configuration.Databases.FrequencyToCheckForIdle.AsTimeSpan;

        }

        public DatabaseInfoCache DatabaseInfoCache { get; set; }

        public TransactionContextPool ContextPool;

        private Timer _timer;

        public void Initialize()
        {
            _shutdownNotification = new CancellationTokenSource();

            AbstractLowMemoryNotification.Initialize(ServerShutdown, Configuration);

            if (_logger.IsInfoEnabled)
                _logger.Info("Starting to open server store for " + (Configuration.Core.RunInMemory ? "<memory>" : Configuration.Core.DataDirectory));

            var options = Configuration.Core.RunInMemory
                ? StorageEnvironmentOptions.CreateMemoryOnly(Configuration.Core.DataDirectory)
                : StorageEnvironmentOptions.ForPath(System.IO.Path.Combine(Configuration.Core.DataDirectory,"System"));

            options.SchemaVersion = 2;

            try
            {
                StorageEnvironment.MaxConcurrentFlushes = Configuration.Storage.MaxConcurrentFlushes;
                _env = new StorageEnvironment(options);
                using (var tx = _env.WriteTransaction())
                {
                    tx.DeleteTree("items");// note the different casing, we remove the old items tree 
                    _itemsSchema.Create(tx, "Items", 16);
                    tx.Commit();
                }

                using (var tx = _env.ReadTransaction())
                {
                    var table = tx.OpenTable(_itemsSchema, "Items");
                    var itemsFromBackwards = table.SeekBackwardFrom(_itemsSchema.FixedSizeIndexes[EtagIndexName], long.MaxValue);
                    var reader = itemsFromBackwards.FirstOrDefault();
                    if (reader == null)
                        _lastEtag = 0;
                    else
                    {
                        int size;
                        _lastEtag = Bits.SwapBytes(*(long*) reader.Read(3, out size));
                    }
                }
            }
            catch (Exception e)
            {
                if (_logger.IsOperationsEnabled)
                    _logger.Operations(
                        "Could not open server store for " + (Configuration.Core.RunInMemory ? "<memory>" : Configuration.Core.DataDirectory), e);
                options.Dispose();
                throw;
            }

            ContextPool = new TransactionContextPool(_env);
            _timer = new Timer(IdleOperations, null, _frequencyToCheckForIdleDatabases, TimeSpan.FromDays(7));
            Alerts.Initialize(_env, ContextPool);
            DatabaseInfoCache.Initialize(_env, ContextPool);
            LicenseStorage.Initialize(_env, ContextPool);
        }

        public long ReadLastEtag(TransactionOperationContext ctx)
        {
            var table = ctx.Transaction.InnerTransaction.OpenTable(_itemsSchema, "Items");
            var itemsFromBackwards = table.SeekBackwardFrom(_itemsSchema.FixedSizeIndexes[EtagIndexName], long.MaxValue);
            var reader = itemsFromBackwards.FirstOrDefault();

            if (reader == null)
                return 0;

            int size;
            return Bits.SwapBytes(*(long*)reader.Read(3, out size));
        }

        public BlittableJsonReaderObject Read(TransactionOperationContext ctx, string id, out long etag)
        {
            var items = ctx.Transaction.InnerTransaction.OpenTable(_itemsSchema, "Items");
            etag = 0;
            TableValueReader reader;
            Slice key;
            using (Slice.From(ctx.Allocator, id.ToLowerInvariant(), out key))
            {
                reader = items.ReadByKey(key);
            }
            if (reader == null)
                return null;
            int size;
            etag = Bits.SwapBytes(*(long*) reader.Read(3, out size));
            var ptr = reader.Read(2, out size);
            return new BlittableJsonReaderObject(ptr, size, ctx);
        }


        public BlittableJsonReaderObject Read(TransactionOperationContext ctx, string id)
        {
            var items = ctx.Transaction.InnerTransaction.OpenTable(_itemsSchema, "Items");

            TableValueReader reader;
            Slice key;
            using (Slice.From(ctx.Allocator, id.ToLowerInvariant(), out key))
            {
                reader = items.ReadByKey(key);
            }
            if (reader == null)
                return null;
            int size;
            var ptr = reader.Read(2, out size);
            return new BlittableJsonReaderObject(ptr, size, ctx);
        }

        public Tuple<BlittableJsonReaderObject,long> ReadWithEtag(TransactionOperationContext ctx, string id)
        {
            var items = ctx.Transaction.InnerTransaction.OpenTable(_itemsSchema, "Items");

            TableValueReader reader;
            Slice key;
            using (Slice.From(ctx.Allocator, id.ToLowerInvariant(), out key))
            {
                reader = items.ReadByKey(key);
            }
            if (reader == null)
                return null;
            int size;
            var ptr = reader.Read(2, out size);
            return Tuple.Create(new BlittableJsonReaderObject(ptr, size, ctx),Bits.SwapBytes(*(long*)reader.Read(3,out size)));
        }

        public void Delete(TransactionOperationContext ctx, string id)
        {
            TrackChangeAfterTransactionCommit(ctx, "Delete", id);

            var items = ctx.Transaction.InnerTransaction.OpenTable(_itemsSchema, "Items");
            Slice key;
            using (Slice.From(ctx.Allocator, id.ToLowerInvariant(), out key))
            {
                items.DeleteByKey(key);
                DatabaseInfoCache.DeleteInternal(ctx, key);
            }
        }

        public class Item
        {
            public string Key;
            public BlittableJsonReaderObject Data;
            public long Etag;
        }

        public IEnumerable<Item> StartingWith(TransactionOperationContext ctx, string prefix, int start, int take)
        {
            var items = ctx.Transaction.InnerTransaction.OpenTable(_itemsSchema, "Items");

            Slice loweredPrefix;
            using (Slice.From(ctx.Allocator, prefix.ToLowerInvariant(), out loweredPrefix))
            {
                foreach (var result in items.SeekByPrimaryKey(loweredPrefix, startsWith: true))
                {
                    if (start > 0)
                    {
                        start--;
                        continue;
                    }
                    if (take-- <= 0)
                        yield break;
                    yield return GetCurrentItem(ctx, result);
                }
            }
        }

        private static Item GetCurrentItem(JsonOperationContext ctx, TableValueReader reader)
        {
            int size;
            return new Item
            {
                Data = new BlittableJsonReaderObject(reader.Read(2, out size), size, ctx),
                Key = Encoding.UTF8.GetString(reader.Read(1, out size), size),
                Etag = Bits.SwapBytes(*(long*)reader.Read(3,out size))
            };
        }

        public long Write(TransactionOperationContext ctx, string id, BlittableJsonReaderObject doc, long? expectedEtag = null)
        {
            TrackChangeAfterTransactionCommit(ctx, "Write", id);
            var newEtag = _lastEtag + 1;

            Slice idAsSlice;
            Slice loweredId;
            using (Slice.From(ctx.Allocator, id, out idAsSlice))
            using (Slice.From(ctx.Allocator, id.ToLowerInvariant(), out loweredId))
            {
                var newEtagBigEndian = Bits.SwapBytes(newEtag);
                var itemTable = ctx.Transaction.InnerTransaction.OpenTable(_itemsSchema, "Items");

                var oldValue = itemTable.ReadByKey(loweredId);
                if (oldValue == null)
                {
                    if (expectedEtag != null && expectedEtag != 0)
                    {
                        throw new ConcurrencyException(
                            $"Server store item {id} does not exists, but Write was called with etag {expectedEtag}. Optimistic concurrency violation, transaction will be aborted.")
                        {
                            ExpectedETag = (long) expectedEtag
                        };
                    }
                  
                    itemTable.Insert(new TableValueBuilder
                    {
                        loweredId,
                        idAsSlice,
                        {doc.BasePointer, doc.Size},
                        {&newEtagBigEndian, sizeof(long)}
                    });
                }
                else
                {
                    int size;
                    var test = GetCurrentItem(ctx, oldValue);
                    var oldEtag = Bits.SwapBytes(*(long*)oldValue.Read(3, out size));
                    if (expectedEtag != null && oldEtag != expectedEtag)
                        throw new ConcurrencyException(
                            $"Server store item {id} has etag {oldEtag}, but Write was called with etag {expectedEtag}. Optimistic concurrency violation, transaction will be aborted.")
                        {
                            ActualETag = oldEtag,
                            ExpectedETag = (long)expectedEtag
                        };

                    itemTable.Update(oldValue.Id,new TableValueBuilder
                    {
                        loweredId,
                        idAsSlice,
                        {doc.BasePointer, doc.Size},
                        {&newEtagBigEndian, sizeof(long)}
                    });
                }
            }
            _lastEtag++;
            return newEtag;
        }

        

        public void Dispose()
        {
            _shutdownNotification.Cancel();

            toDispose.Add(DatabasesLandlord);
            toDispose.Add(_env);
            toDispose.Add(ContextPool);

            var exceptionAggregator = new ExceptionAggregator(_logger, $"Could not dispose {nameof(ServerStore)}.");

            foreach (var disposable in toDispose)
                exceptionAggregator.Execute(() => disposable?.Dispose());

            exceptionAggregator.ThrowIfNeeded();
        }

        public void IdleOperations(object state)
        {
            try
            {
                foreach (var db in DatabasesLandlord.ResourcesStoresCache)
                {
                    try
                    {
                        if (db.Value.Status != TaskStatus.RanToCompletion)
                            continue;

                        var database = db.Value.Result;

                        if (DatabaseNeedsToRunIdleOperations(database))                        
                            database.RunIdleOperations();                        
                    }

                    catch (Exception e)
                    {
                        if (_logger.IsInfoEnabled)
                            _logger.Info("Error during idle operation run for " + db.Key, e);
                    }
                }

                try
                {
                    var maxTimeDatabaseCanBeIdle = Configuration.Databases.MaxIdleTime.AsTimeSpan;

                    var databasesToCleanup = DatabasesLandlord.LastRecentlyUsed
                       .Where(x => SystemTime.UtcNow - x.Value > maxTimeDatabaseCanBeIdle)
                       .Select(x => x.Key)
                       .ToArray();

                    foreach (var db in databasesToCleanup)
                    {
                        // intentionally inside the loop, so we get better concurrency overall
                        // since shutting down a database can take a while
                        DatabasesLandlord.UnloadResource(db, skipIfActiveInDuration: maxTimeDatabaseCanBeIdle, shouldSkip: database => database.Configuration.Core.RunInMemory);
                    }

                }
                catch (Exception e)
                {
                    if (_logger.IsInfoEnabled)
                        _logger.Info("Error during idle operations for the server", e);
                }
            }
            finally
            {
                try
                {
                    _timer.Change(_frequencyToCheckForIdleDatabases, TimeSpan.FromDays(7));
                }
                catch (ObjectDisposedException)
                {
                }
            }
        }

        private static bool DatabaseNeedsToRunIdleOperations(DocumentDatabase database) 
        {
            var now = DateTime.UtcNow;

            var envs = database.GetAllStoragesEnvironment();

            var maxLastWork = DateTime.MinValue;

            foreach (var env in envs)
            {
                if (env.Environment.LastWorkTime > maxLastWork)
                    maxLastWork = env.Environment.LastWorkTime;
            }

            return ((now - maxLastWork).TotalMinutes > 5) || ((now - database.LastIdleTime).TotalMinutes > 10);
        }

        public IDisposable TrackChanges(AsyncQueue<GlobalAlertNotification> asyncQueue)
        {
            _changes.TryAdd(asyncQueue);
            return new DisposableAction(() => _changes.TryRemove(asyncQueue));
        }

        public void TrackChange(string operation, string id)
        {
            if (_changes.Count == 0)
                return;

            var djv = new GlobalAlertNotification
            {
                Operation = operation,
                Id = id
            };
            foreach (var asyncQueue in _changes)
            {
                asyncQueue.Enqueue(djv);
            }
        }

        public void TrackChangeAfterTransactionCommit(TransactionOperationContext ctx, string operation, string id)
        {
            if (_changes.Count == 0)
                return;

            var llt = ctx.Transaction.InnerTransaction.LowLevelTransaction;
          

            // need to do this after the transaction is over
            llt.OnDispose += _ =>
            {
                if (llt.Committed == false)
                    return;
                var djv = new GlobalAlertNotification
                {
                    Operation = operation,
                    Id = id
                };
                foreach (var asyncQueue in _changes)
                {
                    asyncQueue.Enqueue(djv);
                }
            };
        }

    }
}
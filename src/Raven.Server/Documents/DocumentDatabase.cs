﻿using System;
using System.Collections.Generic;
using System.Linq;
using System.Threading;
using System.Threading.Tasks;
using Raven.Abstractions;
using Raven.Client.Data;
using Raven.Server.Alerts;
using Raven.Server.Config;
using Raven.Server.Documents.Indexes;
using Raven.Server.Documents.Patch;
using Raven.Server.Documents.Replication;
using Raven.Server.Documents.SqlReplication;
using Raven.Server.Documents.TcpHandlers;
using Raven.Server.Documents.Transformers;
using Raven.Server.ServerWide;
using Raven.Server.Utils;
using Sparrow;
using Sparrow.Collections;
using Sparrow.Json.Parsing;
using Sparrow.Logging;
using Voron;
using Voron.Impl.Backup;

namespace Raven.Server.Documents
{
    public class DocumentDatabase : IResourceStore
    {
        private readonly Logger _logger;

        private readonly CancellationTokenSource _databaseShutdown = new CancellationTokenSource();

        private readonly object _idleLocker = new object();
        private Task _indexStoreTask;
        private Task _transformerStoreTask;
        private long _usages;
        private readonly ManualResetEventSlim _waitForUsagesOnDisposal = new ManualResetEventSlim(false);
        private long _lastIdleTicks = DateTime.UtcNow.Ticks;

        public void ResetIdleTime()
        {
            _lastIdleTicks = DateTime.MinValue.Ticks;
        }

        public DocumentDatabase(string name, RavenConfiguration configuration, ServerStore serverStore)
        {
            StartTime = SystemTime.UtcNow;
            Name = name;
            ResourceName = "db/" + name;
            Configuration = configuration;
            _logger = LoggingSource.Instance.GetLogger<DocumentDatabase>(Name);
            Notifications = new DocumentsNotifications();
            DocumentsStorage = new DocumentsStorage(this);
            IndexStore = new IndexStore(this);
            TransformerStore = new TransformerStore(this);
            SqlReplicationLoader = new SqlReplicationLoader(this);
            DocumentReplicationLoader = new DocumentReplicationLoader(this);
            DocumentTombstoneCleaner = new DocumentTombstoneCleaner(this);
            SubscriptionStorage = new SubscriptionStorage(this);
            Operations = new DatabaseOperations(this);
            Metrics = new MetricsCountersManager();
            IoMetrics = serverStore?.IoMetrics ?? new IoMetrics(256, 256);
            Patch = new PatchDocument(this);
            TxMerger = new TransactionOperationsMerger(this, DatabaseShutdown);
            HugeDocuments = new HugeDocuments(configuration.Databases.MaxCollectionSizeHugeDocuments,
                configuration.Databases.MaxWarnSizeHugeDocuments);
            ConfigurationStorage = new ConfigurationStorage(this, serverStore);
            DatabaseInfoCache = serverStore?.DatabaseInfoCache;
        }

        public DateTime LastIdleTime => new DateTime(_lastIdleTicks);

        public DatabaseInfoCache DatabaseInfoCache { get; set; }

        public SystemTime Time = new SystemTime();

        public readonly PatchDocument Patch;

        public TransactionOperationsMerger TxMerger;

        public SubscriptionStorage SubscriptionStorage { get; }

        public string Name { get; }

        public Guid DbId => DocumentsStorage.Environment?.DbId ?? Guid.Empty;

        public string ResourceName { get; }

        public RavenConfiguration Configuration { get; }

        public CancellationToken DatabaseShutdown => _databaseShutdown.Token;

        public DocumentsStorage DocumentsStorage { get; private set; }

        public BundleLoader BundleLoader { get; private set; }

        public DocumentTombstoneCleaner DocumentTombstoneCleaner { get; private set; }

        public DocumentsNotifications Notifications { get; }

        public DatabaseOperations Operations { get; private set; }

        public HugeDocuments HugeDocuments { get; }

        public MetricsCountersManager Metrics { get; }

        public IoMetrics IoMetrics { get; }

        public IndexStore IndexStore { get; private set; }

        public TransformerStore TransformerStore { get; }

        public ConfigurationStorage ConfigurationStorage { get; private set; }

        public IndexesEtagsStorage IndexMetadataPersistence => ConfigurationStorage.IndexesEtagsStorage;

        public AlertsStorage Alerts => ConfigurationStorage.AlertsStorage;

        public SqlReplicationLoader SqlReplicationLoader { get; private set; }

        public DocumentReplicationLoader DocumentReplicationLoader { get; private set; }

        public ConcurrentSet<TcpConnectionOptions> RunningTcpConnections = new ConcurrentSet<TcpConnectionOptions>();

        public DateTime StartTime { get; }

        public void Initialize()
        {
            DocumentsStorage.Initialize();
            InitializeInternal();
        }

        public void Initialize(StorageEnvironmentOptions options)
        {
            DocumentsStorage.Initialize(options);
            InitializeInternal();
        }

        public DatabaseUsage DatabaseInUse()
        {
            return new DatabaseUsage(this);
        }

        public struct DatabaseUsage : IDisposable
        {
            private readonly DocumentDatabase _parent;

            public DatabaseUsage(DocumentDatabase parent)
            {
                _parent = parent;
                Interlocked.Increment(ref _parent._usages);
                if (_parent._databaseShutdown.IsCancellationRequested)
                {
                    Dispose();
                    ThrowOperationCancelled();
                }
            }

            private void ThrowOperationCancelled()
            {
                throw new OperationCanceledException("The database " + _parent.Name + " is shutting down");
            }


            public void Dispose()
            {
                Interlocked.Decrement(ref _parent._usages);
                if (_parent._databaseShutdown.IsCancellationRequested)
                    _parent._waitForUsagesOnDisposal.Set();

            }
        }

        private void InitializeInternal()
        {
            TxMerger.Start();
            _indexStoreTask = IndexStore.InitializeAsync();
            _transformerStoreTask = TransformerStore.InitializeAsync();
            SqlReplicationLoader.Initialize();

            DocumentTombstoneCleaner.Initialize();
            BundleLoader = new BundleLoader(this);

            try
            {
                _indexStoreTask.Wait(DatabaseShutdown);
            }
            finally
            {
                _indexStoreTask = null;
            }

            try
            {
                _transformerStoreTask.Wait(DatabaseShutdown);
            }
            finally
            {
                _transformerStoreTask = null;
            }

            SubscriptionStorage.Initialize();

            //Index Metadata Store shares Voron env and context pool with documents storage, 
            //so replication of both documents and indexes/transformers can be made within one transaction
            ConfigurationStorage.Initialize(IndexStore, TransformerStore);
            DocumentReplicationLoader.Initialize();
        }

        public void Dispose()
        {
            //before we dispose of the database we take its latest info to be displayed in the studio
            var databaseInfo = GenerateDatabaseInfo();
            DatabaseInfoCache?.InsertDatabaseInfo(databaseInfo, Name);

            _databaseShutdown.Cancel();
            // we'll wait for 1 minute to drain all the requests
            // from the database
            for (int i = 0; i < 60; i++)
            {
                if (Interlocked.Read(ref _usages) == 0)
                    break;
                _waitForUsagesOnDisposal.Wait(100);
            }
            var exceptionAggregator = new ExceptionAggregator(_logger, $"Could not dispose {nameof(DocumentDatabase)}");

            foreach (var connection in RunningTcpConnections)
            {
                exceptionAggregator.Execute(() =>
                {
                    connection.Dispose();
                });
            }

            exceptionAggregator.Execute(() =>
            {
                TxMerger.Dispose();
            });

            exceptionAggregator.Execute(() =>
            {
                DocumentReplicationLoader.Dispose();
            });

            if (_indexStoreTask != null)
            {
                exceptionAggregator.Execute(() =>
                {
                    _indexStoreTask.Wait(DatabaseShutdown);
                    _indexStoreTask = null;
                });
            }

            if (_transformerStoreTask != null)
            {
                exceptionAggregator.Execute(() =>
                {
                    _transformerStoreTask.Wait(DatabaseShutdown);
                    _transformerStoreTask = null;
                });
            }

            exceptionAggregator.Execute(() =>
            {
                IndexStore?.Dispose();
                IndexStore = null;
            });

            exceptionAggregator.Execute(() =>
            {
                BundleLoader?.Dispose();
                BundleLoader = null;
            });

            exceptionAggregator.Execute(() =>
            {
                DocumentTombstoneCleaner?.Dispose();
                DocumentTombstoneCleaner = null;
            });

            exceptionAggregator.Execute(() =>
            {
                DocumentReplicationLoader?.Dispose();
                DocumentReplicationLoader = null;
            });

            exceptionAggregator.Execute(() =>
            {
                SqlReplicationLoader?.Dispose();
                SqlReplicationLoader = null;
            });

            exceptionAggregator.Execute(() =>
            {
                Operations?.Dispose(exceptionAggregator);
                Operations = null;
            });

            exceptionAggregator.Execute(() =>
            {
                SubscriptionStorage?.Dispose();
            });

            exceptionAggregator.Execute(() =>
            {
                ConfigurationStorage?.Dispose();
            });

            exceptionAggregator.Execute(() =>
            {
                DocumentsStorage?.Dispose();
                DocumentsStorage = null;
            });

            exceptionAggregator.ThrowIfNeeded();
        }

        private static readonly string CachedDatabaseInfo = "CachedDatabaseInfo";
        public DynamicJsonValue GenerateDatabaseInfo()
        {
            Size size = new Size(GetAllStoragesEnvironment().Sum(env => env.Environment.Stats().AllocatedDataFileSizeInBytes));
            var databaseInfo = new DynamicJsonValue
            {
                [nameof(ResourceInfo.Bundles)] = new DynamicJsonArray(BundleLoader.GetActiveBundles()),
                [nameof(ResourceInfo.IsAdmin)] = true, //TODO: implement me!
                [nameof(ResourceInfo.Name)] = Name,
                [nameof(ResourceInfo.Disabled)] = false, //TODO: this value should be overwritten by the studio since it is cached
                [nameof(ResourceInfo.TotalSize)] = new DynamicJsonValue
                {
                    [nameof(Size.HumaneSize)] = size.HumaneSize,
                    [nameof(Size.SizeInBytes)] = size.SizeInBytes
                },
                [nameof(ResourceInfo.Errors)] = IndexStore.GetIndexes().Sum(index => index.GetErrors().Count),
                [nameof(ResourceInfo.Alerts)] = Alerts.GetAlertCount(),
                [nameof(ResourceInfo.UpTime)] = null, //it is shutting down
                [nameof(ResourceInfo.BackupInfo)] = BundleLoader.GetBackupInfo(),
                [nameof(DatabaseInfo.DocumentsCount)] = DocumentsStorage.GetNumberOfDocuments(),
                [nameof(DatabaseInfo.IndexesCount)] = IndexStore.GetIndexes().Count(),
                [nameof(DatabaseInfo.RejectClients)] = false, //TODO: implement me!
                [nameof(DatabaseInfo.IndexingStatus)] = IndexStore.Status.ToString(),
                [CachedDatabaseInfo] = true
            };
            return databaseInfo;
        }

        public void RunIdleOperations()
        {
            if (Monitor.TryEnter(_idleLocker) == false)
                return;

            try
            {
                _lastIdleTicks = DateTime.UtcNow.Ticks;
                IndexStore?.RunIdleOperations();
                Operations?.CleanupOperations();
            }

            finally
            {
                Monitor.Exit(_idleLocker);
            }
        }

        public IEnumerable<StorageEnvironmentWithType> GetAllStoragesEnvironment()
        {
            // TODO :: more storage environments ?
            yield return new StorageEnvironmentWithType(Name, StorageEnvironmentWithType.StorageEnvironmentType.Documents, DocumentsStorage.Environment);
            yield return new StorageEnvironmentWithType("Subscriptions", StorageEnvironmentWithType.StorageEnvironmentType.Subscriptions, SubscriptionStorage.Environment());
            yield return new StorageEnvironmentWithType("Configuration", StorageEnvironmentWithType.StorageEnvironmentType.Configuration, ConfigurationStorage.Environment);
            foreach (var index in IndexStore.GetIndexes())
            {
                var env = index._indexStorage?.Environment();
                if (env != null)
                    yield return new StorageEnvironmentWithType(index.Name, StorageEnvironmentWithType.StorageEnvironmentType.Index, env);
            }
        }

        private IEnumerable<FullBackup.StorageEnvironmentInformation> GetAllStoragesEnvironmentInformation()
        {
            yield return (new FullBackup.StorageEnvironmentInformation()
            {
                Name = "",
                Folder = "Subscriptions",
                Env = SubscriptionStorage.Environment()
            });
            var i = 1;
            foreach (var index in IndexStore.GetIndexes())
            {
                var env = index._indexStorage.Environment();
                if (env != null)
                    yield return (new FullBackup.StorageEnvironmentInformation()
                    {
                        Name = i++.ToString(),
                        Folder = "Indexes",
                        Env = env
                    });
            }
            yield return (new FullBackup.StorageEnvironmentInformation()
            {
                Name = "",
                Folder = "",
                Env = DocumentsStorage.Environment
            });
        }

        public void FullBackupTo(string backupPath)
        {
            BackupMethods.Full.ToFile(GetAllStoragesEnvironmentInformation(), backupPath);
        }

        public void IncrementalBackupTo(string backupPath)
        {
            BackupMethods.Incremental.ToFile(GetAllStoragesEnvironmentInformation(), backupPath);
        }
    }

    public class StorageEnvironmentWithType
    {
        public string Name { get; set; }
        public StorageEnvironmentType Type { get; set; }
        public StorageEnvironment Environment { get; set; }

        public StorageEnvironmentWithType(string name, StorageEnvironmentType type, StorageEnvironment environment)
        {
            Name = name;
            Type = type;
            Environment = environment;
        }

        public enum StorageEnvironmentType
        {
            Documents,
            Subscriptions,
            Index,
            Configuration
        }
    }
}
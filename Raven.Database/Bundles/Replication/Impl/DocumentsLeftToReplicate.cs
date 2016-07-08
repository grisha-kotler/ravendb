using System;
using System.Collections.Generic;
using System.Linq;
using System.Threading;
using Raven.Abstractions.Connection;
using Raven.Abstractions.Data;
using Raven.Abstractions.Extensions;
using Raven.Abstractions.Replication;
using Raven.Abstractions.Util;
using Raven.Bundles.Replication.Tasks;
using Raven.Database.Actions;
using Raven.Database.Config.Retriever;
using Raven.Database.Indexing;
using Raven.Json.Linq;

namespace Raven.Database.Bundles.Replication.Impl
{
    internal class DocumentsLeftToReplicate
    {
        private readonly DocumentDatabase database;

        private readonly string databaseId;

        private readonly HttpRavenRequestFactory requestFactory;
        
        private const int MaxDocumentsToCheck = 1000;

        public DocumentsLeftToReplicate(DocumentDatabase database)
        {
            this.database = database;
            this.databaseId = database.TransactionalStorage.Id.ToString();
            requestFactory = new HttpRavenRequestFactory();
        }

        public DocumentCount Calculate(ServerInfo serverInfo)
        {
            var replicationTask = database.StartupTasks.OfType<ReplicationTask>().FirstOrDefault();
            if (replicationTask == null)
            {
                throw new InvalidOperationException("Couldn't locate ReplicationTask");
            }

            ConfigurationDocument<ReplicationDocument<ReplicationDestination.ReplicationDestinationWithConfigurationOrigin>> configurationDocument = null;
            try
            {
                configurationDocument = database.ConfigurationRetriever.GetConfigurationDocument<ReplicationDocument<ReplicationDestination.ReplicationDestinationWithConfigurationOrigin>>(Constants.RavenReplicationDestinations);
            }
            catch (Exception e)
            {
                throw new InvalidOperationException("Couldn't find Raven/Replication/Destinations document", e);
            }

            if (configurationDocument == null)
            {
                throw new InvalidOperationException("Couldn't find Raven/Replication/Destinations document");
            }

            var replicationDocument = configurationDocument.MergedDocument;

            if (serverInfo.SourceId != databaseId)
            {
                return GetDocumentsLeftCountFromAnotherSourceServer(replicationDocument, serverInfo);
            }

            var replicationDestination = replicationDocument
                .Destinations
                .FirstOrDefault(x => FetchTargetServerUrl(x).Equals(serverInfo.DestinationUrl, StringComparison.CurrentCultureIgnoreCase) &&
                            x.Database.Equals(serverInfo.DatabaseName, StringComparison.CurrentCultureIgnoreCase));

            if (replicationDestination == null)
            {
                throw new InvalidOperationException($"Couldn't find replication destination for url: {serverInfo.DestinationUrl} and database: {serverInfo.DatabaseName}");
            }

            var replicationStrategy = ReplicationTask.GetConnectionOptions(replicationDestination, database);
            if (replicationStrategy.SpecifiedCollections == null || replicationStrategy.SpecifiedCollections.Count == 0)
            {
                return GetDocumentsLeftCount(replicationTask, replicationStrategy, serverInfo.DestinationUrl, serverInfo.DatabaseName);
            }

            var entityNames = replicationStrategy.SpecifiedCollections.Keys.ToHashSet();
            return GetDocumentsLeftCountForEtl(entityNames, replicationStrategy, replicationTask, serverInfo.DestinationUrl, serverInfo.DatabaseName);
        }

        private string FetchTargetServerUrl(ReplicationDestination replicationDestination)
        {
            var url = $"{replicationDestination.Url}/debug/config";

            try
            {
                var replicationStrategy = ReplicationTask.GetConnectionOptions(replicationDestination, database);
                var request = requestFactory.Create(url, HttpMethods.Get, replicationStrategy.ConnectionStringOptions);
                var ravenConfig = request.ExecuteRequest<RavenJObject>();
                var serverUrlFromTargetConfig = ravenConfig.Value<string>("ServerUrl");

                // replace host name with target hostname
                return new UriBuilder(replicationDestination.Url) { Host = new Uri(serverUrlFromTargetConfig).Host }.Uri.ToString();
            }
            catch (Exception e)
            {
                if (replicationDestination.Url.EndsWith("/"))
                    return replicationDestination.Url;

                return replicationDestination.Url + "/";
            }
        }

        private DocumentCount GetDocumentsLeftCountFromAnotherSourceServer(
            ReplicationDocument<ReplicationDestination.ReplicationDestinationWithConfigurationOrigin> replicationDocument, 
            ServerInfo serverInfo)
        {
            var replicationDestination = replicationDocument
                .Destinations
                .FirstOrDefault(x => FetchTargetServerUrl(x).Equals(serverInfo.SourceUrl, StringComparison.CurrentCultureIgnoreCase) &&
                            x.Database.Equals(serverInfo.DatabaseName, StringComparison.CurrentCultureIgnoreCase));

            if (replicationDestination != null)
            {
                return DocumentsLeftCountFromAnotherSourceServer(serverInfo, replicationDestination);
            }

            if (serverInfo.SourcesToIgnore.Contains(databaseId) == false)
            {
                serverInfo.SourcesToIgnore.Add(databaseId);
                //couldn't find replication destination for this url,
                //going to try to do it through other destinations
                foreach (var destination in replicationDocument.Destinations)
                {
                    try
                    {
                        return DocumentsLeftCountFromAnotherSourceServer(serverInfo, destination);
                    }
                    catch (Exception e)
                    {
                        //couldn't reach this destination through this one, will continue
                    }
                }
            }
            
            throw new InvalidOperationException($"Couldn't find replication destination for url: {serverInfo.SourceUrl} and database: {serverInfo.DatabaseName}");
        }

        private DocumentCount DocumentsLeftCountFromAnotherSourceServer(ServerInfo serverInfo, 
            ReplicationDestination.ReplicationDestinationWithConfigurationOrigin replicationDestination)
        {
            var replicationStrategy = ReplicationTask.GetConnectionOptions(replicationDestination, database);
            var url = $"{replicationStrategy.ConnectionStringOptions.Url}/admin/replication/docs-left-to-replicate";
            var request = requestFactory.Create(url, HttpMethods.Post, replicationStrategy.ConnectionStringOptions);
            request.Write(RavenJObject.FromObject(serverInfo));

            var documentsLeftCount = request.ExecuteRequest<DocumentCount>();
            return documentsLeftCount;
        }

        private DocumentCount GetDocumentsLeftCount(ReplicationTask replicationTask, 
            ReplicationStrategy replicationStrategy, string destinationUrl, string databaseName)
        {
            //first check the stats
            var localDocumentCount = database.Statistics.CountOfDocuments;
            var url = $"{replicationStrategy.ConnectionStringOptions.Url}/stats";
            var request = requestFactory.Create(url, HttpMethods.Get, replicationStrategy.ConnectionStringOptions);
            var remoteDocumentCount = request.ExecuteRequest<DatabaseStatistics>().CountOfDocuments;
            var difference = localDocumentCount - remoteDocumentCount;
            if (difference > MaxDocumentsToCheck)
            {
                return new DocumentCount
                {
                    Count = difference,
                    Type = CountType.Approximate,
                    IsEtl = false
                };
            }

            var sourcesDocument = replicationTask.GetLastReplicatedEtagFrom(replicationStrategy);
            if (sourcesDocument == null)
            {
                throw new InvalidOperationException($"Couldn't get last replicated etag for destination url: {destinationUrl} and database: {databaseName}");
            }

            long count = 0;
            var earlyExit = new Reference<bool>();
            database.TransactionalStorage.Batch(actions =>
            {
                //get document count since last replicated etag
                count = actions.Documents.GetDocumentsCountAfterEtag(
                    sourcesDocument.LastDocumentEtag, 
                    database.WorkContext.CancellationToken,
                    earlyExit, MaxDocumentsToCheck);
            });

            //there might be some system documents or documents from other destinations that shouldn't be replicated
            if (count < ReplicationTask.SystemDocsLimitForRemoteEtagUpdate ||
                count < ReplicationTask.DestinationDocsLimitForRemoteEtagUpdate)
            {
                database.TransactionalStorage.Batch(actions =>
                {
                    var take = Math.Max(ReplicationTask.SystemDocsLimitForRemoteEtagUpdate,
                        ReplicationTask.DestinationDocsLimitForRemoteEtagUpdate);
                    var docs = actions.Documents.GetDocumentsAfter(
                        sourcesDocument.LastDocumentEtag,
                        take, database.WorkContext.CancellationToken, earlyExit: earlyExit);

                    var newCount = docs.Count(doc => replicationStrategy.IsSystemDocumentId(doc.Key) == false &&
                                                     replicationStrategy.OriginatedAtOtherDestinations(databaseId, doc.Metadata) == false);

                    count = newCount;
                });
            }

            return new DocumentCount
            {
                Count = count,
                Type = earlyExit.Value == false ? CountType.Accurate : CountType.Approximate,
                IsEtl = false
            };
        }

        private DocumentCount GetDocumentsLeftCountForEtl(
            HashSet<string> entityNames, ReplicationStrategy replicationStrategy, 
            ReplicationTask replicationTask, string destinationUrl, string databaseName)
        {
            var query = QueryBuilder.GetQueryForAllMatchingDocumentsForIndex(database, entityNames);

            //get count of documents with specified collections on this server
            var localDocumentCount = GetDocumentCountForEntityNames(query);

            //get count of documents with specified collections on destination server
            var url = $"{replicationStrategy.ConnectionStringOptions.Url}/admin/replication/replicated-docs-by-entity-names";
            var request = requestFactory.Create(url, HttpMethods.Post, replicationStrategy.ConnectionStringOptions);
            request.Write(query);
            var remoteDocumentCount = request.ExecuteRequest<long>();

            var countFromIndex = localDocumentCount - remoteDocumentCount;
            //Raven/DocumentsByEntityName might not be up to date on both servers
            //we are going to try to get a more accurate number

            var lastReplicatedEtag = replicationTask.GetLastReplicatedEtagFrom(replicationStrategy);
            if (lastReplicatedEtag == null)
            {
                throw new InvalidOperationException($"Couldn't get last replicated etag for destination url: {destinationUrl} and database: {databaseName}");
            }

            long storageCount = 0;
            var earlyExit = new Reference<bool>();
            database.TransactionalStorage.Batch(actions =>
            {
                //get document count since last replicated etag
                storageCount = actions.Documents.GetDocumentsCountAfterEtag(
                    lastReplicatedEtag.LastDocumentEtag,
                    database.WorkContext.CancellationToken,
                    earlyExit, MaxDocumentsToCheck, entityNames);
            });

            return new DocumentCount
            {
                Count = earlyExit.Value == false ? storageCount : Math.Max(storageCount, countFromIndex),
                Type = earlyExit.Value == false ? CountType.Accurate : CountType.Approximate,
                IsEtl = true,
            };
        }

        public long GetDocumentCountForEntityNames(string query)
        {
            long localDocumentCount = 0;

            database.TransactionalStorage.Batch(actions =>
            {
                using (var internalCts = new CancellationTokenSource())
                using (var linked = CancellationTokenSource.CreateLinkedTokenSource(database.WorkContext.CancellationToken, internalCts.Token))
                using (var op = new QueryActions.DatabaseQueryOperation(database, Constants.DocumentsByEntityNameIndex, new IndexQuery
                {
                    Query = query,
                    PageSize = int.MaxValue
                }, actions, linked)
                {
                    ShouldSkipDuplicateChecking = true
                })
                {
                    op.Init();
                    localDocumentCount = op.Header.TotalResults;
                }
            });

            return localDocumentCount;
        }
    }

    public class DocumentCount
    {
        public DocumentCount()
        {
            IsEtl = false;
        }

        public long Count { get; set; }

        public CountType Type { get; set; }

        public bool IsEtl { get; set; }
    }

    public enum CountType
    {
        Accurate,
        Approximate
    }
}

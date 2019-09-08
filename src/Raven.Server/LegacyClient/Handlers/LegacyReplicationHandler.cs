// -----------------------------------------------------------------------
//  <copyright file="LegacyReplicationHandler.cs" company="Hibernating Rhinos LTD">
//      Copyright (c) Hibernating Rhinos LTD. All rights reserved.
//  </copyright>
// -----------------------------------------------------------------------

using System;
using System.Collections.Generic;
using System.Linq;
using System.Threading.Tasks;
using Raven.Server.Documents;
using Raven.Server.Routing;
using Raven.Server.ServerWide.Context;
using Sparrow.Json;
using Sparrow.Json.Parsing;

namespace Raven.Server.LegacyClient.Handlers
{
    public class LegacyReplicationHandler : DatabaseRequestHandler
    {
        [RavenAction("/legacy/replication/topology", "GET", AuthorizationStatus.ValidUser)]
        public Task ReplicationTopology()
        {
            using (ContextPool.AllocateOperationContext(out DocumentsOperationContext context))
            using (var writer = new BlittableJsonTextWriter(context, ResponseBodyStream()))
            {
                context.Write(writer, new LegacyReplicationTopologyResult().ToJson());
            }

            return Task.CompletedTask;
        }

        [RavenAction("/legacy/databases/*/replication/topology", "GET", AuthorizationStatus.ValidUser)]
        public Task DatabaseReplicationTopology()
        {
            using (ServerStore.ContextPool.AllocateOperationContext(out TransactionOperationContext serverContext))
            using (serverContext.OpenReadTransaction())
            using (var rawRecord = ServerStore.Cluster.ReadRawDatabaseRecord(serverContext, Database.Name, out _))
            using (ContextPool.AllocateOperationContext(out DocumentsOperationContext context))
            using (context.OpenReadTransaction())
            {
                if (rawRecord == null)
                    throw new InvalidOperationException($"Couldn't find the database record for database: {Database.Name}");

                var result = new LegacyReplicationTopologyResult
                {
                    Source = ServerStore.GetServerId()
                };

                var clusterTopology = ServerStore.GetClusterTopology(serverContext);
                var databaseTopology = rawRecord.GetTopology();
                var disabled = rawRecord.IsDisabled();

                foreach (var nodeTag in databaseTopology.Members)
                {
                    AddToDestinations(nodeTag);
                }
                
                foreach (var nodeTag in databaseTopology.Rehabs)
                {
                    AddToDestinations(nodeTag);
                }

                void AddToDestinations(string nodeTag)
                {
                    result.Destinations.Add(new LegacyDestination
                    {
                        Database = Database.Name,
                        Url = clusterTopology.GetUrlFromTag(nodeTag),
                        Disabled = disabled
                    });
                }

                using (var writer = new BlittableJsonTextWriter(context, ResponseBodyStream()))
                {
                    context.Write(writer, result.ToJson());
                }

                return Task.CompletedTask;
            }
        }

        private class LegacyReplicationTopologyResult : IDynamicJson
        {
            public LegacyReplicationTopologyResult()
            {
                ClusterInformation = new LegacyClusterInformation();
                Destinations = new List<LegacyDestination>();
                Id = "Raven/Replication/Destinations";
            }

            public LegacyClusterInformation ClusterInformation { get; set; }

            public long Term { get; set; }

            public long ClusterCommitIndex { get; set; }

            public bool HasLeader { get; set; }

            public List<LegacyDestination> Destinations { get; }

            public string Id { get; set; }

            public Guid Source { get; set; }

            public string ClientConfiguration { get; set; }

            public DynamicJsonValue ToJson()
            {
                return new DynamicJsonValue
                {
                    [nameof(ClusterInformation)] = ClusterInformation.ToJson(),
                    [nameof(Term)] = -1,
                    [nameof(ClusterCommitIndex)] = false,
                    [nameof(HasLeader)] = false,
                    [nameof(Destinations)] = new DynamicJsonArray(Destinations.Select(x => x.ToJson())),
                    [nameof(Id)] = Id,
                    [nameof(Source)] = Source.ToString(),
                    [nameof(ClientConfiguration)] = ClientConfiguration
                };
            }
        }

        private class LegacyDestination
        {
            public string Id { get; set; }

            public LegacyClusterInformation ClusterInformation { get; set; } = new LegacyClusterInformation();

            public string Url { get; set; }

            public string Username { get; set; }

            public string Password { get; set; }

            public string Domain { get; set; }

            public string ApiKey { get; set; }

            public string Database { get; set; }

            public string TransitiveReplicationBehavior { get; set; }

            public bool SkipIndexReplication { get; set; }

            public bool IgnoredClient { get; set; }

            public bool Disabled { get; set; }

            public string AuthenticationScheme { get; set; }

            public string ClientVisibleUrl { get; set; }

            public Dictionary<string, string> SpecifiedCollections { get; set; }

            public bool ReplicateAttachmentsInEtl { get; set; }

            public string Humane => $"{Url} {Database}";

            public DynamicJsonValue ToJson()
            {
                return new DynamicJsonValue
                {
                    [nameof(ClusterInformation)] = ClusterInformation.ToJson(),
                    [nameof(Url)] = Url,
                    [nameof(Username)] = null,
                    [nameof(Password)] = null,
                    [nameof(Domain)] = null,
                    [nameof(ApiKey)] = null,
                    [nameof(Database)] = Database,
                    [nameof(TransitiveReplicationBehavior)] = "Replicate",
                    [nameof(SkipIndexReplication)] = false,
                    [nameof(IgnoredClient)] = false,
                    [nameof(Disabled)] = Disabled,
                    [nameof(AuthenticationScheme)] = null,
                    [nameof(ClientVisibleUrl)] = null,
                    [nameof(SpecifiedCollections)] = null,
                    [nameof(ReplicateAttachmentsInEtl)] = false,
                    [nameof(Humane)] = Humane,
                };
            }
        }

        private class LegacyClusterInformation
        {
            public bool IsInCluster { get; set; }

            public bool IsLeader { get; set; }

            public bool WithClusterFailoverHeader { get; set; }

            public DynamicJsonValue ToJson()
            {
                return new DynamicJsonValue
                {
                    [nameof(IsInCluster)] = IsInCluster,
                    [nameof(IsLeader)] = IsLeader,
                    [nameof(WithClusterFailoverHeader)] = WithClusterFailoverHeader
                };
            }
        }
    }
}

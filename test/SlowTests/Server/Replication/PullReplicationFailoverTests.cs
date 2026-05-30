using System;
using System.IO;
using System.Linq;
using System.Security.Cryptography.X509Certificates;
using System.Text;
using System.Threading.Tasks;
using Raven.Client.Documents;
using Raven.Client.Documents.Conventions;
using Raven.Client.Documents.Operations.Replication;
using Raven.Client.Documents.Operations.Revisions;
using Raven.Server.Documents.Replication;
using Raven.Server.ServerWide;
using Raven.Server.ServerWide.Context;
using Raven.Server.Utils;
using SlowTests.Core.Utils.Entities;
using Tests.Infrastructure;
using Xunit;

namespace SlowTests.Server.Replication;

public class PullReplicationFailoverTests : ReplicationTestBase
{
    public PullReplicationFailoverTests(ITestOutputHelper output) : base(output)
    {
    }

    [RavenFact(RavenTestCategory.Replication)]
    public async Task SinkToHub_HubShouldNotReceiveDuplicateDocumentsAfterHubNodeFailover()
    {
        DebuggerAttachedTimeout.DisableLongTimespan = true;

        var (hubNodes, hub, certs) = await CreateRaftClusterWithSsl(3);
        using (var hubStore = GetDocumentStore(new Options
        {
            Server = hub,
            ReplicationFactor = 3,
            ClientCertificate = certs.ServerCertificateForCommunication.Value,
            AdminCertificate = certs.ServerCertificateForCommunication.Value
        }))
        using (var sinkStore = GetDocumentStore(new Options
        {
            AdminCertificate = certs.ServerCertificateForCommunication.Value,
            ClientCertificate = certs.ServerCertificateForCommunication.Value
        }))
        {
#pragma warning disable SYSLIB0057
            var pullCert = new X509Certificate2(
                await File.ReadAllBytesAsync(certs.ClientCertificate2Path), (string)null,
                X509KeyStorageFlags.Exportable);
#pragma warning restore SYSLIB0057

            var name = $"pull-replication {GetDatabaseName()}";

            await hubStore.Maintenance.ForDatabase(hubStore.Database).SendAsync(
                new PutPullReplicationAsHubOperation(new PullReplicationDefinition(name)
                {
                    Mode = PullReplicationMode.SinkToHub,
                    MentorNode = "A"
                }));

            await hubStore.Maintenance.SendAsync(new RegisterReplicationHubAccessOperation(name,
                new ReplicationHubAccess
                {
                    Name = "SinkAccess",
                    CertificateBase64 = Convert.ToBase64String(pullCert.Export(X509ContentType.Cert))
                }));

            var hubUrls = hubNodes.Select(s => s.WebUrl).ToArray();
            var pullReplication = new PullReplicationAsSink(hubStore.Database, $"ConnectionString-{hubStore.Database}", name)
            {
                Mode = PullReplicationMode.SinkToHub,
                CertificateWithPrivateKey = Convert.ToBase64String(pullCert.Export(X509ContentType.Pfx))
            };
            var result = await AddWatcherToReplicationTopology((DocumentStore)sinkStore, pullReplication, hubUrls);

            using (var bulkInsert = sinkStore.BulkInsert())
            {
                for (int i = 0; i < 1024; i++)
                    bulkInsert.Store(new User { Name = $"User{i}" }, $"users/{i}");
            }

            Assert.True(WaitForDocument(hubStore, "users/1023", 30_000));

            Assert.True(WaitForValue(() =>
            {
                using (Server.ServerStore.ContextPool.AllocateOperationContext(out TransactionOperationContext ctx))
                using (ctx.OpenReadTransaction())
                {
                    var key = ExternalReplicationState.GenerateItemName(
                        sinkStore.Database, result.TaskId,
                        ExternalReplicationState.ReplicationStateType.SinkCursor);
                    var blittable = Server.ServerStore.Cluster.Read(ctx, key);
                    if (blittable == null)
                        return false;

                    var state = JsonDeserializationCluster.ExternalReplicationState(blittable);
                    return state.SourceChangeVector != null && state.SourceChangeVector.StartsWith("A:1024");
                }
            }, true, 30_000));

            var nodeAUrl = hub.ServerStore.GetClusterTopology().GetUrlFromTag("A");
            var nodeAServer = Servers.Single(s => s.WebUrl == nodeAUrl);
            await DisposeServerAndWaitForFinishOfDisposalAsync(nodeAServer);

            using (var session = sinkStore.OpenSession())
            {
                session.Store(new User { Name = "Marker" }, "marker/post-failover");
                session.SaveChanges();
            }

            Assert.True(WaitForDocument(hubStore, "marker/post-failover", 30_000));

            var statsAfter = await sinkStore.Maintenance.SendAsync(new GetReplicationPerformanceStatisticsOperation());

            var docsInNewConnection = statsAfter.Outgoing
                ?.Sum(o => o.Performance?.Sum(p => p.Network?.DocumentOutputCount ?? 0) ?? 0) ?? 0;

            Assert.True(docsInNewConnection <= 1,
                $"After failover, expected <= 1 document sent on new connection but got {docsInNewConnection}. " +
                "Sink is re-sending already-replicated documents after connecting to a new hub node.");
        }
    }

    [RavenFact(RavenTestCategory.Replication)]
    public async Task SinkToHub_HubShouldNotReceiveDuplicateRevisionsAfterHubNodeFailover()
    {
        DebuggerAttachedTimeout.DisableLongTimespan = true;

        var (hubNodes, hub, certs) = await CreateRaftClusterWithSsl(3);
        using (var hubStore = GetDocumentStore(new Options
        {
            Server = hub,
            ReplicationFactor = 3,
            ClientCertificate = certs.ServerCertificateForCommunication.Value,
            AdminCertificate = certs.ServerCertificateForCommunication.Value
        }))
        using (var sinkStore = GetDocumentStore(new Options
        {
            AdminCertificate = certs.ServerCertificateForCommunication.Value,
            ClientCertificate = certs.ServerCertificateForCommunication.Value
        }))
        {
#pragma warning disable SYSLIB0057
            var pullCert = new X509Certificate2(
                await File.ReadAllBytesAsync(certs.ClientCertificate2Path), (string)null,
                X509KeyStorageFlags.Exportable);
#pragma warning restore SYSLIB0057

            var name = $"pull-replication {GetDatabaseName()}";

            await hubStore.Maintenance.ForDatabase(hubStore.Database).SendAsync(
                new PutPullReplicationAsHubOperation(new PullReplicationDefinition(name)
                {
                    Mode = PullReplicationMode.SinkToHub,
                    MentorNode = "A"
                }));

            await hubStore.Maintenance.SendAsync(new RegisterReplicationHubAccessOperation(name,
                new ReplicationHubAccess
                {
                    Name = "SinkAccess",
                    CertificateBase64 = Convert.ToBase64String(pullCert.Export(X509ContentType.Cert))
                }));

            var hubUrls = hubNodes.Select(s => s.WebUrl).ToArray();
            var pullReplication = new PullReplicationAsSink(hubStore.Database, $"ConnectionString-{hubStore.Database}", name)
            {
                Mode = PullReplicationMode.SinkToHub,
                CertificateWithPrivateKey = Convert.ToBase64String(pullCert.Export(X509ContentType.Pfx))
            };
            var result = await AddWatcherToReplicationTopology((DocumentStore)sinkStore, pullReplication, hubUrls);

            await sinkStore.Maintenance.SendAsync(new ConfigureRevisionsOperation(new RevisionsConfiguration
            {
                Default = new RevisionsCollectionConfiguration { Disabled = false }
            }));

            await hubStore.Maintenance.SendAsync(new ConfigureRevisionsOperation(new RevisionsConfiguration
            {
                Default = new RevisionsCollectionConfiguration { Disabled = false }
            }));

            using (var session = sinkStore.OpenAsyncSession())
            {
                for (int i = 0; i < 1024; i++)
                    await session.StoreAsync(new User { Name = $"User{i}" }, $"users/{i}");
                await session.SaveChangesAsync();
            }

            Assert.True(WaitForDocument(hubStore, "users/1023", 30_000));

            Assert.True(WaitForValue(() =>
            {
                using var session = hubStore.OpenSession();
                var revisions = session.Advanced.Revisions.GetFor<User>("users/1023");
                return revisions != null && revisions.Count >= 1;
            }, true, 30_000));

            Assert.True(WaitForValue(() =>
            {
                using (Server.ServerStore.ContextPool.AllocateOperationContext(out TransactionOperationContext ctx))
                using (ctx.OpenReadTransaction())
                {
                    var key = ExternalReplicationState.GenerateItemName(
                        sinkStore.Database, result.TaskId,
                        ExternalReplicationState.ReplicationStateType.SinkCursor);
                    var blittable = Server.ServerStore.Cluster.Read(ctx, key);
                    if (blittable == null)
                        return false;

                    var state = JsonDeserializationCluster.ExternalReplicationState(blittable);
                    return state.SourceChangeVector != null;
                }
            }, true, 30_000));

            var nodeAUrl = hub.ServerStore.GetClusterTopology().GetUrlFromTag("A");
            var nodeAServer = Servers.Single(s => s.WebUrl == nodeAUrl);
            await DisposeServerAndWaitForFinishOfDisposalAsync(nodeAServer);

            using (var session = sinkStore.OpenSession())
            {
                session.Store(new User { Name = "Marker" }, "marker/post-failover");
                session.SaveChanges();
            }

            Assert.True(WaitForDocument(hubStore, "marker/post-failover", 30_000));

            var statsAfter = await sinkStore.Maintenance.SendAsync(new GetReplicationPerformanceStatisticsOperation());

            var revisionsInNewConnection = statsAfter.Outgoing
                ?.Sum(o => o.Performance?.Sum(p => p.Network?.RevisionOutputCount ?? 0) ?? 0) ?? 0;

            Assert.True(revisionsInNewConnection <= 2,
                $"After failover, expected <= 2 revisions sent on new connection but got {revisionsInNewConnection}. " +
                "Sink is re-sending already-replicated revisions after connecting to a new hub node.");
        }
    }

    [RavenFact(RavenTestCategory.Replication)]
    public async Task SinkToHub_HubShouldNotReceiveDuplicateCountersAfterHubNodeFailover()
    {
        DebuggerAttachedTimeout.DisableLongTimespan = true;

        var (hubNodes, hub, certs) = await CreateRaftClusterWithSsl(3);
        using (var hubStore = GetDocumentStore(new Options
        {
            Server = hub,
            ReplicationFactor = 3,
            ClientCertificate = certs.ServerCertificateForCommunication.Value,
            AdminCertificate = certs.ServerCertificateForCommunication.Value
        }))
        using (var sinkStore = GetDocumentStore(new Options
        {
            AdminCertificate = certs.ServerCertificateForCommunication.Value,
            ClientCertificate = certs.ServerCertificateForCommunication.Value
        }))
        {
#pragma warning disable SYSLIB0057
            var pullCert = new X509Certificate2(
                await File.ReadAllBytesAsync(certs.ClientCertificate2Path), (string)null,
                X509KeyStorageFlags.Exportable);
#pragma warning restore SYSLIB0057

            var name = $"pull-replication {GetDatabaseName()}";

            await hubStore.Maintenance.ForDatabase(hubStore.Database).SendAsync(
                new PutPullReplicationAsHubOperation(new PullReplicationDefinition(name)
                {
                    Mode = PullReplicationMode.SinkToHub,
                    MentorNode = "A"
                }));

            await hubStore.Maintenance.SendAsync(new RegisterReplicationHubAccessOperation(name,
                new ReplicationHubAccess
                {
                    Name = "SinkAccess",
                    CertificateBase64 = Convert.ToBase64String(pullCert.Export(X509ContentType.Cert))
                }));

            var hubUrls = hubNodes.Select(s => s.WebUrl).ToArray();
            var pullReplication = new PullReplicationAsSink(hubStore.Database, $"ConnectionString-{hubStore.Database}", name)
            {
                Mode = PullReplicationMode.SinkToHub,
                CertificateWithPrivateKey = Convert.ToBase64String(pullCert.Export(X509ContentType.Pfx))
            };
            var result = await AddWatcherToReplicationTopology((DocumentStore)sinkStore, pullReplication, hubUrls);

            using (var session = sinkStore.OpenSession())
            {
                for (int i = 0; i < 1024; i++)
                {
                    session.Store(new User { Name = $"User{i}" }, $"users/{i}");
                    session.CountersFor($"users/{i}").Increment("likes");
                }
                session.SaveChanges();
            }

            Assert.True(WaitForDocument(hubStore, "users/1023", 30_000));

            Assert.True(WaitForValue(() =>
            {
                using var session = hubStore.OpenSession();
                return session.CountersFor("users/1023").Get("likes") != null;
            }, true, 30_000));

            Assert.True(WaitForValue(() =>
            {
                using (Server.ServerStore.ContextPool.AllocateOperationContext(out TransactionOperationContext ctx))
                using (ctx.OpenReadTransaction())
                {
                    var key = ExternalReplicationState.GenerateItemName(
                        sinkStore.Database, result.TaskId,
                        ExternalReplicationState.ReplicationStateType.SinkCursor);
                    var blittable = Server.ServerStore.Cluster.Read(ctx, key);
                    if (blittable == null)
                        return false;

                    var state = JsonDeserializationCluster.ExternalReplicationState(blittable);
                    return state.SourceChangeVector != null && state.SourceChangeVector.StartsWith("A:4096");
                }
            }, true, 30_000));

            var nodeAUrl = hub.ServerStore.GetClusterTopology().GetUrlFromTag("A");
            var nodeAServer = Servers.Single(s => s.WebUrl == nodeAUrl);
            await DisposeServerAndWaitForFinishOfDisposalAsync(nodeAServer);

            using (var session = sinkStore.OpenSession())
            {
                session.Store(new User { Name = "Marker" }, "marker/post-failover");
                session.CountersFor("marker/post-failover").Increment("likes");
                session.SaveChanges();
            }

            Assert.True(WaitForDocument(hubStore, "marker/post-failover", 30_000));

            var statsAfter = await sinkStore.Maintenance.SendAsync(new GetReplicationPerformanceStatisticsOperation());

            var countersInNewConnection = statsAfter.Outgoing
                ?.Sum(o => o.Performance?.Sum(p => p.Network?.CounterOutputCount ?? 0) ?? 0) ?? 0;

            Assert.True(countersInNewConnection <= 1,
                $"After failover, expected <= 1 counter batch sent on new connection but got {countersInNewConnection}. " +
                "Sink is re-sending already-replicated counters after connecting to a new hub node.");
        }
    }

    [RavenFact(RavenTestCategory.Replication)]
    public async Task SinkToHub_HubShouldNotReceiveDuplicateTimeSeriesAfterHubNodeFailover()
    {
        DebuggerAttachedTimeout.DisableLongTimespan = true;

        var (hubNodes, hub, certs) = await CreateRaftClusterWithSsl(3);
        using (var hubStore = GetDocumentStore(new Options
        {
            Server = hub,
            ReplicationFactor = 3,
            ClientCertificate = certs.ServerCertificateForCommunication.Value,
            AdminCertificate = certs.ServerCertificateForCommunication.Value
        }))
        using (var sinkStore = GetDocumentStore(new Options
        {
            AdminCertificate = certs.ServerCertificateForCommunication.Value,
            ClientCertificate = certs.ServerCertificateForCommunication.Value
        }))
        {
#pragma warning disable SYSLIB0057
            var pullCert = new X509Certificate2(
                await File.ReadAllBytesAsync(certs.ClientCertificate2Path), (string)null,
                X509KeyStorageFlags.Exportable);
#pragma warning restore SYSLIB0057

            var name = $"pull-replication {GetDatabaseName()}";

            await hubStore.Maintenance.ForDatabase(hubStore.Database).SendAsync(
                new PutPullReplicationAsHubOperation(new PullReplicationDefinition(name)
                {
                    Mode = PullReplicationMode.SinkToHub,
                    MentorNode = "A"
                }));

            await hubStore.Maintenance.SendAsync(new RegisterReplicationHubAccessOperation(name,
                new ReplicationHubAccess
                {
                    Name = "SinkAccess",
                    CertificateBase64 = Convert.ToBase64String(pullCert.Export(X509ContentType.Cert))
                }));

            var hubUrls = hubNodes.Select(s => s.WebUrl).ToArray();
            var pullReplication = new PullReplicationAsSink(hubStore.Database, $"ConnectionString-{hubStore.Database}", name)
            {
                Mode = PullReplicationMode.SinkToHub,
                CertificateWithPrivateKey = Convert.ToBase64String(pullCert.Export(X509ContentType.Pfx))
            };
            var result = await AddWatcherToReplicationTopology((DocumentStore)sinkStore, pullReplication, hubUrls);

            var baseline = new DateTime(2024, 1, 1, 0, 0, 0, DateTimeKind.Utc);

            using (var session = sinkStore.OpenAsyncSession())
            {
                await session.StoreAsync(new User { Name = "TimeSeries" }, "users/ts");
                var tsf = session.TimeSeriesFor("users/ts", "heartbeat");
                for (int i = 0; i < 1024; i++)
                    tsf.Append(baseline.AddMinutes(i), i);
                await session.SaveChangesAsync();
            }

            Assert.True(WaitForDocument(hubStore, "users/ts", 30_000));

            Assert.True(WaitForValue(() =>
            {
                using var session = hubStore.OpenSession();
                var entries = session.TimeSeriesFor("users/ts", "heartbeat").Get();
                return entries != null && entries.Length == 1024;
            }, true, 30_000));

            Assert.True(WaitForValue(() =>
            {
                using (Server.ServerStore.ContextPool.AllocateOperationContext(out TransactionOperationContext ctx))
                using (ctx.OpenReadTransaction())
                {
                    var key = ExternalReplicationState.GenerateItemName(
                        sinkStore.Database, result.TaskId,
                        ExternalReplicationState.ReplicationStateType.SinkCursor);
                    var blittable = Server.ServerStore.Cluster.Read(ctx, key);
                    if (blittable == null)
                        return false;

                    var state = JsonDeserializationCluster.ExternalReplicationState(blittable);
                    return state.SourceChangeVector != null;
                }
            }, true, 30_000));

            var nodeAUrl = hub.ServerStore.GetClusterTopology().GetUrlFromTag("A");
            var nodeAServer = Servers.Single(s => s.WebUrl == nodeAUrl);
            await DisposeServerAndWaitForFinishOfDisposalAsync(nodeAServer);

            using (var session = sinkStore.OpenSession())
            {
                session.TimeSeriesFor("users/ts", "heartbeat").Append(baseline.AddMinutes(2000), 9999);
                session.SaveChanges();
            }

            Assert.True(WaitForValue(() =>
            {
                using var session = hubStore.OpenSession();
                var entries = session.TimeSeriesFor("users/ts", "heartbeat").Get(baseline.AddMinutes(2000), baseline.AddMinutes(2001));
                return entries != null && entries.Length == 1;
            }, true, 30_000));

            var statsAfter = await sinkStore.Maintenance.SendAsync(new GetReplicationPerformanceStatisticsOperation());

            var tsSegmentsInNewConnection = statsAfter.Outgoing
                ?.Sum(o => o.Performance?.Sum(p => p.Network?.TimeSeriesSegmentsOutputCount ?? 0) ?? 0) ?? 0;

            Assert.True(tsSegmentsInNewConnection <= 1,
                $"After failover, expected <= 1 time series segment sent on new connection but got {tsSegmentsInNewConnection}. " +
                "Sink is re-sending already-replicated time series after connecting to a new hub node.");
        }
    }

    [RavenFact(RavenTestCategory.Replication)]
    public async Task SinkToHub_HubShouldNotReceiveDuplicateAttachmentsAfterHubNodeFailover()
    {
        DebuggerAttachedTimeout.DisableLongTimespan = true;

        var (hubNodes, hub, certs) = await CreateRaftClusterWithSsl(3);
        using (var hubStore = GetDocumentStore(new Options
        {
            Server = hub,
            ReplicationFactor = 3,
            ClientCertificate = certs.ServerCertificateForCommunication.Value,
            AdminCertificate = certs.ServerCertificateForCommunication.Value
        }))
        using (var sinkStore = GetDocumentStore(new Options
        {
            AdminCertificate = certs.ServerCertificateForCommunication.Value,
            ClientCertificate = certs.ServerCertificateForCommunication.Value
        }))
        {
#pragma warning disable SYSLIB0057
            var pullCert = new X509Certificate2(
                await File.ReadAllBytesAsync(certs.ClientCertificate2Path), (string)null,
                X509KeyStorageFlags.Exportable);
#pragma warning restore SYSLIB0057

            var name = $"pull-replication {GetDatabaseName()}";

            await hubStore.Maintenance.ForDatabase(hubStore.Database).SendAsync(
                new PutPullReplicationAsHubOperation(new PullReplicationDefinition(name)
                {
                    Mode = PullReplicationMode.SinkToHub,
                    MentorNode = "A"
                }));

            await hubStore.Maintenance.SendAsync(new RegisterReplicationHubAccessOperation(name,
                new ReplicationHubAccess
                {
                    Name = "SinkAccess",
                    CertificateBase64 = Convert.ToBase64String(pullCert.Export(X509ContentType.Cert))
                }));

            var hubUrls = hubNodes.Select(s => s.WebUrl).ToArray();
            var pullReplication = new PullReplicationAsSink(hubStore.Database, $"ConnectionString-{hubStore.Database}", name)
            {
                Mode = PullReplicationMode.SinkToHub,
                CertificateWithPrivateKey = Convert.ToBase64String(pullCert.Export(X509ContentType.Pfx))
            };
            var result = await AddWatcherToReplicationTopology((DocumentStore)sinkStore, pullReplication, hubUrls);

            using (var session = sinkStore.OpenSession())
            {
                for (int i = 0; i < 1024; i++)
                {
                    session.Store(new User { Name = $"User{i}" }, $"users/{i}");
                    var bytes = Encoding.UTF8.GetBytes($"attachment content {i}");
                    session.Advanced.Attachments.Store($"users/{i}", "file.txt", new MemoryStream(bytes));
                }
                session.SaveChanges();
            }

            Assert.True(WaitForDocument(hubStore, "users/1023", 30_000));

            Assert.True(WaitForValue(() =>
            {
                using var session = hubStore.OpenSession();
                using var attachment = session.Advanced.Attachments.Get("users/1023", "file.txt");
                return attachment != null;
            }, true, 30_000));

            Assert.True(WaitForValue(() =>
            {
                using (Server.ServerStore.ContextPool.AllocateOperationContext(out TransactionOperationContext ctx))
                using (ctx.OpenReadTransaction())
                {
                    var key = ExternalReplicationState.GenerateItemName(
                        sinkStore.Database, result.TaskId,
                        ExternalReplicationState.ReplicationStateType.SinkCursor);
                    var blittable = Server.ServerStore.Cluster.Read(ctx, key);
                    if (blittable == null)
                        return false;

                    var state = JsonDeserializationCluster.ExternalReplicationState(blittable);
                    return state.SourceChangeVector != null && state.SourceChangeVector.StartsWith("A:3072");
                }
            }, true, 30_000));

            var nodeAUrl = hub.ServerStore.GetClusterTopology().GetUrlFromTag("A");
            var nodeAServer = Servers.Single(s => s.WebUrl == nodeAUrl);
            await DisposeServerAndWaitForFinishOfDisposalAsync(nodeAServer);

            using (var session = sinkStore.OpenSession())
            {
                session.Store(new User { Name = "Marker" }, "marker/post-failover");
                var bytes = Encoding.UTF8.GetBytes("marker attachment");
                session.Advanced.Attachments.Store("marker/post-failover", "file.txt", new MemoryStream(bytes));
                session.SaveChanges();
            }

            Assert.True(WaitForValue(() =>
            {
                using var session = hubStore.OpenSession();
                using var attachment = session.Advanced.Attachments.Get("marker/post-failover", "file.txt");
                return attachment != null;
            }, true, 30_000));

            var statsAfter = await sinkStore.Maintenance.SendAsync(new GetReplicationPerformanceStatisticsOperation());

            var attachmentsInNewConnection = statsAfter.Outgoing
                ?.Sum(o => o.Performance?.Sum(p => p.Network?.AttachmentOutputCount ?? 0) ?? 0) ?? 0;

            Assert.True(attachmentsInNewConnection <= 1,
                $"After failover, expected <= 1 attachment sent on new connection but got {attachmentsInNewConnection}. " +
                "Sink is re-sending already-replicated attachments after connecting to a new hub node.");
        }
    }

    [RavenFact(RavenTestCategory.Replication)]
    public async Task SinkToHub_HubShouldNotReceiveDuplicateDocumentsAfterSinkNodeFailover()
    {
        DebuggerAttachedTimeout.DisableLongTimespan = true;

        var (_, hub, certs) = await CreateRaftClusterWithSsl(1);
        var (sinkNodes, sinkLeader) = await CreateRaftCluster(3);

        var sinkDB = GetDatabaseName();
        await CreateDatabaseInCluster(sinkDB, 3, sinkLeader.WebUrl);

        using (var hubStore = GetDocumentStore(new Options
        {
            Server = hub,
            ReplicationFactor = 1,
            ClientCertificate = certs.ServerCertificateForCommunication.Value,
            AdminCertificate = certs.ServerCertificateForCommunication.Value
        }))
        using (var sinkStoreA = new DocumentStore
        {
            Urls = new[] { sinkNodes.Single(n => n.ServerStore.NodeTag == "A").WebUrl },
            Database = sinkDB,
            Conventions = new DocumentConventions()
            {
                DisableTopologyUpdates = true
            }
        }.Initialize())
        using (var sinkStoreB = new DocumentStore
        {
            Urls = new[] { sinkNodes.Single(n => n.ServerStore.NodeTag == "B").WebUrl },
            Database = sinkDB,
            Conventions = new DocumentConventions()
            {
                DisableTopologyUpdates = true
            }
        }.Initialize())
        {
#pragma warning disable SYSLIB0057
            var pullCert = new X509Certificate2(
                await File.ReadAllBytesAsync(certs.ClientCertificate2Path), (string)null,
                X509KeyStorageFlags.Exportable);
#pragma warning restore SYSLIB0057

            var name = $"pull-replication {GetDatabaseName()}";

            await hubStore.Maintenance.ForDatabase(hubStore.Database).SendAsync(
                new PutPullReplicationAsHubOperation(new PullReplicationDefinition(name)
                {
                    Mode = PullReplicationMode.SinkToHub
                }));

            await hubStore.Maintenance.SendAsync(new RegisterReplicationHubAccessOperation(name,
                new ReplicationHubAccess
                {
                    Name = "SinkAccess",
                    CertificateBase64 = Convert.ToBase64String(pullCert.Export(X509ContentType.Cert))
                }));

            var pullReplication = new PullReplicationAsSink(hubStore.Database, $"ConnectionString-{hubStore.Database}", name)
            {
                Mode = PullReplicationMode.SinkToHub,
                CertificateWithPrivateKey = Convert.ToBase64String(pullCert.Export(X509ContentType.Pfx)),
                MentorNode = "A"
            };
            var result = await AddWatcherToReplicationTopology((DocumentStore)sinkStoreA, pullReplication, new[] { hub.WebUrl });

            using (var session = sinkStoreA.OpenSession())
            {
                for (int i = 0; i < 1024; i++)
                    session.Store(new User { Name = $"User{i}" }, $"users/{i}");

                session.Advanced.WaitForReplicationAfterSaveChanges(replicas: 2);
                session.SaveChanges();
            }

            Assert.True(WaitForDocument(hubStore, "users/1023", 30_000));

            using (var session = sinkStoreB.OpenSession())
            {
                session.Store(new User { Name = "Transition" }, "transition/doc");
                session.SaveChanges();
            }

            Assert.True(WaitForDocument(hubStore, "transition/doc", 30_000));

            var sinkNodeA = sinkNodes.Single(n => n.ServerStore.NodeTag == "A");
            Assert.True(WaitForValue(() =>
            {
                using (sinkNodeA.ServerStore.ContextPool.AllocateOperationContext(out TransactionOperationContext ctx))
                using (ctx.OpenReadTransaction())
                {
                    var key = ExternalReplicationState.GenerateItemName(
                        sinkDB, result.TaskId,
                        ExternalReplicationState.ReplicationStateType.SinkCursor);
                    var blittable = sinkNodeA.ServerStore.Cluster.Read(ctx, key);
                    if (blittable == null)
                        return false;
                    var state = JsonDeserializationCluster.ExternalReplicationState(blittable);
                    return state.SourceChangeVector != null
                           && state.SourceChangeVector.Contains("A:1024") 
                           && state.SourceChangeVector.Contains("B:1025")
                           && state.SourceChangeVector.Contains("C:1024");
                }
            }, true, 30_000));

            pullReplication.TaskId = result.TaskId;
            pullReplication.MentorNode = "B";
            await sinkStoreA.Maintenance.SendAsync(new UpdatePullReplicationAsSinkOperation(pullReplication));

            var nodeAUrl = sinkNodes.Single(n => n.ServerStore.NodeTag == "A").ServerStore.GetClusterTopology().GetUrlFromTag("A");
            var nodeAServer = Servers.Single(s => s.WebUrl == nodeAUrl);
            await DisposeServerAndWaitForFinishOfDisposalAsync(nodeAServer);

            using (var session = sinkStoreB.OpenSession())
            {
                var user = new User { Name = "Marker" };
                session.Store(user, "marker/post-failover");
                session.Advanced.WaitForReplicationAfterSaveChanges(replicas: 1);
                session.SaveChanges();
            }

            Assert.True(WaitForDocument(hubStore, "marker/post-failover", 30_000));

            var sinkNodeB = sinkNodes.Single(n => n.ServerStore.NodeTag == "B");

            Assert.True(WaitForValue(() =>
            {
                using (sinkNodeB.ServerStore.ContextPool.AllocateOperationContext(out TransactionOperationContext ctx))
                using (ctx.OpenReadTransaction())
                {
                    var key = ExternalReplicationState.GenerateItemName(
                        sinkDB, result.TaskId,
                        ExternalReplicationState.ReplicationStateType.SinkCursor);
                    var blittable = sinkNodeB.ServerStore.Cluster.Read(ctx, key);
                    if (blittable == null)
                        return false;
                    var state = JsonDeserializationCluster.ExternalReplicationState(blittable);
                    return state.SourceChangeVector != null && state.SourceChangeVector.Contains("B:1026");
                }
            }, true, 30_000));

            using (var sinkBStore = new DocumentStore
            {
                Urls = new[] { sinkNodeB.WebUrl },
                Database = sinkDB
            }.Initialize())
            {
                var statsAfter = await sinkBStore.Maintenance.SendAsync(new GetReplicationPerformanceStatisticsOperation());

                var docsInNewConnection = statsAfter.Outgoing
                    ?.Sum(o => o.Performance?.Sum(p => p.Network?.DocumentOutputCount ?? 0) ?? 0) ?? 0;

                Assert.True(docsInNewConnection <= 3,
                    $"After sink task migration to node B, expected <= 1 document sent on new connection but got {docsInNewConnection}. " +
                    "Sink is re-sending already-replicated documents after the replication task moved to node B.");
            }
        }
    }

    [RavenFact(RavenTestCategory.Replication)]
    public async Task SinkToHub_HubShouldNotReceiveDuplicateTimeSeriesAfterSinkNodeFailover()
    {
        DebuggerAttachedTimeout.DisableLongTimespan = true;

        var (_, hub, certs) = await CreateRaftClusterWithSsl(1);
        var (sinkNodes, sinkLeader) = await CreateRaftCluster(3);

        var sinkDB = GetDatabaseName();
        await CreateDatabaseInCluster(sinkDB, 3, sinkLeader.WebUrl);

        using (var hubStore = GetDocumentStore(new Options
        {
            Server = hub,
            ReplicationFactor = 1,
            ClientCertificate = certs.ServerCertificateForCommunication.Value,
            AdminCertificate = certs.ServerCertificateForCommunication.Value
        }))
        using (var sinkStoreA = new DocumentStore
        {
            Urls = new[] { sinkNodes.Single(n => n.ServerStore.NodeTag == "A").WebUrl },
            Database = sinkDB,
            Conventions = new DocumentConventions { DisableTopologyUpdates = true }
        }.Initialize())
        using (var sinkStoreB = new DocumentStore
        {
            Urls = new[] { sinkNodes.Single(n => n.ServerStore.NodeTag == "B").WebUrl },
            Database = sinkDB,
            Conventions = new DocumentConventions { DisableTopologyUpdates = true }
        }.Initialize())
        {
#pragma warning disable SYSLIB0057
            var pullCert = new X509Certificate2(
                await File.ReadAllBytesAsync(certs.ClientCertificate2Path), (string)null,
                X509KeyStorageFlags.Exportable);
#pragma warning restore SYSLIB0057

            var name = $"pull-replication {GetDatabaseName()}";

            await hubStore.Maintenance.ForDatabase(hubStore.Database).SendAsync(
                new PutPullReplicationAsHubOperation(new PullReplicationDefinition(name)
                {
                    Mode = PullReplicationMode.SinkToHub
                }));

            await hubStore.Maintenance.SendAsync(new RegisterReplicationHubAccessOperation(name,
                new ReplicationHubAccess
                {
                    Name = "SinkAccess",
                    CertificateBase64 = Convert.ToBase64String(pullCert.Export(X509ContentType.Cert))
                }));

            var pullReplication = new PullReplicationAsSink(hubStore.Database, $"ConnectionString-{hubStore.Database}", name)
            {
                Mode = PullReplicationMode.SinkToHub,
                CertificateWithPrivateKey = Convert.ToBase64String(pullCert.Export(X509ContentType.Pfx)),
                MentorNode = "A"
            };
            var result = await AddWatcherToReplicationTopology((DocumentStore)sinkStoreA, pullReplication, new[] { hub.WebUrl });

            var baseline = new DateTime(2024, 1, 1, 0, 0, 0, DateTimeKind.Utc);

            using (var session = sinkStoreA.OpenAsyncSession())
            {
                await session.StoreAsync(new User { Name = "TimeSeries" }, "users/ts");
                var tsf = session.TimeSeriesFor("users/ts", "heartbeat");
                for (int i = 0; i < 1024; i++)
                    tsf.Append(baseline.AddMinutes(i), i);
                session.Advanced.WaitForReplicationAfterSaveChanges(replicas: 2);
                await session.SaveChangesAsync();
            }

            Assert.True(WaitForDocument(hubStore, "users/ts", 30_000));
            Assert.True(WaitForValue(() =>
            {
                using var session = hubStore.OpenSession();
                var entries = session.TimeSeriesFor("users/ts", "heartbeat").Get();
                return entries != null && entries.Length == 1024;
            }, true, 30_000));

            using (var session = sinkStoreB.OpenSession())
            {
                session.TimeSeriesFor("users/ts", "heartbeat").Append(baseline.AddMinutes(2000), 9999);
                session.SaveChanges();
            }

            Assert.True(WaitForValue(() =>
            {
                using var session = hubStore.OpenSession();
                var entries = session.TimeSeriesFor("users/ts", "heartbeat").Get(baseline.AddMinutes(2000), baseline.AddMinutes(2001));
                return entries != null && entries.Length == 1;
            }, true, 30_000));

            var sinkNodeA = sinkNodes.Single(n => n.ServerStore.NodeTag == "A");
            Assert.True(WaitForValue(() =>
            {
                using (sinkNodeA.ServerStore.ContextPool.AllocateOperationContext(out TransactionOperationContext ctx))
                using (ctx.OpenReadTransaction())
                {
                    var key = ExternalReplicationState.GenerateItemName(sinkDB, result.TaskId, ExternalReplicationState.ReplicationStateType.SinkCursor);
                    var blittable = sinkNodeA.ServerStore.Cluster.Read(ctx, key);
                    if (blittable == null)
                        return false;
                    var state = JsonDeserializationCluster.ExternalReplicationState(blittable);
                    return state.SourceChangeVector != null;
                }
            }, true, 30_000));

            pullReplication.TaskId = result.TaskId;
            pullReplication.MentorNode = "B";
            await sinkStoreA.Maintenance.SendAsync(new UpdatePullReplicationAsSinkOperation(pullReplication));

            var nodeAServer = Servers.Single(s => s.WebUrl == sinkNodes.Single(n => n.ServerStore.NodeTag == "A").WebUrl);
            await DisposeServerAndWaitForFinishOfDisposalAsync(nodeAServer);

            using (var session = sinkStoreB.OpenSession())
            {
                session.TimeSeriesFor("users/ts", "heartbeat").Append(baseline.AddMinutes(3000), 1111);
                session.SaveChanges();
            }

            Assert.True(WaitForValue(() =>
            {
                using var session = hubStore.OpenSession();
                var entries = session.TimeSeriesFor("users/ts", "heartbeat").Get(baseline.AddMinutes(3000), baseline.AddMinutes(3001));
                return entries != null && entries.Length == 1;
            }, true, 30_000));

            var sinkNodeB = sinkNodes.Single(n => n.ServerStore.NodeTag == "B");
            using (var sinkBStore = new DocumentStore
            {
                Urls = new[] { sinkNodeB.WebUrl },
                Database = sinkDB
            }.Initialize())
            {
                var statsAfter = await sinkBStore.Maintenance.SendAsync(new GetReplicationPerformanceStatisticsOperation());
                var tsInNewConnection = statsAfter.Outgoing
                    ?.Sum(o => o.Performance?.Sum(p => p.Network?.TimeSeriesSegmentsOutputCount ?? 0) ?? 0) ?? 0;

                Assert.True(tsInNewConnection <= 3,
                    $"After sink task migration to node B, expected <= 3 time series segments on new connection but got {tsInNewConnection}. " +
                    "Sink is re-sending already-replicated time series after the replication task moved to node B.");
            }
        }
    }

    [RavenFact(RavenTestCategory.Replication)]
    public async Task SinkToHub_HubShouldNotReceiveDuplicateAttachmentsAfterSinkNodeFailover()
    {
        DebuggerAttachedTimeout.DisableLongTimespan = true;

        var (_, hub, certs) = await CreateRaftClusterWithSsl(1);
        var (sinkNodes, sinkLeader) = await CreateRaftCluster(3);

        var sinkDB = GetDatabaseName();
        await CreateDatabaseInCluster(sinkDB, 3, sinkLeader.WebUrl);

        using (var hubStore = GetDocumentStore(new Options
        {
            Server = hub,
            ReplicationFactor = 1,
            ClientCertificate = certs.ServerCertificateForCommunication.Value,
            AdminCertificate = certs.ServerCertificateForCommunication.Value
        }))
        using (var sinkStoreA = new DocumentStore
        {
            Urls = new[] { sinkNodes.Single(n => n.ServerStore.NodeTag == "A").WebUrl },
            Database = sinkDB,
            Conventions = new DocumentConventions { DisableTopologyUpdates = true }
        }.Initialize())
        using (var sinkStoreB = new DocumentStore
        {
            Urls = new[] { sinkNodes.Single(n => n.ServerStore.NodeTag == "B").WebUrl },
            Database = sinkDB,
            Conventions = new DocumentConventions { DisableTopologyUpdates = true }
        }.Initialize())
        {
#pragma warning disable SYSLIB0057
            var pullCert = new X509Certificate2(
                await File.ReadAllBytesAsync(certs.ClientCertificate2Path), (string)null,
                X509KeyStorageFlags.Exportable);
#pragma warning restore SYSLIB0057

            var name = $"pull-replication {GetDatabaseName()}";

            await hubStore.Maintenance.ForDatabase(hubStore.Database).SendAsync(
                new PutPullReplicationAsHubOperation(new PullReplicationDefinition(name)
                {
                    Mode = PullReplicationMode.SinkToHub
                }));

            await hubStore.Maintenance.SendAsync(new RegisterReplicationHubAccessOperation(name,
                new ReplicationHubAccess
                {
                    Name = "SinkAccess",
                    CertificateBase64 = Convert.ToBase64String(pullCert.Export(X509ContentType.Cert))
                }));

            var pullReplication = new PullReplicationAsSink(hubStore.Database, $"ConnectionString-{hubStore.Database}", name)
            {
                Mode = PullReplicationMode.SinkToHub,
                CertificateWithPrivateKey = Convert.ToBase64String(pullCert.Export(X509ContentType.Pfx)),
                MentorNode = "A"
            };
            var result = await AddWatcherToReplicationTopology((DocumentStore)sinkStoreA, pullReplication, new[] { hub.WebUrl });

            using (var session = sinkStoreA.OpenSession())
            {
                for (int i = 0; i < 1024; i++)
                {
                    session.Store(new User { Name = $"User{i}" }, $"users/{i}");
                    session.Advanced.Attachments.Store($"users/{i}", "file.txt",
                        new MemoryStream(Encoding.UTF8.GetBytes($"attachment content {i}")));
                }
                session.Advanced.WaitForReplicationAfterSaveChanges(replicas: 2);
                session.SaveChanges();
            }

            Assert.True(WaitForDocument(hubStore, "users/1023", 30_000));
            Assert.True(WaitForValue(() =>
            {
                using var session = hubStore.OpenSession();
                using var attachment = session.Advanced.Attachments.Get("users/1023", "file.txt");
                return attachment != null;
            }, true, 30_000));

            using (var session = sinkStoreB.OpenSession())
            {
                session.Store(new User { Name = "Transition" }, "transition/doc");
                session.Advanced.Attachments.Store("transition/doc", "file.txt",
                    new MemoryStream(Encoding.UTF8.GetBytes("transition attachment")));
                session.SaveChanges();
            }

            Assert.True(WaitForValue(() =>
            {
                using var session = hubStore.OpenSession();
                using var attachment = session.Advanced.Attachments.Get("transition/doc", "file.txt");
                return attachment != null;
            }, true, 30_000));

            var sinkNodeA = sinkNodes.Single(n => n.ServerStore.NodeTag == "A");
            Assert.True(WaitForValue(() =>
            {
                using (sinkNodeA.ServerStore.ContextPool.AllocateOperationContext(out TransactionOperationContext ctx))
                using (ctx.OpenReadTransaction())
                {
                    var key = ExternalReplicationState.GenerateItemName(sinkDB, result.TaskId, ExternalReplicationState.ReplicationStateType.SinkCursor);
                    var blittable = sinkNodeA.ServerStore.Cluster.Read(ctx, key);
                    if (blittable == null)
                        return false;
                    var state = JsonDeserializationCluster.ExternalReplicationState(blittable);
                    return state.SourceChangeVector != null;
                }
            }, true, 30_000));

            pullReplication.TaskId = result.TaskId;
            pullReplication.MentorNode = "B";
            await sinkStoreA.Maintenance.SendAsync(new UpdatePullReplicationAsSinkOperation(pullReplication));

            var nodeAServer = Servers.Single(s => s.WebUrl == sinkNodes.Single(n => n.ServerStore.NodeTag == "A").WebUrl);
            await DisposeServerAndWaitForFinishOfDisposalAsync(nodeAServer);

            using (var session = sinkStoreB.OpenSession())
            {
                session.Store(new User { Name = "Marker" }, "marker/post-failover");
                session.Advanced.Attachments.Store("marker/post-failover", "file.txt",
                    new MemoryStream(Encoding.UTF8.GetBytes("marker attachment")));
                session.SaveChanges();
            }

            Assert.True(WaitForValue(() =>
            {
                using var session = hubStore.OpenSession();
                using var attachment = session.Advanced.Attachments.Get("marker/post-failover", "file.txt");
                return attachment != null;
            }, true, 30_000));

            var sinkNodeB = sinkNodes.Single(n => n.ServerStore.NodeTag == "B");
            using (var sinkBStore = new DocumentStore
            {
                Urls = new[] { sinkNodeB.WebUrl },
                Database = sinkDB
            }.Initialize())
            {
                var statsAfter = await sinkBStore.Maintenance.SendAsync(new GetReplicationPerformanceStatisticsOperation());
                var attachmentsInNewConnection = statsAfter.Outgoing
                    ?.Sum(o => o.Performance?.Sum(p => p.Network?.AttachmentOutputCount ?? 0) ?? 0) ?? 0;

                Assert.True(attachmentsInNewConnection <= 3,
                    $"After sink task migration to node B, expected <= 3 attachments on new connection but got {attachmentsInNewConnection}. " +
                    "Sink is re-sending already-replicated attachments after the replication task moved to node B.");
            }
        }
    }

    [RavenFact(RavenTestCategory.Replication)]
    public async Task SinkToHub_HubShouldNotReceiveDuplicateCountersAfterSinkNodeFailover()
    {
        DebuggerAttachedTimeout.DisableLongTimespan = true;

        var (_, hub, certs) = await CreateRaftClusterWithSsl(1);
        var (sinkNodes, sinkLeader) = await CreateRaftCluster(3);

        var sinkDB = GetDatabaseName();
        await CreateDatabaseInCluster(sinkDB, 3, sinkLeader.WebUrl);

        using (var hubStore = GetDocumentStore(new Options
        {
            Server = hub,
            ReplicationFactor = 1,
            ClientCertificate = certs.ServerCertificateForCommunication.Value,
            AdminCertificate = certs.ServerCertificateForCommunication.Value
        }))
        using (var sinkStoreA = new DocumentStore
        {
            Urls = new[] { sinkNodes.Single(n => n.ServerStore.NodeTag == "A").WebUrl },
            Database = sinkDB,
            Conventions = new DocumentConventions { DisableTopologyUpdates = true }
        }.Initialize())
        using (var sinkStoreB = new DocumentStore
        {
            Urls = new[] { sinkNodes.Single(n => n.ServerStore.NodeTag == "B").WebUrl },
            Database = sinkDB,
            Conventions = new DocumentConventions { DisableTopologyUpdates = true }
        }.Initialize())
        {
#pragma warning disable SYSLIB0057
            var pullCert = new X509Certificate2(
                await File.ReadAllBytesAsync(certs.ClientCertificate2Path), (string)null,
                X509KeyStorageFlags.Exportable);
#pragma warning restore SYSLIB0057

            var name = $"pull-replication {GetDatabaseName()}";

            await hubStore.Maintenance.ForDatabase(hubStore.Database).SendAsync(
                new PutPullReplicationAsHubOperation(new PullReplicationDefinition(name)
                {
                    Mode = PullReplicationMode.SinkToHub
                }));

            await hubStore.Maintenance.SendAsync(new RegisterReplicationHubAccessOperation(name,
                new ReplicationHubAccess
                {
                    Name = "SinkAccess",
                    CertificateBase64 = Convert.ToBase64String(pullCert.Export(X509ContentType.Cert))
                }));

            var pullReplication = new PullReplicationAsSink(hubStore.Database, $"ConnectionString-{hubStore.Database}", name)
            {
                Mode = PullReplicationMode.SinkToHub,
                CertificateWithPrivateKey = Convert.ToBase64String(pullCert.Export(X509ContentType.Pfx)),
                MentorNode = "A"
            };
            var result = await AddWatcherToReplicationTopology((DocumentStore)sinkStoreA, pullReplication, new[] { hub.WebUrl });

            using (var session = sinkStoreA.OpenSession())
            {
                for (int i = 0; i < 1024; i++)
                {
                    session.Store(new User { Name = $"User{i}" }, $"users/{i}");
                    session.CountersFor($"users/{i}").Increment("likes");
                }
                session.Advanced.WaitForReplicationAfterSaveChanges(replicas: 2);
                session.SaveChanges();
            }

            Assert.True(WaitForDocument(hubStore, "users/1023", 30_000));
            Assert.True(WaitForValue(() =>
            {
                using var session = hubStore.OpenSession();
                return session.CountersFor("users/1023").Get("likes") != null;
            }, true, 30_000));

            using (var session = sinkStoreB.OpenSession())
            {
                session.Store(new User { Name = "Transition" }, "transition/doc");
                session.CountersFor("transition/doc").Increment("likes");
                session.SaveChanges();
            }

            Assert.True(WaitForDocument(hubStore, "transition/doc", 30_000));
            Assert.True(WaitForValue(() =>
            {
                using var session = hubStore.OpenSession();
                return session.CountersFor("transition/doc").Get("likes") != null;
            }, true, 30_000));

            var sinkNodeA = sinkNodes.Single(n => n.ServerStore.NodeTag == "A");
            Assert.True(WaitForValue(() =>
            {
                using (sinkNodeA.ServerStore.ContextPool.AllocateOperationContext(out TransactionOperationContext ctx))
                using (ctx.OpenReadTransaction())
                {
                    var key = ExternalReplicationState.GenerateItemName(sinkDB, result.TaskId, ExternalReplicationState.ReplicationStateType.SinkCursor);
                    var blittable = sinkNodeA.ServerStore.Cluster.Read(ctx, key);
                    if (blittable == null)
                        return false;
                    var state = JsonDeserializationCluster.ExternalReplicationState(blittable);
                    return state.SourceChangeVector != null;
                }
            }, true, 30_000));

            pullReplication.TaskId = result.TaskId;
            pullReplication.MentorNode = "B";
            await sinkStoreA.Maintenance.SendAsync(new UpdatePullReplicationAsSinkOperation(pullReplication));

            var nodeAServer = Servers.Single(s => s.WebUrl == sinkNodes.Single(n => n.ServerStore.NodeTag == "A").WebUrl);
            await DisposeServerAndWaitForFinishOfDisposalAsync(nodeAServer);

            using (var session = sinkStoreB.OpenSession())
            {
                session.Store(new User { Name = "Marker" }, "marker/post-failover");
                session.CountersFor("marker/post-failover").Increment("likes");
                session.SaveChanges();
            }

            Assert.True(WaitForDocument(hubStore, "marker/post-failover", 30_000));
            Assert.True(WaitForValue(() =>
            {
                using var session = hubStore.OpenSession();
                return session.CountersFor("marker/post-failover").Get("likes") != null;
            }, true, 30_000));

            var sinkNodeB = sinkNodes.Single(n => n.ServerStore.NodeTag == "B");
            using (var sinkBStore = new DocumentStore
            {
                Urls = new[] { sinkNodeB.WebUrl },
                Database = sinkDB
            }.Initialize())
            {
                var statsAfter = await sinkBStore.Maintenance.SendAsync(new GetReplicationPerformanceStatisticsOperation());
                var countersInNewConnection = statsAfter.Outgoing
                    ?.Sum(o => o.Performance?.Sum(p => p.Network?.CounterOutputCount ?? 0) ?? 0) ?? 0;

                Assert.True(countersInNewConnection <= 3,
                    $"After sink task migration to node B, expected <= 3 counter batches on new connection but got {countersInNewConnection}. " +
                    "Sink is re-sending already-replicated counters after the replication task moved to node B.");
            }
        }
    }

}

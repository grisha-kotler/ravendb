// -----------------------------------------------------------------------
//  <copyright file="RavenDB-4802.cs" company="Hibernating Rhinos LTD">
//      Copyright (c) Hibernating Rhinos LTD. All rights reserved.
//  </copyright>
// -----------------------------------------------------------------------
using System.Threading;
using Raven.Abstractions.Data;
using Raven.Json.Linq;
using Raven.Tests.Common;
using Xunit;
using Xunit.Extensions;

namespace Raven.Tests.Issues
{
    public class RavenDB_4802 : RavenTest
    {
        [Theory]
        [PropertyData("Storages")]
        public void can_get_serialize_size_on_disk(string requestedStorage)
        {
            using (var storage = NewTransactionalStorage(requestedStorage))
            {
                storage.Batch(actions =>
                {
                    actions.Documents.AddDocument("a", null, new RavenJObject(), new RavenJObject());
                    actions.Documents.AddDocument("b", null, new RavenJObject(), new RavenJObject());
                    actions.Documents.AddDocument("c", null, new RavenJObject(), new RavenJObject());
                });

                storage.Batch(actions =>
                {
                    var doc = actions.Documents.DocumentByKey("a");
                    Assert.True(doc.SerializedSizeOnDisk > 0);
                    doc = actions.Documents.DocumentByKey("b");
                    Assert.True(doc.SerializedSizeOnDisk > 0);
                    doc = actions.Documents.DocumentByKey("c");
                    Assert.True(doc.SerializedSizeOnDisk > 0);
                });
            }
        }

        [Theory]
        [PropertyData("Storages")]
        public void can_get_serialize_size_on_disk_enumerable1(string requestedStorage)
        {
            using (var storage = NewTransactionalStorage(requestedStorage))
            {
                storage.Batch(actions =>
                {
                    actions.Documents.AddDocument("a", null, new RavenJObject(), new RavenJObject());
                    actions.Documents.AddDocument("b", null, new RavenJObject(), new RavenJObject());
                    actions.Documents.AddDocument("c", null, new RavenJObject(), new RavenJObject());
                });

                storage.Batch(actions =>
                {
                    var docs = actions.Documents.GetDocuments(0);
                    foreach (var doc in docs)
                    {
                        Assert.True(doc.SerializedSizeOnDisk > 0);
                    }
                });
            }
        }

        [Theory]
        [PropertyData("Storages")]
        public void can_get_serialize_size_on_disk_enumerable2(string requestedStorage)
        {
            using (var storage = NewTransactionalStorage(requestedStorage))
            {
                storage.Batch(actions =>
                {
                    actions.Documents.AddDocument("a", null, new RavenJObject(), new RavenJObject());
                    actions.Documents.AddDocument("b", null, new RavenJObject(), new RavenJObject());
                    actions.Documents.AddDocument("c", null, new RavenJObject(), new RavenJObject());
                });

                storage.Batch(actions =>
                {
                    var docs = actions.Documents.GetDocumentsAfter(Etag.Empty, 3, new CancellationToken());
                    foreach (var doc in docs)
                    {
                        Assert.True(doc.SerializedSizeOnDisk > 0);
                    }
                });
            }
        }
    }
}

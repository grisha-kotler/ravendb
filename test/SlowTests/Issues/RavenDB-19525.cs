using System;
using System.Collections.Generic;
using System.Linq;
using System.Threading.Tasks;
using FastTests;
using Raven.Client.Documents.Indexes;
using Raven.Client.Documents.Operations.Indexes;
using Xunit;
using Xunit.Abstractions;

namespace SlowTests.Issues
{
    public class RavenDB_19525 : RavenTestBase
    {
        public RavenDB_19525(ITestOutputHelper output) : base(output)
        {
        }

        [Fact]
        public async Task DateOnly_TimeOnly_Support_In_Indexing()
        {
            using var store = GetDocumentStore();
            await store.ExecuteIndexAsync(new DateOnlyIndex());

            using (var session = store.OpenSession())
            {
                session.Store(new TestDocument
                {
                    Values = new Dictionary<string, object>
                    {
                        { "DateOnly", new DateOnly(2022, 8, 11) },
                        { "TimeOnly", new TimeOnly(13, 55, 30) }
                    }
                });
                session.SaveChanges();
            }

            Indexes.WaitForIndexing(store);

            var indexes = await store.Maintenance.SendAsync(new GetIndexErrorsOperation());
            Assert.Empty(indexes.Where(idx => idx.Errors.Any()));
        }

        private class DateOnlyIndex : AbstractIndexCreationTask<TestDocument>
        {
            public DateOnlyIndex()
            {
                Map = entities => from entity in entities
                    select new
                    {
                        DateOnly = (DateOnly)entity.Values["DateOnly"],
                        TimeOnly = (TimeOnly)entity.Values["TimeOnly"]
                    };
            }
        }

        private class TestDocument
        {
            public Dictionary<string, object> Values { get; set; }
        }
    }
}

using System;
using System.Collections.Generic;
using System.Linq;
using System.Threading;
using System.Threading.Tasks;
using BenchmarkDotNet.Attributes;
using Raven.Client.Documents;
using Raven.Client.Documents.Commands;
using Raven.Client.Documents.Indexes;
using Raven.Client.Documents.Operations;
using Raven.Client.Documents.Session;
using Raven.Client.Util;

namespace RequestHandler.Benchmark;

[MemoryDiagnoser]
public class StreamDocumentsFromIndexBenchmark
{
    private const int NumberOfDocuments = 1_000_000;

    private RavenDbInstance _instance;

    public class StreamDoc
    {
        public string Name { get; set; }
        public int Value { get; set; }
    }

    public class StreamDocs_ByName : AbstractIndexCreationTask<StreamDoc>
    {
        public StreamDocs_ByName()
        {
            Map = docs => from doc in docs
                          select new
                          {
                              doc.Name,
                              doc.Value
                          };
        }
    }

    [GlobalSetup]
    public void GlobalSetup()
    {
        _instance = new RavenDbInstance();
        _instance.InitializeDatabase();

        Console.WriteLine("Creating index...");

        var index = new StreamDocs_ByName();
        index.Execute(_instance.Store);

        Console.WriteLine($"Inserting {NumberOfDocuments:N0} documents...");

        AsyncHelpers.RunSync(() => InsertDocumentsAsync(_instance.Store));

        Console.WriteLine("Waiting for index to become non-stale...");

        WaitForNonStaleIndex(_instance.Store, index.IndexName);

        Console.WriteLine("Setup complete.");
    }

    [GlobalCleanup]
    public void GlobalCleanup()
    {
        _instance.Dispose();
    }

    [Benchmark]
    public async Task<long> StreamAllDocuments()
    {
        long count = 0;

        using IAsyncDocumentSession session = _instance.Store.OpenAsyncSession();

        var query = session.Query<StreamDoc, StreamDocs_ByName>();
        await using IAsyncEnumerator<StreamResult<StreamDoc>> stream =
            await session.Advanced.StreamAsync(query);

        while (await stream.MoveNextAsync())
        {
            count++;

            if (count % 10000 == 0)
                Console.WriteLine($"Got {count:N0} documents");
        }

        return count;
    }

    private static async Task InsertDocumentsAsync(IDocumentStore store)
    {
        using var bulkInsert = store.BulkInsert();


        for (int i = 0; i < NumberOfDocuments; i++)
        {
            await bulkInsert.StoreAsync(
                new StreamDoc { Name = $"doc-{i}", Value = i });

            if (i % 10000 == 0)
                Console.WriteLine($"Inserted {i:N0} documents");
        }
    }

    private static void WaitForNonStaleIndex(IDocumentStore store, string indexName)
    {
        var admin = store.Maintenance.ForDatabase(store.Database);

        while (true)
        {
            var databaseStatistics = admin.Send(new GetStatisticsOperation());
            var index = databaseStatistics.Indexes
                .Where(x => x.State != IndexState.Disabled)
                .FirstOrDefault(x => x.Name == indexName);

            if (index == null)
                return;

            if (index.IsStale == false)
                return;

            Console.WriteLine($"Waiting for the index to become non stale, last indexing time: {index.LastIndexingTime}");
            Thread.Sleep(100);
        }
    }
}

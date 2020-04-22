using System;
using System.Collections.Generic;
using System.Diagnostics;
using System.Globalization;
using System.IO;
using System.Threading.Tasks;
using FastTests.Graph;
using Raven.Client.Documents;
using Raven.Client.Documents.Session;
using Raven.Client.ServerWide;
using Raven.Client.ServerWide.Operations;
using Sparrow.Json;

namespace Tryouts
{
    public static class Program
    {
        private const string _documentId = "test";

        public static async Task Main(string[] args)
        {
            using (var store = new DocumentStore
            {
                Database = "Telemetry",
                Urls = new[] {"http://localhost:8080"}
            }.Initialize())
            {
                await store.Maintenance.Server.SendAsync(new DeleteDatabasesOperation(store.Database, true));
                await store.Maintenance.Server.SendAsync(new CreateDatabaseOperation(new DatabaseRecord(store.Database)
                {
                    Settings = new Dictionary<string, string>()
                    {
                        {"DataDir", "g:\\Telemetry"}
                    }
                }));

                using (var session = store.OpenAsyncSession())
                {
                    await session.StoreAsync(new User(), _documentId);
                    await session.SaveChangesAsync();
                }

                var sp = Stopwatch.StartNew();

                var dictionary = Parse();
                sp.Stop();
                Console.WriteLine($"Parsing took: {sp.Elapsed}");

                sp.Restart();
                await BulkInsert(store, dictionary);

                sp.Stop();
                Console.WriteLine($"Took: {sp.Elapsed}");
                Console.ReadLine();
            }
        }

        private static Dictionary<string, List<(DateTime, double)>> Parse()
        {
            var dictionary = new Dictionary<string, List<(DateTime, double)>>();
            var index = 0;

            foreach (var line in File.ReadLines(@"F:\telemetryexport.csv"))
            {
                string[] parts = line.Split(',');
                var time = DateTime.ParseExact(parts[0], "yyyy-MM-ddTHH.mm.ssZ", CultureInfo.CurrentCulture);
                var ts = parts[1];

                if (dictionary.TryGetValue(ts, out var list) == false)
                {
                    list = dictionary[ts] = new List<(DateTime, double)>();
                }

                list.Add((time, double.Parse(parts[2])));

                if (index == 50_000_000)
                    return dictionary;

                if (index++ % (16 * 1024) == 0)
                {
                    Console.WriteLine($"Parsing {index:#,#}");
                }
            }

            return dictionary;
        }

        private static async Task Session(IDocumentStore store, Dictionary<string, List<(DateTime, double)>> dictionary)
        {
            var session = store.OpenAsyncSession();
            Task saveChangesAsync = Task.CompletedTask;
            var index = 0;

            foreach (var keyValue in dictionary)
            {
                foreach (var append in keyValue.Value)
                {
                    session.TimeSeriesFor(_documentId, keyValue.Key).Append(append.Item1, new[] { append.Item2 });

                    if (index++ % (16 * 1024) == 0)
                    {
                        Console.WriteLine($"Writing {index:#,#}");

                        await saveChangesAsync;
                        IAsyncDocumentSession current = session;
                        saveChangesAsync = current.SaveChangesAsync().ContinueWith(t =>
                        {
                            current.Dispose();
                            return t;
                        }).Unwrap();
                        session = store.OpenAsyncSession();
                    }
                }
                
            }

            await saveChangesAsync;
            await session.SaveChangesAsync();
            session.Dispose();
        }

        private static async Task BulkInsert(IDocumentStore store, Dictionary<string, List<(DateTime, double)>> dictionary)
        {
            var tasks = new List<Task>();

            foreach (var keyValue in dictionary)
            {
                var copy = keyValue;
                var task = Task.Run(async() =>
                {
                    using (var bulkInsert = store.BulkInsert())
                    {
                        using (var ts = bulkInsert.TimeSeriesFor(_documentId, copy.Key))
                        {
                            foreach (var append in copy.Value)
                            {
                                await ts.AppendAsync(append.Item1, append.Item2);
                            }
                        }
                    }
                });

                tasks.Add(task);
            }

            await Task.WhenAll(tasks);
        }
    }
}

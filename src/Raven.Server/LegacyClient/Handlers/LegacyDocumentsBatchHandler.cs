using System;
using System.Linq;
using System.Threading.Tasks;
using Raven.Client;
using Raven.Client.Exceptions;
using Raven.Server.Documents;
using Raven.Server.Routing;
using Raven.Server.ServerWide.Context;
using Sparrow.Json;
using Sparrow.Json.Parsing;

namespace Raven.Server.LegacyClient.Handlers
{
    public class LegacyDocumentsBatchHandler : DatabaseRequestHandler
    {
        [RavenAction("databases/*/bulk_docs", "POST", AuthorizationStatus.ValidUser, DisableOnCpuCreditsExhaustion = true)]
        public async Task BulkPost()
        {
            //var isDebugEnabled = log.IsDebugEnabled;

            using (ContextPool.AllocateOperationContext(out DocumentsOperationContext context))
            {
                var commandsArray = await StreamArrayReader.Get(RequestBodyStream(), context);

                /*Stopwatch sp = null;
                if (isDebugEnabled)
                    sp = Stopwatch.StartNew();

                if (isDebugEnabled)
                    sp.Stop();*/

                var commands = (from BlittableJsonReaderObject jsonCommand in commandsArray
                        select CreateCommand(jsonCommand))
                    .ToArray();

                throw new NotImplementedException();
            }

            /*if (isDebugEnabled)
            {
                log.Debug(() =>
                {
                    var baseMessage = string.Format(
                        "\tRead bulk_docs data, {0:#,#;;0} commands, size: {1:#,#;;0} bytes, took: {2:#,#;;0}ms",
                        commands.Length, jsonCommandsSize, sp.ElapsedMilliseconds);

                    if (commands.Length > 15
                    ) // this is probably an import method, we will input minimal information, to avoid filling up the log
                    {
                        return baseMessage + Environment.NewLine + "\tExecuting "
                               + string.Join(
                                   ", ",
                                   commands.GroupBy(x => x.Method).Select(x =>
                                       string.Format("{0:#,#;;0} {1} operations", x.Count(), x.Key))) + "" +
                               string.Format(", number of concurrent BulkPost: {0:#,#;;0}",
                                   Interlocked.Read(ref numberOfConcurrentBulkPosts));
                    }

                    var sb = new StringBuilder();
                    sb.AppendFormat("{0}", baseMessage);
                    foreach (var commandData in commands)
                    {
                        sb.AppendFormat("\t{0} {1}{2}", commandData.Method, commandData.Key, Environment.NewLine);
                    }

                    return sb.ToString();
                });

                sp.Restart();
            }

            var batchResult = Database.Batch(commands);

            if (isDebugEnabled)
            {
                log.Debug(string.Format(
                    "Executed {0:#,#;;0} operations, took: {1:#,#;;0}ms, number of concurrent BulkPost: {2:#,#;;0}",
                    commands.Length, sp.ElapsedMilliseconds, numberOfConcurrentBulkPosts));
            }

            context.WriteJson(batchResult);*/
        }

        [RavenAction("databases/*/bulk_docs/$", "DELETE", AuthorizationStatus.ValidUser, DisableOnCpuCreditsExhaustion = true)]
        public async Task BulkDelete()
        {

        }

        [RavenAction("databases/*/bulk_docs/$", "PATCH", AuthorizationStatus.ValidUser, DisableOnCpuCreditsExhaustion = true)]
        public async Task BulkPatch()
        {

        }

        [RavenAction("databases/*/bulk_docs/$", "EVAL", AuthorizationStatus.ValidUser, DisableOnCpuCreditsExhaustion = true)]
        public async Task BulkEval()
        {

        }

        private class LegacyBatchCommand : TransactionOperationsMerger.MergedTransactionCommand, IDisposable
        {
            private readonly DocumentDatabase _database;
            private readonly LegacyICommandData[] _commands;

            public LegacyBatchCommand(DocumentDatabase database, LegacyICommandData[] commands)
            {
                _database = database;
                _commands = commands;
            }

            protected override int ExecuteCmd(DocumentsOperationContext context)
            {
                var results = new BatchResult[_commands.Length];

                for (int i = 0; i < _commands.Length; i++)
                {
                    var command = _commands[i];

                    BatchResult result;
                    switch (command)
                    {
                        case LegacyDeleteCommandData deleteCommandData:
                            result = HandleDeleted(context, deleteCommandData);
                            break;
                        case LegacyPutCommandData putCommandData:
                            result = HandlePut(context, putCommandData);
                            break;
                        default:
                            throw new ArgumentOutOfRangeException();
                    }

                    results[i] = result;
                }

                return _commands.Length;
            }

            private BatchResult HandlePut(DocumentsOperationContext context, LegacyPutCommandData putCommandData)
            {
                var documentId = putCommandData.Key;
                var etag = putCommandData.Etag;

                if (etag != null && etag != 0)
                {
                    var document = _database.DocumentsStorage.Get(context, documentId, DocumentFields.Etag);
                    if (document == null)
                    {
                        throw new ConcurrencyException($"PUT attempted on document '{documentId}' " +
                                                       "using a non current etag (document deleted)");
                    }

                    if (etag != document.Etag)
                    {
                        throw new ConcurrencyException($"PUT attempted on document '{documentId}' using a non current etag");
                    }
                }

                BlittableJsonReaderObject documentToSave;
                using (putCommandData.Document)
                {
                    putCommandData.Document.Modifications = new DynamicJsonValue();
                    putCommandData.Document.Modifications = new DynamicJsonValue(putCommandData.Document)
                    {
                        [Constants.Documents.Metadata.Key] = putCommandData.Metadata
                    };

                    documentToSave = context.ReadObject(putCommandData.Document, documentId);
                }

                var putResult = _database.DocumentsStorage.Put(context, documentId, null, documentToSave);

                return new BatchResult
                {
                    Method = putCommandData.Method,
                    Key = putCommandData.Key,
                    Etag = putResult.Etag,
                    Metadata = putCommandData.Metadata
                };
            }

            private BatchResult HandleDeleted(DocumentsOperationContext context, LegacyDeleteCommandData deleteCommandData)
            {
                var documentId = deleteCommandData.Key;
                var etag = deleteCommandData.Etag;

                if (etag != null && etag != 0)
                {
                    var document = _database.DocumentsStorage.Get(context, documentId, DocumentFields.Etag);
                    if (document != null && etag != document.Etag)
                    {
                        throw new ConcurrencyException($"PUT attempted on document '{documentId}' using a non current etag");
                    }
                }

                _database.DocumentsStorage.Delete(context, documentId, null);

                return new BatchResult
                {
                    Method = deleteCommandData.Method,
                    Key = deleteCommandData.Key,
                    Etag = etag,
                    Deleted = true
                };
            }

            public override TransactionOperationsMerger.IReplayableCommandDto<TransactionOperationsMerger.MergedTransactionCommand> ToDto(JsonOperationContext context)
            {
                throw new NotImplementedException();
                /*return new MergedPutCommandDto
                {
                    Id = _id,
                    Etag = _etag,
                    Document = _document
                };*/
            }

            private class MergedPutCommandDto : TransactionOperationsMerger.IReplayableCommandDto<LegacyBatchCommand>
            {
                public string Id { get; set; }
                public long? Etag { get; set; }
                public BlittableJsonReaderObject Document { get; set; }

                public LegacyBatchCommand ToCommand(DocumentsOperationContext context, DocumentDatabase database)
                {
                    throw new NotImplementedException();
                    //return new LegacyBatchCommand(Document, Id, Etag, database);
                }
            }

            public void Dispose()
            {
                throw new NotImplementedException();
            }
        }

        public class LegacyPutCommandData : LegacyICommandData
        {
            public string Method => "PUT";

            public string Key { get; set; }

            public long? Etag { get; set; }

            public BlittableJsonReaderObject Document { get; set; }

            public BlittableJsonReaderObject Metadata { get; set; }
        }

        public class LegacyDeleteCommandData : LegacyICommandData
        {
            public string Method => "DELETE";

            public string Key { get; set; }

            public long? Etag { get; set; }
        }

        public static LegacyICommandData CreateCommand(BlittableJsonReaderObject commandBlittable)
        {
            if (commandBlittable.TryGet("Key", out string key) == false)
                throw new InvalidOperationException($"Failed to get Key for command: {commandBlittable}");

            if (commandBlittable.TryGet("Method", out string method) == false)
                throw new InvalidOperationException($"Failed to get Method for command: {commandBlittable}");

            var etag = GetEtagFromCommand(commandBlittable);

            switch (method)
            {
                case "PUT":
                    if (commandBlittable.TryGet("Document", out BlittableJsonReaderObject document) == false)
                        throw new InvalidOperationException($"Missing Document from PUT command: {commandBlittable}");

                    if (commandBlittable.TryGet("Metadata", out BlittableJsonReaderObject metadata) == false)
                        throw new InvalidOperationException($"Missing Metadata from PUT command: {commandBlittable}");

                    return new LegacyPutCommandData
                    {
                        Key = key,
                        Etag = etag,
                        Document = document,
                        Metadata = metadata
                    };
                case "DELETE":
                    return new LegacyDeleteCommandData
                    {
                        Key = key,
                        Etag = GetEtagFromCommand(commandBlittable)
                    };
                //case "PATCH":
                //    return new PatchCommandData
                //    {
                //        Key = key,
                //        Etag = GetEtagFromCommand(jsonCommand),
                //        TransactionInformation = transactionInformation,
                //        Metadata = jsonCommand["Metadata"] as RavenJObject,
                //        Patches = jsonCommand
                //            .Value<RavenJArray>("Patches")
                //            .Cast<RavenJObject>()
                //            .Select(PatchRequest.FromJson)
                //            .ToArray(),
                //        PatchesIfMissing = jsonCommand["PatchesIfMissing"] == null ? null : jsonCommand
                //            .Value<RavenJArray>("PatchesIfMissing")
                //            .Cast<RavenJObject>()
                //            .Select(PatchRequest.FromJson)
                //            .ToArray(),
                //    };
                //case "EVAL":
                //    var debug = jsonCommand["DebugMode"].Value<bool>();
                //    return new ScriptedPatchCommandData
                //    {
                //        Key = key,
                //        Etag = GetEtagFromCommand(jsonCommand),
                //        Metadata = jsonCommand["Metadata"] as RavenJObject,
                //        TransactionInformation = transactionInformation,
                //        Patch = ScriptedPatchRequest.FromJson(jsonCommand.Value<RavenJObject>("Patch")),
                //        PatchIfMissing = jsonCommand["PatchIfMissing"] == null ? null : ScriptedPatchRequest.FromJson(jsonCommand.Value<RavenJObject>("PatchIfMissing")),
                //        DebugMode = debug
                //    };
                default:
                    throw new ArgumentException("Batching only supports PUT, PATCH, EVAL and DELETE.");
            }
        }

        public class BatchResult : IDynamicJson
        {
            public long? Etag { get; set; }

            public string Method { get; set; }

            public string Key { get; set; }

            public BlittableJsonReaderObject Metadata { get; set; }

            public BlittableJsonReaderObject AdditionalData { get; set; }

            public PatchResult? PatchResult { get; set; }

            public bool? Deleted { get; set; }

            public DynamicJsonValue ToJson()
            {
                return new DynamicJsonValue
                {
                    [nameof(Etag)] = new LegacyEtag(Etag ?? 0),
                    [nameof(Method)] = Method,
                    [nameof(Key)] = Key,
                    [nameof(Metadata)] = Metadata,
                    [nameof(AdditionalData)] = AdditionalData,
                    [nameof(PatchResult)] = PatchResult,
                    [nameof(Deleted)] = Deleted
                };
            }
        }

        public enum PatchResult
        {
            DocumentDoesNotExists,
            Patched,
            Tested,
            Skipped,
            NotModified
        }

        private static long? GetEtagFromCommand(BlittableJsonReaderObject commandBlittable)
        {
            if (commandBlittable.TryGet("Etag", out string etagAsString) == false)
                return null;

            if (string.IsNullOrEmpty(etagAsString))
                return null;

            var legacyEtag = LegacyEtag.Parse(etagAsString);
            return legacyEtag.Etag;
        }

        public interface LegacyICommandData
        {
            string Method { get; }

            string Key { get; }

            long? Etag { get; }
        }
    }
}

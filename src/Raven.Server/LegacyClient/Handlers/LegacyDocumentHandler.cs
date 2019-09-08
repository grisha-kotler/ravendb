using System;
using System.Net;
using System.Runtime.ExceptionServices;
using System.Threading.Tasks;
using Raven.Client;
using Raven.Client.Documents.Operations.Attachments;
using Raven.Client.Exceptions;
using Raven.Server.Documents;
using Raven.Server.Routing;
using Raven.Server.ServerWide.Context;
using Sparrow.Json;
using Sparrow.Server;
using Voron;

namespace Raven.Server.LegacyClient.Handlers
{
    public class LegacyDocumentHandler : DatabaseRequestHandler
    {
        [RavenAction("/legacy/databases/*/docs/$", "PUT", AuthorizationStatus.ValidUser, DisableOnCpuCreditsExhaustion = true)]
        public async Task Put()
        {
            var docId = Uri.UnescapeDataString(RouteMatch.Url.Substring(
                RouteMatch.MatchLength,
                RouteMatch.Url.Length - RouteMatch.MatchLength
            ));

            using (ContextPool.AllocateOperationContext(out DocumentsOperationContext context))
            {
                var doc = await context.ReadForDiskAsync(RequestBodyStream(), docId).ConfigureAwait(false);

                var etagAsString = GetStringFromHeaders("If-None-Match");
                var legacyEtag = new LegacyEtag(etagAsString);

                using (var cmd = new LegacyMergedPutCommand(doc, docId, legacyEtag.Etag, Database, shouldValidateAttachments: true))
                {
                    await Database.TxMerger.Enqueue(cmd);

                    cmd.ExceptionDispatchInfo?.Throw();

                    HttpContext.Response.StatusCode = (int)HttpStatusCode.Created;

                    using (var writer = new BlittableJsonTextWriter(context, ResponseBodyStream()))
                    {
                        writer.WriteStartObject();

                        writer.WritePropertyName(nameof(LegacyPutDocumentResult.Key));
                        writer.WriteString(cmd.PutResult.Key);
                        writer.WriteComma();

                        writer.WritePropertyName(nameof(LegacyPutDocumentResult.ETag));
                        writer.WriteString(cmd.PutResult.ETag);

                        writer.WriteEndObject();
                    }
                }
            }
        }

        private class LegacyMergedPutCommand : TransactionOperationsMerger.MergedTransactionCommand, IDisposable
        {
            private string _id;
            private readonly long? _etag;
            private readonly BlittableJsonReaderObject _document;
            private readonly DocumentDatabase _database;
            private readonly bool _shouldValidateAttachments;
            public ExceptionDispatchInfo ExceptionDispatchInfo;
            public LegacyPutDocumentResult PutResult;

            private static string GenerateNonConflictingId(DocumentDatabase database, string prefix)
            {
                return prefix + database.DocumentsStorage.GenerateNextEtag().ToString("D19") + "-" + Guid.NewGuid().ToBase64Unpadded();
            }

            public LegacyMergedPutCommand(BlittableJsonReaderObject doc, string id, long? etag, DocumentDatabase database, bool shouldValidateAttachments = false)
            {
                _document = doc;
                _id = id;
                _etag = etag;
                _database = database;
                _shouldValidateAttachments = shouldValidateAttachments;
            }

            protected override int ExecuteCmd(DocumentsOperationContext context)
            {
                if (_shouldValidateAttachments)
                {
                    if (_document.TryGet(Constants.Documents.Metadata.Key, out BlittableJsonReaderObject metadata)
                        && metadata.TryGet(Constants.Documents.Metadata.Attachments, out BlittableJsonReaderArray attachments))
                    {
                        ValidateAttachments(attachments, context, _id);
                    }
                }

                try
                {
                    if (_etag != null && _etag != 0)
                    {
                        var document = _database.DocumentsStorage.Get(context, _id, DocumentFields.Etag);
                        if (document == null)
                        {
                            throw new ConcurrencyException($"PUT attempted on document '{_id}' " +
                                                           "using a non current etag (document deleted)");
                        }

                        if (_etag != document.Etag)
                        {
                            throw new ConcurrencyException($"PUT attempted on document '{_id}' using a non current etag");
                        }
                    }

                    var putResult = _database.DocumentsStorage.Put(context, _id, null, _document);
                    PutResult = new LegacyPutDocumentResult
                    {
                        Key = putResult.Id,
                        ETag = new LegacyEtag(putResult.Etag).ToString()
                    };
                }
                catch (Voron.Exceptions.VoronConcurrencyErrorException)
                {
                    // RavenDB-10581 - If we have a concurrency error on "doc-id/" 
                    // this means that we have existing values under the current etag
                    // we'll generate a new (random) id for them. 

                    // The TransactionMerger will re-run us when we ask it to as a 
                    // separate transaction
                    if (_id?.EndsWith('/') == true)
                    {
                        _id = GenerateNonConflictingId(_database, _id);
                        RetryOnError = true;
                    }
                    throw;
                }
                catch (ConcurrencyException e)
                {
                    ExceptionDispatchInfo = ExceptionDispatchInfo.Capture(e);
                }

                return 1;
            }

            private void ValidateAttachments(BlittableJsonReaderArray attachments, DocumentsOperationContext context, string id)
            {
                if (attachments == null)
                {
                    throw new InvalidOperationException($"Can not put document (id={id}) with '{Constants.Documents.Metadata.Attachments}': null");
                }

                foreach (BlittableJsonReaderObject attachment in attachments)
                {
                    if (attachment.TryGet(nameof(AttachmentName.Hash), out string hash) == false || hash == null)
                    {
                        throw new InvalidOperationException($"Can not put document (id={id}) because it contains an attachment without an hash property.");
                    }

                    using (Slice.From(context.Allocator, hash, out var hashSlice))
                    {
                        if (AttachmentsStorage.GetCountOfAttachmentsForHash(context, hashSlice) < 1)
                        {
                            throw new InvalidOperationException($"Can not put document (id={id}) because it contains an attachment with hash={hash} but no such attachment is stored.");
                        }
                    }
                }
            }

            public void Dispose()
            {
                _document?.Dispose();
            }

            public override TransactionOperationsMerger.IReplayableCommandDto<TransactionOperationsMerger.MergedTransactionCommand> ToDto(JsonOperationContext context)
            {
                return new MergedPutCommandDto
                {
                    Id = _id,
                    Etag = _etag,
                    Document = _document
                };
            }

            private class MergedPutCommandDto : TransactionOperationsMerger.IReplayableCommandDto<LegacyMergedPutCommand>
            {
                public string Id { get; set; }
                public long? Etag { get; set; }
                public BlittableJsonReaderObject Document { get; set; }

                public LegacyMergedPutCommand ToCommand(DocumentsOperationContext context, DocumentDatabase database)
                {
                    return new LegacyMergedPutCommand(Document, Id, Etag, database);
                }
            }
        }

        public class LegacyPutDocumentResult
        {
            public string Key { get; set; }

            public string ETag { get; set; }
        }
    }
}

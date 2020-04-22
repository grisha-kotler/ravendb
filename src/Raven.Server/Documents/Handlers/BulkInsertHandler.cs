﻿using System;
using System.Collections.Concurrent;
using System.Collections.Generic;
using System.Diagnostics;
using System.IO;
using System.Linq;
using System.Net;
using System.Text;
using System.Threading;
using System.Threading.Tasks;
using Raven.Client.Documents.Commands.Batches;
using Raven.Client.Documents.Operations;
using Raven.Client.Documents.Operations.TimeSeries;
using Raven.Server.Routing;
using Raven.Server.ServerWide.Context;
using Raven.Server.Smuggler.Documents;
using Sparrow;
using Sparrow.Json;
using Sparrow.Logging;
using Voron.Exceptions;
using Size = Sparrow.Size;

namespace Raven.Server.Documents.Handlers
{
    public class BulkInsertHandler : DatabaseRequestHandler
    {
        [RavenAction("/databases/*/bulk_insert", "POST", AuthorizationStatus.ValidUser, DisableOnCpuCreditsExhaustion = true)]
        public async Task BulkInsert()
        {
            var operationCancelToken = CreateOperationToken();
            var id = GetLongQueryString("id");

            await Database.Operations.AddOperation(Database, "Bulk Insert", Operations.Operations.OperationType.BulkInsert,
                progress => DoBulkInsert(progress, operationCancelToken.Token),
                id,
                token: operationCancelToken
            );
        }

        private async Task<IOperationResult> DoBulkInsert(Action<IOperationProgress> onProgress, CancellationToken token)
        {
            var progress = new BulkInsertProgress();
            try
            {
                var logger = LoggingSource.Instance.GetLogger<MergedInsertBulkCommand>(Database.Name);
                IDisposable currentCtxReset = null, previousCtxReset = null;

                try
                {
                    using (ContextPool.AllocateOperationContext(out JsonOperationContext context))
                    using (var buffer = JsonOperationContext.ManagedPinnedBuffer.LongLivedInstance())
                    {
                        currentCtxReset = ContextPool.AllocateOperationContext(out JsonOperationContext docsCtx);
                        var requestBodyStream = RequestBodyStream();

                        using (var parser = new BatchRequestParser.ReadMany(context, requestBodyStream, buffer, token))
                        {
                            await parser.Init();

                            var array = new BatchRequestParser.CommandData[8];
                            var numberOfCommands = 0;
                            long totalSize = 0;
                            BatchRequestParser.CommandData? bulkInsertCommandData = null;
                            Task previousTask = null;

                            while (true)
                            {
                                using (var modifier = new BlittableMetadataModifier(docsCtx))
                                {
                                    var task = parser.MoveNext(docsCtx, modifier);
                                    if (task == null)
                                        break;

                                    token.ThrowIfCancellationRequested();

                                    // if we are going to wait on the network, flush immediately
                                    if ((task.Wait(5) == false && numberOfCommands > 0) ||
                                        // but don't batch too much anyway
                                        totalSize > 4 * Voron.Global.Constants.Size.Megabyte)
                                    {
                                        if (previousTask != null)
                                            await previousTask;

                                        var disposable = ReplaceContextIfCurrentlyInUse(task, numberOfCommands, array);
                                        var copyArray = new BatchRequestParser.CommandData[numberOfCommands];
                                        for (int i = 0; i < numberOfCommands; i++)
                                        {
                                            copyArray[i] = array[i];
                                        }

                                        previousTask = Database.TxMerger.Enqueue(new MergedInsertBulkCommand
                                        {
                                            Commands = copyArray,
                                            NumberOfCommands = copyArray.Length,
                                            Database = Database,
                                            Logger = logger,
                                            TotalSize = totalSize
                                        });

                                        numberOfCommands = 0;
                                        totalSize = 0;
                                        previousCtxReset?.Dispose();
                                        previousCtxReset = currentCtxReset;
                                        currentCtxReset = ContextPool.AllocateOperationContext(out docsCtx);

                                        if (bulkInsertCommandData != null)
                                        {
                                            bulkInsertCommandData = new BatchRequestParser.CommandData
                                            {
                                                Id = bulkInsertCommandData.Value.Id,
                                                Type = CommandType.TimeSeriesBulkInsert,
                                                TimeSeries = new TimeSeriesOperation
                                                {
                                                    SortedForBulkInsert = new SortedList<long, TimeSeriesOperation.AppendOperation>(),
                                                    Name = bulkInsertCommandData.Value.TimeSeries.Name
                                                }
                                            };
                                        }

                                        var t = previousTask.ContinueWith(_ =>
                                        {
                                            disposable?.Dispose();
                                            //TODO: ClearStreamsTempFiles();

                                            progress.BatchCount++;
                                            progress.Processed += copyArray.Length;
                                            progress.LastProcessedId = copyArray[copyArray.Length - 1].Id;

                                            onProgress(progress);

                                        }, TaskContinuationOptions.None);

                                    }

                                    var commandData = await task;
                                    if (commandData.Type == CommandType.None)
                                        break;

                                    if (commandData.Type == CommandType.AttachmentPUT)
                                    {
                                        commandData.AttachmentStream = await WriteAttachment(commandData.ContentLength, parser.GetBlob(commandData.ContentLength));
                                    }
                                    else if (commandData.Type == CommandType.TimeSeriesBulkInsert)
                                    {
                                        bulkInsertCommandData = commandData;
                                    }
                                    else if (commandData.Type == CommandType.Append)
                                    {
                                        if (bulkInsertCommandData.Value.TimeSeries.SortedForBulkInsert.Count == 0)
                                            numberOfCommands++;

                                        bulkInsertCommandData.Value.TimeSeries.SortedForBulkInsert[commandData.AppendOperation.Timestamp.Ticks] = commandData.AppendOperation;
                                        totalSize += sizeof(long) + commandData.AppendOperation.Tag?.Length ?? 0 + commandData.AppendOperation.Values.Length * sizeof(double);
                                        continue;
                                    }

                                    totalSize += GetSize(commandData);
                                    if (numberOfCommands >= array.Length)
                                        Array.Resize(ref array, array.Length + Math.Min(1024, array.Length));
                                    array[numberOfCommands++] = commandData;
                                }
                            }

                            if (numberOfCommands > 0)
                            {
                                await Database.TxMerger.Enqueue(new MergedInsertBulkCommand
                                {
                                    Commands = array,
                                    NumberOfCommands = numberOfCommands,
                                    Database = Database,
                                    Logger = logger,
                                    TotalSize = totalSize
                                });

                                progress.BatchCount++;
                                progress.Processed += numberOfCommands;
                                progress.LastProcessedId = array[numberOfCommands - 1].Id;

                                onProgress(progress);
                            }
                        }
                    }
                }
                finally
                {
                    currentCtxReset?.Dispose();
                    previousCtxReset?.Dispose();
                    ClearStreamsTempFiles();
                }

                HttpContext.Response.StatusCode = (int)HttpStatusCode.Created;

                return new BulkOperationResult
                {
                    Total = progress.Processed
                };
            }
            catch (Exception e)
            {
                HttpContext.Response.Headers["Connection"] = "close";
                throw new InvalidOperationException("Failed to process bulk insert. " + progress, e);
            }
        }

        private void ClearStreamsTempFiles()
        {
            if (_streamsTempFiles == null)
                return;

            foreach (var file in _streamsTempFiles)
            {
                file.Dispose();
            }

            _streamsTempFiles = null;
        }

        private List<StreamsTempFile> _streamsTempFiles;

        private async Task<BatchHandler.MergedBatchCommand.AttachmentStream> WriteAttachment(long size, Stream stream)
        {
            var attachmentStream = new BatchHandler.MergedBatchCommand.AttachmentStream();

            if (size <= 32 * 1024)
            {
                attachmentStream.Stream = new MemoryStream();
            }
            else
            {
                StreamsTempFile attachmentStreamsTempFile = Database.DocumentsStorage.AttachmentsStorage.GetTempFile("bulk");
                attachmentStream.Stream = attachmentStreamsTempFile.StartNewStream();

                if (_streamsTempFiles == null)
                    _streamsTempFiles = new List<StreamsTempFile>();

                _streamsTempFiles.Add(attachmentStreamsTempFile);
            }

            using (Database.DocumentsStorage.ContextPool.AllocateOperationContext(out DocumentsOperationContext ctx))
            using (ctx.OpenWriteTransaction())
            {
                attachmentStream.Hash = await AttachmentsStorageHelper.CopyStreamToFileAndCalculateHash(ctx, stream, attachmentStream.Stream, Database.DatabaseShutdown);
                await attachmentStream.Stream.FlushAsync();
            }

            return attachmentStream;
        }

        private int? _changeVectorSize;

        private long GetSize(BatchRequestParser.CommandData commandData)
        {
            long size = 0;
            switch (commandData.Type)
            {
                case CommandType.PUT:
                    return commandData.Document.Size;
                case CommandType.Counters:
                    foreach (var operation in commandData.Counters.Operations)
                    {
                        size += operation.CounterName.Length
                                + sizeof(long) // etag 
                                + sizeof(long) // counter value
                                + GetChangeVectorSizeInternal() // estimated change vector size
                                + 10; // estimated collection name size
                    }

                    return size;
                case CommandType.AttachmentPUT:
                    return commandData.ContentLength;
                case CommandType.TimeSeries:
                case CommandType.TimeSeriesBulkInsert:
                    // we don't know the size of the change so we are just estimating
                    foreach (var append in commandData.TimeSeries.SortedForBulkInsert.Values)
                    {
                        size += sizeof(long); // DateTime
                        if (string.IsNullOrWhiteSpace(append.Tag) == false)
                            size += append.Tag.Length;

                        size += append.Values.Length * sizeof(double);
                    }

                    return size;                
                default:
                    throw new ArgumentOutOfRangeException($"'{commandData.Type}' isn't supported");
            }


            int GetChangeVectorSizeInternal()
            {
                if (_changeVectorSize.HasValue)
                    return _changeVectorSize.Value;

                using (Database.DocumentsStorage.ContextPool.AllocateOperationContext(out DocumentsOperationContext ctx))
                using (ctx.OpenReadTransaction())
                {
                    var databaseChangeVector = DocumentsStorage.GetDatabaseChangeVector(ctx);
                    _changeVectorSize = Encoding.UTF8.GetBytes(databaseChangeVector).Length;
                    return _changeVectorSize.Value;
                }
            }
        }

        private IDisposable ReplaceContextIfCurrentlyInUse(Task<BatchRequestParser.CommandData> task, int numberOfCommands, BatchRequestParser.CommandData[] array)
        {
            if (task.IsCompleted)
                return null;

            var disposable = ContextPool.AllocateOperationContext(out JsonOperationContext tempCtx);
            // the docsCtx is currently in use, so we 
            // cannot pass it to the tx merger, we'll just
            // copy the documents to a temporary ctx and 
            // use that ctx instead. Copying the documents
            // is safe, because they are immutables

            for (int i = 0; i < numberOfCommands; i++)
            {
                if (array[i].Document != null)
                {
                    array[i].Document = array[i].Document.Clone(tempCtx);
                }
            }
            return disposable;
        }

        public class MergedInsertBulkCommand : TransactionOperationsMerger.MergedTransactionCommand
        {
            public Logger Logger;
            public DocumentDatabase Database;
            public BatchRequestParser.CommandData[] Commands;
            public int NumberOfCommands;
            public long TotalSize;

            protected override long ExecuteCmd(DocumentsOperationContext context)
            {
                for (int i = 0; i < NumberOfCommands; i++)
                {
                    var cmd = Commands[i];

                    Debug.Assert(cmd.Type == CommandType.PUT || cmd.Type == CommandType.Counters || cmd.Type == CommandType.TimeSeries || cmd.Type == CommandType.TimeSeriesBulkInsert || cmd.Type == CommandType.AttachmentPUT);

                    switch (cmd.Type)
                    {
                        case CommandType.PUT:
                            try
                            {
                                Database.DocumentsStorage.Put(context, cmd.Id, null, cmd.Document);
                            }
                            catch (VoronConcurrencyErrorException)
                            {
                                // RavenDB-10581 - If we have a concurrency error on "doc-id/" 
                                // this means that we have existing values under the current etag
                                // we'll generate a new (random) id for them. 

                                // The TransactionMerger will re-run us when we ask it to as a 
                                // separate transaction

                                for (; i < NumberOfCommands; i++)
                                {
                                    cmd = Commands[i];
                                    if (cmd.Type != CommandType.PUT)
                                        continue;

                                    if (cmd.Id?.EndsWith(Database.IdentityPartsSeparator) == true)
                                    {
                                        cmd.Id = MergedPutCommand.GenerateNonConflictingId(Database, cmd.Id);
                                        RetryOnError = true;
                                    }
                                }

                                throw;
                            }

                            break;
                        case CommandType.Counters:
                        {
                            var collection = CountersHandler.ExecuteCounterBatchCommand.GetDocumentCollection(cmd.Id, Database, context, fromEtl: false, out _);

                            foreach (var counterOperation in cmd.Counters.Operations)
                            {
                                counterOperation.DocumentId = cmd.Counters.DocumentId;
                                Database.DocumentsStorage.CountersStorage.IncrementCounter(
                                    context, cmd.Id, collection, counterOperation.CounterName, counterOperation.Delta, out _);
                            }

                            break;
                        }
                        case CommandType.TimeSeries:
                        case CommandType.TimeSeriesBulkInsert:
                        {
                            if (cmd.Type == CommandType.TimeSeriesBulkInsert)
                            {
                                cmd.TimeSeries.Appends = new List<TimeSeriesOperation.AppendOperation>(cmd.TimeSeries.SortedForBulkInsert.Values);
                            }

                            var docCollection = TimeSeriesHandler.ExecuteTimeSeriesBatchCommand.GetDocumentCollection(Database, context, cmd.Id, fromEtl: false);
                            Database.DocumentsStorage.TimeSeriesStorage.AppendTimestamp(context,
                                cmd.Id,
                                docCollection,
                                cmd.TimeSeries.Name,
                                cmd.TimeSeries.Appends
                            );
                            break;
                        }
                        case CommandType.AttachmentPUT:
                        {
                            using (cmd.AttachmentStream.Stream)
                            {
                                Database.DocumentsStorage.AttachmentsStorage.PutAttachment(context, cmd.Id, cmd.Name,
                                    cmd.ContentType ?? "", cmd.AttachmentStream.Hash, cmd.ChangeVector, cmd.AttachmentStream.Stream, updateDocument: false);
                            }

                            break;
                        }
                    }
                }

                if (Logger.IsInfoEnabled)
                {
                    Logger.Info($"Executed {NumberOfCommands:#,#;;0} bulk insert operations, size: ({new Size(TotalSize, SizeUnit.Bytes)})");
                }

                return NumberOfCommands;
            }

            public override TransactionOperationsMerger.IReplayableCommandDto<TransactionOperationsMerger.MergedTransactionCommand> ToDto(JsonOperationContext context)
            {
                return new MergedInsertBulkCommandDto
                {
                    Commands = Commands.Take(NumberOfCommands).ToArray()
                };
            }
        }
    }

    public class MergedInsertBulkCommandDto : TransactionOperationsMerger.IReplayableCommandDto<BulkInsertHandler.MergedInsertBulkCommand>
    {
        public BatchRequestParser.CommandData[] Commands { get; set; }

        public BulkInsertHandler.MergedInsertBulkCommand ToCommand(DocumentsOperationContext context, DocumentDatabase database)
        {
            return new BulkInsertHandler.MergedInsertBulkCommand
            {
                NumberOfCommands = Commands.Length,
                TotalSize = Commands.Sum(c => c.Document.Size),
                Commands = Commands,
                Database = database,
                Logger = LoggingSource.Instance.GetLogger<DatabaseDestination>(database.Name)
            };
        }
    }
}

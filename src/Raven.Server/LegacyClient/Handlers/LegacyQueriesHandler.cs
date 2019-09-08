using System;
using System.Collections.Generic;
using System.IO;
using System.Linq;
using System.Net;
using System.Threading.Tasks;
using Microsoft.Extensions.Primitives;
using Raven.Client;
using Raven.Client.Documents.Commands;
using Raven.Server.Documents;
using Raven.Server.Json;
using Raven.Server.Routing;
using Raven.Server.ServerWide.Context;
using Raven.Server.Smuggler.Migration;
using Raven.Server.Smuggler.Migration.ApiKey;
using Sparrow.Json;
using Sparrow.Json.Parsing;

namespace Raven.Server.LegacyClient.Handlers
{
    public class LegacyQueriesHandler : DatabaseRequestHandler
    {
        [RavenAction("/legacy/databases/*/queries/", "GET", AuthorizationStatus.ValidUser, DisableOnCpuCreditsExhaustion = true)]
        public async Task QueriesGet()
        {
            await GetQueriesResponse(true);
        }

        [RavenAction("/legacy/databases/*/queries/", "POST", AuthorizationStatus.ValidUser, DisableOnCpuCreditsExhaustion = true)]
        public async Task QueriesPost()
        {
            await GetQueriesResponse(false);
        }

        private async Task GetQueriesResponse(bool isGet)
        {
            IEnumerable<string> idsToLoad;

            using (ContextPool.AllocateOperationContext(out DocumentsOperationContext context))
            {
                if (isGet)
                {
                    idsToLoad = GetStringValuesQueryString("id", required: true);
                }
                else
                {
                    var blittableArray = await StreamArrayReader.Get(RequestBodyStream(), context);
                    idsToLoad = blittableArray.Select(x => x.ToString());
                }

                var documents = new List<Document>();
                var includes = new List<Document>();
                var loadedIds = new HashSet<string>();
                var includedIds = new HashSet<string>();
                var idsToInclude = GetStringValuesQueryString("include", required: false).ToArray() ?? new string[0];
                //var transformer = GetQueryStringValue("transformer") ?? GetQueryStringValue("resultTransformer");
                //var transformerParameters = this.ExtractTransformerParameters();
                var includedEtags = new List<byte>();


                using (context.OpenReadTransaction())
                {
                    foreach (var id in idsToLoad)
                    {
                        if (loadedIds.Add(id) == false)
                            continue;

                        var document = Database.DocumentsStorage.Get(context, id);
                        //TODO: handle transformer???
                        if (document == null)
                            continue;



                        /*JsonDocument documentByKey = string.IsNullOrEmpty(transformer)
                            ? Database.Get(value, transactionInformation)
                            : Database.GetWithTransformer(value, transformer, transactionInformation, queryInputs);*/

                        

                        document.Data = UpdateMetadata(document, context);

                        documents.Add(document);
                        includedEtags.AddRange(BitConverter.GetBytes(document.Etag));
                    }

                    foreach (var includedId in includedIds)
                    {
                        var document = Database.DocumentsStorage.Get(context, includedId);
                        if (document == null)
                            continue;

                        includedEtags.AddRange(new LegacyEtag(document.Etag).ToByteArray());
                        includes.Add(document);
                    }

                    var computeHash = Encryptor.Current.Hash.Compute16(includedEtags.ToArray());
                    var computedEtag = LegacyEtag.Parse(computeHash);

                    if (MatchEtag(computedEtag))
                    {
                        HttpContext.Response.StatusCode = (int)HttpStatusCode.NotModified;
                        return;
                    }

                    HttpContext.Response.Headers.Add("ETag", new StringValues(computedEtag.ToString()));
                    await WriteDocumentsJsonAsync(context, metadataOnly: false, documents, includes);
                }


                /*var result = new MultiLoadResult();
			var loadedIds = new HashSet<string>();
			var includes = context.Request.QueryString.GetValues("include") ?? new string[0];
			var transformer = context.Request.QueryString["transformer"] ?? context.Request.QueryString["resultTransformer"];

		    var queryInputs = context.ExtractQueryInputs();
            
            var transactionInformation = GetRequestTransaction(context);
		    var includedEtags = new List<byte>();
			Database.TransactionalStorage.Batch(actions =>
			{
				foreach (RavenJToken item in itemsToLoad)
				{
					JsonDocument documentByKey = string.IsNullOrEmpty(transformer)
				                        ? Database.Get(value, transactionInformation)
                                        : Database.GetWithTransformer(value, transformer, transactionInformation, queryInputs);
				}

				var addIncludesCommand = new AddIncludesCommand(Database, transactionInformation, (etag, includedDoc) =>
				{
					includedEtags.AddRange(etag.ToByteArray());
					result.Includes.Add(includedDoc);
				}, includes, loadedIds);

				foreach (var item in result.Results.Where(item => item != null))
				{
					addIncludesCommand.Execute(item);
				}
			});

			Etag computedEtag;
            
			using (var md5 = MD5.Create())
			{
				var computeHash = md5.ComputeHash(includedEtags.ToArray());
				computedEtag = Etag.Parse(computeHash);
			}

			if (context.MatchEtag(computedEtag))
			{
				context.SetStatusToNotModified();
				return;
			}

			context.WriteETag(computedEtag);
			context.WriteJson(result);*/



            }

        }

        private static BlittableJsonReaderObject UpdateMetadata(Document document, DocumentsOperationContext context)
        {
            DynamicJsonValue mutatedMetadata = null;
            if (document.TryGetMetadata(out BlittableJsonReaderObject metadata))
            {
                if (metadata.Modifications == null)
                    metadata.Modifications = new DynamicJsonValue(metadata);

                mutatedMetadata = metadata.Modifications;
            }

            document.Data.Modifications = new DynamicJsonValue(document.Data)
            {
                [Constants.Documents.Metadata.Key] = (object)metadata ?? (mutatedMetadata = new DynamicJsonValue())
            };

            if (metadata != null && metadata.TryGet(Constants.Documents.Metadata.Collection, out string collection))
            {
                mutatedMetadata["Raven-Entity-Name"] = collection;
            }

            mutatedMetadata["Last-Modified"] = document.LastModified;
            mutatedMetadata["Raven-Last-Modified"] = document.LastModified;
            mutatedMetadata["@etag"] = new LegacyEtag(document.Etag).ToString();

            mutatedMetadata.Remove(Constants.Documents.Metadata.Collection);
            mutatedMetadata.Remove(Constants.Documents.Metadata.ChangeVector);
            mutatedMetadata.Remove(Constants.Documents.Metadata.LastModified);

            using (document.Data)
                return context.ReadObject(document.Data, document.Id);
        }

        protected bool MatchEtag(LegacyEtag etag)
        {
            return EtagHeaderToEtag().Etag == etag.Etag;
        }

        private LegacyEtag EtagHeaderToEtag()
        {
            try
            {
                var responseHeader = GetStringFromHeaders("If-None-Match");
                if (string.IsNullOrEmpty(responseHeader))
                    return LegacyEtag.InvalidEtag;

                if (responseHeader[0] == '\"')
                    return LegacyEtag.Parse(responseHeader.Substring(1, responseHeader.Length - 2));

                return LegacyEtag.Parse(responseHeader);
            }
            catch (Exception e)
            {
                //TODO: log this
                return LegacyEtag.InvalidEtag;
            }
        }

        private async Task WriteDocumentsJsonAsync(JsonOperationContext context, bool metadataOnly, IEnumerable<Document> documentsToWrite, List<Document> includes)
        {
            using (var writer = new AsyncBlittableJsonTextWriter(context, ResponseBodyStream(), Database.DatabaseShutdown))
            {
                writer.WriteStartObject();
                writer.WritePropertyName(nameof(GetDocumentsResult.Results));
                await writer.WriteDocumentsAsync(context, documentsToWrite, metadataOnly);

                writer.WriteComma();
                writer.WritePropertyName(nameof(GetDocumentsResult.Includes));
                if (includes.Count > 0)
                {
                    // TODO: write includes correctly
                    await writer.WriteIncludesAsync(context, includes);
                }
                else
                {
                    writer.WriteStartArray();
                    writer.WriteEndArray();
                }

                writer.WriteEndObject();
                await writer.OuterFlushAsync();
            }
        }
    }
}

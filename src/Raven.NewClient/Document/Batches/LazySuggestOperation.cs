using System;
using System.Globalization;
using System.Linq;
using Raven.NewClient.Abstractions.Data;
using Raven.NewClient.Client.Data;
using Raven.NewClient.Client.Data.Queries;
using Raven.NewClient.Client.Shard;
using Sparrow.Json;


namespace Raven.NewClient.Client.Document.Batches
{
    public class LazySuggestOperation : ILazyOperation
    {
        private readonly string index;
        private readonly SuggestionQuery suggestionQuery;

        public LazySuggestOperation(string index, SuggestionQuery suggestionQuery)
        {
            this.index = index;
            this.suggestionQuery = suggestionQuery;
        }

        public GetRequest CreateRequest()
        {
            var query = string.Format(
                "term={0}&field={1}&max={2}",
                suggestionQuery.Term,
                suggestionQuery.Field,
                suggestionQuery.MaxSuggestions);

            if (suggestionQuery.Accuracy.HasValue)
                query += "&accuracy=" + suggestionQuery.Accuracy.Value.ToString(CultureInfo.InvariantCulture);

            if (suggestionQuery.Distance.HasValue)
                query += "&distance=" + suggestionQuery.Distance;

            return new GetRequest
            {
                Url = "/suggest/" + index,
                Query = query
            };
        }

        public object Result { get; private set; }
        public QueryResult QueryResult { get; set; }
        public bool RequiresRetry { get; private set; }
        public void HandleResponse(BlittableJsonReaderObject response)
        {
            throw new NotImplementedException();

            /*if (response.Status != 200 && response.Status != 304)
            {
                throw new InvalidOperationException("Got an unexpected response code for the request: " + response.Status + "\r\n" +
                                                    response.Result);
            }

            var result = (RavenJObject)response.Result;
            Result = new SuggestionQueryResult
            {
                Suggestions = ((RavenJArray)result["Suggestions"]).Select(x => x.Value<string>()).ToArray(),
            };*/
        }

        public IDisposable EnterContext()
        {
            return null;
        }
    }
}

using System;
using System.Collections.Generic;
using System.Linq;
using Raven.NewClient.Client.Document;
using Sparrow.Json;
using Sparrow.Logging;

namespace Raven.NewClient.Client.Commands
{
    public class LoadOperation
    {
        private readonly InMemoryDocumentSessionOperations _session;
        private static readonly Logger _logger = LoggingSource.Instance.GetLogger<LoadOperation>("Raven.NewClient.Client");

        private string[] _ids;
        private string[] _includes;
        private readonly List<string> _idsToCheckOnServer = new List<string>();

        public LoadOperation(InMemoryDocumentSessionOperations session, string[] ids = null, string[] includes = null)
        {
            _session = session;
            if (ids != null)
                _ids = ids;
            if (includes != null)
                _includes = includes;
        }

        public GetDocumentCommand CreateRequest()
        {
            if (_idsToCheckOnServer.Count == 0)
                return null;

            if (_session.CheckIfIdAlreadyIncluded(_ids, _includes))
                return null;

            _session.IncrementRequestCount();
            if (_logger.IsInfoEnabled)
                _logger.Info($"Requesting the following ids '{string.Join(", ", _idsToCheckOnServer)}' from {_session.StoreIdentifier}");
            return new GetDocumentCommand
            {
                Ids = _idsToCheckOnServer.ToArray(),
                Includes = _includes,
                Context = _session.Context
            };
        }

        public void ById(string id)
        {
            if (id == null)
                return;

            if (_ids == null)
                _ids = new[] {id};

            if (_session.IsLoadedOrDeleted(id))
                return;

            _idsToCheckOnServer.Add(id);
        }

        public void WithIncludes(string[] includes)
        {
            this._includes = includes;
        }

        public void ByIds(IEnumerable<string> ids)
        {
            _ids = ids.ToArray();
            foreach (var id in _ids.Distinct(StringComparer.OrdinalIgnoreCase))
            {
                ById(id);
            }
        }

        public T GetDocument<T>()
        {
            return GetDocument<T>(_ids[0]);
        }

        private T GetDocument<T>(string id)
        {
            if (id == null)
                return default(T);

            if (_session.IsDeleted(id))
                return default(T);

            DocumentInfo doc;
            if (_session.DocumentsById.TryGetValue(id, out doc))
                return _session.TrackEntity<T>(doc);

            if (_session.includedDocumentsByKey.TryGetValue(id, out doc))
                return _session.TrackEntity<T>(doc);

            return default(T);
        }

        public Dictionary<string, T> GetDocuments<T>()
        {
            var finalResults = new Dictionary<string, T>(StringComparer.OrdinalIgnoreCase);
            for (int i = 0; i < _ids.Length; i++)
            {
                var id = _ids[i];
                if(id == null)
                    continue;
                finalResults[id] = GetDocument<T>(id);
            }
            return finalResults;
        }

        public void SetResult(GetDocumentResult result)
        {
            if (result.Includes != null)
            {
                foreach (BlittableJsonReaderObject include in result.Includes)
                {
                    var newDocumentInfo = DocumentInfo.GetNewDocumentInfo(include);
                    _session.includedDocumentsByKey[newDocumentInfo.Id] = newDocumentInfo;
                }
            }

            foreach (BlittableJsonReaderObject document in result.Results)
            {
                var newDocumentInfo = DocumentInfo.GetNewDocumentInfo(document);
                _session.DocumentsById[newDocumentInfo.Id] = newDocumentInfo;
            }

            if (_includes != null && _includes.Length > 0)
            {
                _session.RegisterMissingIncludes(result.Results, _includes);
            }
        }
    }
}
using Raven.Abstractions.Indexing;

namespace Raven.Client.Indexes
{
    ///<summary>
    /// Create an index that allows to tag entities by their entity name
    ///</summary>
    public class RavenDocumentsByEntityName : AbstractIndexCreationTask
    {
        public override bool IsMapReduce
        {
            get { return false; }
        }
        /// <summary>
        /// Return the actual index name
        /// </summary>
        public override string IndexName
        {
            get { return "Raven/DocumentsByEntityName"; }
        }
        
        /// <summary>
        /// Creates the Raven/DocumentsByEntityName index
        /// </summary>
        public override IndexDefinition CreateIndexDefinition()
        {
            return new IndexDefinition
            {
                Map = @"from doc in docs
let etag = Etag.Parse(doc[""@metadata""][""@etag""]) 
select new 
{ 
    Tag = doc[""@metadata""][""Raven-Entity-Name""], 
    LastModified = (DateTime)doc[""@metadata""][""Last-Modified""],
    LastModifiedTicks = ((DateTime)doc[""@metadata""][""Last-Modified""]).Ticks,
    Restarts = etag.Restarts,
    Changes = etag.Changes
};",
                Indexes =
                    {
                        {"Tag", FieldIndexing.NotAnalyzed},
                        {"LastModified", FieldIndexing.NotAnalyzed},
                        {"LastModifiedTicks", FieldIndexing.NotAnalyzed}
                    },
                    SortOptions =
                    {
                        {"LastModified",SortOptions.String},
                        {"LastModifiedTicks", SortOptions.Long},
                        {"Restarts", SortOptions.Long},
                        {"Changes", SortOptions.Long},
                    },
            
                DisableInMemoryIndexing = true,
                LockMode = IndexLockMode.LockedIgnore,
            };
        }
    }
}

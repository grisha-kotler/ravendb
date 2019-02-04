using System.Collections.Generic;
using Raven.Server.Documents;
using Raven.Server.Json;
using Raven.Server.ServerWide;
using Raven.Server.ServerWide.Context;
using Sparrow;
using Sparrow.Json;
using Sparrow.Threading;
using Voron;
using Voron.Data.Tables;
using static Raven.Server.ServerWide.ClusterStateMachine;
using static Raven.Server.Documents.DocumentsStorage;
using static Raven.Server.ServerWide.Commands.CompareExchangeCommandBase;

namespace Raven.Server.Storage.Schema.Updates.Server
{
    public unsafe class From11 : ISchemaUpdate
    {
        public bool Update(UpdateStep step)
        {
            var items = step.ReadTx.OpenTable(ItemsSchema, Items);
            var dbs = new List<string>();
            const string dbKey = "db/";
            using (Slice.From(step.ReadTx.Allocator, dbKey, out Slice loweredPrefix))
            {
                foreach (var result in items.SeekByPrimaryKeyPrefix(loweredPrefix, Slices.Empty, 0))
                {
                    dbs.Add(GetCurrentItemKey(result.Value).Substring(3));
                }
            }

            foreach (var db in dbs)
            {
                var readTable = step.ReadTx.OpenTable(CompareExchangeSchema, CompareExchange);
                if (readTable != null)
                {
                    var writeTable = step.WriteTx.OpenTable(CompareExchangeSchema, CompareExchange);
                    using (Slice.From(step.ReadTx.Allocator, db.ToLowerInvariant() + "/", out var keyPrefix))
                    {
                        foreach (var item in readTable.SeekByPrimaryKeyPrefix(keyPrefix, Slices.Empty, 0))
                        {
                            var index = TableValueToLong((int)UniqueItems.Index, ref item.Value.Reader);
                            GetPrefixIndexSlices(step.ReadTx.Allocator, db, index, out var indexTuple);

                            using (indexTuple.Scope)
                            {
                                using (Slice.External(step.WriteTx.Allocator, indexTuple.Buffer.Ptr, indexTuple.Buffer.Length, out var prefixIndexSlice))
                                using (writeTable.Allocate(out TableValueBuilder write))
                                using (var ctx = JsonOperationContext.ShortTermSingleUse())
                                {
                                    var bjro = new BlittableJsonReaderObject(item.Value.Reader.Read((int)UniqueItems.Value, out var size1), size1, ctx);
                                    var pk = item.Value.Reader.Read((int)UniqueItems.Key, out int size);
                                    using (Slice.External(step.WriteTx.Allocator, pk, size, out var pkSlice))
                                    {
                                        write.Add(pkSlice);
                                        write.Add(index);
                                        write.Add(bjro.BasePointer, bjro.Size);
                                        write.Add(prefixIndexSlice);
                                        writeTable.DeleteByKey(pkSlice);
                                    }

                                    writeTable.Set(write);
                                }
                            }
                        }
                    }
                }
            }
            return true;
        }
    }
}

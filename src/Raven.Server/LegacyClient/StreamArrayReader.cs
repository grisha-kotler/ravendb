using System.IO;
using System.Threading.Tasks;
using Raven.Server.Smuggler.Migration;
using Sparrow.Json;

namespace Raven.Server.LegacyClient
{
    public static class StreamArrayReader
    {
        public static async Task<BlittableJsonReaderArray> Get(Stream stream, JsonOperationContext context)
        {
            const string arrayPropertyList = "List";
            using (var requestStream = stream)
            using (var idsListStream = new ArrayStream(requestStream, arrayPropertyList))
            {
                var blittableJsonReaderObject = await context.ReadForMemoryAsync(idsListStream, "list");
                if (blittableJsonReaderObject.TryGet(arrayPropertyList, out BlittableJsonReaderArray array) == false)
                    throw new InvalidDataException($"Couldn't find property {arrayPropertyList}");

                return array;
            }
        }
    }
}

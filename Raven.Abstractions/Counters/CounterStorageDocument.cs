using Raven.Abstractions.Data;

namespace Raven.Abstractions.Counters
{
	public class CounterStorageDocument : ResourceDocumentBase
	{
		public string StoreName { get; set; }
    }
}

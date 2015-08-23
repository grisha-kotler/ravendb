using System.Collections.Generic;

namespace Raven.Abstractions.Data
{
	public interface IResourceDocument
	{
		string Id { get; set; }
		Dictionary<string, string> Settings { get; set; }
		Dictionary<string, string> SecuredSettings { get; set; }
		bool Disabled { get; set; }
	}
}
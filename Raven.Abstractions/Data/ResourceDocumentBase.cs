using System;
using System.Collections.Generic;

namespace Raven.Abstractions.Data
{
	public class ResourceDocumentBase : IResourceDocument
	{
		/// <summary>
		/// The ID of a resource. Can be either the resource name ("Northwind") or the full document name ("Raven/{ResourceType}/Northwind").
		/// </summary>
		public string Id { get; set; }
		/// <summary>
		/// Resource settings (unsecured).
		/// </summary>
		public Dictionary<string, string> Settings { get; set; }
		/// <summary>
		/// Resource settings (secured).
		/// </summary>
		public Dictionary<string, string> SecuredSettings { get; set; }
		/// <summary>
		/// Indicates if resource is disabled or not.
		/// </summary>
		public bool Disabled { get; set; }

		protected ResourceDocumentBase()
		{
			Settings = new Dictionary<string, string>(StringComparer.OrdinalIgnoreCase);
			SecuredSettings = new Dictionary<string, string>(StringComparer.OrdinalIgnoreCase);
		}
	}
}
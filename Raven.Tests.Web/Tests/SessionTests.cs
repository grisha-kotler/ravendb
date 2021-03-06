﻿// -----------------------------------------------------------------------
//  <copyright file="SessionTests.cs" company="Hibernating Rhinos LTD">
//      Copyright (c) Hibernating Rhinos LTD. All rights reserved.
//  </copyright>
// -----------------------------------------------------------------------
using System.Threading.Tasks;

using Raven.Tests.Common.Attributes;

namespace Raven.Tests.Web.Tests
{
	public class SessionTests : WebTestBase
	{
		[IISExpressInstalledFact]
		public async Task Sync()
		{
			await TestControllerAsync("SyncSession");
		}

		[IISExpressInstalledFact]
		public async Task Async()
		{
			await TestControllerAsync("AsyncSession");
		}

		[IISExpressInstalledFact]
		public async Task Mixed()
		{
			await TestControllerAsync("MixedSession");
		}
	}
}
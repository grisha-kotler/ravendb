﻿using System;
using System.Linq;
using Raven.Client.Indexes;
using Raven.Tests.Common;

using Xunit;

namespace Raven.Tests.Issues.Ravendb718
{
	public class DateTime_QueryStaticTests : RavenTest
	{
		[Fact]
		public void DateTime_SavedAs_Local_OnStaticIndex_QueriesAs_Unspecified()
		{
			// There is no representation of Local once the DateTime is serialized,
			// so this is the one case where the date does not come back exactly as it was saved.
			// This is a known and expected behavior of DateTime.

			DoTest(DateTime.Now, DateTimeKind.Local, DateTimeKind.Unspecified);
		}

		[Fact]
		public void DateTime_SavedAs_Unspecified_OnStaticIndex_QueriesAs_Unspecified()
		{
			DoTest(new DateTime(2012, 1, 1), DateTimeKind.Unspecified, DateTimeKind.Unspecified);
		}

		[Fact]
		public void DateTime_SavedAs_UTC_OnStaticIndex_QueriesAs_UTC()
		{
			DoTest(DateTime.UtcNow, DateTimeKind.Utc, DateTimeKind.Utc);
		}

		private void DoTest(DateTime dt, DateTimeKind inKind, DateTimeKind outKind)
		{
			Assert.Equal(inKind, dt.Kind);

			using (var documentStore = NewDocumentStore())
			{
				new Foos_ByDateTime().Execute(documentStore);

				using (var session = documentStore.OpenSession())
				{
					session.Store(new Foo { Id = "foos/1", DateTime = dt });
					session.SaveChanges();
				}

				using (var session = documentStore.OpenSession())
				{
					var foo = session.Query<Foo, Foos_ByDateTime>()
									 .Customize(x => x.WaitForNonStaleResults())
									 .First(x => x.DateTime == dt);

					Assert.Equal(dt, foo.DateTime);
					Assert.Equal(outKind, foo.DateTime.Kind);
				}
			}
		}

		public class Foo
		{
			public string Id { get; set; }
			public DateTime DateTime { get; set; }
		}

		public class Foos_ByDateTime : AbstractIndexCreationTask<Foo>
		{
			public Foos_ByDateTime()
			{
				Map = foos => from foo in foos
							  select new { foo.DateTime };
			}
		}
	}
}
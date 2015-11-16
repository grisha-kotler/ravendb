using Raven.Abstractions.Data;
using Raven.Client.Document;
using Raven.Database.Server;
using Raven.Database.Server.Security.Windows;
using Raven.Json.Linq;
using Raven.Tests.Core.Utils.Entities;
using System.Collections.Generic;
using System.Net;
using Raven.Tests.Common.Attributes;
using Xunit;

namespace Raven.Tests.Core.Auth
{
    public class Authentication : RavenCoreTestBase
    {
        [Fact]
        public void CanUseApiKeyAuthentication()
        {
            Raven.Database.Server.Security.Authentication.EnableOnce();
            this.Server.Configuration.AnonymousUserAccessMode = AnonymousUserAccessMode.None;
            this.Server.SystemDatabase.Documents.Put(
                       "Raven/ApiKeys/CanUseApiKeyAuthentication",
                       null,
                       RavenJObject.FromObject(new ApiKeyDefinition
                       {
                           Name = "CanUseApiKeyAuthentication",
                           Secret = "ThisIsMySecret",
                           Enabled = true,
                           Databases = new List<ResourceAccess>
                                {
                                    new ResourceAccess {TenantId = "*"},
                                    new ResourceAccess {TenantId = Constants.SystemDatabase}
                                }
                       }), new RavenJObject(), null);

            using (var store = new DocumentStore
            {
                ApiKey = "CanUseApiKeyAuthentication/ThisIsMySecret2",
                Url = this.Server.SystemDatabase.ServerUrl,
                Credentials = null
            }.Initialize())
            {
                Assert.Throws<Raven.Abstractions.Connection.ErrorResponseException>(() => store.DatabaseCommands.Get("aa/1"));
            }

            using (var store = new DocumentStore
            {
                ApiKey = "CanUseApiKeyAuthentication/ThisIsMySecret",
                Url = this.Server.SystemDatabase.ServerUrl,
                Credentials = null
            }.Initialize())
            {
                store.DatabaseCommands.Put("users/1", null, RavenJObject.FromObject(new User { }), new RavenJObject());
                var result = store.DatabaseCommands.Get("users/1");
                Assert.NotNull(result);
            }
        }

        [Fact]
        public void CanUseWindowsAuthentication()
        {
            FactIfWindowsAuthenticationIsAvailable.LoadCredentials();
            Raven.Database.Server.Security.Authentication.EnableOnce();
            this.Server.Configuration.AnonymousUserAccessMode = AnonymousUserAccessMode.None;
            this.Server.SystemDatabase.Documents.Put(
                "Raven/Authorization/WindowsSettings", 
                null,
                RavenJObject.FromObject(new WindowsAuthDocument
                    {
                        RequiredUsers = new List<WindowsAuthData>
                            {
                                new WindowsAuthData()
                                    {
                                        Name = "test-domain\\test-user",
                                        Enabled = true,
                                        Databases = new List<ResourceAccess>
                                            {
                                                new ResourceAccess {TenantId = "*"},
                                                new ResourceAccess {TenantId = Constants.SystemDatabase}
                                            }
                                    }
                            }
                    }), new RavenJObject(), null);

            using (var store = new DocumentStore
                {
                    Credentials = new NetworkCredential(FactIfWindowsAuthenticationIsAvailable.Username, FactIfWindowsAuthenticationIsAvailable.Password, FactIfWindowsAuthenticationIsAvailable.Domain),
                    Url = this.Server.SystemDatabase.ServerUrl
                }.Initialize())
            {
                Assert.Throws<Raven.Abstractions.Connection.ErrorResponseException>(() => store.DatabaseCommands.Put("users/1", null, RavenJObject.FromObject(new User {}), new RavenJObject()));
            }

            using (var store = new DocumentStore
            {
                Credentials = new NetworkCredential(FactIfWindowsAuthenticationIsAvailable.Username, FactIfWindowsAuthenticationIsAvailable.Password, FactIfWindowsAuthenticationIsAvailable.Domain),
                Url = this.Server.SystemDatabase.ServerUrl
            }.Initialize())
            {
                store.DatabaseCommands.Put("users/1", null, RavenJObject.FromObject(new User { }), new RavenJObject());
                var result = store.DatabaseCommands.Get("users/1");
                Assert.NotNull(result);
            }
        }
    }
}